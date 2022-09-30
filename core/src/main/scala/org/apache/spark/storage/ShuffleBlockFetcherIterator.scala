/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io.{InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * 用于获取多个数据块的迭代器。
 * 如果数据块在本地，那么从本地的BlockManager获取；
 * 如果数据块在远端，那么通过ShuffleClient请求远端节点上的BlockTransferService获取。
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage. Note that zero-sized blocks are
 *                        already excluded, which happened in
 *                        [[MapOutputTracker.convertMapStatuses]].
 * @param streamWrapper A function to wrap the returned input stream.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
 * @param detectCorrupt whether to detect any corruption in fetched blocks.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean)
  extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Total number of blocks to fetch. This should be equal to the total number of blocks
   * in [[blocksByAddress]] because we already filter out zero-sized blocks in [[blocksByAddress]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   * 一共要获取的Block数量
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   * 已经处理的Block数量
   */
  private[this] var numBlocksProcessed = 0

  // ShuffleBlockFetcherIterator的启动时间
  private[this] val startTime = System.currentTimeMillis

  /**
   * Local blocks to fetch, excluding zero-sized blocks.
   * 缓存了本地BlockManager管理的Block的BlockId。
   */
  private[this] val localBlocks = scala.collection.mutable.LinkedHashSet[BlockId]()

  /**
   * Remote blocks to fetch, excluding zero-sized blocks.
   * 缓存了远端BlockManager管理的Block的BlockId。
   */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   * 用于保存获取Block的结果信息（FetchResult）
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   * 当前正在处理的FetchResult
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   * 获取Block的请求信息（FetchRequest）的队列。
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * Queue of fetch requests which could not be issued the first time they were dequeued. These
   * requests are tried again when the fetch constraints are satisfied.
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  /** Current bytes in flight from our requests */
  // 当前批次的请求的字节数。
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  // 当前批次的请求的数量。
  private[this] var reqsInFlight = 0

  /** Current number of blocks in flight per host:port */
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
   * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
   * at most once for those corrupted blocks.
   */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  // Shuffle的度量信息
  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   * 如果isZombie为true，则ShuffleBlockFetcherIterator处于非激活状态。
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * A set to store the files used for shuffling remote huge blocks. Files in this set will be
   * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[DownloadFile]()

  // 被构造时初始化
  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(transportConf: TransportConf): DownloadFile = {
    // we never need to do any encryption or decryption here, regardless of configs, because that
    // is handled at another layer in the code.  When encryption is enabled, shuffle data is written
    // to disk encrypted in the first place, and sent over the network still encrypted.
    new SimpleDownloadFile(
      blockManager.diskBlockManager.createTempLocalBlock()._2, transportConf)
  }

  override def registerTempFileToClean(file: DownloadFile): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.path())
      }
    }
  }

  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    // 将请求的所有Block的大小累加到bytesInFlight
    bytesInFlight += req.size
    // 将reqsInFlight累加一
    reqsInFlight += 1

    // so we can look up the size of each blockID
    // [BlockId所对应的文件名, 对应字节数]字典
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    // 还需要拉取的数据块的Set集合
    val remainingBlocks = new HashSet[String]() ++= sizeMap.keys
    // BlockId所对应的文件名的集合
    val blockIds = req.blocks.map(_._1.toString)
    // 批量下载远端的数据块
    val address = req.address

    /*
     * 使用ShuffleClient的fetchBlocks()方法拉取
     * 如果部署了外部的Shuffle服务，则需要配置spark.shuffle.service.enabled属性为true（默认是false），
     * 此时将创建ExternalShuffleClient。
     * 默认情况下，NettyBlockTransferService会作为Shuffle的客户端。
     */
    // 请求回调
    val blockFetchingListener = new BlockFetchingListener {
      // 下载成功后会调用该方法
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // 结果封装为SuccessFetchResult放入results中
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            remainingBlocks -= blockId
            // 将结果封装为SuccessFetchResult放入到results中
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
              remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        results.put(new FailureFetchResult(BlockId(blockId), address, e))
      }
    }

    // Fetch remote shuffle blocks to disk when the request is too large. Since the shuffle data is
    // already encrypted and compressed over the wire(w.r.t. the related configs), we can just fetch
    // the data and write it to file directly.
    // 当请求太大时，将远程 shuffle 块获取到磁盘。
    // 由于 shuffle 数据已经通过网络加密和压缩（w.r.t. 相关配置），
    // 我们可以直接获取数据并将其写入文件。
    if (req.size > maxReqSizeShuffleToMem) {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }

  /**
   * 划分从本地读取和需要远程读取的Block的请求
   * @return
   */
  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    // 每个远程请求的最大尺寸，等于maxBytesInFlight的1/5或者1
    // maxBytesInFlight由spark.reducer.maxSizeInFlight配置控制，默认为48MB，因此targetRequestSize默认为9.6MB
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize
      + ", maxBlocksInFlightPerAddress: " + maxBlocksInFlightPerAddress)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    // 缓存需要远程请求的FetchRequest对象
    val remoteRequests = new ArrayBuffer[FetchRequest]

    for ((address, blockInfos) <- blocksByAddress) {
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // 本地的数据块
        blockInfos.find(_._2 <= 0) match {
          case Some((blockId, size)) if size < 0 =>
            throw new BlockException(blockId, "Negative block size " + size)
          case Some((blockId, size)) if size == 0 =>
            throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
          case None => // do nothing.
        }
        // 将BlockManagerId对应的所有大小不为零的BlockId存入localBlock
        localBlocks ++= blockInfos.map(_._1)
        // 累计需要请求的数据块
        numBlocksToFetch += localBlocks.size
      } else {
        // 远端的数据块
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        /*
         * 远程获取的累加缓存，用于保存每个远程请求的尺寸不超过targetRequestSize的限制，
         * 实现批量发送请求，以提高系统性能；
         * 注意，此时的批量请求是发送到同一个BlockManager上的，只是包含了多个数据块的BlockId。
         */
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          } else if (size == 0) {
            throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
          } else {
            // 将所有大小大于零的BlockId和size累加到curBlocks
            curBlocks += ((blockId, size))
            // 将所有大小不为零的BlockId存入remoteBlocks
            remoteBlocks += blockId
            numBlocksToFetch += 1
            // 增加当前请求要获取的数据块的总大小
            curRequestSize += size
          }
          /*
           * 每当curRequestSize >= targetRequestSize，
           * 就作为一个批次生成一个新的FetchRequest，放入remoteRequests
           */
          if (curRequestSize >= targetRequestSize ||
              curBlocks.size >= maxBlocksInFlightPerAddress) {
            // Add this FetchRequest
            // 新建FetchRequest放入remoteRequests
            remoteRequests += new FetchRequest(address, curBlocks)
            logDebug(s"Creating fetch request of $curRequestSize at $address "
              + s"with ${curBlocks.size} blocks")
            // 新建curBlocks，将curRequestSize置为0，为生成下一个FetchRequest做准备
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            curRequestSize = 0
          }
        }
        // Add in the final request
        // 对剩余需要拉取的数据块新建FetchRequest放入remoteRequests
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks including ${localBlocks.size}" +
        s" local blocks and ${remoteBlocks.size} remote blocks")
    remoteRequests
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   * 获取本地Block
   */
  private[this] def fetchLocalBlocks() {
    logDebug(s"Start fetching local blocks: ${localBlocks.mkString(", ")}")
    // 得到localBlocks数组的迭代器
    val iter = localBlocks.iterator
    // 迭代每一个BlockId
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        // 获取Block数据,创建SuccessFetchResult对象，并添加到results中
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId,
          buf.size(), buf, false))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    // 给TaskContextImpl添加任务完成的监听器，便于任务执行完成后调用cleanup()方法进行一些清理工作
    context.addTaskCompletionListener[Unit](_ => cleanup())

    // Split local and remote blocks.
    // 划分从本地读取和需要远程读取的Block的请求
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    // 将FetchRequest随机排序后存入fetchRequests
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    // 远端拉取
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    // 获取本地block
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

  // 取决于numBlocksProcessed是否小于numBlocksToFetch
  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.currentTimeMillis()
      result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

      result match {
        case r @ SuccessFetchResult(blockId, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          if (!localBlocks.contains(blockId)) {
            bytesInFlight -= size
          }
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }

          if (buf.size == 0) {
            // We will never legitimately receive a zero-size block. All blocks with zero records
            // have zero size and all zero-size blocks have no records (and hence should never
            // have been requested in the first place). This statement relies on behaviors of the
            // shuffle writers, which are guaranteed by the following test cases:
            //
            // - BypassMergeSortShuffleWriterSuite: "write with some empty partitions"
            // - UnsafeShuffleWriterSuite: "writeEmptyIterator"
            // - DiskBlockObjectWriterSuite: "commit() and close() without ever opening or writing"
            //
            // There is not an explicit test for SortShuffleWriter but the underlying APIs that
            // uses are shared by the UnsafeShuffleWriter (both writers use DiskBlockObjectWriter
            // which returns a zero-size from commitAndGet() in case no records were written
            // since the last call.
            val msg = s"Received a zero-size buffer for block $blockId from $address " +
              s"(expectedApproxSize = $size, isNetworkReqDone=$isNetworkReqDone)"
            throwFetchFailedException(blockId, address, new IOException(msg))
          }

          val in = try {
            buf.createInputStream()
          } catch {
            // The exception could only be throwed by local shuffle block
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              logError("Failed to create input stream from local block", e)
              buf.release()
              throwFetchFailedException(blockId, address, e)
          }
          var isStreamCopied: Boolean = false
          try {
            input = streamWrapper(blockId, in)
            // Only copy the stream if it's wrapped by compression or encryption, also the size of
            // block is small (the decompressed block is smaller than maxBytesInFlight)
            if (detectCorrupt && !input.eq(in) && size < maxBytesInFlight / 3) {
              isStreamCopied = true
              val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
              // Decompress the whole block at once to detect any corruption, which could increase
              // the memory usage tne potential increase the chance of OOM.
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              Utils.copyStream(input, out, closeStreams = true)
              input = out.toChunkedByteBuffer.toInputStream(dispose = true)
            }
          } catch {
            case e: IOException =>
              buf.release()
              if (buf.isInstanceOf[FileSegmentManagedBuffer]
                || corruptedBlocks.contains(blockId)) {
                throwFetchFailedException(blockId, address, e)
              } else {
                logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                corruptedBlocks += blockId
                fetchRequests += FetchRequest(address, Array((blockId, size)))
                result = null
              }
          } finally {
            // TODO: release the buf here to free memory earlier
            if (isStreamCopied) {
              in.close()
            }
          }

        case FailureFetchResult(blockId, address, e) =>
          throwFetchFailedException(blockId, address, e)
      }

      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId, new BufferReleasingInputStream(input, this))
  }

  /**
   * 用于向远端发送请求，以获取数据块
   */
  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight. If you cannot fetch from a remote host
    // immediately, defer the request until the next time it can be processed.

    // Process any outstanding deferred fetch requests if possible.
    // 处理延迟请求
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // Process any regular fetch requests if possible.
    /*
     * 遍历fetchRequest中所有的FetchRequest，需要满足下面的条件才会发送请求：
     * 1. fetchRequests不为空，表示还有远程请求需要发送。
     * 2. 满足下面条件之一：
     *    - bytesInFlight为0，表示当前正在拉取的字节数为0。
     *    - 当前正在拉取的字节数bytesInFlight与fetchRequests队首请求拉取的字节数之和，
     *      小于或等于maxBytesInFlight规定的单次最多请求拉取的字节数。
     */
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      // 检查远端请求数据量是否超过最大数据量，是则放入延迟队列
      // 由spark.reducer.maxReqsInFlight参数控制，默认Int.maxValue
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress, new Queue[FetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        // 发生FetchRequest远程请求，获取数据块中间结果
        send(remoteAddress, request)
      }
    }

    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      sendRequest(request)
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }

    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
    // given remote address.
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    // 返回FetchRequest要下载的所有Block的大小之和。
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * 从远程成功拉取数据块得到的结果
   * @param blockId block id
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block. Note that this is NOT the exact bytes.
   *             Size of remote block is used to calculate bytesInFlight.
   *             数据块的估算大小，用于计算bytesInFlight，与真实大小由一定的差别
   * @param buf `ManagedBuffer` for the content.
   *            装载数据的Buffer
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   *                         本次拉取的结果是否是最后的结果
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}
