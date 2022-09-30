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

package org.apache.spark.shuffle.sort;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;
import org.apache.spark.internal.config.package$;

@Private
public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  @VisibleForTesting
  static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final BlockManager blockManager;
  private final IndexShuffleBlockResolver shuffleBlockResolver;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int inputBufferSizeInBytes;
  private final int outputBufferSizeInBytes;

  @Nullable private MapStatus mapStatus;
  @Nullable private ShuffleExternalSorter sorter;
  private long peakMemoryUsedBytes = 0;

  /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  private MyByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  private class CloseAndFlushShieldOutputStream extends CloseShieldOutputStream {

    CloseAndFlushShieldOutputStream(OutputStream outputStream) {
      super(outputStream);
    }

    @Override
    public void flush() {
      // do nothing
    }
  }

  public UnsafeShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf sparkConf) throws IOException {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
        "UnsafeShuffleWriter can only be used for shuffles with at most " +
        SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
        " reduce partitions");
    }
    this.blockManager = blockManager;
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    // 指定的是ShuffleInMemorySorter初始化内存大小，通过spark.shuffle.sort.initialBufferSize参数配置，默认为4096。
    this.initialSortBufferSize = sparkConf.getInt("spark.shuffle.sort.initialBufferSize",
                                                  DEFAULT_INITIAL_SORT_BUFFER_SIZE);
    this.inputBufferSizeInBytes =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.outputBufferSizeInBytes =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    // 初始化调用，创建Shuffle排序器ShuffleExternalSorter
    open();
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * This convenience method should only be called in test code.
   */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  /**
   * 将记录写入到磁盘
   *
   * @param records
   * @throws IOException
   */
  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    boolean success = false;
    try {
      while (records.hasNext()) {
        // 将记录写入到排序器
        insertRecordIntoSorter(records.next());
      }
      // 将map任务输出的数据持久化到磁盘
      closeAndWriteOutput();
      success = true;
    } finally {
      if (sorter != null) {
        try {
          // 数据写出完成后，需要使用ShuffleExternalSorter进行资源清理
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during " +
                         "cleanup.", e);
          }
        }
      }
    }
  }

  private void open() {
    assert (sorter == null);
    sorter = new ShuffleExternalSorter(
      memoryManager,
      blockManager,
      taskContext,
            // 初始化缓冲大小
      initialSortBufferSize,
            // 分区总数
      partitioner.numPartitions(),
      sparkConf,
      writeMetrics);
    // 序列化缓冲
    serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    // 包装为序列化流对象
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert(sorter != null);
    // 更新使用内存的峰值
    updatePeakMemoryUsed();
    serBuffer = null;
    serOutputStream = null;
    // 关闭ShuffleExternalSorter, 完成最后一次溢写操作，将内存中的剩余的数据全部溢写到磁盘，获得溢出文件信息的数组
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    sorter = null;
    final long[] partitionLengths;
    // 获取正式的输出数据文件
    final File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    // 创建临时文件
    final File tmp = Utils.tempFileWith(output);
    try {
      try {
        // 合并所有溢出文件到正式的输出数据文件，返回的是每个分区的数据长度
        partitionLengths = mergeSpills(spills, tmp);
      } finally {
        for (SpillInfo spill : spills) {
          // 因为合并成了一个文件，因此删除溢写的文件
          if (spill.file.exists() && ! spill.file.delete()) {
            logger.error("Error while deleting spill file {}", spill.file.getPath());
          }
        }
      }
      // 根据partitionLengths数组创建索引文件
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    // 构造并返回MapStatus对象
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  /**
   * 将记录写入到排序器
   *
   * @param record
   * @throws IOException
   */
  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert(sorter != null);
    final K key = record._1();
    // 计算记录的分区ID
    final int partitionId = partitioner.getPartition(key);
    // 重置serBuffer
    serBuffer.reset();
    // 将记录写入到serOutputStream中进行序列化
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    // 得到序列化后的数据大小
    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    // 将serBuffer底层的序列化字节数组插入到Tungsten的内存中
    sorter.insertRecord(
      serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /**
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpills(SpillInfo[] spills, File outputFile) throws IOException {
    // 是否开启了解压缩，默认为true
    final boolean compressionEnabled = sparkConf.getBoolean("spark.shuffle.compress", true);
    // 压缩编解码器，默认lz4
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    // 是否开启了快速合并，默认为true
    final boolean fastMergeEnabled =
      sparkConf.getBoolean("spark.shuffle.unsafe.fastMergeEnabled", true);
    /*
     * 检查是否支持快速合并：
     * 1. 没有开启压缩，则可以快速合并；
     * 2. 开启了压缩，但需要压缩编解码器支持对级联序列化流进行解压缩。
     *    支持该功能的压缩编解码有Snappy、LZ4、LZF, ZSTD四种。
     * 二者满足其中之一即可
     */
    final boolean fastMergeIsSupported = !compressionEnabled ||
      CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    // 是否开启了数据加密
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();
    try {
      if (spills.length == 0) {
        // 没有溢写文件，创建空文件
        new FileOutputStream(outputFile).close(); // Create an empty file
        return new long[partitioner.numPartitions()];
      } else if (spills.length == 1) {
        // 有一个溢写文件
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        // 直接将溢写文件重命名为outputFile
        Files.move(spills[0].file, outputFile);
        return spills[0].partitionLengths;
      } else {
        // 有多个溢写文件
        final long[] partitionLengths;
        // There are multiple spills to merge, so none of these spill files' lengths were counted
        // towards our shuffle write count or shuffle write time. If we use the slow merge path,
        // then the final output file's size won't necessarily be equal to the sum of the spill
        // files' sizes. To guard against this case, we look at the output file's actual size when
        // computing shuffle bytes written.
        // 有多个溢出要合并，因此这些溢出文件的长度都没有计入我们的随机写入计数或随机写入时间。
        // 如果我们使用慢速合并路径，那么最终输出文件的大小不一定等于溢出文件大小的总和。
        // 为了防止这种情况，我们在计算写入的 shuffle 字节时查看输出文件的实际大小。
        //
        // We allow the individual merge methods to report their own IO times since different merge
        // strategies use different IO techniques.  We count IO during merge towards the shuffle
        // shuffle write time, which appears to be consistent with the "not bypassing merge-sort"
        // branch in ExternalSorter.
        // 我们允许各个合并方法报告自己的 IO 时间，因为不同的合并策略使用不同的 IO 技术。
        // 我们将合并期间的 IO 计入 shuffle shuffle 写入时间，这似乎与 ExternalSorter 中的“不绕过合并排序”分支一致。
        if (fastMergeEnabled && fastMergeIsSupported) {
          // 开启并且支持快速合并
          // Compression is disabled or we are using an IO compression codec that supports
          // decompression of concatenated compressed streams, so we can perform a fast spill merge
          // that doesn't need to interpret the spilled bytes.
          if (transferToEnabled && !encryptionEnabled) {
            // 开启了NIO复制模式，且未开启加解密
            logger.debug("Using transferTo-based fast merge");
            // 使用mergeSpillsWithTransferTo方法进行transferTo-based fast方式进行合并
            partitionLengths = mergeSpillsWithTransferTo(spills, outputFile);
          } else {
            logger.debug("Using fileStream-based fast merge");
            // 使用mergeSpillsWithFileStream()方法的slow方式进行合并，压缩编解码器传的是null
            partitionLengths = mergeSpillsWithFileStream(spills, outputFile, null);
          }
        } else {
          logger.debug("Using slow merge");
          // 使用mergeSpillsWithFileStream()方法的slow方式进行合并，传递了压缩编解码器
          partitionLengths = mergeSpillsWithFileStream(spills, outputFile, compressionCodec);
        }
        // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
        // in-memory records, we write out the in-memory records to a file but do not count that
        // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
        // to be counted as shuffle write, but this will lead to double-counting of the final
        // SpillInfo's bytes.
        // 记录度量信息
        writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
        writeMetrics.incBytesWritten(outputFile.length());
        return partitionLengths;
      }
    } catch (IOException e) {
      if (outputFile.exists() && !outputFile.delete()) {
        logger.error("Unable to delete output file {}", outputFile.getPath());
      }
      throw e;
    }
  }

  /**
   * Merges spill files using Java FileStreams. This code path is typically slower than
   * the NIO-based merge, {@link UnsafeShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[],
   * File)}, and it's mostly used in cases where the IO compression codec does not support
   * concatenation of compressed data, when encryption is enabled, or when users have
   * explicitly disabled use of {@code transferTo} in order to work around kernel bugs.
   * This code path might also be faster in cases where individual partition size in a spill
   * is small and UnsafeShuffleWriter#mergeSpillsWithTransferTo method performs many small
   * disk ios which is inefficient. In those case, Using large buffers for input and output
   * files helps reducing the number of disk ios, making the file merging faster.
   *
   * 采用文件流方式进行合并溢写文件，这种方式比NIO TransferTo的方式要慢
   * 满足以下三个条件之一会使用这种方式：
   * 1. 需要支持加解密功能。
   * 2. 压缩解编码器不支持对级联序列化流进行解压缩
   * 3. 当开发者自行禁用了transferTo的功能
   *
   * @param spills the spills to merge.
   * @param outputFile the file to write the merged data to.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithFileStream(
      SpillInfo[] spills,
      File outputFile,
      @Nullable CompressionCodec compressionCodec) throws IOException {
    assert (spills.length >= 2);
    // 分区总数
    final int numPartitions = partitioner.numPartitions();
    // 创建存储分区长度的数组
    final long[] partitionLengths = new long[numPartitions];
    // 创建保存溢写文件输入流的数组
    final InputStream[] spillInputStreams = new InputStream[spills.length];

    final OutputStream bos = new BufferedOutputStream(
            new FileOutputStream(outputFile),
            outputBufferSizeInBytes);
    // Use a counting output stream to avoid having to close the underlying file and ask
    // the file system for its size after each partition is written.
    // 创建合并输出流，该流对FileOutPutStream进行了包装，提供了字节计数功能
    final CountingOutputStream mergedFileOutputStream = new CountingOutputStream(bos);

    boolean threwException = true;
    try {
      // 遍历所有的溢写文件信息对象SpillInfo，为每个溢写文件创建NioBufferedFileInputStream
      for (int i = 0; i < spills.length; i++) {
        spillInputStreams[i] = new NioBufferedFileInputStream(
            spills[i].file,
            inputBufferSizeInBytes);
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        final long initialFileLength = mergedFileOutputStream.getByteCount();
        // Shield the underlying output stream from close() and flush() calls, so that we can close
        // the higher level streams to make sure all data is really flushed and internal state is
        // cleaned.
        /*
         * 再次进行包装，得到针对当前分区号的输出流
         * 1. TimeTrackingOutputStream的包装提供了写出时时间记录功能。
         * 2. CloseAndFlushShieldOutputStream继承了CloseShieldOutputStream，包装了close()方法，屏蔽了对被包装流的关闭操作。
         */
        OutputStream partitionOutput = new CloseAndFlushShieldOutputStream(
          new TimeTrackingOutputStream(writeMetrics, mergedFileOutputStream));
        // 加解密包装
        partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);
        // 解压缩包装
        if (compressionCodec != null) {
          partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
        }
        /*
         * 遍历溢写文件信息对象SpillInfo
         * 由于每个溢写文件中包含了多个分区的数据，因此需要遍历每个溢写文件，
         * 并得到每个溢写文件中记录的对应分区的数据
         */
        for (int i = 0; i < spills.length; i++) {
          // 获取每个溢写文件中记录的对应分区（即外层for循环中循环到的partition分区）的数据大小
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          if (partitionLengthInSpill > 0) {
            // 将当前溢写文件的输入流，根据对应分区的数据大小包装为LimitedInputStream流
            InputStream partitionInputStream = new LimitedInputStream(spillInputStreams[i],
              partitionLengthInSpill, false);
            try {
              // 加解密包装
              partitionInputStream = blockManager.serializerManager().wrapForEncryption(
                partitionInputStream);
              // 解压缩包装
              if (compressionCodec != null) {
                partitionInputStream = compressionCodec.compressedInputStream(partitionInputStream);
              }
              // 将数据拷贝到partitionOutput
              ByteStreams.copy(partitionInputStream, partitionOutput);
            } finally {
              // 关闭LimitedInputStream流，但不会关闭底层被包装的流
              partitionInputStream.close();
            }
          }
        }
        // 刷新数据
        partitionOutput.flush();
        partitionOutput.close();
        // 记录分区数据长度
        partitionLengths[partition] = (mergedFileOutputStream.getByteCount() - initialFileLength);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      // 关闭溢写文件的输入流
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
      // 关闭合并文件的输出流
      Closeables.close(mergedFileOutputStream, threwException);
    }
    return partitionLengths;
  }

  /**
   * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes.
   * This is only safe when the IO compression codec and serializer support concatenation of
   * serialized streams.
   * 使用NIO TransferTo来合并溢写文件每个分区的数据。
   * 只有在压缩编解码及序列化器支持对级联序列化流进行解压缩时才可以使用。
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithTransferTo(SpillInfo[] spills, File outputFile) throws IOException {
    // 溢写文件数需大于等于2
    assert (spills.length >= 2);
    // 获取总分区数
    final int numPartitions = partitioner.numPartitions();
    // 创建存储分区数据大小的数组
    final long[] partitionLengths = new long[numPartitions];
    // 创建FileChannel数组，用于存放每个溢写文件的FileChannel对象
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    // 创建存放每个溢写文件的FileChannel的position的数组
    final long[] spillInputChannelPositions = new long[spills.length];
    // 合并输出文件的FileChannel
    FileChannel mergedFileOutputChannel = null;

    boolean threwException = true;
    try {
      // 获取每个溢写文件的FileChannel，存放到spillInputChannels数组
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
      }
      // This file needs to opened in append mode in order to work around a Linux kernel bug that
      // affects transferTo; see SPARK-3948 for more details.
      // 获取合并输出文件的FileChannel
      mergedFileOutputChannel = new FileOutputStream(outputFile, true).getChannel();

      // 传输总字节数
      long bytesWrittenToMergedFile = 0;
      // 遍历分区编号
      for (int partition = 0; partition < numPartitions; partition++) {
        // 遍历溢写文件的SpillInfo对象
        for (int i = 0; i < spills.length; i++) {
          // 获取溢写文件信息SpillInfo对象中记录的对应分区（即partition分区）的数据长度
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          // 获取对应溢写文件的FileChannel
          final FileChannel spillInputChannel = spillInputChannels[i];
          // 开始时间
          final long writeStartTime = System.nanoTime();
          // 进行文件NIO拷贝
          Utils.copyFileStreamNIO(
            spillInputChannel,
            mergedFileOutputChannel,
            spillInputChannelPositions[i],
            partitionLengthInSpill);
          spillInputChannelPositions[i] += partitionLengthInSpill;
          writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
          bytesWrittenToMergedFile += partitionLengthInSpill;
          partitionLengths[partition] += partitionLengthInSpill;
        }
      }
      // Check the position after transferTo loop to see if it is in the right position and raise an
      // exception if it is incorrect. The position will not be increased to the expected length
      // after calling transferTo in kernel version 2.6.32. This issue is described at
      // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
      // 检查合并输出文件的position是否等于合并的总字节数
      if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
        throw new IOException(
          "Current position " + mergedFileOutputChannel.position() + " does not equal expected " +
            "position " + bytesWrittenToMergedFile + " after transferTo. Please check your kernel" +
            " version to see if it is 2.6.32, as there is a kernel bug which will lead to " +
            "unexpected behavior when using transferTo. You can set spark.file.transferTo=false " +
            "to disable this NIO feature."
        );
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      // 检查每个溢写文件的FileChannel的position是否与文件的大小相同
      for (int i = 0; i < spills.length; i++) {
        assert(spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], threwException);
      }
      // 关闭合并文件的FileChannel
      Closeables.close(mergedFileOutputChannel, threwException);
    }
    // 返回存放了每个分区的数据长度的数组
    return partitionLengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupResources();
      }
    }
  }
}
