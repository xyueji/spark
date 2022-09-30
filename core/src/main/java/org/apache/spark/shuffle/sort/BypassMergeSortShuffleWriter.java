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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import javax.annotation.Nullable;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;

/**
 * This class implements sort-based shuffle's hash-style shuffle fallback path. This write path
 * writes incoming records to separate files, one file per reduce partition, then concatenates these
 * per-partition files to form a single output file, regions of which are served to reducers.
 * Records are not buffered in memory. It writes output in a format
 * that can be served / consumed via {@link org.apache.spark.shuffle.IndexShuffleBlockResolver}.
 * <p>
 * This write path is inefficient for shuffles with large numbers of reduce partitions because it
 * simultaneously opens separate serializers and file streams for all partitions. As a result,
 * {@link SortShuffleManager} only selects this write path when
 * <ul>
 *    <li>no Ordering is specified,</li>
 *    <li>no Aggregator is specified, and</li>
 *    <li>the number of partitions is less than
 *      <code>spark.shuffle.sort.bypassMergeThreshold</code>.</li>
 * </ul>
 *
 * This code used to be part of {@link org.apache.spark.util.collection.ExternalSorter} but was
 * refactored into its own class in order to reduce code complexity; see SPARK-7855 for details.
 * <p>
 * There have been proposals to completely remove this code path; see SPARK-6026 for details.
 *
 * 不在map端持久化数据之前进行聚合、排序等操作，
 */
final class BypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(BypassMergeSortShuffleWriter.class);

  // 文件缓冲大小。可通过spark.shuffle.file.buffer属性配置，默认为32MB。
  private final int fileBufferSize;
  // 是否采用NIO的从文件流到文件流的复制方式。可通过spark.file.transferTo属性配置，默认为true。
  private final boolean transferToEnabled;
  // 分区数
  private final int numPartitions;
  private final BlockManager blockManager;
  // 分区计算器
  private final Partitioner partitioner;
  // 对Shuffle写入（也就是map任务输出到磁盘）的度量，即ShuffleWrite Metrics。
  private final ShuffleWriteMetrics writeMetrics;
  // Shuffle的唯一标识
  private final int shuffleId;
  // Map任务的身份标识
  private final int mapId;
  private final Serializer serializer;
  private final IndexShuffleBlockResolver shuffleBlockResolver;

  /** Array of file writers, one for each partition */
  // 每一个DiskBlockObjectWriter处理一个分区的数据
  private DiskBlockObjectWriter[] partitionWriters;
  // 每一个FileSegment对应一个DiskBlockObjectWriter处理的文件片。
  private FileSegment[] partitionWriterSegments;
  // Map任务的状态
  @Nullable private MapStatus mapStatus;
  // 每个元素记录一个分区的数据长度
  private long[] partitionLengths;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  BypassMergeSortShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      BypassMergeSortShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf conf) {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    // 文件缓冲区大小，默认为32MB
    this.fileBufferSize = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    // 是否开启NIO的TransferTo，默认为true
    this.transferToEnabled = conf.getBoolean("spark.file.transferTo", true);
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.mapId = mapId;
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.serializer = dep.serializer();
    this.shuffleBlockResolver = shuffleBlockResolver;
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    // 如果没有输出的记录，则只生成索引文件
    if (!records.hasNext()) {
      partitionLengths = new long[numPartitions];
      // 生成索引文件，此时创建的索引文件中只有0这一个偏移量
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, null);
      // 创建MapStatus对象
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
      return;
    }
    final SerializerInstance serInstance = serializer.newInstance();
    final long openStartTime = System.nanoTime();
    // 创建partitionWriters和partitionWriterSegments数组
    partitionWriters = new DiskBlockObjectWriter[numPartitions];
    partitionWriterSegments = new FileSegment[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      // 给每个分区创建分区数据待写入的文件
      final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
        blockManager.diskBlockManager().createTempShuffleBlock();
      final File file = tempShuffleBlockIdPlusFile._2();
      final BlockId blockId = tempShuffleBlockIdPlusFile._1();
      // 创建向待写入文件进行写入的DiskBlockObjectWriter
      partitionWriters[i] =
        blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
    }
    // Creating the file to write to and creating a disk writer both involve interacting with
    // the disk, and can take a long time in aggregate when we open many files, so should be
    // included in the shuffle write time.
    writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

    // 向临时Shuffle文件的输出流中写入键值对
    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final K key = record._1();
      // 使用分区计算器并通过每条记录的key，获取记录的分区ID，
      // 调用此分区ID对应的DiskBlockObjectWriter的write方法，向临时Shuffle文件的输出流中写入键值对。
      partitionWriters[partitioner.getPartition(key)].write(key, record._2());
    }

    for (int i = 0; i < numPartitions; i++) {
      // 将临时Shuffle文件的输出流中的数据写入到磁盘
      // 将返回的FileSegment放入partitionWriterSegments数组中，以此分区ID为索引的位置。
      final DiskBlockObjectWriter writer = partitionWriters[i];
      partitionWriterSegments[i] = writer.commitAndGet();
      writer.close();
    }

    // 获取Shuffle数据文件
    File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    File tmp = Utils.tempFileWith(output);
    try {
      // 将每个分区的文件合并到Shuffle数据文件中
      partitionLengths = writePartitionedFile(tmp);
      // 生成Block文件对应的索引文件，
      // 此索引文件用于记录各个分区在Block文件中对应的偏移量，以便于reduce任务拉取时使用。
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  @VisibleForTesting
  long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * Concatenate all of the per-partition files into a single combined file.
   * 合并所有分区产生的文件中的数据到一个合并文件中
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedFile(File outputFile) throws IOException {
    // Track location of the partition starts in the output file
    // 创建长整型数组lengths，大小为分区数
    final long[] lengths = new long[numPartitions];
    if (partitionWriters == null) {
      // We were passed an empty iterator
      return lengths;
    }

    // 打开正式输出的文件输出流
    final FileOutputStream out = new FileOutputStream(outputFile, true);
    final long writeStartTime = System.nanoTime();
    boolean threwException = true;
    try {
      // 遍历partitionWriterSegments中每个分区ID
      for (int i = 0; i < numPartitions; i++) {
        // 获取对应的文件对象
        final File file = partitionWriterSegments[i].file();
        if (file.exists()) {
          // 创建对应的文件输入流
          final FileInputStream in = new FileInputStream(file);
          boolean copyThrewException = true;
          try {
            // 将输入流中的字节拷贝到输出流中
            // 由于遍历分区ID是从0开始的，因此最后写入分区数据文件的数据也是按照分区ID排好序的。
            // 将返回拷贝的字节数保存在lengths数组的对应分区的位置。
            lengths[i] = Utils.copyStream(in, out, false, transferToEnabled);
            copyThrewException = false;
          } finally {
            Closeables.close(in, copyThrewException);
          }
          if (!file.delete()) {
            logger.error("Unable to delete file for partition {}", i);
          }
        }
      }
      threwException = false;
    } finally {
      Closeables.close(out, threwException);
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    }
    partitionWriters = null;
    return lengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (DiskBlockObjectWriter writer : partitionWriters) {
              // This method explicitly does _not_ throw exceptions:
              File file = writer.revertPartialWritesAndClose();
              if (!file.delete()) {
                logger.error("Error while deleting file {}", file.getAbsolutePath());
              }
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}
