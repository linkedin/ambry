/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Log is an abstraction over a group of files that are loaded as {@link LogSegment} instances. It provides a
 * unified view of these segments and provides a way to interact and query different properties of this group of
 * segments.
 */
class Log implements Write {
  private final String dataDir;
  private final long capacityInBytes;
  private final StoreMetrics metrics;
  private final AtomicLong remainingUnallocatedCapacity;
  private final ConcurrentSkipListMap<String, LogSegment> segmentsByName =
      new ConcurrentSkipListMap<>(LogSegmentNameHelper.COMPARATOR);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private LogSegment activeSegment;

  /**
   * Create a Log instance
   * @param dataDir the directory where the segments of the log need to be created.
   * @param totalCapacityInBytes the total capacity of this log.
   * @param segmentCapacityInBytes the capacity of a single segment in the log.
   * @param metrics the {@link StoreMetrics} instance to use.
   * @throws IOException if there is any I/O error loading the segment files.
   * @throws IllegalArgumentException if {@code totalCapacityInBytes} or {@code segmentCapacityInBytes} <= 0 or if
   * {@code totalCapacityInBytes} > {@code segmentCapacityInBytes} and {@code totalCapacityInBytes} is not a perfect
   * multiple of {@code segmentCapacityInBytes}.
   */
  public Log(String dataDir, long totalCapacityInBytes, long segmentCapacityInBytes, StoreMetrics metrics)
      throws IOException {
    if (totalCapacityInBytes <= 0 || segmentCapacityInBytes <= 0) {
      throw new IllegalArgumentException(
          "One of totalCapacityInBytes [" + totalCapacityInBytes + "] or " + "segmentCapacityInBytes ["
              + segmentCapacityInBytes + "] is <=0");
    }
    long segmentCapacity = Math.min(totalCapacityInBytes, segmentCapacityInBytes);
    // all segments should be the same size.
    if (totalCapacityInBytes % segmentCapacity != 0) {
      throw new IllegalArgumentException(
          "Capacity of log [" + totalCapacityInBytes + "] should be a perfect multiple of segment capacity ["
              + segmentCapacityInBytes + "]");
    }
    this.dataDir = dataDir;
    this.capacityInBytes = totalCapacityInBytes;
    this.metrics = metrics;
    remainingUnallocatedCapacity = new AtomicLong(totalCapacityInBytes);
    long numSegments = (totalCapacityInBytes - 1) / segmentCapacityInBytes + 1;
    loadSegments(segmentCapacity, numSegments);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Appends the given {@code buffer} to the active log segment. The {@code buffer} will be written to a single log
   * segment i.e. its data will not exist across segments.
   * @param buffer The buffer from which data needs to be written from
   * @return the number of bytes written.
   * @throws IllegalArgumentException if the {@code buffer.remaining()} is greater than a single segment's size.
   * @throws IllegalStateException if there no more capacity in the log.
   * @throws IOException if there was an I/O error while writing.
   */
  @Override
  public int appendFrom(ByteBuffer buffer)
      throws IOException {
    rollOverIfRequired(buffer.remaining());
    return activeSegment.appendFrom(buffer);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Appends the given data to the active log segment. The data will be written to a single log segment i.e. the data
   * will not exist across segments.
   * @param channel The channel from which data needs to be written from
   * @param size The amount of data in bytes to be written from the channel
   * @throws IllegalArgumentException if the {@code size} is greater than a single segment's size.
   * @throws IllegalStateException if there no more capacity in the log.
   * @throws IOException if there was an I/O error while writing.
   */
  @Override
  public void appendFrom(ReadableByteChannel channel, long size)
      throws IOException {
    rollOverIfRequired(size);
    activeSegment.appendFrom(channel, size);
  }

  /**
   * Sets the active segment in the log.
   * @param name the name of the log segment that is to be marked active.
   */
  void setActiveSegment(String name) {
    if (!segmentsByName.containsKey(name)) {
      throw new IllegalArgumentException("There is no log segment with name: " + name);
    }
    activeSegment = segmentsByName.get(name);
  }

  /**
   * @return a segment iterator that can iterate over all the log segments in this log
   */
  Iterator<Map.Entry<String, LogSegment>> getSegmentIterator() {
    return segmentsByName.entrySet().iterator();
  }

  /**
   * @param name the name of the segment required.
   * @return a {@link LogSegment} with {@code name} if it exists.
   */
  LogSegment getSegment(String name) {
    return segmentsByName.get(name);
  }

  /**
   * @return the end offset of the log abstraction.
   */
  Offset getLogEndOffset() {
    LogSegment segment = activeSegment;
    return new Offset(segment.getName(), segment.getEndOffset());
  }

  /**
   * @return the currently capacity used of the log abstraction. Includes "wasted" space at the end of
   * {@link LogSegment} instances that are not fully filled.
   */
  long getUsedCapacity() {
    long usedCapacity = 0;
    for (Map.Entry<String, LogSegment> entry : segmentsByName.entrySet()) {
      LogSegment segment = entry.getValue();
      if (segment != activeSegment) {
        usedCapacity += segment.getCapacityInBytes();
      } else {
        usedCapacity += segment.getEndOffset();
        break;
      }
    }
    return usedCapacity;
  }

  /**
   * @return the number of valid segments starting from the first segment.
   */
  long getSegmentCount() {
    return segmentsByName.size();
  }

  /**
   * Flushes the Log and all its segments.
   * @throws IOException if the flush encountered an I/O error.
   */
  void flush()
      throws IOException {
    for (Map.Entry<String, LogSegment> entry : segmentsByName.entrySet()) {
      entry.getValue().flush();
    }
  }

  /**
   * Closes the Log and all its segments.
   * @throws IOException if the flush encountered an I/O error.
   */
  void close()
      throws IOException {
    for (Map.Entry<String, LogSegment> entry : segmentsByName.entrySet()) {
      entry.getValue().close();
    }
  }

  /**
   * Loads segment files (if they exist) and creates {@link LogSegment} instances. If no segment files exist, the first
   * segment file is created.
   * @param segmentCapacity the intended capacity of each log segment. This is of importance on first startup.
   * @param numSegments the total number of segments that the log abstraction will contain.
   * @throws IOException if there is an I/O error creating/loading the segment files or creating the {@link LogSegment}
   * instances
   */
  private void loadSegments(long segmentCapacity, long numSegments)
      throws IOException {
    File dir = new File(dataDir);
    File[] segmentFiles = dir.listFiles(LogSegmentNameHelper.LOG_FILE_FILTER);
    if (segmentFiles == null || segmentFiles.length == 0) {
      String firstSegmentName = LogSegmentNameHelper.generateFirstSegmentName(numSegments);
      File segmentFile = allocate(LogSegmentNameHelper.nameToFilename(firstSegmentName), segmentCapacity);
      // to be backwards compatible, headers are not written for a log segment if it is the only log segment.
      LogSegment segment = new LogSegment(firstSegmentName, segmentFile, segmentCapacity, metrics, numSegments > 1);
      segmentsByName.put(segment.getName(), segment);
    } else {
      for (File segmentFile : segmentFiles) {
        String name = LogSegmentNameHelper.nameFromFilename(segmentFile.getName());
        LogSegment segment;
        if (numSegments == 1) {
          // for backwards compatibility, a single segment log is loaded by providing capacity since the old logs have
          // no headers
          segment = new LogSegment(name, segmentFile, segmentCapacity, metrics, false);
        } else {
          segment = new LogSegment(name, segmentFile, metrics);
        }
        segmentsByName.put(segment.getName(), segment);
        remainingUnallocatedCapacity.addAndGet(-segmentCapacity);
      }
    }
    activeSegment = segmentsByName.firstEntry().getValue();
  }

  /**
   * Allocates a file named {@code filename} and of capacity {@code size}.
   * @param filename the intended filename of the file.
   * @param size the intended size of the file.
   * @return a {@link File} instance that points to the created file named {@code filename} and capacity {@code size}.
   * @throws IOException if the there is any I/O error in allocating the file.
   */
  private File allocate(String filename, long size)
      throws IOException {
    // TODO (DiskManager changes): This is intended to "request" the segment file from the DiskManager which will have
    // TODO (DiskManager changes): a pool of segments.
    File segmentFile = new File(dataDir, filename);
    Utils.preAllocateFileIfNeeded(segmentFile, size);
    remainingUnallocatedCapacity.addAndGet(-size);
    return segmentFile;
  }

  /**
   * Rolls the log segment over if required. Also ensures enough capacity.
   * @param writeSize the size of the incoming write.
   * @throws IllegalArgumentException if the {@code writeSize} is greater than a single segment's size
   * @throws IllegalStateException if there no more capacity in the log.
   * @throws IOException if any I/O error occurred as a part of ensuring capacity.
   *
   */
  private void rollOverIfRequired(long writeSize)
      throws IOException {
    if (activeSegment.getCapacityInBytes() - activeSegment.getEndOffset() < writeSize) {
      ensureCapacity(writeSize);
      // this cannot be null since capacity has either been ensured or has thrown.
      LogSegment nextActiveSegment = segmentsByName.higherEntry(activeSegment.getName()).getValue();
      logger.info("Rolling over writes to {} from {} on write of data of size {}. End offset was {} and capacity is {}",
          nextActiveSegment.getName(), activeSegment.getName(), writeSize, activeSegment.getEndOffset(),
          activeSegment.getCapacityInBytes());
      activeSegment = nextActiveSegment;
    }
  }

  /**
   * Ensures that there is enough capacity of a write of size {@code writeSize}.
   * @param writeSize the size of a subsequent write on the active log segment.
   * @throws IllegalArgumentException if the {@code writeSize} is greater than a single segment's size
   * @throws IllegalStateException if there no more capacity in the log.
   * @throws IOException if any I/O error occurred as a part of ensuring capacity.
   */
  private void ensureCapacity(long writeSize)
      throws IOException {
    if (segmentsByName.higherEntry(activeSegment.getName()) != null) {
      return;
    }
    // all segments are (should be) the same size.
    long segmentCapacity = activeSegment.getCapacityInBytes();
    if (segmentCapacity > remainingUnallocatedCapacity.get()) {
      metrics.overflowWriteError.inc();
      throw new IllegalStateException(
          "There is no more capacity left in [" + dataDir + "]. Max capacity is [" + capacityInBytes + "]");
    }
    if (writeSize > segmentCapacity - LogSegment.HEADER_SIZE) {
      metrics.overflowWriteError.inc();
      throw new IllegalArgumentException("Write of size [" + writeSize + "] cannot be serviced because it is greater "
          + "than a single segment's capacity [" + (segmentCapacity - LogSegment.HEADER_SIZE) + "]");
    }
    String lastSegmentName = segmentsByName.lastEntry().getKey();
    String newSegmentName = LogSegmentNameHelper.getNextPositionName(lastSegmentName);
    File newSegmentFile = allocate(LogSegmentNameHelper.nameToFilename(newSegmentName), segmentCapacity);
    LogSegment newSegment = new LogSegment(newSegmentName, newSegmentFile, segmentCapacity, metrics, true);
    segmentsByName.put(newSegmentName, newSegment);
  }

  /**
   * Gets a {@link StoreMessageReadSet} with the file and file channel of the first segment of the log.
   * @param readOptions the {@link BlobReadOptions} to include in the {@link StoreMessageReadSet}.
   * @return a {@link StoreMessageReadSet} with the file and file channel of the first segment and the given {@code }
   * @deprecated this function is deprecated and is available for use until {@link PersistentIndex} and
   * {@link HardDeleter} are rewritten to understand segmented logs.
   */
  @Deprecated
  StoreMessageReadSet getView(List<BlobReadOptions> readOptions) {
    LogSegment firstSegment = segmentsByName.firstEntry().getValue();
    Pair<File, FileChannel> view = firstSegment.getView();
    return new StoreMessageReadSet(view.getFirst(), view.getSecond(), readOptions, firstSegment.getEndOffset());
  }
}
