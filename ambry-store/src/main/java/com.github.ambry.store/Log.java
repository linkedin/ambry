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

import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
  private final ConcurrentSkipListMap<String, LogSegment> segmentsByName =
      new ConcurrentSkipListMap<>(LogSegmentNameHelper.COMPARATOR);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private long remainingUnallocatedSegments;
  private LogSegment activeSegment;

  /**
   * Create a Log instance
   * @param dataDir the directory where the segments of the log need to be loaded from.
   * @param totalCapacityInBytes the total capacity of this log.
   * @param segmentCapacityInBytes the capacity of a single segment in the log.
   * @param metrics the {@link StoreMetrics} instance to use.
   * @throws IOException if there is any I/O error loading the segment files.
   * @throws IllegalArgumentException if {@code totalCapacityInBytes} or {@code segmentCapacityInBytes} <= 0 or if
   * {@code totalCapacityInBytes} > {@code segmentCapacityInBytes} and {@code totalCapacityInBytes} is not a perfect
   * multiple of {@code segmentCapacityInBytes}.
   */
  Log(String dataDir, long totalCapacityInBytes, long segmentCapacityInBytes, StoreMetrics metrics) throws IOException {
    this.dataDir = dataDir;
    this.capacityInBytes = totalCapacityInBytes;
    this.metrics = metrics;

    File dir = new File(dataDir);
    File[] segmentFiles = dir.listFiles(LogSegmentNameHelper.LOG_FILE_FILTER);
    if (segmentFiles == null) {
      throw new IOException("Could not read from directory: " + dataDir);
    } else if (segmentFiles.length == 0) {
      // first startup
      checkArgsAndAllocateFirstSegment(totalCapacityInBytes, segmentCapacityInBytes);
    } else {
      // subsequent startup
      loadSegments(segmentFiles, totalCapacityInBytes);
    }
    activeSegment = segmentsByName.lastEntry().getValue();
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
  public int appendFrom(ByteBuffer buffer) throws IOException {
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
  public void appendFrom(ReadableByteChannel channel, long size) throws IOException {
    rollOverIfRequired(size);
    activeSegment.appendFrom(channel, size);
  }

  /**
   * Sets the active segment in the log.
   * </p>
   * Frees all segments that follow the active segment. Therefore, this should be
   * used only after the active segment is conclusively determined.
   * @param name the name of the log segment that is to be marked active.
   * @throws IllegalArgumentException if there no segment with name {@code name}.
   * @throws IOException if there is any I/O error freeing segments.
   */
  void setActiveSegment(String name) throws IOException {
    if (!segmentsByName.containsKey(name)) {
      throw new IllegalArgumentException("There is no log segment with name: " + name);
    }
    ConcurrentNavigableMap<String, LogSegment> extraSegments = segmentsByName.tailMap(name, false);
    Iterator<Map.Entry<String, LogSegment>> iterator = extraSegments.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, LogSegment> entry = iterator.next();
      logger.info("Freeing extra segment with name [{}] ", entry.getValue().getName());
      free(entry.getValue());
      iterator.remove();
    }
    logger.info("Setting active segment to [{}]", name);
    activeSegment = segmentsByName.get(name);
  }

  /**
   * @return the first {@link LogSegment} instance in the log.
   */
  LogSegment getFirstSegment() {
    return segmentsByName.firstEntry().getValue();
  }

  /**
   * Returns the {@link LogSegment} that is logically after the given {@code segment}.
   * @param segment the {@link LogSegment} whose "next" segment is required.
   * @return the {@link LogSegment} that is logically after the given {@code segment}.
   */
  LogSegment getNextSegment(LogSegment segment) {
    String name = segment.getName();
    if (!segmentsByName.containsKey(name)) {
      throw new IllegalArgumentException("Invalid log segment name: " + name);
    }
    Map.Entry<String, LogSegment> nextEntry = segmentsByName.higherEntry(name);
    return nextEntry == null ? null : nextEntry.getValue();
  }

  /**
   * @param name the name of the segment required.
   * @return a {@link LogSegment} with {@code name} if it exists.
   */
  LogSegment getSegment(String name) {
    return segmentsByName.get(name);
  }

  /**
   * @return the start offset of the log abstraction.
   */
  Offset getStartOffset() {
    LogSegment segment = getFirstSegment();
    return new Offset(segment.getName(), segment.getStartOffset());
  }

  /**
   * @return the end offset of the log abstraction.
   */
  Offset getEndOffset() {
    LogSegment segment = activeSegment;
    return new Offset(segment.getName(), segment.getEndOffset());
  }

  /**
   * @return the currently capacity used of the log abstraction. Includes "wasted" space at the end of
   * {@link LogSegment} instances that are not fully filled.
   */
  long getUsedCapacity() {
    return getDifference(getEndOffset(), new Offset(getFirstSegment().getName(), 0));
  }

  /**
   * @return the number of valid segments starting from the first segment.
   */
  long getSegmentCount() {
    return segmentsByName.size();
  }

  /**
   * Gets the absolute difference in bytes between {@code o1} and {@code o2}. The difference returned also includes the
   * sizes of log segment headers if the offsets are across segments.
   * @param o1 the first {@link Offset}.
   * @param o2 the second {@link Offset}.
   * @return the difference between {@code o1} and {@code o2}. If {@code o1} > {@code o2}, the difference returned is
   * positive, else, unless equal, it is negative
   */
  long getDifference(Offset o1, Offset o2) {
    LogSegment firstSegment = segmentsByName.get(o1.getName());
    LogSegment secondSegment = segmentsByName.get(o2.getName());
    if (firstSegment == null || secondSegment == null) {
      throw new IllegalArgumentException(
          "One of the log segments provided [" + o1.getName() + ", " + o2.getName() + "] does not belong to this log");
    }
    if (o1.getOffset() > firstSegment.getCapacityInBytes() || o2.getOffset() > secondSegment.getCapacityInBytes()) {
      throw new IllegalArgumentException("One of the offsets provided [" + o1.getOffset() + ", " + o2.getOffset()
          + "] is out of range of the segment it refers to [" + firstSegment.getCapacityInBytes() + ", " + secondSegment
          .getCapacityInBytes() + "]");
    }
    if (o1.getName().equals(o2.getName())) {
      return o1.getOffset() - o2.getOffset();
    }
    Offset higher = o1.compareTo(o2) > 0 ? o1 : o2;
    Offset lower = higher == o1 ? o2 : o1;
    int interveningSegmentsCount = segmentsByName.subMap(lower.getName(), false, higher.getName(), false).size();
    // segment capacity is the same for every segment.
    long segmentCapacity = firstSegment.getCapacityInBytes();
    // adding the left over capacity in lower segment + capacities of all segments inbetween + offset in higher segment
    long difference =
        segmentCapacity - lower.getOffset() + segmentCapacity * interveningSegmentsCount + higher.getOffset();
    return o1.compareTo(o2) * difference;
  }

  /**
   * Flushes the Log and all its segments.
   * @throws IOException if the flush encountered an I/O error.
   */
  void flush() throws IOException {
    // TODO: try to flush only segments that require flushing.
    logger.trace("Flushing log");
    for (LogSegment segment : segmentsByName.values()) {
      segment.flush();
    }
  }

  /**
   * Closes the Log and all its segments.
   * @throws IOException if the flush encountered an I/O error.
   */
  void close() throws IOException {
    for (LogSegment segment : segmentsByName.values()) {
      segment.close();
    }
  }

  /**
   * Checks the provided arguments for consistency and allocates the first segment file and creates the
   * {@link LogSegment} instance for it.
   * @param totalCapacity the intended total capacity of the log.
   * @param segmentCapacity the intended capacity of each segment of the log.
   * @throws IOException if there is an I/O error creating the segment files or creating {@link LogSegment} instances.
   */
  private void checkArgsAndAllocateFirstSegment(long totalCapacity, long segmentCapacity) throws IOException {
    if (totalCapacity <= 0 || segmentCapacity <= 0) {
      throw new IllegalArgumentException(
          "One of totalCapacityInBytes [" + totalCapacity + "] or " + "segmentCapacityInBytes [" + segmentCapacity + "]"
              + " is <=0");
    }
    segmentCapacity = Math.min(totalCapacity, segmentCapacity);
    // all segments should be the same size.
    long numSegments = totalCapacity / segmentCapacity;
    if (totalCapacity % segmentCapacity != 0) {
      throw new IllegalArgumentException(
          "Capacity of log [" + totalCapacity + "] should be a multiple of segment capacity [" + segmentCapacity + "]");
    }
    remainingUnallocatedSegments = numSegments;
    String firstSegmentName = LogSegmentNameHelper.generateFirstSegmentName(numSegments);
    logger.info("Allocating first segment with name [{}] and capacity {} bytes. Total number of segments is {}",
        firstSegmentName, segmentCapacity, numSegments);
    File segmentFile = allocate(LogSegmentNameHelper.nameToFilename(firstSegmentName), segmentCapacity);
    // to be backwards compatible, headers are not written for a log segment if it is the only log segment.
    LogSegment segment = new LogSegment(firstSegmentName, segmentFile, segmentCapacity, metrics, numSegments > 1);
    segmentsByName.put(segment.getName(), segment);
  }

  /**
   * Loads segment files and creates {@link LogSegment} instances.
   * @param segmentFiles the files that form the segments of the log.
   * @param totalCapacity the total capacity of the log. This is used only if this is a single segment log.
   * @throws IOException if there is an I/O error loading the segment files or creating {@link LogSegment} instances.
   */
  private void loadSegments(File[] segmentFiles, long totalCapacity) throws IOException {
    long totalSegments = -1;
    for (File segmentFile : segmentFiles) {
      String name = LogSegmentNameHelper.nameFromFilename(segmentFile.getName());
      logger.info("Loading segment with name [{}]", name);
      LogSegment segment;
      if (name.isEmpty()) {
        totalSegments = 1;
        // for backwards compatibility, a single segment log is loaded by providing capacity since the old logs have
        // no headers
        segment = new LogSegment(name, segmentFile, totalCapacity, metrics, false);
      } else {
        segment = new LogSegment(name, segmentFile, metrics);
        totalSegments = totalSegments == -1 ? totalCapacity / segment.getCapacityInBytes() : totalSegments;
      }
      logger.info("Segment [{}] has capacity of {} bytes", name, segment.getCapacityInBytes());
      segmentsByName.put(segment.getName(), segment);
    }
    remainingUnallocatedSegments = totalSegments - segmentsByName.size();
  }

  /**
   * Allocates a file named {@code filename} and of capacity {@code size}.
   * @param filename the intended filename of the file.
   * @param size the intended size of the file.
   * @return a {@link File} instance that points to the created file named {@code filename} and capacity {@code size}.
   * @throws IOException if the there is any I/O error in allocating the file.
   */
  private File allocate(String filename, long size) throws IOException {
    // TODO (DiskManager changes): This is intended to "request" the segment file from the DiskManager which will have
    // TODO (DiskManager changes): a pool of segments.
    File segmentFile = new File(dataDir, filename);
    Utils.preAllocateFileIfNeeded(segmentFile, size);
    remainingUnallocatedSegments--;
    return segmentFile;
  }

  /**
   * Frees the given {@link LogSegment} and its backing segment file.
   * @param logSegment the {@link LogSegment} instance whose backing file needs to be freed.
   * @throws IOException if there is any I/O error freeing the log segment.
   */
  private void free(LogSegment logSegment) throws IOException {
    // TODO (DiskManager changes): This will actually return the segment to the DiskManager pool.
    File segmentFile = logSegment.getView().getFirst();
    logSegment.close();
    if (!segmentFile.delete()) {
      throw new IllegalStateException("Could not delete segment file: " + segmentFile.getAbsolutePath());
    }
    remainingUnallocatedSegments++;
  }

  /**
   * Rolls the active log segment over if required. If rollover is required, a new segment is allocated.
   * @param writeSize the size of the incoming write.
   * @throws IllegalArgumentException if the {@code writeSize} is greater than a single segment's size
   * @throws IllegalStateException if there is no more capacity in the log.
   * @throws IOException if any I/O error occurred as part of ensuring capacity.
   *
   */
  private void rollOverIfRequired(long writeSize) throws IOException {
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
   * Ensures that there is enough capacity for a write of size {@code writeSize} in the log. As a part of ensuring
   * capacity, this function will also allocate more segments if required.
   * @param writeSize the size of a subsequent write on the active log segment.
   * @throws IllegalArgumentException if the {@code writeSize} is greater than a single segment's size
   * @throws IllegalStateException if there no more capacity in the log.
   * @throws IOException if any I/O error occurred as a part of ensuring capacity.
   */
  private void ensureCapacity(long writeSize) throws IOException {
    if (remainingUnallocatedSegments == 0) {
      metrics.overflowWriteError.inc();
      throw new IllegalStateException(
          "There is no more capacity left in [" + dataDir + "]. Max capacity is [" + capacityInBytes + "]");
    }
    // all segments are (should be) the same size.
    long segmentCapacity = activeSegment.getCapacityInBytes();
    if (writeSize > segmentCapacity - LogSegment.HEADER_SIZE) {
      metrics.overflowWriteError.inc();
      throw new IllegalArgumentException("Write of size [" + writeSize + "] cannot be serviced because it is greater "
          + "than a single segment's capacity [" + (segmentCapacity - LogSegment.HEADER_SIZE) + "]");
    }
    String newSegmentName = LogSegmentNameHelper.getNextPositionName(activeSegment.getName());
    logger.info("Allocating new segment with name: " + newSegmentName);
    File newSegmentFile = allocate(LogSegmentNameHelper.nameToFilename(newSegmentName), segmentCapacity);
    LogSegment newSegment = new LogSegment(newSegmentName, newSegmentFile, segmentCapacity, metrics, true);
    segmentsByName.put(newSegmentName, newSegment);
  }

  /**
   * Gets the {@link FileSpan} for a message that is written starting at {@code endOffsetOfPrevMessage} and is of size
   * {@code size}.
   * @param endOffsetOfPrevMessage the end offset of the message that is before this one.
   * @param size the size of the write.
   * @return the {@link FileSpan} for a message that is written starting at {@code endOffsetOfPrevMessage} and is of
   * size {@code size}.
   */
  FileSpan getFileSpanForMessage(Offset endOffsetOfPrevMessage, long size) {
    LogSegment segment = segmentsByName.get(endOffsetOfPrevMessage.getName());
    long startOffset = endOffsetOfPrevMessage.getOffset();
    if (startOffset > segment.getEndOffset()) {
      throw new IllegalArgumentException("Start offset provided is greater than segment end offset");
    } else if (startOffset == segment.getEndOffset()) {
      // current segment has ended. Since a blob will be wholly contained within one segment, this blob is in the
      // next segment
      segment = getNextSegment(segment);
      startOffset = segment.getStartOffset();
    } else if (startOffset + size > segment.getEndOffset()) {
      throw new IllegalStateException("Args indicate that blob is not wholly contained within a single segment");
    }
    return new FileSpan(new Offset(segment.getName(), startOffset), new Offset(segment.getName(), startOffset + size));
  }
}
