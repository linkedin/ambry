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

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
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
  private final DiskSpaceAllocator diskSpaceAllocator;
  private final StoreConfig config;
  private final StoreMetrics metrics;
  private final Iterator<Pair<String, String>> segmentNameAndFileNameIterator;
  private final ConcurrentSkipListMap<String, LogSegment> segmentsByName =
      new ConcurrentSkipListMap<>(LogSegmentNameHelper.COMPARATOR);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicLong remainingUnallocatedSegments = new AtomicLong(0);
  private final String storeId;

  private boolean isLogSegmented;
  private LogSegment activeSegment = null;

  /**
   * Create a Log instance
   * @param dataDir the directory where the segments of the log need to be loaded from.
   * @param totalCapacityInBytes the total capacity of this log.
   * @param diskSpaceAllocator the {@link DiskSpaceAllocator} to use to allocate new log segments.
   * @param config The store config used to initialize this log.
   * @param metrics the {@link StoreMetrics} instance to use.
   * @throws StoreException if there is any store exception loading the segment files.
   * @throws IllegalArgumentException if {@code totalCapacityInBytes} or {@code segmentCapacityInBytes} <= 0 or if
   * {@code totalCapacityInBytes} > {@code segmentCapacityInBytes} and {@code totalCapacityInBytes} is not a perfect
   * multiple of {@code segmentCapacityInBytes}.
   */
  Log(String dataDir, long totalCapacityInBytes, DiskSpaceAllocator diskSpaceAllocator, StoreConfig config,
      StoreMetrics metrics) throws StoreException {
    this.dataDir = dataDir;
    this.capacityInBytes = totalCapacityInBytes;
    this.isLogSegmented = totalCapacityInBytes > config.storeSegmentSizeInBytes;
    this.diskSpaceAllocator = diskSpaceAllocator;
    this.config = config;
    this.metrics = metrics;
    this.segmentNameAndFileNameIterator = Collections.EMPTY_LIST.iterator();
    storeId = dataDir.substring(dataDir.lastIndexOf(File.separator) + File.separator.length());

    File dir = new File(dataDir);
    File[] segmentFiles = dir.listFiles(LogSegmentNameHelper.LOG_FILE_FILTER);
    if (segmentFiles == null) {
      throw new StoreException("Could not read from directory: " + dataDir, StoreErrorCodes.File_Not_Found);
    } else {
      initialize(getSegmentsToLoad(segmentFiles), config.storeSegmentSizeInBytes, false);
      this.isLogSegmented = isExistingLogSegmented();
    }
  }

  /**
   * Create a Log instance in COPY phase during compaction.
   * @param dataDir the directory where the segments of the log need to be loaded from.
   * @param totalCapacityInBytes the total capacity of this log.
   * @param diskSpaceAllocator the {@link DiskSpaceAllocator} to use to allocate new log segments.
   * @param config the store config to initialize this log.
   * @param metrics the {@link StoreMetrics} instance to use.
   * @param isLogSegmented {@code true} if this log is segmented or needs to be segmented.
   * @param segmentsToLoad the list of pre-created {@link LogSegment} instances to load.
   * @param segmentNameAndFileNameIterator an {@link Iterator} that provides the name and filename for newly allocated
   *                                       log segments. Once the iterator ends, the active segment name is used to
   *                                       generate the names of the subsequent segments.
   * @throws StoreException if there is any store exception loading the segment files.
   * @throws IllegalArgumentException if {@code totalCapacityInBytes} or {@code segmentCapacityInBytes} <= 0 or if
   * {@code totalCapacityInBytes} > {@code segmentCapacityInBytes} and {@code totalCapacityInBytes} is not a perfect
   * multiple of {@code segmentCapacityInBytes}.
   */
  Log(String dataDir, long totalCapacityInBytes, DiskSpaceAllocator diskSpaceAllocator, StoreConfig config,
      StoreMetrics metrics, boolean isLogSegmented, List<LogSegment> segmentsToLoad,
      Iterator<Pair<String, String>> segmentNameAndFileNameIterator) throws StoreException {
    this.dataDir = dataDir;
    this.capacityInBytes = totalCapacityInBytes;
    this.isLogSegmented = isLogSegmented;
    this.diskSpaceAllocator = diskSpaceAllocator;
    this.config = config;
    this.metrics = metrics;
    this.segmentNameAndFileNameIterator = segmentNameAndFileNameIterator;
    storeId = dataDir.substring(dataDir.lastIndexOf(File.separator) + File.separator.length());

    initialize(segmentsToLoad, config.storeSegmentSizeInBytes, true);
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
   * @throws StoreException if there was a store exception while writing.
   */
  @Override
  public int appendFrom(ByteBuffer buffer) throws StoreException {
    rollOverIfRequired(buffer.remaining());
    return activeSegment.appendFrom(buffer);
  }

  /**
   * Appends the given {@code byteArray} to the active log segment in direct IO manner.
   * The {@code byteArray} will be written to a single log segment i.e. its data will not exist across segments.
   * @param byteArray The buffer from which data needs to be written from
   * @return the number of bytes written.
   * @throws IllegalArgumentException if the {@code byteArray.length} is greater than a single segment's size.
   * @throws IllegalStateException if there no more capacity in the log.
   * @throws StoreException if there was an I/O error while writing.
   */
  int appendFromDirectly(byte[] byteArray, int offset, int length) throws StoreException {
    rollOverIfRequired(length);
    return activeSegment.appendFromDirectly(byteArray, offset, length);
  }

  /**
   * Appends the given data to the active log segment. The data will be written to a single log segment i.e. the data
   * will not exist across segments.
   * @param channel The channel from which data needs to be written from
   * @param size The amount of data in bytes to be written from the channel
   * @throws IllegalArgumentException if the {@code size} is greater than a single segment's size.
   * @throws IllegalStateException if there no more capacity in the log.
   * @throws StoreException if there was a store exception while writing.
   */
  @Override
  public void appendFrom(ReadableByteChannel channel, long size) throws StoreException {
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
   * @throws StoreException if there is any store exception while freeing segments.
   */
  void setActiveSegment(String name) throws StoreException {
    if (!segmentsByName.containsKey(name)) {
      throw new IllegalArgumentException("There is no log segment with name: " + name);
    }
    ConcurrentNavigableMap<String, LogSegment> extraSegments = segmentsByName.tailMap(name, false);
    Iterator<Map.Entry<String, LogSegment>> iterator = extraSegments.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, LogSegment> entry = iterator.next();
      logger.info("Freeing extra segment with name [{}] ", entry.getValue().getName());
      free(entry.getValue(), false);
      remainingUnallocatedSegments.getAndIncrement();
      iterator.remove();
    }
    logger.info("Setting active segment to [{}]", name);
    LogSegment newActiveSegment = segmentsByName.get(name);
    if (newActiveSegment != activeSegment) {
      // If activeSegment needs to be changed, then drop buffer for old activeSegment and init buffer for new activeSegment.
      activeSegment.dropBufferForAppend();
      activeSegment = newActiveSegment;
      activeSegment.initBufferForAppend();
    }
  }

  /**
   * @return the capacity of a single segment.
   */
  long getSegmentCapacity() {
    // all segments same size
    return getFirstSegment().getCapacityInBytes();
  }

  /**
   * This returns the number of unallocated segments for this log. However, this method can only be used when compaction
   * is not running.
   * @return the number of unallocated segments.
   */
  long getRemainingUnallocatedSegments() {
    return remainingUnallocatedSegments.get();
  }

  /**
   * @return {@code true} if the log is segmented.
   */
  boolean isLogSegmented() {
    return isLogSegmented;
  }

  /**
   * @return the total capacity, in bytes, of this log.
   */
  long getCapacityInBytes() {
    return capacityInBytes;
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
   * Returns the {@link LogSegment} that is logically before the given {@code segment}.
   * @param segment the {@link LogSegment} whose "previous" segment is required.
   * @return the {@link LogSegment} that is logically before the given {@code segment}.
   */
  LogSegment getPrevSegment(LogSegment segment) {
    String name = segment.getName();
    if (!segmentsByName.containsKey(name)) {
      throw new IllegalArgumentException("Invalid log segment name: " + name);
    }
    Map.Entry<String, LogSegment> prevEntry = segmentsByName.lowerEntry(name);
    return prevEntry == null ? null : prevEntry.getValue();
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
  Offset getEndOffset() {
    LogSegment segment = activeSegment;
    return new Offset(segment.getName(), segment.getEndOffset());
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
   * @param skipDiskFlush whether to skip any disk flush operations.
   * @throws IOException if the flush encountered an I/O error.
   */
  void close(boolean skipDiskFlush) throws IOException {
    for (LogSegment segment : segmentsByName.values()) {
      segment.close(skipDiskFlush);
    }
  }

  /**
   * Checks the provided arguments for consistency and allocates the first segment file and creates the
   * {@link LogSegment} instance for it.
   * @param segmentCapacity the intended capacity of each segment of the log.
   * @param needSwapSegment whether a swap segment is needed by {@link BlobStoreCompactor}
   * @return the {@link LogSegment} instance that is created.
   * @throws StoreException if there is store exception when creating the segment files or creating {@link LogSegment} instances.
   */
  private LogSegment checkArgsAndGetFirstSegment(long segmentCapacity, boolean needSwapSegment) throws StoreException {
    if (capacityInBytes <= 0 || segmentCapacity <= 0) {
      throw new IllegalArgumentException(
          "One of totalCapacityInBytes [" + capacityInBytes + "] or " + "segmentCapacityInBytes [" + segmentCapacity
              + "] is <=0");
    }
    segmentCapacity = Math.min(capacityInBytes, segmentCapacity);
    // all segments should be the same size.
    long numSegments = capacityInBytes / segmentCapacity;
    if (capacityInBytes % segmentCapacity != 0) {
      throw new IllegalArgumentException(
          "Capacity of log [" + capacityInBytes + "] should be a multiple of segment capacity [" + segmentCapacity
              + "]");
    }
    Pair<String, String> segmentNameAndFilename = getNextSegmentNameAndFilename();
    logger.info("Allocating first segment with name [{}], back by file {} and capacity {} bytes. Total number of "
            + "segments is {}", segmentNameAndFilename.getFirst(), segmentNameAndFilename.getSecond(), segmentCapacity,
        numSegments);
    File segmentFile = allocate(segmentNameAndFilename.getSecond(), segmentCapacity, needSwapSegment);
    // to be backwards compatible, headers are not written for a log segment if it is the only log segment.
    return new LogSegment(segmentNameAndFilename.getFirst(), segmentFile, segmentCapacity, config, metrics,
        isLogSegmented);
  }

  /**
   * Creates {@link LogSegment} instances from {@code segmentFiles}.
   * @param segmentFiles the files that form the segments of the log.
   * @return {@code List} of {@link LogSegment} instances corresponding to {@code segmentFiles}.
   * @throws StoreException if there is an I/O error loading the segment files or creating {@link LogSegment} instances.
   */
  private List<LogSegment> getSegmentsToLoad(File[] segmentFiles) throws StoreException {
    List<LogSegment> segments = new ArrayList<>(segmentFiles.length);
    for (File segmentFile : segmentFiles) {
      String name = LogSegmentNameHelper.nameFromFilename(segmentFile.getName());
      logger.info("Loading segment with name [{}]", name);
      LogSegment segment;
      if (name.isEmpty()) {
        // for backwards compatibility, a single segment log is loaded by providing capacity since the old logs have
        // no headers
        segment = new LogSegment(name, segmentFile, capacityInBytes, config, metrics, false);
      } else {
        segment = new LogSegment(name, segmentFile, config, metrics);
      }
      logger.info("Segment [{}] has capacity of {} bytes", name, segment.getCapacityInBytes());
      segments.add(segment);
    }
    return segments;
  }

  /**
   * @return {@code true} if the data directory contains segments that aren't old-style single segment log files.
   * This should be run after a log is initialized to get the actual log segmentation state.
   */
  private boolean isExistingLogSegmented() {
    return !segmentsByName.firstKey().isEmpty();
  }

  /**
   * Initializes the log.
   * @param segmentsToLoad the {@link LogSegment} instances to include as a part of the log. These are not in any order
   * @param segmentCapacityInBytes the capacity of a single {@link LogSegment}.
   * @param mayNeedSwapSegment whether compactor may need swap segment.
   * @throws StoreException if there is any store exception during initialization.
   */
  private void initialize(List<LogSegment> segmentsToLoad, long segmentCapacityInBytes, boolean mayNeedSwapSegment)
      throws StoreException {
    if (segmentsToLoad.size() == 0) {
      // bootstrapping log.
      segmentsToLoad =
          Collections.singletonList(checkArgsAndGetFirstSegment(segmentCapacityInBytes, mayNeedSwapSegment));
    }

    LogSegment anySegment = segmentsToLoad.get(0);
    long totalSegments = anySegment.getName().isEmpty() ? 1 : capacityInBytes / anySegment.getCapacityInBytes();
    for (LogSegment segment : segmentsToLoad) {
      // putting the segments in the map orders them
      segmentsByName.put(segment.getName(), segment);
    }
    remainingUnallocatedSegments.set(totalSegments - segmentsByName.size());
    activeSegment = segmentsByName.lastEntry().getValue();
    activeSegment.initBufferForAppend();
  }

  /**
   * Allocates a file named {@code filename} and of capacity {@code size}.
   * @param filename the intended filename of the file.
   * @param size the intended size of the file.
   * @param requestSwapSegment whether swap segment is requested by {@link BlobStoreCompactor}
   * @return a {@link File} instance that points to the created file named {@code filename} and capacity {@code size}.
   * @throws StoreException if the there is any store exception while allocating the file.
   */
  File allocate(String filename, long size, boolean requestSwapSegment) throws StoreException {
    File segmentFile = new File(dataDir, filename);
    if (!segmentFile.exists()) {
      try {
        diskSpaceAllocator.allocate(segmentFile, size, storeId, requestSwapSegment);
      } catch (IOException e) {
        StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
        throw new StoreException(errorCode.toString() + " while allocating the file", e, errorCode);
      }
    }
    return segmentFile;
  }

  /**
   * Frees the given {@link LogSegment} and its backing segment file.
   * @param logSegment the {@link LogSegment} instance whose backing file needs to be freed.
   * @param isSwapSegment whether the segment to free is a swap segment.
   * @throws StoreException if there is any store exception when freeing the log segment.
   */
  private void free(LogSegment logSegment, boolean isSwapSegment) throws StoreException {
    File segmentFile = logSegment.getView().getFirst();
    try {
      logSegment.close(false);
      diskSpaceAllocator.free(segmentFile, logSegment.getCapacityInBytes(), storeId, isSwapSegment);
    } catch (IOException e) {
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException(errorCode.toString() + " while freeing log segment", e, errorCode);
    }
  }

  /**
   * Rolls the active log segment over if required. If rollover is required, a new segment is allocated.
   * @param writeSize the size of the incoming write.
   * @throws IllegalArgumentException if the {@code writeSize} is greater than a single segment's size
   * @throws IllegalStateException if there is no more capacity in the log.
   * @throws StoreException if any store exception occurred as part of ensuring capacity.
   *
   */
  private void rollOverIfRequired(long writeSize) throws StoreException {
    if (activeSegment.getCapacityInBytes() - activeSegment.getEndOffset() < writeSize) {
      ensureCapacity(writeSize);
      // this cannot be null since capacity has either been ensured or has thrown.
      LogSegment nextActiveSegment = segmentsByName.higherEntry(activeSegment.getName()).getValue();
      logger.info("Rolling over writes to {} from {} on write of data of size {}. End offset was {} and capacity is {}",
          nextActiveSegment.getName(), activeSegment.getName(), writeSize, activeSegment.getEndOffset(),
          activeSegment.getCapacityInBytes());
      activeSegment.dropBufferForAppend();
      nextActiveSegment.initBufferForAppend();
      activeSegment = nextActiveSegment;
    }
  }

  /**
   * Ensures that there is enough capacity for a write of size {@code writeSize} in the log. As a part of ensuring
   * capacity, this function will also allocate more segments if required.
   * @param writeSize the size of a subsequent write on the active log segment.
   * @throws IllegalArgumentException if the {@code writeSize} is greater than a single segment's size
   * @throws IllegalStateException if there no more capacity in the log.
   * @throws StoreException if any store exception occurred as a part of ensuring capacity.
   */
  private void ensureCapacity(long writeSize) throws StoreException {
    // all segments are (should be) the same size.
    long segmentCapacity = activeSegment.getCapacityInBytes();
    if (writeSize > segmentCapacity - LogSegment.HEADER_SIZE) {
      metrics.overflowWriteError.inc();
      throw new IllegalArgumentException("Write of size [" + writeSize + "] cannot be serviced because it is greater "
          + "than a single segment's capacity [" + (segmentCapacity - LogSegment.HEADER_SIZE) + "]");
    }
    if (remainingUnallocatedSegments.decrementAndGet() < 0) {
      remainingUnallocatedSegments.incrementAndGet();
      metrics.overflowWriteError.inc();
      throw new IllegalStateException(
          "There is no more capacity left in [" + dataDir + "]. Max capacity is [" + capacityInBytes + "]");
    }
    Pair<String, String> segmentNameAndFilename = getNextSegmentNameAndFilename();
    logger.info("Allocating new segment with name: " + segmentNameAndFilename.getFirst());
    File newSegmentFile = null;
    try {
      newSegmentFile = allocate(segmentNameAndFilename.getSecond(), segmentCapacity, false);
      LogSegment newSegment =
          new LogSegment(segmentNameAndFilename.getFirst(), newSegmentFile, segmentCapacity, config, metrics, true);
      segmentsByName.put(segmentNameAndFilename.getFirst(), newSegment);
    } catch (StoreException e) {
      try {
        if (newSegmentFile != null) {
          diskSpaceAllocator.free(newSegmentFile, segmentCapacity, storeId, false);
        }
        remainingUnallocatedSegments.incrementAndGet();
        throw e;
      } catch (IOException exception) {
        StoreErrorCodes errorCode = StoreException.resolveErrorCode(exception);
        throw new StoreException(
            e.getMessage() + " And then " + errorCode.toString() + " occurred while freeing the log segment "
                + segmentNameAndFilename.getFirst(), exception, errorCode);
      } finally {
        metrics.overflowWriteError.inc();
      }
    }
  }

  /**
   * @return the name and filename of the segment that is to be created.
   */
  private Pair<String, String> getNextSegmentNameAndFilename() {
    Pair<String, String> nameAndFilename;
    if (segmentNameAndFileNameIterator != null && segmentNameAndFileNameIterator.hasNext()) {
      nameAndFilename = segmentNameAndFileNameIterator.next();
    } else if (activeSegment == null) {
      // this code path gets exercised only on first startup
      String name = LogSegmentNameHelper.generateFirstSegmentName(isLogSegmented);
      nameAndFilename = new Pair<>(name, LogSegmentNameHelper.nameToFilename(name));
    } else {
      String name = LogSegmentNameHelper.getNextPositionName(activeSegment.getName());
      nameAndFilename = new Pair<>(name, LogSegmentNameHelper.nameToFilename(name));
    }
    return nameAndFilename;
  }

  /**
   * Adds a {@link LogSegment} instance to the log.
   * @param segment the {@link LogSegment} instance to add.
   * @param increaseUsedSegmentCount {@code true} if the number of segments used has to be incremented, {@code false}
   *                                             otherwise.
   * @throws IllegalArgumentException if the {@code segment} being added is past the active segment
   */
  void addSegment(LogSegment segment, boolean increaseUsedSegmentCount) {
    if (LogSegmentNameHelper.COMPARATOR.compare(segment.getName(), activeSegment.getName()) >= 0) {
      throw new IllegalArgumentException(
          "Cannot add segments past the current active segment. Active segment is [" + activeSegment.getName()
              + "]. Tried to add [" + segment.getName() + "]");
    }
    segment.dropBufferForAppend();
    if (increaseUsedSegmentCount) {
      remainingUnallocatedSegments.decrementAndGet();
    }
    segmentsByName.put(segment.getName(), segment);
  }

  /**
   * Drops an existing {@link LogSegment} instance from the log.
   * @param segmentName the {@link LogSegment} instance to drop.
   * @param decreaseUsedSegmentCount {@code true} if the number of segments used has to be decremented, {@code false}
   *                                             otherwise.
   * @throws IllegalArgumentException if {@code segmentName} is not a part of the log.
   * @throws StoreException if there is any store exception while cleaning up the log segment.
   */
  void dropSegment(String segmentName, boolean decreaseUsedSegmentCount) throws StoreException {
    LogSegment segment = segmentsByName.get(segmentName);
    if (segment == null || segment == activeSegment) {
      throw new IllegalArgumentException("Segment does not exist or is the active segment: " + segmentName);
    }
    segmentsByName.remove(segmentName);
    free(segment, !decreaseUsedSegmentCount);
    if (decreaseUsedSegmentCount) {
      remainingUnallocatedSegments.incrementAndGet();
    }
  }

  /**
   * Gets the {@link FileSpan} for a message that is written starting at {@code endOffsetOfPrevMessage} and is of size
   * {@code size}. This function is safe to use only immediately after a write to the log to get the {@link FileSpan}
   * for an index entry.
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
