/*
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
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Time;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents a record of the compaction process and helps in recovery in case of crashes.
 */
class CompactionLog implements Closeable {
  static final short VERSION_0 = 0;
  static final short VERSION_1 = 1;
  static final short VERSION_2 = 2;
  static final short CURRENT_VERSION = VERSION_2;
  static final String COMPACTION_LOG_SUFFIX = "_compactionLog";

  private static final byte[] ZERO_LENGTH_ARRAY = new byte[0];
  private static final long UNINITIALIZED_TIMESTAMP = -1;

  private static final Logger logger = LoggerFactory.getLogger(CompactionLog.class);

  /**
   * The {@link Phase} of the current compaction cycle.
   */
  enum Phase {
    PREPARE, COPY, COMMIT, CLEANUP, DONE
  }

  final Long startTime;
  final List<CycleLog> cycleLogs;
  private final File file;
  private final Time time;
  private final TreeMap<Offset, Offset> beforeAndAfterIndexSegmentOffsets = new TreeMap<>();
  private final short version;
  private final StoreConfig config;

  private int currentIdx = 0;
  private Offset startOffsetOfLastIndexSegmentForDeleteCheck = null;

  /**
   * Used to determine whether compaction is in progress.
   * @param dir the directory where the compaction log is expected to exist (if any).
   * @param storeId the ID of the store under compaction.
   * @return whether compaction is in progress for the store.
   */
  static boolean isCompactionInProgress(String dir, String storeId) {
    return new File(dir, storeId + COMPACTION_LOG_SUFFIX).exists();
  }

  /**
   * The {@link CompactionLog} processor.
   */
  @FunctionalInterface
  interface CompactionLogProcessor {

    /**
     * Process a given {@link CompactionLog}.
     * @param log The {@link CompactionLog} to process.
     * @return True if you want to process more compaction log.
     */
    boolean process(CompactionLog log);
  }

  /**
   * Process all the available {@link CompactionLog}s in descending order. The CompactionLog created most recently would
   * be processed first.
   * @param dir The data dir that contains all the compaction log files.
   * @param storeId The store id.
   * @param storeKeyFactory The {@link StoreKeyFactory}.
   * @param time The time instance.
   * @param config The {@link StoreConfig} object.
   * @param processor The {@link CompactionLogProcessor}.
   * @throws IOException
   */
  static void processCompactionLogs(String dir, String storeId, StoreKeyFactory storeKeyFactory, Time time,
      StoreConfig config, CompactionLogProcessor processor) throws IOException {
    // Ignore current in process compaction.
    List<File> sortedFiles = getCompactedLogFilesInDescendingOrder(dir, storeId);
    File inProgressCompactionLog = new File(dir, storeId + COMPACTION_LOG_SUFFIX);
    if (inProgressCompactionLog.exists()) {
      sortedFiles.add(0, inProgressCompactionLog); // If there is an in progress compaction log, put it in the front.
    }
    for (File file : sortedFiles) {
      if (!processor.process(new CompactionLog(file, storeKeyFactory, time, config))) {
        break;
      }
    }
  }

  /**
   * Remove all the compaction files that are started before the given {@code beforeTime}.
   * @param dir The data dir that contains all the compaction log files.
   * @param storeId The store id.
   * @param beforeTime The cutoff time. Compaction log file started before this time will be removed.
   */
  static void cleanupCompactionLogs(String dir, String storeId, long beforeTime) {
    List<File> sortedFiles = getCompactedLogFilesInDescendingOrder(dir, storeId);
    if (sortedFiles.isEmpty()) {
      logger.info("{}: No compaction log to cleanup", storeId);
      return;
    }
    // The list is in descending order, the most recent compaction logs are in the front, we have to reverse the list
    for (int i = sortedFiles.size() - 1; i >= 0; i--) {
      File compactionFile = sortedFiles.get(i);
      long startTime = getStartTimeFromFile(compactionFile);
      if (startTime >= beforeTime) {
        break;
      } else {
        logger.info("{}: Removing compaction file: {}, cutoff time: {}", storeId, compactionFile.getName(), beforeTime);
        compactionFile.delete();
      }
    }
  }

  /**
   * Get all the compaction file (not including inprogress compaction file) in descending order based on the start time
   * @param dir The data dir that contains all the compaction log files.
   * @param storeId The store id.
   * @return A list of {@link File}s that are sorted at descending order
   */
  static List<File> getCompactedLogFilesInDescendingOrder(String dir, String storeId) {
    File storeDir = new File(dir);
    File[] files =
        storeDir.listFiles((fileDir, name) -> name.startsWith(storeId + COMPACTION_LOG_SUFFIX + BlobStore.SEPARATOR));
    List<File> sortedFiles = Stream.of(files)
        .sorted((file1, file2) -> getStartTimeFromFile(file2) - getStartTimeFromFile(file1) > 0 ? 1 : -1)
        .collect(Collectors.toList());
    return sortedFiles;
  }

  /**
   * Getting start time from the compaction log file.
   * @param file The compaction log file.
   * @return The start time in milliseconds.
   */
  static long getStartTimeFromFile(File file) {
    return Long.parseLong(file.getName().split(BlobStore.SEPARATOR)[2]);
  }

  /**
   * Creates a new compaction log.
   * @param dir the directory at which the compaction log must be created.
   * @param storeId the ID of the store.
   * @param time the {@link Time} instance to use.
   * @param compactionDetails the details about the compaction.
   * @param config the store config to use in this compaction log.
   */
  CompactionLog(String dir, String storeId, Time time, CompactionDetails compactionDetails, StoreConfig config)
      throws IOException {
    this(dir, storeId, CURRENT_VERSION, time, compactionDetails, config);
  }

  /**
   * Creates a new compaction log, with custom version. This should only be used in test.
   * @param dir the directory at which the compaction log must be created.
   * @param storeId the ID of the store.
   * @param version the version number.
   * @param time the {@link Time} instance to use.
   * @param compactionDetails the details about the compaction.
   * @param config the store config to use in this compaction log.
   */
  CompactionLog(String dir, String storeId, short version, Time time, CompactionDetails compactionDetails,
      StoreConfig config) throws IOException {
    this.time = time;
    if (version < VERSION_0 || version > VERSION_2) {
      throw new IllegalArgumentException("Invalid version number: " + version);
    }
    this.version = version;
    file = new File(dir, storeId + COMPACTION_LOG_SUFFIX);
    if (!file.createNewFile()) {
      throw new IllegalArgumentException(file.getAbsolutePath() + " already exists");
    }
    startTime = time.milliseconds();
    cycleLogs = new ArrayList<>();
    cycleLogs.add(new CycleLog(compactionDetails));
    flush();
    if (config.storeSetFilePermissionEnabled) {
      Files.setPosixFilePermissions(file.toPath(), config.storeOperationFilePermission);
    }
    this.config = config;
    logger.trace("Created compaction log: {}", file);
  }

  /**
   * Loads an existing compaction log.
   * @param dir the directory at which the log exists.
   * @param storeId the ID of the store.
   * @param storeKeyFactory the {@link StoreKeyFactory} that is used for keys in the {@link BlobStore} being compacted.
   * @param time the {@link Time} instance to use.
   * @param config the store config to use in this compaction log.
   */
  CompactionLog(String dir, String storeId, StoreKeyFactory storeKeyFactory, Time time, StoreConfig config)
      throws IOException {
    this(new File(dir, storeId + COMPACTION_LOG_SUFFIX), storeKeyFactory, time, config);
  }

  /**
   * Loads an existing compaction log from {@link File}.
   * @param compactionLogFile The compaction log {@link File}.
   * @param storeKeyFactory The {@link StoreKeyFactory} that is used for keys in the {@link BlobStore} being compacted.
   * @param time The {@link Time} instance to use.
   * @param config The store config to use in this compaction log.
   * @throws IOException
   */
  CompactionLog(File compactionLogFile, StoreKeyFactory storeKeyFactory, Time time, StoreConfig config)
      throws IOException {
    if (!compactionLogFile.exists()) {
      throw new IllegalArgumentException(compactionLogFile.getAbsolutePath() + " does not exist");
    }
    this.file = compactionLogFile;
    this.time = time;
    this.config = config;
    try (FileInputStream fileInputStream = new FileInputStream(file)) {
      CrcInputStream crcInputStream = new CrcInputStream(fileInputStream);
      DataInputStream stream = new DataInputStream(crcInputStream);
      this.version = stream.readShort();
      switch (version) {
        case VERSION_0:
          startTime = stream.readLong();
          currentIdx = stream.readInt();
          int cycleLogsSize = stream.readInt();
          cycleLogs = new ArrayList<>(cycleLogsSize);
          while (cycleLogs.size() < cycleLogsSize) {
            cycleLogs.add(CycleLog.fromBytes(stream, storeKeyFactory));
          }
          long crc = crcInputStream.getValue();
          if (crc != stream.readLong()) {
            throw new IllegalStateException("CRC of data read does not match CRC in file");
          }
          break;
        case VERSION_1:
        case VERSION_2:
          startTime = stream.readLong();
          if (stream.readByte() == (byte) 1) {
            startOffsetOfLastIndexSegmentForDeleteCheck = Offset.fromBytes(stream);
          }
          currentIdx = stream.readInt();
          cycleLogsSize = stream.readInt();
          cycleLogs = new ArrayList<>(cycleLogsSize);
          while (cycleLogs.size() < cycleLogsSize) {
            cycleLogs.add(CycleLog.fromBytes(stream, storeKeyFactory));
          }
          if (version == VERSION_2) {
            int mapSize = stream.readInt();
            for (int i = 0; i < mapSize; i++) {
              Offset before = Offset.fromBytes(stream);
              Offset after = Offset.fromBytes(stream);
              beforeAndAfterIndexSegmentOffsets.put(before, after);
            }
          }
          crc = crcInputStream.getValue();
          if (crc != stream.readLong()) {
            throw new IllegalStateException("CRC of data read does not match CRC in file");
          }
          break;
        default:
          throw new IllegalArgumentException("Unrecognized version");
      }
      if (config.storeSetFilePermissionEnabled) {
        Files.setPosixFilePermissions(file.toPath(), config.storeOperationFilePermission);
      }
      logger.trace("Loaded compaction log: {}", file);
    }
  }

  long getStartTime() {
    return startTime;
  }

  /**
   * @return the current phase of compaction.
   */
  Phase getCompactionPhase() {
    return currentIdx >= cycleLogs.size() ? Phase.DONE : getCurrentCycleLog().getPhase();
  }

  /**
   * @return the index of the {@link CompactionDetails} being provided.
   */
  int getCurrentIdx() {
    return currentIdx < cycleLogs.size() ? currentIdx : -1;
  }

  /**
   * @return the {@link CompactionDetails} for the compaction cycle in progress.
   */
  CompactionDetails getCompactionDetails() {
    return getCurrentCycleLog().compactionDetails;
  }

  /**
   * @return the {@link StoreFindToken} until which data has been copied and flushed. Returns {@code null} if nothing
   * has been set yet.
   */
  StoreFindToken getSafeToken() {
    return getCurrentCycleLog().safeToken;
  }

  /**
   * Sets the {@link StoreFindToken} until which data is copied and flushed.
   * @param safeToken the {@link StoreFindToken} until which data is copied and flushed.
   */
  void setSafeToken(StoreFindToken safeToken) {
    CycleLog cycleLog = getCurrentCycleLog();
    if (!cycleLog.getPhase().equals(Phase.COPY)) {
      throw new IllegalStateException("Cannot set a safe token - not in COPY phase");
    }
    cycleLog.safeToken = safeToken;
    flush();
    logger.trace("{}: Set safe token to {} during compaction of {}", file, cycleLog.safeToken,
        cycleLog.compactionDetails);
  }

  /**
   * Add a pair of index segment start offsets. The first offset has to be an index segment start offset that belongs to
   * an under compaction log segment. The second offset hast to be an index segment start offset that belongs to a log
   * segment created this compaction.
   * @param before The index segment start offset before compaction
   * @param after The index segment start offset after compaction
   */
  void addBeforeAndAfterIndexSegmentOffsetPair(Offset before, Offset after) {
    if (!isIndexSegmentOffsetsPersisted()) {
      return;
    }
    // If the before offset already exists, it means the index segment has already been copied, or half way through
    // copying. Either way, we should keep the original after offset, since the after offset should reference to the
    // first index segment that has data from before index segment.
    if (beforeAndAfterIndexSegmentOffsets.containsKey(before)) {
      logger.trace("{}: Offset {} already exist in the map", file, before);
      return;
    }
    LogSegmentName logSegmentName = before.getName();
    if (!getCompactionDetails().getLogSegmentsUnderCompaction().contains(logSegmentName)) {
      throw new IllegalArgumentException("Offset is not part of the under compaction log segments: " + before);
    }
    // TODO: Can we find a way to validate the after offset?
    beforeAndAfterIndexSegmentOffsets.put(before, after);
    logger.trace("{}: Add offsets pair to {}: {}", file, before, after);
    flush();
  }

  /**
   * Update all the index segment offset values added through method {@link #addBeforeAndAfterIndexSegmentOffsetPair}. If the input is
   * null, the index segment offsets map would be cleared.
   * @param validOffset The valid {@link Offset} to replace all the previous invalid {@link Offset}.
   */
  void updateBeforeAndAfterIndexSegmentOffsetsWithValidOffset(Offset validOffset) {
    if (!isIndexSegmentOffsetsPersisted()) {
      return;
    }
    logger.info("{}: Validate offsets map with: {}", file, validOffset);
    if (validOffset == null) {
      beforeAndAfterIndexSegmentOffsets.clear();
    } else {
      for (Offset before : beforeAndAfterIndexSegmentOffsets.keySet()) {
        beforeAndAfterIndexSegmentOffsets.put(before, validOffset);
      }
    }
    flush();
  }

  /**
   * @return The index segment offset map. Each key and value is before and after pair passed to {@link #addBeforeAndAfterIndexSegmentOffsetPair}.
   */
  NavigableMap<Offset, Offset> getBeforeAndAfterIndexSegmentOffsets() {
    return Collections.unmodifiableNavigableMap(beforeAndAfterIndexSegmentOffsets);
  }

  /**
   * Return index segment offset map for completed cycles. Each key offset's LogSegmentName has to finish the compaction.
   */
  SortedMap<Offset, Offset> getBeforeAndAfterIndexSegmentOffsetsForCompletedCycles() {
    if (beforeAndAfterIndexSegmentOffsets.size() == 0 || currentIdx >= cycleLogs.size()) {
      return beforeAndAfterIndexSegmentOffsets;
    }

    if (currentIdx == 0) {
      return new TreeMap<>();
    }

    List<CycleLog> cycles = cycleLogs.subList(0, currentIdx);
    List<LogSegmentName> completedLogSegmentNames = cycles.stream()
        .flatMap(cycleLog -> cycleLog.compactionDetails.getLogSegmentsUnderCompaction().stream())
        .collect(Collectors.toList());
    return getBeforeAndAfterIndexSegmentOffsetsForLogSegments(completedLogSegmentNames);
  }

  /**
   * Return index segment offset map for current cycle. Each key offset's LogSegmentName has to be in current cycle.
   */
  SortedMap<Offset, Offset> getBeforeAndAfterIndexSegmentOffsetsForCurrentCycle() {
    if (currentIdx == -1) {
      throw new IllegalArgumentException(file.getName() + "Current Index is -1, no current cycle");
    }
    return getBeforeAndAfterIndexSegmentOffsetsForLogSegments(
        getCurrentCycleLog().compactionDetails.getLogSegmentsUnderCompaction());
  }

  private SortedMap<Offset, Offset> getBeforeAndAfterIndexSegmentOffsetsForLogSegments(
      List<LogSegmentName> logSegmentNames) {
    if (logSegmentNames.size() == 0) {
      return new TreeMap<>();
    }

    Offset start = new Offset(logSegmentNames.get(0), 0);
    Offset end = new Offset(logSegmentNames.get(logSegmentNames.size() - 1), config.storeSegmentSizeInBytes);
    return beforeAndAfterIndexSegmentOffsets.subMap(start, end);
  }

  /**
   * True when the {@link CompactionLog} persists index segment offset. Starting from version 2, compaction log should
   * persist index segment offsets.
   * @return
   */
  boolean isIndexSegmentOffsetsPersisted() {
    return version >= VERSION_2;
  }

  /**
   * @return the start {@link Offset} of the last index segment for checking deletes. This is initially {@code null} and
   * has to be set.
   */
  Offset getStartOffsetOfLastIndexSegmentForDeleteCheck() {
    return startOffsetOfLastIndexSegmentForDeleteCheck;
  }

  /**
   * Sets the start {@link Offset} of the last index segment for checking deletes.
   * @param startOffsetOfLastIndexSegmentForDeleteCheck he start {@link Offset} of the last index segment for checking
   *                                                    deletes.
   */
  void setStartOffsetOfLastIndexSegmentForDeleteCheck(Offset startOffsetOfLastIndexSegmentForDeleteCheck) {
    this.startOffsetOfLastIndexSegmentForDeleteCheck = startOffsetOfLastIndexSegmentForDeleteCheck;
    flush();
    logger.trace("{}: Set startOffsetOfLastIndexSegmentForDeleteCheck to {}", file,
        this.startOffsetOfLastIndexSegmentForDeleteCheck);
  }

  /**
   * Marks the start of the copy phase.
   */
  void markCopyStart() {
    CycleLog cycleLog = getCurrentCycleLog();
    if (!cycleLog.getPhase().equals(Phase.PREPARE)) {
      throw new IllegalStateException("Should be in PREPARE phase to transition to COPY phase");
    }
    cycleLog.copyStartTime = time.milliseconds();
    flush();
    logger.trace("{}: Marked copy as started for {}", file, cycleLog.compactionDetails);
  }

  /**
   * Splits the current cycle at {@code nextCycleStartSegment}. This means that a new next cycle will be created that
   * starts at {@code nextCycleStartSegment} and ends at the end segment of the current cycle and the new current cycle
   * starts at the first segment in the current cycle and ends at the segment just before {@code nextCycleStartSegment}.
   * For e.g if the current cycle is 0_1.log,0_2.log,0_3.log,0_4.log,0_5.log and {@code nextCycleStartSegment} is
   * 0_4.log, the new next cycle will be 0_4.log,0_5.log and the new current cycle will be 0_1.log,0_2.log,0_3.log.
   * {@link CompactionLog}
   * @param nextCycleStartSegment the segment to split the current cycle at.
   */
  void splitCurrentCycle(LogSegmentName nextCycleStartSegment) {
    CompactionDetails currentDetails = getCurrentCycleLog().compactionDetails;
    List<LogSegmentName> updatedList = new ArrayList<>();
    List<LogSegmentName> newList = new ArrayList<>();
    boolean encounteredSplitPoint = false;
    for (LogSegmentName segmentUnderCompaction : currentDetails.getLogSegmentsUnderCompaction()) {
      if (!encounteredSplitPoint && !segmentUnderCompaction.equals(nextCycleStartSegment)) {
        updatedList.add(segmentUnderCompaction);
      } else {
        encounteredSplitPoint = true;
        newList.add(segmentUnderCompaction);
      }
    }
    getCurrentCycleLog().compactionDetails =
        new CompactionDetails(currentDetails.getReferenceTimeMs(), updatedList, null);
    cycleLogs.add(new CycleLog(new CompactionDetails(currentDetails.getReferenceTimeMs(), newList, null)));
    flush();
    logger.trace("{}: Split current cycle into two lists: {} and {}", file, updatedList, newList);
  }

  /**
   * Marks the start of the commit phase.
   */
  void markCommitStart() {
    CycleLog cycleLog = getCurrentCycleLog();
    if (!cycleLog.getPhase().equals(Phase.COPY)) {
      throw new IllegalStateException("Should be in COPY phase to transition to SWITCH phase");
    }
    cycleLog.commitStartTime = time.milliseconds();
    flush();
    logger.trace("{}: Marked commit as started for {}", file, cycleLog.compactionDetails);
  }

  /**
   * Marks the start of the cleanup phase.
   */
  void markCleanupStart() {
    CycleLog cycleLog = getCurrentCycleLog();
    if (!cycleLog.getPhase().equals(Phase.COMMIT)) {
      throw new IllegalStateException("Should be in SWITCH phase to transition to CLEANUP phase");
    }
    cycleLog.cleanupStartTime = time.milliseconds();
    flush();
    logger.trace("{}: Marked cleanup as started for {}", file, cycleLog.compactionDetails);
  }

  /**
   * Marks the current compaction cycle as complete.
   */
  void markCycleComplete() {
    CycleLog cycleLog = getCurrentCycleLog();
    if (!cycleLog.getPhase().equals(Phase.CLEANUP)) {
      throw new IllegalStateException("Should be in CLEANUP phase to complete cycle");
    }
    cycleLog.cycleEndTime = time.milliseconds();
    currentIdx++;
    flush();
    logger.trace("{}: Marked cycle as complete for {}", file, cycleLog.compactionDetails);
  }

  /**
   * Closes the compaction log. If compaction is complete, renames the log to keep a permanent record.
   */
  @Override
  public void close() {
    if (file.exists() && getCompactionPhase().equals(Phase.DONE)) {
      String dateString = new Date(startTime).toString();
      File savedLog =
          new File(file.getAbsolutePath() + BlobStore.SEPARATOR + startTime + BlobStore.SEPARATOR + dateString);
      if (!file.renameTo(savedLog)) {
        throw new IllegalStateException("Compaction log could not be renamed after completion of compaction");
      }
    }
  }

  /**
   * @return the {@link CycleLog} for the current compaction cycle.
   */
  private CycleLog getCurrentCycleLog() {
    if (currentIdx >= cycleLogs.size()) {
      throw new IllegalStateException("Operation not possible because there are no more compaction cycles left");
    }
    return cycleLogs.get(currentIdx);
  }

  /**
   * Flushes all changes to the file backing this compaction log.
   */
  private void flush() {
    File tempFile = new File(file.getAbsolutePath() + ".tmp");
    try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
      CrcOutputStream crcOutputStream = new CrcOutputStream(fileOutputStream);
      DataOutputStream stream = new DataOutputStream(crcOutputStream);
      stream.writeShort(version); // If we read this log from file, then keep this the same version.
      switch (version) {
        case VERSION_0:
          flushVersion0(stream);
          break;
        case VERSION_1:
        case VERSION_2:
          flushVersion1And2(stream);
          break;
      }
      stream.writeLong(crcOutputStream.getValue());
      fileOutputStream.getChannel().force(true);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    if (!tempFile.renameTo(file)) {
      throw new IllegalStateException("Newly written compaction log could not be saved");
    }
  }

  /**
   * Flush the CompactionLog following the version 0 format.
   * Version 0:
   *    version
   *    startTime
   *    index of current cycle's log
   *    size of cycle log list
   *    cycleLog1 (see CycleLog#toBytes())
   *    cycleLog2
   *    ...
   *    crc
   * @param stream The output stream to write bytes to
   * @throws IOException
   */
  private void flushVersion0(DataOutputStream stream) throws IOException {
    stream.writeLong(startTime);
    stream.writeInt(currentIdx);
    stream.writeInt(cycleLogs.size());
    for (CycleLog cycleLog : cycleLogs) {
      stream.write(cycleLog.toBytes());
    }
  }

  /**
   * Flush the CompactionLog following the version 1 or 2 format.
   * Version 1:
   *     version
   *     startTime
   *     byte to indicate whether startOffsetOfLastIndexSegmentForDeleteCheck is present (1) or not (0)
   *     startOffsetOfLastIndexSegmentForDeleteCheck if not null
   *     index of current cycle's log
   *     size of cycle log list
   *     cycleLog1 (see CycleLog#toBytes())
   *     cycleLog2
   *     ...
   *     crc
   *
   * Version 2:
   *     version
   *     startTime
   *     byte to indicate whether startOffsetOfLastIndexSegmentForDeleteCheck is present (1) or not (0)
   *     startOffsetOfLastIndexSegmentForDeleteCheck if not null
   *     index of current cycle's log
   *     size of cycle log list
   *     cycleLog1 (see CycleLog#toBytes())
   *     cycleLog2
   *     ...
   *     size of beforeAndAfterIndexSegmentOffsetsMap
   *     beforeOffset1, afterOffset1
   *     beforeOffset2, afterOffset2
   *     ...
   *     crc
   * @param stream The output stream to write bytes to
   * @throws IOException
   */
  private void flushVersion1And2(DataOutputStream stream) throws IOException {
    stream.writeLong(startTime);
    if (startOffsetOfLastIndexSegmentForDeleteCheck == null) {
      stream.writeByte(0);
    } else {
      stream.writeByte(1);
      stream.write(startOffsetOfLastIndexSegmentForDeleteCheck.toBytes());
    }
    stream.writeInt(currentIdx);
    stream.writeInt(cycleLogs.size());
    for (CycleLog cycleLog : cycleLogs) {
      stream.write(cycleLog.toBytes());
    }
    if (version >= VERSION_2) {
      stream.writeInt(beforeAndAfterIndexSegmentOffsets.size());
      // BeforeAndAfterIndexSegmentOffsets is a tree map that would return offset in order
      for (Map.Entry<Offset, Offset> entry : beforeAndAfterIndexSegmentOffsets.entrySet()) {
        stream.write(entry.getKey().toBytes());
        stream.write(entry.getValue().toBytes());
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Version:")
        .append(CURRENT_VERSION)
        .append("\n")
        .append("StartTime:")
        .append(startTime)
        .append("\n")
        .append("StartOffsetOfLastIndexSegmentForDeleteCheck");
    if (startOffsetOfLastIndexSegmentForDeleteCheck == null) {
      sb.append("null");
    } else {
      sb.append(startOffsetOfLastIndexSegmentForDeleteCheck.toString());
    }
    sb.append("\n");
    sb.append("CurrentIndex:")
        .append(currentIdx)
        .append("\n")
        .append("CycleLogSize:")
        .append(cycleLogs.size())
        .append("\n")
        .append("CycleLogs:");
    for (CycleLog cycleLog : cycleLogs) {
      sb.append(cycleLog.toString()).append("\n\n");
    }
    return sb.toString();
  }

  /**
   * Details and log for a single compaction cycle.
   */
  static class CycleLog {
    private static final int TIMESTAMP_SIZE = 8;
    private static final int STORE_TOKEN_PRESENT_FLAG_SIZE = 1;

    private static final byte STORE_TOKEN_PRESENT = 1;
    private static final byte STORE_TOKEN_ABSENT = 0;

    // details about the cycle
    CompactionDetails compactionDetails;
    // start time of the copy phase
    long copyStartTime = UNINITIALIZED_TIMESTAMP;
    // start time of the commit phase
    long commitStartTime = UNINITIALIZED_TIMESTAMP;
    // start time of the cleanup phase
    long cleanupStartTime = UNINITIALIZED_TIMESTAMP;
    // end time of the current cycle
    long cycleEndTime = UNINITIALIZED_TIMESTAMP;
    // point until which copying is complete
    StoreFindToken safeToken = null;

    /**
     * Create a log for a single cycle of compaction.
     * @param compactionDetails the details for the compaction cycle.
     */
    CycleLog(CompactionDetails compactionDetails) {
      this.compactionDetails = compactionDetails;
    }

    /**
     * Create a log for a compaction cycle from a {@code stream}.
     * @param stream the {@link DataInputStream} that represents the serialized object.
     * @param storeKeyFactory the {@link StoreKeyFactory} used to generate the {@link StoreFindToken}.
     * @return a {@link CycleLog} that represents a cycle.
     * @throws IOException if there is an I/O error while reading the stream.
     */
    static CycleLog fromBytes(DataInputStream stream, StoreKeyFactory storeKeyFactory) throws IOException {
      CompactionDetails compactionDetails = CompactionDetails.fromBytes(stream);
      CycleLog cycleLog = new CycleLog(compactionDetails);
      cycleLog.copyStartTime = stream.readLong();
      cycleLog.commitStartTime = stream.readLong();
      cycleLog.cleanupStartTime = stream.readLong();
      cycleLog.cycleEndTime = stream.readLong();
      cycleLog.safeToken =
          stream.readByte() == STORE_TOKEN_PRESENT ? StoreFindToken.fromBytes(stream, storeKeyFactory) : null;
      return cycleLog;
    }

    /**
     * @return the current phase of this cycle of compaction.
     */
    Phase getPhase() {
      Phase phase;
      if (copyStartTime == UNINITIALIZED_TIMESTAMP) {
        phase = Phase.PREPARE;
      } else if (commitStartTime == UNINITIALIZED_TIMESTAMP) {
        phase = Phase.COPY;
      } else if (cleanupStartTime == UNINITIALIZED_TIMESTAMP) {
        phase = Phase.COMMIT;
      } else if (cycleEndTime == UNINITIALIZED_TIMESTAMP) {
        phase = Phase.CLEANUP;
      } else {
        phase = Phase.DONE;
      }
      return phase;
    }

    /**
     * @return serialized version of the {@link CycleLog}.
     */
    byte[] toBytes() {
      /*
        Description of serialized format

        compactionDetails (see CompactionDetails#toBytes())
        copyStartTime
        commitStartTime
        cleanupStartTime
        cycleEndTime
        storeTokenPresent flag
        safeToken if not null (see StoreFindToken#toBytes())
       */
      byte[] compactionDetailsBytes = compactionDetails.toBytes();
      byte[] safeTokenBytes = safeToken != null ? safeToken.toBytes() : ZERO_LENGTH_ARRAY;
      int size =
          compactionDetailsBytes.length + 4 * TIMESTAMP_SIZE + STORE_TOKEN_PRESENT_FLAG_SIZE + safeTokenBytes.length;
      byte[] buf = new byte[size];
      ByteBuffer bufWrap = ByteBuffer.wrap(buf);
      bufWrap.put(compactionDetailsBytes);
      bufWrap.putLong(copyStartTime);
      bufWrap.putLong(commitStartTime);
      bufWrap.putLong(cleanupStartTime);
      bufWrap.putLong(cycleEndTime);
      bufWrap.put(safeToken != null ? STORE_TOKEN_PRESENT : STORE_TOKEN_ABSENT);
      bufWrap.put(safeTokenBytes);
      return buf;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("CompactionDetails:")
          .append(compactionDetails.toString())
          .append("\n")
          .append("CopyStartTime:")
          .append(copyStartTime)
          .append("\n")
          .append("CommitStartTime:")
          .append(commitStartTime)
          .append("\n")
          .append("CleanupStartTime:")
          .append(cleanupStartTime)
          .append("\n")
          .append("CycleEndTime:")
          .append(cycleEndTime)
          .append("\n");
      if (safeToken != null) {
        sb.append("SafeToken:").append(safeToken.toString()).append("\n");
      }
      return sb.toString();
    }
  }
}
