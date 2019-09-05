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
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents a record of the compaction process and helps in recovery in case of crashes.
 */
class CompactionLog implements Closeable {
  static final short VERSION_0 = 0;
  static final short VERSION_1 = 1;
  static final short CURRENT_VERSION = VERSION_1;
  static final String COMPACTION_LOG_SUFFIX = "_compactionLog";

  private static final byte[] ZERO_LENGTH_ARRAY = new byte[0];
  private static final long UNINITIALIZED_TIMESTAMP = -1;

  private final Logger logger = LoggerFactory.getLogger(getClass());

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
   * Creates a new compaction log.
   * @param dir the directory at which the compaction log must be created.
   * @param storeId the ID of the store.
   * @param time the {@link Time} instance to use.
   * @param compactionDetails the details about the compaction.
   * @param config the store config to use in this compaction log.
   */
  CompactionLog(String dir, String storeId, Time time, CompactionDetails compactionDetails, StoreConfig config)
      throws IOException {
    this.time = time;
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
    this.time = time;
    file = new File(dir, storeId + COMPACTION_LOG_SUFFIX);
    if (!file.exists()) {
      throw new IllegalArgumentException(file.getAbsolutePath() + " does not exist");
    }
    try (FileInputStream fileInputStream = new FileInputStream(file)) {
      CrcInputStream crcInputStream = new CrcInputStream(fileInputStream);
      DataInputStream stream = new DataInputStream(crcInputStream);
      short version = stream.readShort();
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
  void splitCurrentCycle(String nextCycleStartSegment) {
    CompactionDetails currentDetails = getCurrentCycleLog().compactionDetails;
    List<String> updatedList = new ArrayList<>();
    List<String> newList = new ArrayList<>();
    boolean encounteredSplitPoint = false;
    for (String segmentUnderCompaction : currentDetails.getLogSegmentsUnderCompaction()) {
      if (!encounteredSplitPoint && !segmentUnderCompaction.equals(nextCycleStartSegment)) {
        updatedList.add(segmentUnderCompaction);
      } else {
        encounteredSplitPoint = true;
        newList.add(segmentUnderCompaction);
      }
    }
    getCurrentCycleLog().compactionDetails = new CompactionDetails(currentDetails.getReferenceTimeMs(), updatedList);
    cycleLogs.add(new CycleLog(new CompactionDetails(currentDetails.getReferenceTimeMs(), newList)));
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
    /*
        Description of serialized format
        Version 0:
          version
          startTime
          index of current cycle's log
          size of cycle log list
          cycleLog1 (see CycleLog#toBytes())
          cycleLog2
          ...
          crc
         Version 1:
          version
          startTime
          byte to indicate whether startOffsetOfLastIndexSegmentForDeleteCheck is present (1) or not (0)
          startOffsetOfLastIndexSegmentForDeleteCheck if not null
          index of current cycle's log
          size of cycle log list
          cycleLog1 (see CycleLog#toBytes())
          cycleLog2
          ...
          crc
       */
    File tempFile = new File(file.getAbsolutePath() + ".tmp");
    try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
      CrcOutputStream crcOutputStream = new CrcOutputStream(fileOutputStream);
      DataOutputStream stream = new DataOutputStream(crcOutputStream);
      stream.writeShort(CURRENT_VERSION);
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
  }
}
