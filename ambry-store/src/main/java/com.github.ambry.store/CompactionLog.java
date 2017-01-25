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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * Represents a record of the compaction process and helps in recovery in case of crashes.
 */
class CompactionLog implements Closeable {
  private static final String COMPACTION_LOG_SUFFIX = "_compaction_log";
  private static final short VERSION_0 = 0;

  /**
   * The {@link Phase} of the current compaction cycle.
   */
  enum Phase {
    PREPARE, COPY, SWITCH, CLEANUP, DONE
  }

  private final File file;
  private final Time time;
  private final Long startTime;
  private final List<CycleLog> cycleLogs;

  private int currentIdx = 0;

  /**
   * Used to determine whether compaction is in progress.
   * @param dir the directory where the compaction log is expected to exist (if any).
   * @param name the unique name for the store under compaction.
   * @return whether compaction is in progress for the store.
   */
  static boolean isCompactionInProgress(String dir, String name) {
    return new File(dir, name + COMPACTION_LOG_SUFFIX).exists();
  }

  /**
   * Creates a new compaction log.
   * @param dir the directory at which the compaction log must be created.
   * @param name unique name of the store.
   * @param time the {@link Time} instance to use.
   * @param compactionDetailsList the details about the compaction.
   */
  CompactionLog(String dir, String name, Time time, List<CompactionDetails> compactionDetailsList) throws IOException {
    this.time = time;
    file = new File(dir, name + COMPACTION_LOG_SUFFIX);
    if (file.exists() || !file.createNewFile()) {
      throw new IllegalArgumentException(file.getAbsolutePath() + " already exists or could not be created");
    }
    startTime = time.milliseconds();
    cycleLogs = new ArrayList<>(compactionDetailsList.size());
    for (CompactionDetails details : compactionDetailsList) {
      cycleLogs.add(new CycleLog(details));
    }
    flush();
  }

  /**
   * Loads an existing compaction log.
   * @param dir the directory at which the log exists.
   * @param name unique name of the store.
   * @param storeKeyFactory the {@link StoreKeyFactory} that is used for keys in the {@link BlobStore} being compacted.
   * @param time the {@link Time} instance to use.
   */
  CompactionLog(String dir, String name, StoreKeyFactory storeKeyFactory, Time time) throws IOException {
    this.time = time;
    file = new File(dir, name + COMPACTION_LOG_SUFFIX);
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
        default:
          throw new IllegalArgumentException("Unrecognized version");
      }
    }
  }

  /**
   * @return the current phase of compaction.
   */
  Phase getCompactionPhase() {
    return currentIdx >= cycleLogs.size() ? Phase.DONE : getCurrentCycleLog().getPhase();
  }

  /**
   * @return the {@link CompactionDetails} for the compaction cycle in progress.
   */
  CompactionDetails getCompactionDetails() {
    return getCurrentCycleLog().compactionDetails;
  }

  /**
   * @return the {@link StoreFindToken} until which data has been copied and flushed.
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
  }

  /**
   * Marks the start of the switch phase.
   */
  void markSwitchStart() {
    CycleLog cycleLog = getCurrentCycleLog();
    if (!cycleLog.getPhase().equals(Phase.COPY)) {
      throw new IllegalStateException("Should be in COPY phase to transition to SWITCH phase");
    }
    cycleLog.switchStartTime = time.milliseconds();
    flush();
  }

  /**
   * Marks the start of the cleanup phase.
   */
  void markCleanupStart() {
    CycleLog cycleLog = getCurrentCycleLog();
    if (!cycleLog.getPhase().equals(Phase.SWITCH)) {
      throw new IllegalStateException("Should be in SWITCH phase to transition to CLEANUP phase");
    }
    cycleLog.cleanupStartTime = time.milliseconds();
    flush();
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

        version
        startTime
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
      stream.writeShort(VERSION_0);
      stream.writeLong(startTime);
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
  private static class CycleLog {
    private static final int TIMESTAMP_SIZE = 8;

    // details about the cycle
    final CompactionDetails compactionDetails;
    // start time of the copy phase
    long copyStartTime = -1;
    // start time of the switch phase
    long switchStartTime = -1;
    // start time of the cleanup phase
    long cleanupStartTime = -1;
    // end time of the current cycle
    long cycleEndTime = -1;
    // point until which copying is complete
    StoreFindToken safeToken = new StoreFindToken();

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
      cycleLog.switchStartTime = stream.readLong();
      cycleLog.cleanupStartTime = stream.readLong();
      cycleLog.cycleEndTime = stream.readLong();
      cycleLog.safeToken = StoreFindToken.fromBytes(stream, storeKeyFactory);
      return cycleLog;
    }

    /**
     * @return the current phase of this cycle of compaction.
     */
    Phase getPhase() {
      Phase phase;
      if (copyStartTime == -1) {
        phase = Phase.PREPARE;
      } else if (switchStartTime == -1) {
        phase = Phase.COPY;
      } else if (cleanupStartTime == -1) {
        phase = Phase.SWITCH;
      } else if (cycleEndTime == -1) {
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
        switchStartTime
        cleanupStartTime
        cycleEndTime
        safeToken (see StoreFindToken#toBytes())
       */
      byte[] compactionDetailsBytes = compactionDetails.toBytes();
      byte[] safeTokenBytes = safeToken.toBytes();
      int size = compactionDetailsBytes.length + 4 * TIMESTAMP_SIZE + safeTokenBytes.length;
      byte[] buf = new byte[size];
      ByteBuffer bufWrap = ByteBuffer.wrap(buf);
      bufWrap.put(compactionDetailsBytes);
      bufWrap.putLong(copyStartTime);
      bufWrap.putLong(switchStartTime);
      bufWrap.putLong(cleanupStartTime);
      bufWrap.putLong(cycleEndTime);
      bufWrap.put(safeTokenBytes);
      return buf;
    }
  }
}
