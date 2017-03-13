/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link BlobStoreCompactor}.
 */
public class BlobStoreCompactorTest {

  private static final String STORE_ID = "compactor_example_store";
  private static final DiskIOScheduler DISK_IO_SCHEDULER = new DiskIOScheduler(null);
  private static final String EXCEPTION_MSG = UtilsTest.getRandomString(10);

  private final File tempDir;
  private final String tempDirStr;

  private CuratedLogIndexState state = null;
  private BlobStoreCompactor compactor = null;

  // indicates whether any of InterruptionInducers induced the shutdown/crash.
  private boolean shutdownOrExceptionInduced = false;
  // for InterruptionInducingLog and InterruptionInducingIndex, an exception is thrown after the operation
  // if throwExceptionBeforeOperation is not true.
  private boolean throwExceptionInsteadOfShutdown = false;
  // not applicable to the InterruptionInducingDiskIOScheduler. Throws in InterruptionInducingLog and
  // InterruptionInducingIndex irrespective of the value of throwExceptionInsteadOfShutdown
  private boolean throwExceptionBeforeOperation = false;

  /**
   * Creates a temporary directory for the store.
   * @throws Exception
   */
  public BlobStoreCompactorTest() throws Exception {
    tempDir = StoreTestUtils.createTempDirectory("compactorDir-" + UtilsTest.getRandomString(10));
    tempDirStr = tempDir.getAbsolutePath();
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws Exception
   */
  @After
  public void cleanup() throws Exception {
    destroyStateAndCleanDir();
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Tests basic startup/shutdown.
   * @throws Exception
   */
  @Test
  public void startShutdownTest() throws Exception {
    refreshState(false, true);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.start(state.index);
    compactor.shutdown(0);
  }

  /**
   * Tests shutting down without starting up.
   * @throws Exception
   */
  @Test
  public void shutdownWithoutStartTest() throws Exception {
    refreshState(false, true);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.shutdown(0);
  }

  /**
   * Tests that attempt to use the service without starting the service.
   * @throws Exception
   */
  @Test
  public void useServiceWithoutStartTest() throws Exception {
    refreshState(false, true);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    String firstSegmentName = state.log.getFirstSegment().getName();
    CompactionDetails details =
        new CompactionDetails(state.time.milliseconds(), Collections.singletonList(firstSegmentName));

    try {
      compactor.compact(details);
      fail("Should have failed to do anything because compactor has not been started");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }

    // create compaction log so that resumeCompaction() thinks there is a compaction in progress
    try (CompactionLog cLog = new CompactionLog(tempDirStr, STORE_ID, state.time, details)) {
      compactor.resumeCompaction();
      fail("Should have failed to do anything because compactor has not been started");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests APIs with bad input and ensures that they fail fast.
   * @throws Exception
   */
  @Test
  public void badInputTest() throws Exception {
    refreshState(false, true);
    // compaction range contains a log segment that is still in the journal
    String lastLogSegmentName = state.referenceIndex.lastKey().getName();
    CompactionDetails details =
        new CompactionDetails(state.time.milliseconds() + Time.MsPerSec, Collections.singletonList(lastLogSegmentName));
    ensureArgumentFailure(details, "Should have failed because compaction range contains offsets still in the journal");

    // compaction contains segments that don't exist
    details = new CompactionDetails(0,
        Collections.singletonList(LogSegmentNameHelper.getNextPositionName(lastLogSegmentName)));
    ensureArgumentFailure(details, "Should have failed because compaction range contains offsets still in the journal");
  }

  /**
   * Tests the case where {@link BlobStoreCompactor#resumeCompaction()} is called without any compaction being in
   * progress.
   * @throws Exception
   */
  @Test
  public void resumeCompactionWithoutAnyInProgressTest() throws Exception {
    refreshState(false, true);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.start(state.index);
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID));
    try {
      compactor.resumeCompaction();
      fail("Should have failed because there is no compaction in progress");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    } finally {
      compactor.shutdown(0);
    }
  }

  /**
   * A basic test for compaction that selects the first two log segments and compacts them into one log segment.
   * @throws Exception
   */
  @Test
  public void basicTest() throws Exception {
    refreshState(false, true);
    List<String> segmentsUnderCompaction = getLogSegments(0, 2);
    reduceValidDataSizeInLogSegments(segmentsUnderCompaction, state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE);
    long deleteReferenceTimeMs = state.time.milliseconds();
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, true);
  }

  /**
   * Compacts the whole log (except the last log segment) but without any changes expected i.e all data is valid and is
   * simply copied over from the old log segments to the new log segments.
   * @throws Exception
   */
  @Test
  public void compactWholeLogWithNoChangeExpectedTest() throws Exception {
    long delayBeforeLastLogSegmentWrite = 20 * Time.MsPerSec;
    refreshState(false, false);
    // write data until the very last segment is reached
    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity();
    // write entries with an expiry time such that no records are actually expired at the time of compaction
    long expiryTimeMs = getInvalidationTime(requiredCount) + delayBeforeLastLogSegmentWrite;
    writeDataToMeetRequiredSegmentCount(requiredCount - 1, Collections.singletonList(expiryTimeMs));
    state.advanceTime(delayBeforeLastLogSegmentWrite);
    writeDataToMeetRequiredSegmentCount(requiredCount, Collections.singletonList(expiryTimeMs));
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    long deleteReferenceTimeMs = state.time.milliseconds();
    List<String> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, false);
  }

  /**
   * Compacts the whole log (except the last log segment) and a changed size is expected i.e. there is some invalid
   * data.
   * @throws Exception
   */
  @Test
  public void compactWholeLogWithChangeExpectedTest() throws Exception {
    refreshState(false, true);
    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity() - 2;
    writeDataToMeetRequiredSegmentCount(requiredCount,
        Arrays.asList(state.time.milliseconds() / 2, state.time.milliseconds(), state.time.milliseconds() * 2));
    // do some random deleting.
    int deleteCount = Math.min(state.liveKeys.size() / 3,
        (int) (1.8 * state.log.getSegmentCapacity() / CuratedLogIndexState.DELETE_RECORD_SIZE));
    List<MockId> allLiveKeys = new ArrayList<>(state.liveKeys);
    for (int i = 0; i < deleteCount; i++) {
      MockId idToDelete = allLiveKeys.remove(TestUtils.RANDOM.nextInt(allLiveKeys.size()));
      state.addDeleteEntry(idToDelete);
    }
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    long deleteReferenceTimeMs = state.time.milliseconds();
    List<String> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, true);
  }

  /**
   * Compacts the whole log (except the last log segment) and a changed size is expected i.e. there is some invalid
   * data. All this is done with hard delete enabled (compactor is expected to pause it).
   * @throws Exception
   */
  @Test
  public void compactWholeLogWithHardDeleteEnabledTest() throws Exception {
    // no interruptions
    doCompactWholeLogWithHardDeleteEnabledTest(false, false);
    // shutdown in the middle of copying
    doCompactWholeLogWithHardDeleteEnabledTest(true, true);
    // crash in the middle of copying
    throwExceptionInsteadOfShutdown = true;
    doCompactWholeLogWithHardDeleteEnabledTest(true, true);
    // crash in the middle of commit
    doCompactWholeLogWithHardDeleteEnabledTest(true, false);
  }

  /**
   * Tests the case where there is no valid data at all in the segments under compaction and they are essentially
   * dropped.
   * @throws Exception
   */
  @Test
  public void dropAllSegmentsUnderCompactionTest() throws Exception {
    Pair<Long, List<String>> deleteTimeAndSegmentsUnderCompaction = setupStateWithDeletedBlobsAtSpecificTime();
    List<String> segmentsUnderCompaction = deleteTimeAndSegmentsUnderCompaction.getSecond();
    // delete all the blobs in the segments under compaction
    Set<MockId> ids = getIdsInSegments(segmentsUnderCompaction);
    for (MockId id : ids) {
      if (state.liveKeys.contains(id)) {
        state.addDeleteEntry(id);
      }
    }
    long deleteReferenceTimeMs = state.time.milliseconds() + Time.MsPerSec;
    state.advanceTime(deleteReferenceTimeMs - state.time.milliseconds());
    assertEquals("Valid size in the segments under compaction should be 0", 0,
        getValidDataSize(segmentsUnderCompaction, deleteReferenceTimeMs));
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, true);
  }

  /**
   * Tests the case where expiration time is enforced i.e. data is considered valid before expiry time and is copied
   * over and data is considered invalid after expiry time and is not copied over.
   * @throws Exception
   */
  @Test
  public void expirationTimeEnforcementTest() throws Exception {
    // no change before expiry time
    Pair<Long, List<String>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    Map<String, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(expiryTimeAndSegmentsUnderCompaction.getSecond());
    compactAndVerify(expiryTimeAndSegmentsUnderCompaction.getSecond(), state.time.milliseconds(), false);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

    // no change at expiry time.
    expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    oldSegmentNamesAndEndOffsets = getEndOffsets(expiryTimeAndSegmentsUnderCompaction.getSecond());
    state.advanceTime(expiryTimeAndSegmentsUnderCompaction.getFirst() - state.time.milliseconds());
    compactAndVerify(expiryTimeAndSegmentsUnderCompaction.getSecond(), state.time.milliseconds(), false);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

    // there will be changes past expiration time
    expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    state.advanceTime(expiryTimeAndSegmentsUnderCompaction.getFirst() + Time.MsPerSec - state.time.milliseconds());
    compactAndVerify(expiryTimeAndSegmentsUnderCompaction.getSecond(), state.time.milliseconds(), true);
  }

  /**
   * Tests the case where the segments being compacted have keys that are deleted and expired but the deleted keys
   * don't count as deleted at the provided reference time (but the expired keys need to be cleaned up).
   * @throws Exception
   */
  @Test
  public void differentDeleteAndExpiryTimesTest() throws Exception {
    Pair<Long, List<String>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    state.advanceTime(expiryTimeAndSegmentsUnderCompaction.getFirst() + Time.MsPerSec - state.time.milliseconds());
    long deleteReferenceTimeMs = state.time.milliseconds();
    state.advanceTime(Time.MsPerSec);
    int deleteCount = 10;
    Set<MockId> idsInSegments = getIdsInSegments(expiryTimeAndSegmentsUnderCompaction.getSecond());
    List<MockId> idsToExamine = new ArrayList<>();
    for (MockId id : idsInSegments) {
      if (state.liveKeys.contains(id)) {
        state.addDeleteEntry(id);
        idsToExamine.add(id);
        if (idsToExamine.size() == deleteCount) {
          break;
        }
      }
    }
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    // compact with deleteReferenceTimeMs < the earliest delete time. None of the deleted keys count as deleted.
    compactAndVerify(expiryTimeAndSegmentsUnderCompaction.getSecond(), deleteReferenceTimeMs, true);
    // ensure that idsToExamine can still be fetched
    state.reloadIndex(true, false);
    for (MockId id : idsToExamine) {
      // should not throw exception since they should be untouched.
      // Data has already been verified if this is true (by the verifiers).
      state.index.getBlobReadInfo(id, EnumSet.of(StoreGetOptions.Store_Include_Deleted));
    }
  }

  /**
   * Compacts the whole log repeatedly with some data compacted each time.
   * @throws Exception
   */
  @Test
  public void compactWholeLogRepeatedlyTest() throws Exception {
    refreshState(false, true);
    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity() - 3;
    long expiryTimeMs = getInvalidationTime(requiredCount);
    List<Long> expiryTimesMs = Arrays.asList(state.time.milliseconds() / 2, expiryTimeMs, expiryTimeMs * 2);
    writeDataToMeetRequiredSegmentCount(requiredCount, expiryTimesMs);
    List<String> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    Set<MockId> idsInCompactedLogSegments = getIdsInSegments(segmentsUnderCompaction);

    for (long setTimeMs : expiryTimesMs) {
      if (state.time.milliseconds() < setTimeMs + Time.MsPerSec) {
        state.advanceTime(setTimeMs + Time.MsPerSec - state.time.milliseconds());
      }
      long deleteReferenceTimeMs = state.time.milliseconds();
      long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
      CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction);

      compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
      compactor.start(state.index);
      try {
        compactor.compact(details);
      } finally {
        compactor.shutdown(0);
      }
      assertFalse("Sum of size of log segments did not change after compaction",
          logSegmentSizeSumBeforeCompaction == getSumOfLogSegmentEndOffsets());
      verifyDataPostCompaction(idsInCompactedLogSegments, deleteReferenceTimeMs);
      state.reloadLog(true);
      verifyDataPostCompaction(idsInCompactedLogSegments, deleteReferenceTimeMs);
      segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
      state.verifyRealIndexSanity();
      // no clean shutdown file should exist
      assertFalse("Clean shutdown file not deleted",
          new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
      // there should be no temp files
      assertEquals("There are some temp log segments", 0,
          tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);
    }
  }

  /**
   * Tests the case where deletion time is enforced i.e. data is considered valid before reference time and is copied
   * over and data is considered invalid after reference time and is not copied over.
   * @throws Exception
   */
  @Test
  public void deletionTimeEnforcementTest() throws Exception {
    // no change before delete time
    Pair<Long, List<String>> deleteTimeAndSegmentsUnderCompaction = setupStateWithDeletedBlobsAtSpecificTime();
    long deleteReferenceTimeMs = deleteTimeAndSegmentsUnderCompaction.getFirst() - Time.MsPerSec;
    Map<String, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(deleteTimeAndSegmentsUnderCompaction.getSecond());
    compactAndVerify(deleteTimeAndSegmentsUnderCompaction.getSecond(), deleteReferenceTimeMs, false);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

    // no change at delete time.
    deleteTimeAndSegmentsUnderCompaction = setupStateWithDeletedBlobsAtSpecificTime();
    deleteReferenceTimeMs = deleteTimeAndSegmentsUnderCompaction.getFirst();
    oldSegmentNamesAndEndOffsets = getEndOffsets(deleteTimeAndSegmentsUnderCompaction.getSecond());
    compactAndVerify(deleteTimeAndSegmentsUnderCompaction.getSecond(), deleteReferenceTimeMs, false);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

    // there will be changes past delete time
    deleteTimeAndSegmentsUnderCompaction = setupStateWithDeletedBlobsAtSpecificTime();
    state.advanceTime(Time.MsPerSec);
    compactAndVerify(deleteTimeAndSegmentsUnderCompaction.getSecond(), state.time.milliseconds(), true);
  }

  /**
   * Tests the case where there is an interruption (crash/shutdown) of compaction during log commit or cleanup.
   * @throws Exception
   */
  @Test
  public void interruptionDuringLogCommitAndCleanupTest() throws Exception {
    // shutdown testing
    // shutdown during commit
    doTestWithInterruptionInducingLog(1, Integer.MAX_VALUE);
    // shutdown during cleanup
    doTestWithInterruptionInducingLog(Integer.MAX_VALUE, 1);

    // crash testing
    // crash after executing operation
    throwExceptionInsteadOfShutdown = true;
    throwExceptionBeforeOperation = false;
    // crash after commit
    doTestWithInterruptionInducingLog(1, Integer.MAX_VALUE);
    // crash after cleanup
    doTestWithInterruptionInducingLog(Integer.MAX_VALUE, 1);
    // crash before executing operation
    throwExceptionBeforeOperation = true;
    // crash before commit
    doTestWithInterruptionInducingLog(1, Integer.MAX_VALUE);
    // crash before cleanup
    doTestWithInterruptionInducingLog(Integer.MAX_VALUE, 1);
  }

  /**
   * Tests the case where there is an interruption (crash/shutdown) of compaction during index commit.
   * @throws Exception
   */
  @Test
  public void interruptionDuringIndexCommitTest() throws Exception {
    // shutdown testing
    doInterruptionDuringIndexCommitTest();
    // crash testing
    // crash after executing operation
    throwExceptionInsteadOfShutdown = true;
    throwExceptionBeforeOperation = false;
    doInterruptionDuringIndexCommitTest();
    // crash before executing operation
    throwExceptionBeforeOperation = true;
    doInterruptionDuringIndexCommitTest();
  }

  /**
   * Tests the case where there is an interruption (crash/shutdown) of compaction during copy after a few index segments
   * have been processed.
   * @throws Exception
   */
  @Test
  public void interruptionDuringOrAfterIndexSegmentProcessingTest() throws Exception {
    // shutdown testing
    doInterruptionDuringOrAfterIndexSegmentProcessingTest();
    // crash testing
    // crash after executing operation
    throwExceptionInsteadOfShutdown = true;
    throwExceptionBeforeOperation = false;
    doInterruptionDuringOrAfterIndexSegmentProcessingTest();
    // crash before executing operation
    throwExceptionBeforeOperation = true;
    doInterruptionDuringOrAfterIndexSegmentProcessingTest();
  }

  /**
   * Tests the case where there is an interruption (crash/shutdown) of compaction during copy when a few records from
   * an index segment have been copied over.
   * @throws Exception
   */
  @Test
  public void interruptDuringRecordCopyTest() throws Exception {
    // shutdown testing
    doInterruptionDuringRecordCopyTest();
    // crash testing
    throwExceptionInsteadOfShutdown = true;
    doInterruptionDuringRecordCopyTest();
  }

  /**
   * Tests the case where there is an interruption (crash/shutdown) of compaction during log commit of the very last
   * cycle of compaction (tests the case where compaction finishes in {@link BlobStoreCompactor#fixStateIfRequired()}.
   * @throws Exception
   */
  @Test
  public void interruptionDuringLastCommitTest() throws Exception {
    // keep hard delete enabled
    refreshState(true, true);
    List<String> segmentsUnderCompaction = getLogSegments(0, 2);
    reduceValidDataSizeInLogSegments(segmentsUnderCompaction, state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE);
    throwExceptionBeforeOperation = true;
    Log log = new InterruptionInducingLog(1, Integer.MAX_VALUE);
    assertTrue("Hard delete should be running", state.index.hardDeleter.isRunning());
    compactWithRecoveryAndVerify(log, DISK_IO_SCHEDULER, state.index, segmentsUnderCompaction,
        state.time.milliseconds(), true);
    assertTrue("Hard delete should be running", state.index.hardDeleter.isRunning());
  }

  // helpers

  // general

  /**
   * Destroys the existing state and cleans the store directory of all files.
   * @throws Exception
   */
  private void destroyStateAndCleanDir() throws Exception {
    if (state != null) {
      state.destroy();
    }
    StoreTestUtils.cleanDirectory(tempDir, false);
  }

  /**
   * Refreshes the state by destroying any existing state and creating a new one.
   * @param hardDeleteEnabled {@code true} if hard delete needs to be enabled. {@code false} otherwise.
   * @param initState {@code true} if some test state has to be created. {@code false} otherwise.
   * @throws Exception
   */
  private void refreshState(boolean hardDeleteEnabled, boolean initState) throws Exception {
    destroyStateAndCleanDir();
    state = new CuratedLogIndexState(true, tempDir, hardDeleteEnabled, initState);
  }

  /**
   * Gets an instance of {@link BlobStoreCompactor}.
   * @param log the {@link Log} instance to use.
   * @param ioScheduler the {@link DiskIOScheduler} instance to use.
   * @return an instance of {@link BlobStoreCompactor}.
   * @throws IOException
   */
  private BlobStoreCompactor getCompactor(Log log, DiskIOScheduler ioScheduler) throws IOException {
    shutdownOrExceptionInduced = false;
    StoreConfig config = new StoreConfig(new VerifiableProperties(state.properties));
    return new BlobStoreCompactor(tempDirStr, STORE_ID, CuratedLogIndexState.STORE_KEY_FACTORY, config,
        new StoreMetrics(STORE_ID, new MetricRegistry()), ioScheduler, log, state.scheduler, state.recovery, state.time,
        state.sessionId, state.incarnationId);
  }

  /**
   * Gets {@code count} number of log segments starting at position {@code startPos} (inclusive and indexed on 0).
   * <p/>
   * For e.g., to get the first two log segments, the call would be getLogSegments(0, 2).
   * @param startPos the position of the first log segment required (inclusive and indexed on 0).
   * @param count number of log segments require.
   * @return {@code count} number of log segments starting at position {@code startPos} (inclusive and indexed on 0).
   */
  private List<String> getLogSegments(long startPos, long count) {
    List<String> segmentsUnderCompaction = new ArrayList<>();
    LogSegment segment = state.log.getFirstSegment();
    int idx = 0;
    while (segmentsUnderCompaction.size() < count) {
      if (idx >= startPos) {
        segmentsUnderCompaction.add(segment.getName());
      }
      segment = state.log.getNextSegment(segment);
      idx++;
    }
    return segmentsUnderCompaction;
  }

  /**
   * Gets all the ids in {@code logSegmentNames}.
   * @param logSegmentNames the names of log segments whose IDs are required.
   * @return all the ids in {@code logSegmentNames}.
   */
  private Set<MockId> getIdsInSegments(List<String> logSegmentNames) {
    Set<MockId> idsInCompactedLogSegments = new HashSet<>();
    for (String segmentName : logSegmentNames) {
      idsInCompactedLogSegments.addAll(state.getIdsInLogSegment(state.log.getSegment(segmentName)));
    }
    return idsInCompactedLogSegments;
  }

  /**
   * Gets the sum of size of valid data in {@code logSegments} at {@code deleteReferenceTimeMs}.
   * @param logSegments the names of log segments whose sum of size of valid data is required.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @return the sum of size of valid data in {@code logSegments} at {@code deleteReferenceTimeMs}.
   */
  private long getValidDataSize(List<String> logSegments, long deleteReferenceTimeMs) {
    long size = 0;
    for (String segment : logSegments) {
      size += state.getValidDataSizeForLogSegment(state.log.getSegment(segment), deleteReferenceTimeMs,
          state.time.milliseconds());
    }
    return size;
  }

  /**
   * Writes data so that the log contains {@code countRequired} number of log segments. If a non {@code null}
   * {@code expiryTimes} is passed, a fraction of the blobs written will contain those expiry times in round robin order
   * @param countRequired the number of log segments required.
   * @param expiryTimes the expiry times desired. A fraction of the blobs written will contain those expiry times in
   *                    round robin order
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void writeDataToMeetRequiredSegmentCount(long countRequired, List<Long> expiryTimes)
      throws InterruptedException, IOException, StoreException {
    long capacityLimit = countRequired * state.log.getSegmentCapacity();
    int blobsPut = 0;
    int expiredBlobsCount = 0;
    while (state.index.getLogUsedCapacity() + CuratedLogIndexState.PUT_RECORD_SIZE <= capacityLimit) {
      long expiresAtMs = Utils.Infinite_Time;
      if (expiryTimes != null && blobsPut % 4 == 0) {
        expiresAtMs = expiryTimes.get(expiredBlobsCount % expiryTimes.size());
        expiredBlobsCount++;
      }
      state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiresAtMs);
      blobsPut++;
    }
  }

  /**
   * Reduces the valid data size in {@code logSegmentsToReduceFrom} to <= {@code ceilingSize} by deleting keys in these
   * segments.
   * @param logSegmentsToReduceFrom the names of the log segments to reduce the data from.
   * @param ceilingSize the maximum allowed valid size in these log segments.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void reduceValidDataSizeInLogSegments(List<String> logSegmentsToReduceFrom, long ceilingSize)
      throws InterruptedException, IOException, StoreException {
    List<String> logSegments = new ArrayList<>(logSegmentsToReduceFrom);
    long validDataSize = getValidDataSize(logSegmentsToReduceFrom, state.time.milliseconds());
    while (validDataSize > ceilingSize) {
      assertTrue("There are no more segments to delete data from", logSegments.size() > 0);
      int selectedIdx = TestUtils.RANDOM.nextInt(logSegments.size());
      String segmentToDeleteFrom = logSegments.get(selectedIdx);
      MockId idToDelete = state.getIdToDeleteFromLogSegment(state.log.getSegment(segmentToDeleteFrom));
      if (idToDelete == null) {
        logSegments.remove(selectedIdx);
      } else {
        state.addDeleteEntry(idToDelete);
        validDataSize = getValidDataSize(logSegmentsToReduceFrom, state.time.milliseconds());
      }
    }
  }

  /**
   * Compacts the {@code segmentsUnderCompaction} and verifies sanity of store and data. Also verifies that no change
   * occurred if no change was expected and vice versa. Ensures that reloading the log does not have effects on the
   * integrity of the store and data.
   * @param segmentsUnderCompaction the names of the log segments under compaction.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @param changeExpected {@code true} if compaction will cause a change in size of data. {@code false} otherwise.
   * @throws Exception
   */
  private void compactAndVerify(List<String> segmentsUnderCompaction, long deleteReferenceTimeMs,
      boolean changeExpected) throws Exception {
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    long logSegmentCountBeforeCompaction = state.index.getLogSegmentCount();
    long indexSegmentCountBeforeCompaction = state.index.getIndexSegments().size();

    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction);
    long expectedValidDataSize = getValidDataSize(segmentsUnderCompaction, deleteReferenceTimeMs);
    List<String> unaffectedSegments = getUnaffectedSegments(segmentsUnderCompaction);
    List<LogEntry> validLogEntriesInOrder = getValidLogEntriesInOrder(segmentsUnderCompaction, deleteReferenceTimeMs);
    Set<MockId> idsInCompactedLogSegments = getIdsInSegments(segmentsUnderCompaction);

    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.start(state.index);

    try {
      compactor.compact(details);
    } finally {
      compactor.shutdown(0);
    }

    assertFalse("No compaction should be in progress", CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID));
    long logSegmentSizeAfterCompaction = getSumOfLogSegmentEndOffsets();
    long logSegmentCountAfterCompaction = state.index.getLogSegmentCount();
    long indexSegmentCountAfterCompaction = state.index.getIndexSegments().size();
    verifyCompaction(segmentsUnderCompaction, unaffectedSegments, expectedValidDataSize, validLogEntriesInOrder,
        idsInCompactedLogSegments, deleteReferenceTimeMs);

    state.reloadLog(true);
    verifyCompaction(segmentsUnderCompaction, unaffectedSegments, expectedValidDataSize, validLogEntriesInOrder,
        idsInCompactedLogSegments, deleteReferenceTimeMs);

    assertEquals("Sum of log segment capacities changed after reload", logSegmentSizeAfterCompaction,
        getSumOfLogSegmentEndOffsets());
    assertEquals("Log segment count changed after reload", logSegmentCountAfterCompaction,
        state.index.getLogSegmentCount());
    assertEquals("Index segment count changed after reload", indexSegmentCountAfterCompaction,
        state.index.getIndexSegments().size());
    checkVitals(changeExpected, logSegmentSizeSumBeforeCompaction, logSegmentCountBeforeCompaction,
        indexSegmentCountBeforeCompaction);
  }

  /**
   * Similar to {@link #compactAndVerify(List, long, boolean)} but expects an interruption and performs recovery before
   * resuming compaction.
   * @param log the {@link Log} instance to use for the compaction before interruption.
   * @param diskIOScheduler the {@link DiskIOScheduler} instance to use for the compaction before interruption.
   * @param index the {@link PersistentIndex} instance to use for the compaction before interruption.
   * @param segmentsUnderCompaction the names of the log segments under compaction.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @param changeExpected {@code true} if compaction will cause a change in size of data. {@code false} otherwise.
   * @throws Exception
   */
  private void compactWithRecoveryAndVerify(Log log, DiskIOScheduler diskIOScheduler, PersistentIndex index,
      List<String> segmentsUnderCompaction, long deleteReferenceTimeMs, boolean changeExpected) throws Exception {
    // getting these from the "real" log and indexes
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    long logSegmentCountBeforeCompaction = state.index.getLogSegmentCount();
    long indexSegmentCountBeforeCompaction = state.index.getIndexSegments().size();

    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction);
    long expectedValidDataSize = getValidDataSize(segmentsUnderCompaction, deleteReferenceTimeMs);
    List<String> unaffectedSegments = getUnaffectedSegments(segmentsUnderCompaction);
    List<LogEntry> validLogEntriesInOrder = getValidLogEntriesInOrder(segmentsUnderCompaction, deleteReferenceTimeMs);
    Set<MockId> idsInCompactedLogSegments = getIdsInSegments(segmentsUnderCompaction);

    compactor = getCompactor(log, diskIOScheduler);
    compactor.start(index);

    try {
      compactor.compact(details);
      if (throwExceptionInsteadOfShutdown) {
        fail("Compact should have thrown exception");
      }
    } catch (RuntimeException e) {
      assertEquals("Exception not as expected", EXCEPTION_MSG, e.getMessage());
    } finally {
      assertTrue("Shutdown was not induced by the test", shutdownOrExceptionInduced);
      assertTrue("There should be a compaction in progress",
          CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID));
      compactor.shutdown(0);
    }

    // TODO: verify before reload also.
    // have to reload log since the instance changed by the old compactor compactor is different.
    state.reloadLog(false);
    // use the "real" log, index and disk IO schedulers this time.
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.fixStateIfRequired();
    state.initIndex(state.metricRegistry);
    compactor.start(state.index);
    try {
      if (CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID)) {
        compactor.resumeCompaction();
      }
    } finally {
      compactor.shutdown(0);
    }

    verifyCompaction(segmentsUnderCompaction, unaffectedSegments, expectedValidDataSize, validLogEntriesInOrder,
        idsInCompactedLogSegments, deleteReferenceTimeMs);
    checkVitals(changeExpected, logSegmentSizeSumBeforeCompaction, logSegmentCountBeforeCompaction,
        indexSegmentCountBeforeCompaction);
  }

  /**
   * Gets the list of segments in the log but not in {@code segmentsUnderCompaction}.
   * @param segmentsUnderCompaction the names of the log segments under compaction.
   * @return the list of segments in the log but not in {@code segmentsUnderCompaction}.
   */
  private List<String> getUnaffectedSegments(List<String> segmentsUnderCompaction) {
    List<String> unaffectedSegments = new ArrayList<>();
    LogSegment segment = state.log.getFirstSegment();
    while (segment != null) {
      if (!segmentsUnderCompaction.contains(segment.getName())) {
        unaffectedSegments.add(segment.getName());
      }
      segment = state.log.getNextSegment(segment);
    }
    return unaffectedSegments;
  }

  /**
   * @return the sum of all the end offsets of all the log segments.
   */
  private long getSumOfLogSegmentEndOffsets() {
    long sum = 0;
    LogSegment segment = state.log.getFirstSegment();
    while (segment != null) {
      sum += segment.getEndOffset();
      segment = state.log.getNextSegment(segment);
    }
    return sum;
  }

  /**
   * Checks some vitals of the store. If change is expected checks that the sum of end offsets of all the log segments
   * has changed and is lesser than what it was before. If no change was expected, checks that the sum, count of log
   * and index segments is the same as before.
   * @param changeExpected {@code true} if change in log segment end offset sum is expected. {@code false} otherwise.
   * @param originalLogSegmentSizeSum the sum of end offsets of all the log segments before compaction.
   * @param originalLogSegmentCount the number of log segments before compaction.
   * @param originalIndexSegmentCount the number of index segments before compaction.
   */
  private void checkVitals(boolean changeExpected, long originalLogSegmentSizeSum, long originalLogSegmentCount,
      long originalIndexSegmentCount) {
    if (changeExpected) {
      assertTrue("Compaction did not cause in change in sum of log segment sizes",
          originalLogSegmentSizeSum > getSumOfLogSegmentEndOffsets());
    } else {
      assertEquals("Sum of log segment capacities changed after compaction", originalLogSegmentSizeSum,
          getSumOfLogSegmentEndOffsets());
      assertEquals("Log segment count changed after compaction", originalLogSegmentCount,
          state.index.getLogSegmentCount());
      assertEquals("Index segment count changed after compaction", originalIndexSegmentCount,
          state.index.getIndexSegments().size());
    }
  }

  /**
   * Verifies compaction by checking the integrity of the store and data.
   * Calls {@link #verifyStorePostCompaction(List, List, long, List)} and {@link #verifyDataPostCompaction(Set, long)}.
   * @param segmentsCompacted the names of the log segments that were compacted.
   * @param unaffectedSegments the names of the log segments that should have been unaffected.
   * @param targetSegmentsExpectedValidSize the expected valid size of all the new segments that were created due to
   *                                        compaction.
   * @param validLogEntriesInOrder the log entries for valid data before compaction in order. The new segments should
   *                               have all of these entries and in the same order.
   * @param idsInCompactedLogSegments the ids in the segments that were compacted.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void verifyCompaction(List<String> segmentsCompacted, List<String> unaffectedSegments,
      long targetSegmentsExpectedValidSize, List<LogEntry> validLogEntriesInOrder,
      Set<MockId> idsInCompactedLogSegments, long deleteReferenceTimeMs)
      throws InterruptedException, IOException, StoreException {
    verifyStorePostCompaction(segmentsCompacted, unaffectedSegments, targetSegmentsExpectedValidSize,
        validLogEntriesInOrder);
    verifyDataPostCompaction(idsInCompactedLogSegments, deleteReferenceTimeMs);
  }

  /**
   * Verifies integrity of store post compaction.
   * 1. Checks that there aren't two log segments at the same position.
   * 2. Checks that the generation number of the compacted segments has been incremented.
   * 3. Checks that the valid size of data in the new segments is as expected.
   * 4. Checks that {@code unaffectedSegments} have been left untouched.
   * 5. Checks that all existing index segments refer to valid log segments are mapped correctly/
   * 6. Verifies sanity of the new index by calling {@link CuratedLogIndexState#verifyRealIndexSanity()}.
   * 7. Verifies that the ordering of data in the log segments has not changed.
   * 8. Verifies that no temporary files have been left behind.
   * @param segmentsCompacted the names of the log segments that were compacted.
   * @param unaffectedSegments the names of the log segments that should have been unaffected.
   * @param targetSegmentsExpectedValidSize the expected valid size of all the new segments that were created due to
   *                                        compaction.
   * @param validLogEntriesInOrder the log entries for valid data before compaction in order. The new segments should
   *                               have all of these entries and in the same order.
   * @throws IOException
   * @throws StoreException
   */
  private void verifyStorePostCompaction(List<String> segmentsCompacted, List<String> unaffectedSegments,
      long targetSegmentsExpectedValidSize, List<LogEntry> validLogEntriesInOrder) throws IOException, StoreException {
    long highestGeneration = 0;
    for (String segmentCompacted : segmentsCompacted) {
      highestGeneration = Math.max(highestGeneration, LogSegmentNameHelper.getGeneration(segmentCompacted));
    }
    highestGeneration++;
    long lowestPosition = LogSegmentNameHelper.getPosition(segmentsCompacted.get(0));
    long highestPosition = LogSegmentNameHelper.getPosition(segmentsCompacted.get(segmentsCompacted.size() - 1));
    long sizeOfTargetSegments = 0;
    Set<Long> positionsEncountered = new HashSet<>();
    Set<String> allSegmentNames = new HashSet<>();
    List<String> nonTargetSegmentNames = new ArrayList<>();
    List<String> targetSegmentNames = new ArrayList<>();
    LogSegment segment = state.log.getFirstSegment();
    while (segment != null) {
      String segmentName = segment.getName();
      allSegmentNames.add(segmentName);
      assertFalse("Segment " + segmentName + " should not exist", segmentsCompacted.contains(segmentName));
      long pos = LogSegmentNameHelper.getPosition(segmentName);
      assertFalse("Encountered two log segments with the same position", positionsEncountered.contains(pos));
      positionsEncountered.add(pos);
      long gen = LogSegmentNameHelper.getGeneration(segmentName);
      if (pos >= lowestPosition && pos <= highestPosition) {
        assertEquals("Generation should have changed", highestGeneration, gen);
        sizeOfTargetSegments += segment.getEndOffset() - LogSegment.HEADER_SIZE;
        targetSegmentNames.add(segmentName);
      } else {
        nonTargetSegmentNames.add(segmentName);
      }
      segment = state.log.getNextSegment(segment);
    }
    assertEquals("Valid size of target segments not as expected", targetSegmentsExpectedValidSize,
        sizeOfTargetSegments);
    // if the target segments have no valid size, there should be no entries in targetSegmentNames
    if (targetSegmentsExpectedValidSize == 0) {
      assertEquals("There should be no target segments since valid size is 0", 0, targetSegmentNames.size());
    }
    assertEquals("Segments not under compaction missing", unaffectedSegments, nonTargetSegmentNames);
    Offset lastIndexSegmentStartOffset = state.index.getIndexSegments().lastKey();
    for (Map.Entry<Offset, IndexSegment> indexSegmentEntry : state.index.getIndexSegments().entrySet()) {
      Offset indexSegmentStartOffset = indexSegmentEntry.getKey();
      assertTrue("Index segment does not refer to any active log segments",
          allSegmentNames.contains(indexSegmentEntry.getKey().getName()));
      assertEquals("Index segment mapped state not as expected",
          !indexSegmentStartOffset.equals(lastIndexSegmentStartOffset), indexSegmentEntry.getValue().isMapped());
    }
    // verify sanity of compacted index
    state.verifyRealIndexSanity();
    // verify ordering
    assertEquals("Ordering after compaction is inconsistent", validLogEntriesInOrder,
        getLogEntriesInOrder(targetSegmentNames));
    // no clean shutdown file should exist
    assertFalse("Clean shutdown file not deleted",
        new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
    // there should be no temp files
    assertEquals("There are some temp log segments", 0,
        tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);
  }

  /**
   * Verifies data after compaction is finished.
   * 1. Checks that GET of data of live keys succeeds without any flags.
   * 2. Checks that GET of data of deleted keys returns deleted. Also verifies that when GET is used with
   * {@link StoreGetOptions#Store_Include_Deleted}, data is returned if it was not compacted and not returned if
   * compacted.
   * 3. Checks that GET of data of expired keys returns expired if not compacted and not found if compacted. Also
   * verifies that when GET is used with {@link StoreGetOptions#Store_Include_Expired}, data is returned if it was not
   * compacted and not returned if compacted.
   * @param idsInCompactedLogSegments the ids in the compacted segments.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void verifyDataPostCompaction(Set<MockId> idsInCompactedLogSegments, long deleteReferenceTimeMs)
      throws InterruptedException, IOException, StoreException {
    for (MockId id : state.allKeys.keySet()) {
      if (state.liveKeys.contains(id)) {
        BlobReadOptions options = state.index.getBlobReadInfo(id, EnumSet.noneOf(StoreGetOptions.class));
        checkRecord(id, options);
      } else if (state.deletedKeys.contains(id)) {
        boolean shouldBeCompacted =
            idsInCompactedLogSegments.contains(id) && state.isDeletedAt(id, deleteReferenceTimeMs);
        try {
          state.index.getBlobReadInfo(id, EnumSet.noneOf(StoreGetOptions.class));
          fail("Should not be able to GET " + id);
        } catch (StoreException e) {
          assertEquals(id + " failed with error code " + e.getErrorCode(), StoreErrorCodes.ID_Deleted,
              e.getErrorCode());
        }
        try {
          BlobReadOptions options = state.index.getBlobReadInfo(id, EnumSet.allOf(StoreGetOptions.class));
          if (shouldBeCompacted) {
            fail("Should not be able to GET " + id);
          } else {
            checkRecord(id, options);
          }
        } catch (StoreException e) {
          assertTrue("Blob should have been retrieved", shouldBeCompacted);
          assertEquals(id + " failed with error code " + e.getErrorCode(), StoreErrorCodes.ID_Deleted,
              e.getErrorCode());
        }
      } else if (state.expiredKeys.contains(id)) {
        boolean shouldBeCompacted =
            idsInCompactedLogSegments.contains(id) && state.isExpiredAt(id, state.time.milliseconds());
        try {
          state.index.getBlobReadInfo(id, EnumSet.noneOf(StoreGetOptions.class));
          fail("Should not be able to GET " + id);
        } catch (StoreException e) {
          StoreErrorCodes expectedErrorCode =
              shouldBeCompacted ? StoreErrorCodes.ID_Not_Found : StoreErrorCodes.TTL_Expired;
          assertEquals(id + " failed with error code " + e.getErrorCode(), expectedErrorCode, e.getErrorCode());
        }
        try {
          BlobReadOptions options = state.index.getBlobReadInfo(id, EnumSet.allOf(StoreGetOptions.class));
          if (shouldBeCompacted) {
            fail("Should not be able to GET " + id);
          } else {
            checkRecord(id, options);
          }
        } catch (StoreException e) {
          assertTrue("Blob should have been retrieved", shouldBeCompacted);
          assertEquals(id + " failed with error code " + e.getErrorCode(), StoreErrorCodes.ID_Not_Found,
              e.getErrorCode());
        }
      }
    }
  }

  /**
   * Gets all the valid log entries in {@code logSegmentsUnderConsideration} in order of their occurrence in the log.
   * @param logSegmentsUnderConsideration the log segments whose log entries are required.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @return the valid log entries in {@code logSegmentsUnderConsideration} in order of their occurrence in the log.
   */
  private List<LogEntry> getValidLogEntriesInOrder(List<String> logSegmentsUnderConsideration,
      long deleteReferenceTimeMs) {
    List<LogEntry> validLogEntriesInOrder = new ArrayList<>();
    for (String logSegment : logSegmentsUnderConsideration) {
      List<IndexEntry> validIndexEntries =
          state.getValidIndexEntriesForLogSegment(state.log.getSegment(logSegment), deleteReferenceTimeMs,
              state.time.milliseconds());
      addToLogEntriesInOrder(validIndexEntries, validLogEntriesInOrder);
    }
    return validLogEntriesInOrder;
  }

  /**
   * Gets all the log entries in {@code logSegmentsUnderConsideration} in order of their occurrence in the log.
   * @param logSegmentsUnderConsideration the log segments whose log entries are required.
   * @return the log entries in {@code logSegmentsUnderConsideration} in order of their occurrence in the log.
   * @throws IOException
   * @throws StoreException
   */
  private List<LogEntry> getLogEntriesInOrder(List<String> logSegmentsUnderConsideration)
      throws IOException, StoreException {
    List<LogEntry> logEntriesInOrder = new ArrayList<>();
    NavigableMap<Offset, IndexSegment> indexSegments = state.index.getIndexSegments();
    for (String logSegmentName : logSegmentsUnderConsideration) {
      LogSegment logSegment = state.log.getSegment(logSegmentName);
      Offset indexSegmentStartOffset = new Offset(logSegmentName, logSegment.getStartOffset());
      while (indexSegmentStartOffset != null && indexSegmentStartOffset.getName().equals(logSegmentName)) {
        IndexSegment indexSegment = indexSegments.get(indexSegmentStartOffset);
        List<MessageInfo> infos = new ArrayList<>();
        indexSegment.getEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), infos, new AtomicLong(0));
        List<IndexEntry> indexEntries = new ArrayList<>();
        for (MessageInfo info : infos) {
          IndexValue value = indexSegment.find(info.getStoreKey());
          indexEntries.add(new IndexEntry(info.getStoreKey(), value));
        }
        addToLogEntriesInOrder(indexEntries, logEntriesInOrder);
        indexSegmentStartOffset = indexSegments.higherKey(indexSegmentStartOffset);
      }
    }
    return logEntriesInOrder;
  }

  /**
   * Adds {@link LogEntry} instances for all the {@code indexEntries} to {@code logEntriesInOrder}.
   * @param indexEntries the index entries to process.
   * @param logEntriesInOrder the list of {@link LogEntry} instances to add to.
   */
  private void addToLogEntriesInOrder(List<IndexEntry> indexEntries, List<LogEntry> logEntriesInOrder) {
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    for (IndexEntry entry : indexEntries) {
      MockId id = (MockId) entry.getKey();
      PersistentIndex.IndexEntryType entryType =
          entry.getValue().isFlagSet(IndexValue.Flags.Delete_Index) ? PersistentIndex.IndexEntryType.DELETE
              : PersistentIndex.IndexEntryType.PUT;
      logEntriesInOrder.add(new LogEntry(id, entryType));
    }
  }

  /**
   * Checks the record of an id by ensuring that the {@link IndexValue} received from the store matches what is
   * expected. Also checks the data if the key is not deleted and hard delete enabled.
   * @param id the {@link MockId} whose record needs to be checked.
   * @param options the {@link BlobReadOptions} received from the {@link PersistentIndex}.
   * @throws IOException
   */
  private void checkRecord(MockId id, BlobReadOptions options) throws IOException {
    List<BlobReadOptions> optionsList = new ArrayList<>();
    optionsList.add(options);
    MessageReadSet readSet = new StoreMessageReadSet(optionsList);
    IndexValue value = state.getExpectedValue(id, true);
    assertEquals("Unexpected key in BlobReadOptions", id, options.getStoreKey());
    assertEquals("Unexpected size in BlobReadOptions", value.getSize(), options.getSize());
    assertEquals("Unexpected expiresAtMs in BlobReadOptions", value.getExpiresAtMs(), options.getExpiresAtMs());
    if (state.index.hardDeleter.enabled.get() && !state.deletedKeys.contains(id)) {
      ByteBuffer readBuf = ByteBuffer.allocate((int) value.getSize());
      ByteBufferOutputStream stream = new ByteBufferOutputStream(readBuf);
      WritableByteChannel channel = Channels.newChannel(stream);
      readSet.writeTo(0, channel, 0, value.getSize());
      byte[] expectedData = state.getExpectedData(id, true);
      assertArrayEquals("Data obtained from reset does not match original", expectedData, readBuf.array());
    }
  }

  // badInputTest() helpers

  /**
   * Ensures that {@link BlobStoreCompactor#compact(CompactionDetails)} fails because {@code details} is invalid.
   * @param details the invalid {@link CompactionDetails}
   * @param msg the message to print on failure if no exception is thrown.
   * @throws Exception
   */
  private void ensureArgumentFailure(CompactionDetails details, String msg) throws Exception {
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.start(state.index);
    try {
      compactor.compact(details);
      fail(msg);
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    } finally {
      compactor.shutdown(0);
    }
  }

  // expirationTimeEnforcementTest(), deletionTimeEnforcementTest() and related recovery test helpers

  /**
   * Sets up state where a fixed number of log segments are written and a fraction of the blobs in these log segments
   * expire at a time returned by the function.
   * @return a {@link Pair} that contains the time at which the blobs will expire and the list of segments that should
   * be compacted.
   * @throws Exception
   */
  private Pair<Long, List<String>> setupStateWithExpiredBlobsAtSpecificTime() throws Exception {
    // TODO: can this be simplified to not calculate the "good" time? The caller can just reset?
    refreshState(false, true);
    // we want at least 3 log segments that are outside the journal and with no invalid records
    long extraSegmentCountRequired = 4;
    // send +1 to account for half filled segments from curated log-index state.
    long expiryTimeMs = getInvalidationTime(extraSegmentCountRequired + 1);

    long currentLogSegmentCount = state.index.getLogSegmentCount();
    writeDataToMeetRequiredSegmentCount(currentLogSegmentCount + extraSegmentCountRequired,
        Collections.singletonList(expiryTimeMs));
    List<String> segmentsUnderCompaction = getLogSegments(currentLogSegmentCount, extraSegmentCountRequired - 1);
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    return new Pair<>(expiryTimeMs, segmentsUnderCompaction);
  }

  /**
   * Sets up state where a fixed number of log segments are written and a fraction of the blobs in these log segments
   * are considered deleted at a time returned by the function.
   * @return a {@link Pair} that contains the time at which the blobs are considered deleted and the list of segments
   * that should be compacted.
   * @throws Exception
   */
  private Pair<Long, List<String>> setupStateWithDeletedBlobsAtSpecificTime() throws Exception {
    refreshState(false, true);
    // we want at least 3 log segments that are outside the journal and with no invalid records
    long extraSegmentCountRequired = 4;
    // send +1 to account for half filled segments from curated log-index state.
    long deleteTimeMs = getInvalidationTime(extraSegmentCountRequired + 1);

    long currentLogSegmentCount = state.index.getLogSegmentCount();
    writeDataToMeetRequiredSegmentCount(currentLogSegmentCount + extraSegmentCountRequired, null);

    // advance time and delete some data (some of the data will get deleted at deleteTimeMs + delta).
    state.advanceTime(deleteTimeMs - state.time.milliseconds());
    List<String> segmentsUnderCompaction = getLogSegments(currentLogSegmentCount, extraSegmentCountRequired - 1);
    // we need enough data to fill two log segments after compaction
    // to allow for non alignment of log segment boundaries, reduce valid size required by the put record size
    long validSizeRequired =
        2 * (state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE - CuratedLogIndexState.PUT_RECORD_SIZE);
    // reduce the data to make sure that all the data fits in two segments
    reduceValidDataSizeInLogSegments(segmentsUnderCompaction, validSizeRequired);
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    return new Pair<>(deleteTimeMs, segmentsUnderCompaction);
  }

  /**
   * Calculates a safe time for invalidation (i.e. records should expire at a time later than the last PUT record).
   * @param numSegmentsToWritePutRecordsTo the number of log segments that will be filled with PUT records.
   * @return a safe time for invalidation (i.e. records should expire at a time later than the last PUT record).
   * @throws Exception
   */
  private long getInvalidationTime(long numSegmentsToWritePutRecordsTo) throws Exception {
    // set some invalidation time that is far enough into the future that it cannot be affected by segment rollovers
    long possiblePutRecords =
        numSegmentsToWritePutRecordsTo * state.log.getSegmentCapacity() / CuratedLogIndexState.PUT_RECORD_SIZE + 1;
    long possibleIndexSegments =
        numSegmentsToWritePutRecordsTo + possiblePutRecords / CuratedLogIndexState.MAX_IN_MEM_ELEMENTS + 1;
    long invalidationTimeMs =
        state.time.milliseconds() + possibleIndexSegments * CuratedLogIndexState.DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS;
    // round up to the next second
    return (invalidationTimeMs / Time.MsPerSec + 1) * Time.MsPerSec;
  }

  /**
   * Gets the end offsets of all the {@code logSegmentNames}.
   * @param logSegmentNames the names of the log segments whose
   * @return a map of log segment names to end offsets.
   */
  private Map<String, Long> getEndOffsets(List<String> logSegmentNames) {
    Map<String, Long> oldSegmentNamesAndEndOffsets = new HashMap<>();
    for (String segmentUnderCompaction : logSegmentNames) {
      oldSegmentNamesAndEndOffsets.put(segmentUnderCompaction,
          state.log.getSegment(segmentUnderCompaction).getEndOffset());
    }
    return oldSegmentNamesAndEndOffsets;
  }

  /**
   * Verifies that the new log segments created by compacting the old log segments have the same end offsets.
   * @param oldSegmentNamesAndEndOffsets the names and end offsets of the old log segments.
   */
  private void verifyNoChangeInEndOffsets(Map<String, Long> oldSegmentNamesAndEndOffsets) {
    for (Map.Entry<String, Long> nameAndEndOffset : oldSegmentNamesAndEndOffsets.entrySet()) {
      // there should be a segment with the higher gen
      String oldSegmentName = nameAndEndOffset.getKey();
      assertNull("Old segment should not exist", state.log.getSegment(oldSegmentName));
      String newSegmentName = LogSegmentNameHelper.getNextGenerationName(oldSegmentName);
      LogSegment newSegment = state.log.getSegment(newSegmentName);
      assertNotNull("New segment should exist", newSegment);
      assertEquals("End offset of new segment does not match that of the old segment",
          nameAndEndOffset.getValue().longValue(), newSegment.getEndOffset());
    }
  }

  // interruptionDuringLogCommitAndCleanupTest() helpers.

  /**
   * Does compaction tests where compaction is interrupted (and recovered) during log commit or cleanup.
   * @param addSegmentCallCountToInterruptAt the number calls to {@link Log#addSegment(LogSegment, boolean)} allowed
   *                                         before interruption.
   * @param dropSegmentCallCountToInterruptAt the number calls to {@link Log#dropSegment(String, boolean)} allowed
   *                                          before interruption.
   * @throws Exception
   */
  private void doTestWithInterruptionInducingLog(int addSegmentCallCountToInterruptAt,
      int dropSegmentCallCountToInterruptAt) throws Exception {
    // no change before expiry time
    Pair<Long, List<String>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    List<String> segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
    Map<String, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
    // create another log that wraps over the same files but induces shutdown as required.
    Log log = new InterruptionInducingLog(addSegmentCallCountToInterruptAt, dropSegmentCallCountToInterruptAt);
    compactWithRecoveryAndVerify(log, DISK_IO_SCHEDULER, state.index, segmentsUnderCompaction,
        state.time.milliseconds(), false);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

    // there will be changes past expiration time
    expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
    state.advanceTime(expiryTimeAndSegmentsUnderCompaction.getFirst() + Time.MsPerSec - state.time.milliseconds());
    // create another log that wraps over the same files but induces shutdown as required.
    log = new InterruptionInducingLog(addSegmentCallCountToInterruptAt, dropSegmentCallCountToInterruptAt);
    compactWithRecoveryAndVerify(log, DISK_IO_SCHEDULER, state.index, segmentsUnderCompaction,
        state.time.milliseconds(), true);
  }

  // interruptionDuringIndexCommitTest() helpers

  /**
   * Does compaction tests where compaction is interrupted (and recovered) during index commit.
   * @throws Exception
   */
  private void doInterruptionDuringIndexCommitTest() throws Exception {
    // no change before expiry time
    Pair<Long, List<String>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    List<String> segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
    Map<String, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
    // create another index that wraps over the same files but induces shutdown as required.
    PersistentIndex index = new InterruptionInducingIndex();
    compactWithRecoveryAndVerify(state.log, DISK_IO_SCHEDULER, index, segmentsUnderCompaction,
        state.time.milliseconds(), false);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

    // there will be changes past expiration time
    expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
    state.advanceTime(expiryTimeAndSegmentsUnderCompaction.getFirst() + Time.MsPerSec - state.time.milliseconds());
    // create another index that wraps over the same files but induces shutdown as required.
    index = new InterruptionInducingIndex();
    compactWithRecoveryAndVerify(state.log, DISK_IO_SCHEDULER, index, segmentsUnderCompaction,
        state.time.milliseconds(), true);
  }

  // interruptionDuringOrAfterIndexSegmentProcessingTest() helpers

  /**
   * Does compaction tests where compaction is interrupted (and recovered) after processing a few index segments.
   * @throws Exception
   */
  private void doInterruptionDuringOrAfterIndexSegmentProcessingTest() throws Exception {
    for (int interruptAt : Arrays.asList(1, 2, 7, -3)) {
      // no change before expiry time
      Pair<Long, List<String>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
      List<String> segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
      Map<String, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
      // if negative, set crash count starting from the end
      int countToInterruptAt = interruptAt >= 0 ? interruptAt
          : getIndexSegmentStartOffsetsForLogSegments(segmentsUnderCompaction).size() + interruptAt;
      // create a DiskIOScheduler
      DiskIOScheduler diskIOScheduler = new InterruptionInducingDiskIOScheduler(countToInterruptAt, Integer.MAX_VALUE);
      compactWithRecoveryAndVerify(state.log, diskIOScheduler, state.index, segmentsUnderCompaction,
          state.time.milliseconds(), false);
      verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

      // there will be changes past expiration time
      expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
      segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
      state.advanceTime(expiryTimeAndSegmentsUnderCompaction.getFirst() + Time.MsPerSec - state.time.milliseconds());
      // if negative, set crash count starting from the end
      countToInterruptAt = interruptAt >= 0 ? interruptAt
          : getIndexSegmentStartOffsetsForLogSegments(segmentsUnderCompaction).size() + interruptAt;
      diskIOScheduler = new InterruptionInducingDiskIOScheduler(countToInterruptAt, Integer.MAX_VALUE);
      compactWithRecoveryAndVerify(state.log, diskIOScheduler, state.index, segmentsUnderCompaction,
          state.time.milliseconds(), true);
    }
  }

  /**
   * Gets all the index segment start offsets for the given {@code logSegmentNames}.
   * @param logSegmentNames the names of the log segments whose index segment start offsets are required.
   * @return the index segment start offsets for the given {@code logSegmentNames}.
   */
  private List<Offset> getIndexSegmentStartOffsetsForLogSegments(List<String> logSegmentNames) {
    List<Offset> offsets = new ArrayList<>();
    for (String logSegmentName : logSegmentNames) {
      File[] indexSegmentFiles = PersistentIndex.getIndexSegmentFilesForLogSegment(tempDirStr, logSegmentName);
      for (File indexSegmentFile : indexSegmentFiles) {
        offsets.add(IndexSegment.getIndexSegmentStartOffset(indexSegmentFile.getName()));
      }
    }
    return offsets;
  }

  // interruptDuringRecordCopyTest() helpers

  /**
   * Does compaction tests where compaction is interrupted (and recovered) after copying a few records from an index
   * segment.
   * @throws Exception
   */
  private void doInterruptionDuringRecordCopyTest() throws Exception {
    for (long interruptAt : Arrays.asList(CuratedLogIndexState.PUT_RECORD_SIZE,
        -2 * CuratedLogIndexState.PUT_RECORD_SIZE)) {
      // no change before expiry time
      Pair<Long, List<String>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
      List<String> segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
      Map<String, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
      // if negative, set crash count starting from the end
      long countToInterruptAt = interruptAt;
      if (countToInterruptAt < 0) {
        // while copying each index segment, the bytes copied for the last record is not reported to the diskIOScheduler
        long unreported = getIndexSegmentStartOffsetsForLogSegments(segmentsUnderCompaction).size()
            * CuratedLogIndexState.PUT_RECORD_SIZE;
        countToInterruptAt += getValidDataSize(segmentsUnderCompaction, state.time.milliseconds()) - unreported;
      }
      // create a DiskIOScheduler
      DiskIOScheduler diskIOScheduler = new InterruptionInducingDiskIOScheduler(Integer.MAX_VALUE, countToInterruptAt);
      compactWithRecoveryAndVerify(state.log, diskIOScheduler, state.index, segmentsUnderCompaction,
          state.time.milliseconds(), false);
      verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

      // there will be changes past expiration time
      expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
      segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
      state.advanceTime(expiryTimeAndSegmentsUnderCompaction.getFirst() + Time.MsPerSec - state.time.milliseconds());
      countToInterruptAt = interruptAt;
      if (countToInterruptAt < 0) {
        // while copying each index segment, the bytes copied for the last record is not reported to the diskIOScheduler
        long unreported = getIndexSegmentStartOffsetsForLogSegments(segmentsUnderCompaction).size()
            * CuratedLogIndexState.PUT_RECORD_SIZE;
        countToInterruptAt += getValidDataSize(segmentsUnderCompaction, state.time.milliseconds()) - unreported;
      }
      diskIOScheduler = new InterruptionInducingDiskIOScheduler(Integer.MAX_VALUE, countToInterruptAt);
      compactWithRecoveryAndVerify(state.log, diskIOScheduler, state.index, segmentsUnderCompaction,
          state.time.milliseconds(), true);
    }
  }

  // compactWholeLogWithHardDeleteEnabledTest() helpers

  /**
   * Does compaction of the whole log with hard delete enabled.
   * @param shouldInduceInterruption {@code true} if an interruption has to be induced. {@code false} otherwise
   * @param interruptionDuringCopy interrupts during copy if {@code true}. Interrupts during log commit otherwise. Valid
   *                               only if {@code shouldInduceInterruption} is {@code true}.
   * @throws Exception
   */
  private void doCompactWholeLogWithHardDeleteEnabledTest(boolean shouldInduceInterruption,
      boolean interruptionDuringCopy) throws Exception {
    refreshState(true, true);
    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity() - 2;
    writeDataToMeetRequiredSegmentCount(requiredCount, null);
    // do some random deleting.
    int deleteCount = Math.min(state.liveKeys.size() / 3,
        (int) (1.8 * state.log.getSegmentCapacity() / CuratedLogIndexState.DELETE_RECORD_SIZE));
    List<MockId> allLiveKeys = new ArrayList<>(state.liveKeys);
    for (int i = 0; i < deleteCount; i++) {
      MockId idToDelete = allLiveKeys.remove(TestUtils.RANDOM.nextInt(allLiveKeys.size()));
      state.addDeleteEntry(idToDelete);
    }
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    List<String> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    assertTrue("Hard delete should be running", state.index.hardDeleter.isRunning());
    if (shouldInduceInterruption) {
      Log log = state.log;
      DiskIOScheduler diskIOScheduler = DISK_IO_SCHEDULER;
      if (interruptionDuringCopy) {
        int interruptAt = getIndexSegmentStartOffsetsForLogSegments(segmentsUnderCompaction).size() / 2 - 2;
        diskIOScheduler = new InterruptionInducingDiskIOScheduler(interruptAt, Integer.MAX_VALUE);
      } else {
        log = new InterruptionInducingLog(1, Integer.MAX_VALUE);
      }
      compactWithRecoveryAndVerify(log, diskIOScheduler, state.index, segmentsUnderCompaction,
          state.time.milliseconds(), true);
    } else {
      compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);
    }
    assertTrue("Hard delete should be running", state.index.hardDeleter.isRunning());
  }

  // support class helpers

  /**
   * Throws an exception if {@link #throwExceptionBeforeOperation} is {@code true}.
   */
  private void throwExceptionIfRequired() {
    if (throwExceptionBeforeOperation) {
      shutdownOrExceptionInduced = true;
      throw new RuntimeException(EXCEPTION_MSG);
    }
  }

  /**
   * Interrupts the compactor.
   * 1. Throws an exception if {@link #throwExceptionInsteadOfShutdown} is {@code true}.
   * 2. Shuts down the compactor otherwise.
   */
  private void shutdownCompactorOrThrowException() {
    shutdownOrExceptionInduced = true;
    if (throwExceptionInsteadOfShutdown) {
      throw new RuntimeException(EXCEPTION_MSG);
    }
    try {
      compactor.shutdown(0);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Extension of {@link DiskIOScheduler} that interrupts the compaction process based on provided parameters.
   */
  private class InterruptionInducingDiskIOScheduler extends DiskIOScheduler {
    private final int indexSegmentCountToCutoffAt;
    private final long numBytesToCutoffAt;

    private int indexSegmentsCopied = 0;
    private long numBytesCopied = 0;

    /**
     * Creates an instance of InterruptionInducingDiskIOScheduler.
     * @param indexSegmentCountToCutoffAt interrupts once these many index segments have been reported copied.
     * @param numBytesToCutoffAt interrupts once these many bytes have been reported copied.
     */
    InterruptionInducingDiskIOScheduler(int indexSegmentCountToCutoffAt, long numBytesToCutoffAt) {
      super(null);
      this.indexSegmentCountToCutoffAt = indexSegmentCountToCutoffAt;
      this.numBytesToCutoffAt = numBytesToCutoffAt;
    }

    @Override
    long getSlice(String jobType, String jobId, long usedSinceLastCall) {
      if (shutdownOrExceptionInduced) {
        throw new IllegalStateException("Should not be receiving a getSlice() call after shutdown has been induced");
      }
      if (jobType.equals(BlobStoreCompactor.INDEX_SEGMENT_READ_JOB_NAME)) {
        indexSegmentsCopied += usedSinceLastCall;
      } else if (jobType.equals(BlobStoreCompactor.LOG_SEGMENT_COPY_JOB_NAME)) {
        numBytesCopied += usedSinceLastCall;
      }
      if (indexSegmentsCopied == indexSegmentCountToCutoffAt || numBytesCopied >= numBytesToCutoffAt) {
        shutdownCompactorOrThrowException();
      }
      return Long.MAX_VALUE;
    }
  }

  /**
   * Extension of {@link PersistentIndex} that interrupts the compaction when index commit is being executed.
   */
  private class InterruptionInducingIndex extends PersistentIndex {

    InterruptionInducingIndex() throws StoreException {
      super(tempDirStr, state.scheduler, state.log, new StoreConfig(new VerifiableProperties(state.properties)),
          CuratedLogIndexState.STORE_KEY_FACTORY, state.recovery, state.hardDelete,
          new StoreMetrics(STORE_ID, new MetricRegistry()), state.time, state.sessionId, state.incarnationId);
    }

    @Override
    void changeIndexSegments(List<File> segmentFilesToAdd, Set<Offset> segmentsToRemove) throws StoreException {
      throwExceptionIfRequired();
      super.changeIndexSegments(segmentFilesToAdd, segmentsToRemove);
      shutdownCompactorOrThrowException();
    }
  }

  /**
   * Extension of {@link Log} that interrupts the compaction when a certain number of calls to
   * {@link #addSegment(LogSegment, boolean)} or {@link #dropSegment(String, boolean)} have been made.
   */
  private class InterruptionInducingLog extends Log {
    private final int addSegmentCallCountToInterruptAt;
    private final int dropSegmentCallCountToInterruptAt;

    private int segmentsAdded = 0;
    private int segmentsDropped = 0;

    /**
     * Creates an instance of InterruptionInducingLog.
     * @param addSegmentCallCountToInterruptAt number of allowed calls to {@link #addSegment(LogSegment, boolean)}.
     * @param dropSegmentCallCountToInterruptAt number of allowed calls to {@link #dropSegment(String, boolean)}.
     * @throws IOException
     */
    InterruptionInducingLog(int addSegmentCallCountToInterruptAt, int dropSegmentCallCountToInterruptAt)
        throws IOException {
      super(tempDirStr, state.log.getCapacityInBytes(), state.log.getSegmentCapacity(),
          new StoreMetrics(STORE_ID, new MetricRegistry()));
      if (addSegmentCallCountToInterruptAt <= 0 || dropSegmentCallCountToInterruptAt <= 0) {
        throw new IllegalArgumentException("Arguments cannot be <= 0");
      }
      this.addSegmentCallCountToInterruptAt = addSegmentCallCountToInterruptAt;
      this.dropSegmentCallCountToInterruptAt = dropSegmentCallCountToInterruptAt;
    }

    @Override
    void addSegment(LogSegment segment, boolean increaseUsedSegmentCount) {
      segmentsAdded++;
      if (segmentsAdded == addSegmentCallCountToInterruptAt) {
        throwExceptionIfRequired();
      }
      super.addSegment(segment, increaseUsedSegmentCount);
      if (segmentsAdded == addSegmentCallCountToInterruptAt) {
        shutdownCompactorOrThrowException();
      }
    }

    @Override
    void dropSegment(String segmentName, boolean decreaseUsedSegmentCount) throws IOException {
      segmentsDropped++;
      if (segmentsDropped == dropSegmentCallCountToInterruptAt) {
        throwExceptionIfRequired();
      }
      super.dropSegment(segmentName, decreaseUsedSegmentCount);
      if (segmentsDropped == dropSegmentCallCountToInterruptAt) {
        shutdownCompactorOrThrowException();
      }
    }
  }

  /**
   * A representation of a log entry.
   */
  private class LogEntry {
    /**
     * The ID of the entry.
     */
    MockId id;
    /**
     * The type of the entry.
     */
    PersistentIndex.IndexEntryType entryType;

    /**
     * Create an instance with {@code id} and {@code entryType}
     * @param id the {@link MockId} of the entry.
     * @param entryType the type of the entry.
     */
    LogEntry(MockId id, PersistentIndex.IndexEntryType entryType) {
      this.id = id;
      this.entryType = entryType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      LogEntry logEntry = (LogEntry) o;
      return id.equals(logEntry.id) && entryType == logEntry.entryType;
    }

    @Override
    public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + entryType.hashCode();
      return result;
    }
  }
}
