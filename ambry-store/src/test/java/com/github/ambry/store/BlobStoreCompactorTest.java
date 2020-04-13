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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.store.StoreTestUtils.*;
import static org.junit.Assert.*;


/**
 * Tests for {@link BlobStoreCompactor}.
 */
@RunWith(Parameterized.class)
public class BlobStoreCompactorTest {

  private static final String STORE_ID = "compactor_example_store";
  private static final DiskIOScheduler DISK_IO_SCHEDULER = new DiskIOScheduler(null);
  private static final String EXCEPTION_MSG = TestUtils.getRandomString(10);

  private final File tempDir;
  private final String tempDirStr;
  private final boolean doDirectIO;
  private final boolean withUndelete;
  private final StoreConfig config;

  private CuratedLogIndexState state = null;
  private BlobStoreCompactor compactor = null;

  // indicates whether any of InterruptionInducers induced the close/crash.
  private boolean closeOrExceptionInduced = false;
  // for InterruptionInducingLog and InterruptionInducingIndex, an exception is thrown after the operation
  // if throwExceptionBeforeOperation is not true.
  private boolean throwExceptionInsteadOfClose = false;
  // not applicable to the InterruptionInducingDiskIOScheduler. Throws in InterruptionInducingLog and
  // InterruptionInducingIndex irrespective of the value of throwExceptionInsteadOfClose
  private boolean throwExceptionBeforeOperation = false;

  private MetricRegistry metricRegistry;

  private byte[] bundleReadBuffer = new byte[((int) CuratedLogIndexState.PUT_RECORD_SIZE * 2 + 1)];

  /**
   * Running for both direct IO compactor and general IO compactor.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{true, false}, {false, false}, {false, true}});
  }

  /**
   * Creates a temporary directory for the store.
   * @throws Exception
   */
  public BlobStoreCompactorTest(boolean doDirectIO, boolean withUndelete) throws Exception {
    tempDir = StoreTestUtils.createTempDirectory("compactorDir-" + TestUtils.getRandomString(10));
    tempDirStr = tempDir.getAbsolutePath();
    config = new StoreConfig(new VerifiableProperties(new Properties()));
    this.doDirectIO = doDirectIO;
    this.withUndelete = withUndelete;
    if (doDirectIO) {
      Assume.assumeTrue(Utils.isLinux());
    }
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws Exception
   */
  @After
  public void cleanup() throws Exception {
    if (state != null) {
      state.destroy();
    }
    assertTrue(tempDir + " could not be cleaned", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Tests basic init/close.
   * @throws Exception
   */
  @Test
  public void initCloseTest() throws Exception {
    refreshState(false, true);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.initialize(state.index);
    compactor.close(0);
  }

  /**
   * Tests closing without initialization.
   * @throws Exception
   */
  @Test
  public void closeWithoutInitTest() throws Exception {
    refreshState(false, true);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.close(0);
  }

  /**
   * Tests attempt to use the service without initializing the service.
   * @throws Exception
   */
  @Test
  public void useServiceWithoutInitTest() throws Exception {
    refreshState(false, true);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    String firstSegmentName = state.log.getFirstSegment().getName();
    CompactionDetails details =
        new CompactionDetails(state.time.milliseconds(), Collections.singletonList(firstSegmentName));

    try {
      compactor.compact(details, bundleReadBuffer);
      fail("Should have failed to do anything because compactor has not been initialized");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }

    // create compaction log so that resumeCompaction() thinks there is a compaction in progress
    try (CompactionLog cLog = new CompactionLog(tempDirStr, STORE_ID, state.time, details, config)) {
      compactor.resumeCompaction(bundleReadBuffer);
      fail("Should have failed to do anything because compactor has not been initialized");
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
    String firstLogSegmentName = state.referenceIndex.firstKey().getName();
    String secondLogSegmentName = state.log.getNextSegment(state.log.getSegment(firstLogSegmentName)).getName();
    String lastLogSegmentName = state.referenceIndex.lastKey().getName();
    CompactionDetails details = new CompactionDetails(state.time.milliseconds() + Time.MsPerSec,
        Arrays.asList(firstLogSegmentName, lastLogSegmentName));
    ensureArgumentFailure(details, "Should have failed because compaction range contains offsets still in the journal");

    // compaction range contains segments in the wrong order
    details = new CompactionDetails(state.time.milliseconds() + Time.MsPerSec,
        Arrays.asList(secondLogSegmentName, firstLogSegmentName));
    ensureArgumentFailure(details, "Should have failed because segments are in the wrong order");

    // compaction contains segments that don't exist
    details = new CompactionDetails(0,
        Collections.singletonList(LogSegmentNameHelper.getNextPositionName(lastLogSegmentName)));
    ensureArgumentFailure(details, "Should have failed because compaction range contains offsets still in the journal");
  }

  /**
   * Tests to make sure that {@link BlobStoreCompactor#compact(CompactionDetails, byte[])} fails when a compaction is
   * already in progress.
   * @throws Exception
   */
  @Test
  public void compactWithCompactionInProgressTest() throws Exception {
    refreshState(false, true);
    List<String> segmentsUnderCompaction = getLogSegments(0, 2);
    CompactionDetails details = new CompactionDetails(0, segmentsUnderCompaction);

    // create a compaction log in order to mimic a compaction being in progress
    CompactionLog cLog = new CompactionLog(tempDirStr, STORE_ID, state.time, details, config);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
      fail("compact() should have failed because a compaction is already in progress");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    } finally {
      cLog.close();
      compactor.close(0);
    }
  }

  /**
   * Tests the case where {@link BlobStoreCompactor#resumeCompaction(byte[])} is called without any compaction being in
   * progress.
   * @throws Exception
   */
  @Test
  public void resumeCompactionWithoutAnyInProgressTest() throws Exception {
    refreshState(false, true);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.initialize(state.index);
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID));
    assertEquals("Temp log segment should not be found", 0, compactor.getSwapSegmentsInUse().length);
    try {
      compactor.resumeCompaction(bundleReadBuffer);
      fail("Should have failed because there is no compaction in progress");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    } finally {
      compactor.close(0);
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
    long deleteReferenceTimeMs = reduceValidDataSizeInLogSegments(segmentsUnderCompaction,
        state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE);
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, true);
  }

  /**
   * A test similar to basicTest but doesn't use bundleReadBuffer.
   * @throws Exception
   */
  @Test
  public void basicTestNoBundleReadBuffer() throws Exception {
    bundleReadBuffer = null;
    refreshState(false, true);
    List<String> segmentsUnderCompaction = getLogSegments(0, 2);
    long deleteReferenceTimeMs = reduceValidDataSizeInLogSegments(segmentsUnderCompaction,
        state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE);
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
   * Compacts the whole log multiple times with some data compacted each time.
   * @throws Exception
   */
  @Test
  public void compactWholeLogMultipleTimesTest() throws Exception {
    refreshState(false, true);
    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity() - 3;
    long expiryTimeMs = getInvalidationTime(requiredCount);
    List<Long> expiryTimesMs = Arrays.asList(state.time.milliseconds() / 2, expiryTimeMs, expiryTimeMs * 2);
    writeDataToMeetRequiredSegmentCount(requiredCount, expiryTimesMs);
    List<String> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    Set<MockId> idsInCompactedLogSegments = getIdsWithPutInSegments(segmentsUnderCompaction);

    for (long setTimeMs : expiryTimesMs) {
      if (state.time.milliseconds() < setTimeMs + Time.MsPerSec) {
        state.advanceTime(setTimeMs + Time.MsPerSec - state.time.milliseconds());
      }
      long deleteReferenceTimeMs = state.time.milliseconds();
      long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
      CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction);

      compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
      compactor.initialize(state.index);
      long logSegmentsBeforeCompaction = state.index.getLogSegmentCount();
      try {
        compactor.compact(details, bundleReadBuffer);
      } finally {
        compactor.close(0);
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
      verifySavedBytesCount(logSegmentsBeforeCompaction, 0);
    }
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
    // close in the middle of copying
    doCompactWholeLogWithHardDeleteEnabledTest(true, true);
    // crash in the middle of copying
    throwExceptionInsteadOfClose = true;
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
    Set<MockId> ids = getIdsWithPutInSegments(segmentsUnderCompaction);
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
    Set<MockId> idsInSegments = getIdsWithPutInSegments(expiryTimeAndSegmentsUnderCompaction.getSecond());
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
      state.index.getBlobReadInfo(id, EnumSet.of(StoreGetOptions.Store_Include_Deleted)).close();
    }
  }

  /**
   * Tests the case where deletes and expired blobs are interspersed and the expired blobs are eligible for cleanup
   * but deleted blobs (also includes blobs that have been put and deleted in the same index segment) are not.
   * @throws Exception
   */
  @Test
  public void interspersedDeletedAndExpiredBlobsTest() throws Exception {
    refreshState(false, false);
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(5));
    state.initIndex(null);

    int numFinalSegmentsCount = 3;
    long expiryTimeMs = getInvalidationTime(numFinalSegmentsCount + 1);
    // fill up one segment.
    writeDataToMeetRequiredSegmentCount(1, null);

    // IS 1.1 starts
    // 1. Put entry that contains a delete entry in the same index segment and is not counted as deleted.
    // won't be cleaned up.
    IndexEntry entry = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    assertEquals("There should have been a new log segment created", 2, state.index.getLogSegmentCount());
    String logSegmentName = entry.getValue().getOffset().getName();
    MockId delUnexpPutSameIdxSegId = (MockId) entry.getKey();
    state.addDeleteEntry(delUnexpPutSameIdxSegId);

    // 2. Put entry that has expired and contains a delete entry in the same index segment. Does not count as deleted
    // but is expired.
    // will be cleaned up, but delete record remains
    MockId delExpPutSameIdxSegId =
        (MockId) state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, 0).get(0).getKey();
    state.addDeleteEntry(delExpPutSameIdxSegId);

    // 3. Put entry that will be deleted.
    // won't be cleaned up.
    MockId willBeDelPut =
        (MockId) state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0).getKey();
    // IS 1.1 ends

    // roll over. IS 1.2 starts
    // 4. Put entry that has expired.
    // will be cleaned up.
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, 0).get(0).getKey();

    // 5. Put entry with an expiry time  that is not expired and has a delete entry in the same index segment and is not
    // counted as deleted or expired. In the compacted log, the put entry will be in one index segment and the delete
    // in another
    // won't be cleaned up.
    MockId delUnexpPutDiffIdxSegId =
        (MockId) state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiryTimeMs).get(0).getKey();
    state.addDeleteEntry(delUnexpPutDiffIdxSegId);

    // 6. Put entry that will expire (but is valid right now).
    // won't be cleaned up.
    MockId willExpPut =
        (MockId) state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiryTimeMs).get(0).getKey();

    // 7. Delete entry for an id that is in another index segment
    // won't be cleaned up.
    state.addDeleteEntry(willBeDelPut);
    // IS 1.2 ends

    // rollover. IS 1.3 starts
    // 8. Delete entry for an id that is in another log segment
    // won't be cleaned up.
    MockId idFromAnotherSegment = state.getIdToDeleteFromLogSegment(state.log.getFirstSegment(), false);
    state.addDeleteEntry(idFromAnotherSegment);

    // 9. Delete entry for a Put entry that doesn't exist. However, if it existed, it wouldn't have been eligible for
    // cleanup
    // the delete record itself won't be cleaned up
    MockId uniqueId = state.getUniqueId();
    state.addDeleteEntry(uniqueId,
        new MessageInfo(uniqueId, Integer.MAX_VALUE, expiryTimeMs, uniqueId.getAccountId(), uniqueId.getContainerId(),
            state.time.milliseconds()));

    // fill up the rest of the segment + one more
    writeDataToMeetRequiredSegmentCount(numFinalSegmentsCount, null);
    // reload index to keep the journal only in the last log segment
    state.reloadIndex(true, false);

    long deleteReferenceTimeMs = 0;
    List<String> segmentsUnderCompaction = Collections.singletonList(logSegmentName);
    long endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();
    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.initialize(state.index);
    long logSegmentCountBeforeCompaction = state.index.getLogSegmentCount();
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }

    String compactedLogSegmentName = LogSegmentNameHelper.getNextGenerationName(logSegmentName);
    LogSegment compactedLogSegment = state.log.getSegment(compactedLogSegmentName);
    long cleanedUpSize = 2 * CuratedLogIndexState.PUT_RECORD_SIZE;
    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());

    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE);
    // get the first index segment that refers to the compacted segment
    IndexSegment indexSegment =
        state.index.getIndexSegments().get(new Offset(compactedLogSegmentName, compactedLogSegment.getStartOffset()));
    List<IndexEntry> indexEntries = new ArrayList<>();
    assertTrue("Should have got some index entries",
        indexSegment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), true));
    assertEquals("There should be 5 index entries returned", 4, indexEntries.size());
    indexEntries.sort(PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);

    long logSegmentStartOffset = compactedLogSegment.getStartOffset();
    long currentExpectedOffset = logSegmentStartOffset + CuratedLogIndexState.PUT_RECORD_SIZE;
    // first index entry should be a delete and it should have an original message offset
    verifyIndexEntry(indexEntries.get(0), delUnexpPutSameIdxSegId, currentExpectedOffset,
        CuratedLogIndexState.DELETE_RECORD_SIZE, Utils.Infinite_Time, true, logSegmentStartOffset);
    currentExpectedOffset += CuratedLogIndexState.DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(1), delExpPutSameIdxSegId, currentExpectedOffset,
        CuratedLogIndexState.DELETE_RECORD_SIZE, 0, true, IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET);
    currentExpectedOffset += CuratedLogIndexState.DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(2), willBeDelPut, currentExpectedOffset, CuratedLogIndexState.PUT_RECORD_SIZE,
        Utils.Infinite_Time, false, currentExpectedOffset);
    long willBeDelOffset = currentExpectedOffset;
    currentExpectedOffset += CuratedLogIndexState.PUT_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(3), delUnexpPutDiffIdxSegId, currentExpectedOffset,
        CuratedLogIndexState.PUT_RECORD_SIZE, expiryTimeMs, false, currentExpectedOffset);
    currentExpectedOffset += CuratedLogIndexState.PUT_RECORD_SIZE;

    // get the second index segment
    indexSegment = state.index.getIndexSegments().higherEntry(indexSegment.getStartOffset()).getValue();
    indexEntries.clear();
    assertTrue("Should have got some index entries",
        indexSegment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), true));
    assertEquals("There should be 5 index entries returned", 5, indexEntries.size());
    indexEntries.sort(PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), delUnexpPutDiffIdxSegId, currentExpectedOffset,
        CuratedLogIndexState.DELETE_RECORD_SIZE, expiryTimeMs, true,
        currentExpectedOffset - CuratedLogIndexState.PUT_RECORD_SIZE);
    currentExpectedOffset += CuratedLogIndexState.DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(1), willExpPut, currentExpectedOffset, CuratedLogIndexState.PUT_RECORD_SIZE,
        expiryTimeMs, false, currentExpectedOffset);
    currentExpectedOffset += CuratedLogIndexState.PUT_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(2), willBeDelPut, currentExpectedOffset, CuratedLogIndexState.DELETE_RECORD_SIZE,
        Utils.Infinite_Time, true, willBeDelOffset);
    currentExpectedOffset += CuratedLogIndexState.DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(3), idFromAnotherSegment, currentExpectedOffset,
        CuratedLogIndexState.DELETE_RECORD_SIZE, Utils.Infinite_Time, true, IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET);
    currentExpectedOffset += CuratedLogIndexState.DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(4), uniqueId, currentExpectedOffset, CuratedLogIndexState.DELETE_RECORD_SIZE,
        expiryTimeMs, true, IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET);

    // no clean shutdown file should exist
    assertFalse("Clean shutdown file not deleted",
        new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
    // there should be no temp files
    assertEquals("There are some temp log segments", 0,
        tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);
    verifySavedBytesCount(logSegmentCountBeforeCompaction, 0);
  }

  /**
   * Tests the case where there is an interruption (crash/close) of compaction during log commit or cleanup.
   * @throws Exception
   */
  @Test
  public void interruptionDuringLogCommitAndCleanupTest() throws Exception {
    // close testing
    // close during commit
    doTestWithInterruptionInducingLog(1, Integer.MAX_VALUE);
    // close during cleanup
    doTestWithInterruptionInducingLog(Integer.MAX_VALUE, 1);

    // crash testing
    // crash after executing operation
    throwExceptionInsteadOfClose = true;
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
   * Tests the case where there is an interruption (crash/close) of compaction during index commit.
   * @throws Exception
   */
  @Test
  public void interruptionDuringIndexCommitTest() throws Exception {
    // close testing
    doInterruptionDuringIndexCommitTest();
    // crash testing
    // crash after executing operation
    throwExceptionInsteadOfClose = true;
    throwExceptionBeforeOperation = false;
    doInterruptionDuringIndexCommitTest();
    // crash before executing operation
    throwExceptionBeforeOperation = true;
    doInterruptionDuringIndexCommitTest();
  }

  /**
   * Tests the case where there is an interruption (crash/close) of compaction during copy after a few index segments
   * have been processed.
   * @throws Exception
   */
  @Test
  public void interruptionDuringOrAfterIndexSegmentProcessingTest() throws Exception {
    // close testing
    doInterruptionDuringOrAfterIndexSegmentProcessingTest();
    // crash testing
    // crash after executing operation
    throwExceptionInsteadOfClose = true;
    throwExceptionBeforeOperation = false;
    doInterruptionDuringOrAfterIndexSegmentProcessingTest();
    // crash before executing operation
    throwExceptionBeforeOperation = true;
    doInterruptionDuringOrAfterIndexSegmentProcessingTest();
  }

  /**
   * Tests the case where there is an interruption (crash/close) of compaction during copy when a few records from
   * an index segment have been copied over.
   * @throws Exception
   */
  @Test
  public void interruptionDuringRecordCopyTest() throws Exception {
    // close testing
    doInterruptionDuringRecordCopyTest();
    // crash testing
    throwExceptionInsteadOfClose = true;
    doInterruptionDuringRecordCopyTest();
  }

  /**
   * Tests the case where there is an interruption (crash/close) of compaction during log commit of the very last
   * cycle of compaction (tests the case where compaction finishes in {@link BlobStoreCompactor#fixStateIfRequired()}.
   * @throws Exception
   */
  @Test
  public void interruptionDuringLastCommitTest() throws Exception {
    // keep hard delete enabled
    refreshState(true, true);
    List<String> segmentsUnderCompaction = getLogSegments(0, 2);
    long deleteReferenceTimeMs = reduceValidDataSizeInLogSegments(segmentsUnderCompaction,
        state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE);
    throwExceptionBeforeOperation = true;
    Log log = new InterruptionInducingLog(1, Integer.MAX_VALUE);
    assertTrue("Hard delete should be running", state.index.hardDeleter.isRunning());
    compactWithRecoveryAndVerify(log, DISK_IO_SCHEDULER, state.index, segmentsUnderCompaction, deleteReferenceTimeMs,
        true, false);
    assertTrue("Hard delete should be running", state.index.hardDeleter.isRunning());
  }

  /**
   * Tests compaction on a log that has all combinations of PUT and DELETE records.
   * @throws Exception
   */
  @Test
  public void allEntryTypesTest() throws Exception {
    // NOTE: There is no need to add tests for cases here that are already covered by CuratedLogIndexState. This test
    // repeats a lot of that state (CuratedLogIndexState had some gaps before). Special cases for TTL updates covered
    // in a different test
    List<IndexEntry> otherPuts = new ArrayList<>();

    // types of records
    // put
    // p1 - no expiry, not deleted (retain)
    // p2 - not expired, not deleted (retain)
    // p3 - expired, not deleted (clean)
    // p4 - not expired, deleted, delete not in effect (retain)
    // p5 - not expired, deleted, delete in effect (clean)
    // p6 - expired, deleted, delete not in effect (clean)
    // p7 - expired, deleted, delete in effect (clean)
    // p8 - no expiry, deleted, delete not in effect (retain)
    // p9 - no expiry, deleted, delete in effect (clean)
    // delete
    // d1 - put in the same index segment
    // d2 - put in the same log segment but in diff index segment
    // d3 - put in a diff log segment (also in diff index segment as a result)

    // note on the naming of index entry variables
    // index entry variables will be named as puttype,deletetype
    // for example, index entry for type p4 that will have a delete record of type d2 will be named p4d2

    refreshState(false, false);
    state.properties.setProperty("store.index.max.number.of.inmem.elements", "5");
    state.reloadIndex(true, false);
    long notExpiredMs = state.time.milliseconds() + TimeUnit.SECONDS.toMillis(Short.MAX_VALUE);
    long expiredMs = state.time.milliseconds();

    // LS (Log Segment) 0
    // IS (Index Segment) 0.1
    IndexEntry p1 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    IndexEntry p2 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p3 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p5d1 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, notExpiredMs).get(0);
    state.addDeleteEntry((MockId) p5d1.getKey());
    // IS 0.2
    IndexEntry p5d2 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p7d1 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiredMs).get(0);
    state.addDeleteEntry((MockId) p7d1.getKey());
    IndexEntry p9d1 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p9d1.getKey());
    // IS 0.3
    IndexEntry p7d2 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p9d2 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    IndexEntry p5d3 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p7d3 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p9d3 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    // IS 0.4
    state.addDeleteEntry((MockId) p5d2.getKey());
    state.addDeleteEntry((MockId) p7d2.getKey());
    state.addDeleteEntry((MockId) p9d2.getKey());
    long lastRecSize = state.log.getSegmentCapacity() - state.index.getCurrentEndOffset().getOffset();
    IndexEntry other = state.addPutEntries(1, lastRecSize, Utils.Infinite_Time).get(0);
    otherPuts.add(other);

    // LS 1
    // IS 1.1
    state.addDeleteEntry((MockId) p5d3.getKey());
    state.addDeleteEntry((MockId) p7d3.getKey());
    state.addDeleteEntry((MockId) p9d3.getKey());
    otherPuts.addAll(state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time));
    otherPuts.addAll(state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time));
    // this is where we cut off the time for compaction (retention time)
    long deleteReferenceTimeMs = state.time.milliseconds() + TimeUnit.SECONDS.toMillis(1);
    // IS 1.2
    IndexEntry p4d1 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, notExpiredMs).get(0);
    state.addDeleteEntry((MockId) p4d1.getKey());
    IndexEntry p6d1 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiredMs).get(0);
    state.addDeleteEntry((MockId) p6d1.getKey());
    IndexEntry p4d2 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, notExpiredMs).get(0);
    // IS 1.3
    IndexEntry p8d1 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p8d1.getKey());
    state.addDeleteEntry((MockId) p4d2.getKey());
    IndexEntry p6d2 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p8d2 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    // IS 1.4
    state.addDeleteEntry((MockId) p6d2.getKey());
    state.addDeleteEntry((MockId) p8d2.getKey());
    IndexEntry p4d3 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p6d3 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p8d3 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    // IS 1.5
    lastRecSize = state.log.getSegmentCapacity() - state.index.getCurrentEndOffset().getOffset();
    otherPuts.addAll(state.addPutEntries(1, lastRecSize, Utils.Infinite_Time));

    // LS 2
    // IS 2.1
    state.addDeleteEntry((MockId) p4d3.getKey());
    state.addDeleteEntry((MockId) p6d3.getKey());
    state.addDeleteEntry((MockId) p8d3.getKey());

    // get everything except the last log segment entries out of the journal
    state.reloadIndex(true, false);
    List<String> segmentsUnderCompaction = getLogSegments(0, 2);
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction);

    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.initialize(state.index);
    long logSegmentCountBeforeCompaction = state.index.getLogSegmentCount();
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    assertFalse("Sum of size of log segments did not change after compaction",
        logSegmentSizeSumBeforeCompaction == getSumOfLogSegmentEndOffsets());

    // check all delete records to make sure they remain
    for (MockId deletedKey : state.deletedKeys) {
      assertTrue(deletedKey + " should be deleted", state.index.findKey(deletedKey).isDelete());
      checkIndexValue(deletedKey);
    }

    // make sure all of otherPuts and p1 and p2 are ok.
    otherPuts.add(p1);
    otherPuts.add(p2);
    for (IndexEntry entry : otherPuts) {
      MockId id = (MockId) entry.getKey();
      assertFalse(id + " should not be deleted", state.index.findKey(id).isDelete());
      checkIndexValue(id);
      try (BlobReadOptions options = state.index.getBlobReadInfo(id, EnumSet.noneOf(StoreGetOptions.class))) {
        checkRecord(id, options);
      }
    }

    // p3 should not be found
    assertNull("There should be no record of " + p3.getKey(), state.index.findKey(p3.getKey()));

    // no p5, p6, p7, p9 records
    IndexEntry[] cleaned = {p5d1, p5d2, p5d3, p6d1, p6d2, p6d3, p7d1, p7d2, p7d3, p9d1, p9d2, p9d3};
    for (IndexEntry entry : cleaned) {
      MockId id = (MockId) entry.getKey();
      IndexValue value = state.index.findKey(id);
      // the delete record should remain
      assertTrue(id + " should be deleted", value.isDelete());
      // the put record should be cleaned up
      assertEquals("There should no original message offset", IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET,
          value.getOriginalMessageOffset());
      try {
        state.index.getBlobReadInfo(id, EnumSet.allOf(StoreGetOptions.class));
        fail("Should not be able to GET " + id);
      } catch (StoreException e) {
        assertEquals(id + " failed with error code " + e.getErrorCode(), StoreErrorCodes.ID_Deleted, e.getErrorCode());
      }
    }

    // put records of p4, p8 should remain
    IndexEntry[] retained = {p4d1, p4d2, p4d3, p8d1, p8d2, p8d3};
    for (IndexEntry entry : retained) {
      MockId id = (MockId) entry.getKey();
      IndexValue value = state.index.findKey(id);
      // the delete record should remain
      assertTrue(id + " should be deleted", value.isDelete());
      // the put record however should not be cleaned up
      if (value.getOriginalMessageOffset() == IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET) {
        // PUT record should exist
        try (BlobReadOptions options = state.index.getBlobReadInfo(id, EnumSet.allOf(StoreGetOptions.class))) {
          checkRecord(id, options);
        }
      } else {
        // PUT record exists.
      }
    }
    verifySavedBytesCount(logSegmentCountBeforeCompaction, 0);
  }

  /**
   * Tests compaction on a log that contains a PUT record that has no corresponding entry in the index (this can happen
   * due to recovery corner cases - refer to recovery code in PersistentIndex.java).
   * @throws Exception
   */
  @Test
  public void orphanedPutRecordsTest() throws Exception {
    refreshState(false, false);
    // write a PUT record that will be "lost"
    MockId orphanedId =
        (MockId) state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0).getKey();
    // add a delete entry for  orphanedId
    state.addDeleteEntry(orphanedId);
    // get the index value and "lose" the PUT record. This works because we get a reference to the value in the index.
    IndexValue value = state.index.findKey(orphanedId);
    value.clearOriginalMessageOffset();
    // add a put entry that spans the rest of the log segment
    long lastRecSize = state.log.getSegmentCapacity() - state.index.getCurrentEndOffset().getOffset();
    state.addPutEntries(1, lastRecSize, Utils.Infinite_Time);
    // add an entry so that a new log segment is created
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    state.reloadIndex(true, false);
    List<String> segmentsUnderCompaction = getLogSegments(0, 1);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);

    // make sure the delete still exists
    checkIndexValue(orphanedId);
    // the first log segment should not contain the the PUT that had no index entry
    LogSegment firstLogSegment = state.log.getFirstSegment();
    long size = firstLogSegment.getEndOffset() - firstLogSegment.getStartOffset();
    assertEquals("Segment size not as expected", lastRecSize + CuratedLogIndexState.DELETE_RECORD_SIZE, size);
  }

  /**
   * Tests cases specific to TTL updates
   * @throws Exception
   */
  @Test
  public void ttlUpdateSpecificTest() throws Exception {
    // ensure that puts are not cleaned up if they have ttl updates
    Pair<List<MockId>, Long> idsAndExpiryTimeMs = createStateWithPutAndTtlUpdate();
    assertTrue("Current time should be beyond expiry time of blobs",
        state.time.milliseconds() > idsAndExpiryTimeMs.getSecond());
    List<String> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), false);

    // compact everything
    // using expire time as the ref time
    long expiryTimeMs = createStateWithPutTtlUpdateAndDelete();
    segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, expiryTimeMs, true);

    // using delete time as the ref time
    createStateWithPutTtlUpdateAndDelete();
    state.advanceTime(TimeUnit.SECONDS.toMillis(1));
    segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);

    // compact everything except the first log segment (all the TTL updates and deletes will be retained - no change)
    // using expire time as the ref time
    expiryTimeMs = createStateWithPutTtlUpdateAndDelete();
    segmentsUnderCompaction = getLogSegments(1, state.index.getLogSegmentCount() - 2);
    compactAndVerify(segmentsUnderCompaction, expiryTimeMs, false);

    // using delete time as the ref time
    createStateWithPutTtlUpdateAndDelete();
    state.advanceTime(TimeUnit.SECONDS.toMillis(1));
    segmentsUnderCompaction = getLogSegments(1, state.index.getLogSegmentCount() - 2);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), false);

    // segment that has only ttl updates and deletes (no corresponding puts). All the ttl updates should be cleaned up
    Set<MockId> ids = createStateWithTtlUpdatesAndDeletes();
    state.advanceTime(TimeUnit.SECONDS.toMillis(1));
    segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);

    // there should be no ttl updates in the final index (but the deletes should be there)
    Set<MockId> seenIds = new HashSet<>();
    List<IndexEntry> indexEntries = new ArrayList<>();
    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE);
    AtomicLong currentTotalSize = new AtomicLong(0);
    for (IndexSegment segment : state.index.getIndexSegments().values()) {
      if (LogSegmentNameHelper.getGeneration(segment.getLogSegmentName()) == 0) {
        break;
      }
      segment.getIndexEntriesSince(null, condition, indexEntries, currentTotalSize, false);
    }
    indexEntries.forEach(entry -> {
      assertTrue("There cannot be a non-delete entry", entry.getValue().isDelete());
      assertTrue("Every key should be seen only once", seenIds.add((MockId) entry.getKey()));
    });
    assertEquals("All ids not present", ids, seenIds);
  }

  /**
   * Tests some recovery scenarios related to TTL update records in particular
   * @throws Exception
   */
  @Test
  public void ttlUpdateSpecificRecoveryTest() throws Exception {
    bundleReadBuffer = null;
    // close testing
    doTtlUpdateSrcDupTest();
    doTtlUpdateTgtDupTest();
    // crash testing
    throwExceptionInsteadOfClose = true;
    doTtlUpdateSrcDupTest();
    doTtlUpdateTgtDupTest();
  }

  /**
   * Tests when the PUT, TTL_UPDATE, DELETE AND UNDELETE of the same key are in the same index segment.
   * @throws Exception
   */
  @Test
  public void undeleteSameIndexSegmentTest() throws Exception {
    Assume.assumeTrue(withUndelete);
    refreshState(false, false);
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(5));
    state.initIndex(null);

    // Log Segment 0
    // Index Segment 0.1 P T D U -> P T U
    long p1Expiration = state.time.milliseconds() + 10000;
    IndexEntry p1 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, p1Expiration).get(0);
    state.makePermanent((MockId) p1.getKey(), false);
    state.addDeleteEntry((MockId) p1.getKey());
    state.addUndeleteEntry((MockId) p1.getKey());
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.2 P D U -> P U
    IndexEntry p2 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p2.getKey());
    state.addUndeleteEntry((MockId) p2.getKey());
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.3 P(expired) D U -> []
    IndexEntry p3 =
        state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, state.time.milliseconds() + 10000).get(0);
    state.addDeleteEntry((MockId) p3.getKey());
    state.addUndeleteEntry((MockId) p3.getKey());
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.4 P D U D U -> P U
    IndexEntry p4 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p4.getKey());
    state.addUndeleteEntry((MockId) p4.getKey());
    state.addDeleteEntry((MockId) p4.getKey());
    state.addUndeleteEntry((MockId) p4.getKey());

    // Index Segment 0.5 P D D U -> P U
    IndexEntry p5 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p5.getKey());
    state.addDeleteEntry((MockId) p5.getKey(), null, (short) 3);
    state.addUndeleteEntry((MockId) p5.getKey());
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.6 P D U D -> D
    IndexEntry p6 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p6.getKey());
    state.addUndeleteEntry((MockId) p6.getKey());
    state.addDeleteEntry((MockId) p6.getKey());
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.7 P D D -> D
    IndexEntry p7 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p7.getKey());
    state.addDeleteEntry((MockId) p7.getKey(), null, (short) 3);
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.8 P U -> P U
    IndexEntry p8 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addUndeleteEntry((MockId) p8.getKey(), (short) 2);
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.9 P D U U-> P U
    IndexEntry p9 = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p9.getKey());
    state.addUndeleteEntry((MockId) p9.getKey());
    state.addUndeleteEntry((MockId) p9.getKey(), (short) 2);
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    // Make sure we have two log segments
    writeDataToMeetRequiredSegmentCount(2, null);
    state.reloadIndex(true, false);

    String logSegmentName = p1.getValue().getOffset().getName();
    List<String> segmentsUnderCompaction = Collections.singletonList(logSegmentName);
    long deleteReferenceTimeMs = state.time.milliseconds() + 10000;
    long endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();
    int indexSegmentCountBeforeCompaction = state.index.getIndexSegments().size();
    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.initialize(state.index);

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // Before compaction, the records in the log are
    // P1 T1 D1 U1 P| P2 D2 U2 P P| P3 D3 U3 P P| P4 D4 U4 D4 U4| P5 D5 D5 U5 P| P6 D6 U6 D6 P| P7 D7 D7 P P| P8 U8 P P P | P9 D9 U9 U9 P
    // After compaction, the records in the log are
    // P1 T1 U1 P P2| U2 P P P P| P4 U4 P5 U5 P| D6 P D7 P P| P8 U8 P P P| P9 U9 P

    long ds = CuratedLogIndexState.DELETE_RECORD_SIZE;
    long ps = CuratedLogIndexState.PUT_RECORD_SIZE;
    long us = CuratedLogIndexState.UNDELETE_RECORD_SIZE;
    long ts = CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE;
    long cleanedUpSize = ds + ds + (ps + ds + us) + (2 * ds + us) + 2 * ds + (ps + ds + us) + (ps + ds) + 0 + (ds + us);
    int indexSegmentDiff = 3;
    String compactedLogSegmentName = LogSegmentNameHelper.getNextGenerationName(logSegmentName);
    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());
    assertEquals("Index Segment not as expected after compaction", indexSegmentCountBeforeCompaction - indexSegmentDiff,
        state.index.getIndexSegments().size());

    LogSegment compactedLogSegment = state.log.getSegment(compactedLogSegmentName);
    ConcurrentSkipListMap<Offset, IndexSegment> indexSegments = state.index.getIndexSegments();
    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE);
    long currentExpectedOffset = compactedLogSegment.getStartOffset();

    // Get the entries in the first segment
    IndexSegment segment = indexSegments.get(new Offset(compactedLogSegmentName, compactedLogSegment.getStartOffset()));
    List<IndexEntry> indexEntries = new ArrayList<>();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p1.getKey(), currentExpectedOffset, ps, p1Expiration, false, false,
        false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(1), (MockId) p1.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 0);
    currentExpectedOffset += ts;
    verifyIndexEntry(indexEntries.get(2), (MockId) p1.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        true, true, (short) 1);
    currentExpectedOffset += us + ps; // skip one put
    verifyIndexEntry(indexEntries.get(4), (MockId) p2.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps; // skip one put

    // Get the entries in the second segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p2.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 1);
    currentExpectedOffset += us + 4 * ps; // skip 4 puts

    // Get the entries in the third segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p4.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(1), (MockId) p4.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 2);
    currentExpectedOffset += us;
    verifyIndexEntry(indexEntries.get(2), (MockId) p5.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(3), (MockId) p5.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 4);
    currentExpectedOffset += us + ps; // skip one put

    // Get the entries in the fourth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p6.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        false, false, (short) 1);
    currentExpectedOffset += ds + ps; // skip one put
    verifyIndexEntry(indexEntries.get(2), (MockId) p7.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        false, false, (short) 3);
    currentExpectedOffset += ds + 2 * ps; // skip two puts

    // Get the entries in the fifth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p8.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(1), (MockId) p8.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 2);
    currentExpectedOffset += us + 3 * ps; // skip 3 puts

    // Get the entries in the sixth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p9.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(1), (MockId) p9.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 2);

    // no clean shutdown file should exist
    assertFalse("Clean shutdown file not deleted",
        new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
    // there should be no temp files
    assertEquals("There are some temp log segments", 0,
        tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);
  }

  /**
   * Tests when the PUT DELETE and UNDELETE are located in different log segments.
   * @throws Exception
   */
  @Test
  public void undeleteCrossLogSegmentTest() throws Exception {
    Assume.assumeTrue(withUndelete);
    refreshState(false, false);
    int numPuts = (int) Math.floorDiv(state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE,
        CuratedLogIndexState.PUT_RECORD_SIZE + (CuratedLogIndexState.DELETE_RECORD_SIZE) / 2) - 1;
    List<IndexEntry> entries = state.addPutEntries(numPuts, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    for (IndexEntry entry : entries) {
      state.addDeleteEntry((MockId) entry.getKey());
    }
    writeDataToMeetRequiredSegmentCount(2, null);
    for (IndexEntry entry : entries) {
      state.addUndeleteEntry((MockId) entry.getKey());
    }
    writeDataToMeetRequiredSegmentCount(4, null);
    state.reloadIndex(true, false);

    // Here we have several PUTs in the first log segments and half of the DELETEs in the first log segment
    // and the other half in the second. Then we have all the UNDELETEs in the third log segment.
    List<String> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);
  }

  // helpers

  // general

  /**
   * Refreshes the state by destroying any existing state and creating a new one.
   * @param hardDeleteEnabled {@code true} if hard delete needs to be enabled. {@code false} otherwise.
   * @param initState {@code true} if some test state has to be created. {@code false} otherwise.
   * @throws Exception
   */
  private void refreshState(boolean hardDeleteEnabled, boolean initState) throws Exception {
    if (state != null) {
      state.destroy();
    }
    state = new CuratedLogIndexState(true, tempDir, hardDeleteEnabled, initState, true, withUndelete);
  }

  /**
   * Gets an instance of {@link BlobStoreCompactor}.
   * @param log the {@link Log} instance to use.
   * @param ioScheduler the {@link DiskIOScheduler} instance to use.
   * @return an instance of {@link BlobStoreCompactor}.
   * @throws IOException
   * @throws StoreException
   */
  private BlobStoreCompactor getCompactor(Log log, DiskIOScheduler ioScheduler) throws IOException, StoreException {
    closeOrExceptionInduced = false;
    if (doDirectIO) {
      state.properties.put("store.compaction.enable.direct.io", "true");
    }
    if (withUndelete) {
      state.properties.put("store.compaction.filter", "IndexSegmentValidEntryWithUndelete");
    }
    StoreConfig config = new StoreConfig(new VerifiableProperties(state.properties));
    metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    return new BlobStoreCompactor(tempDirStr, STORE_ID, CuratedLogIndexState.STORE_KEY_FACTORY, config, metrics,
        metrics, ioScheduler, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR, log, state.time, state.sessionId,
        state.incarnationId);
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
   * Gets all the ids that have PUTs in {@code logSegmentNames}.
   * @param logSegmentNames the names of log segments whose IDs are required.
   * @return all the ids that have PUTs in {@code logSegmentNames}.
   */
  private Set<MockId> getIdsWithPutInSegments(List<String> logSegmentNames) {
    Set<MockId> idsInCompactedLogSegments = new HashSet<>();
    for (String segmentName : logSegmentNames) {
      idsInCompactedLogSegments.addAll(state.getIdsWithPutsInLogSegment(state.log.getSegment(segmentName)));
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
          state.time.milliseconds(), getFileSpanForLogSegments(logSegments));
    }
    return size;
  }

  /**
   * Writes data so that the log contains {@code countRequired} number of log segments. If a non {@code null}
   * {@code expiryTimes} is passed, a fraction of the blobs written will contain those expiry times in round robin order
   * @param countRequired the number of log segments required.
   * @param expiryTimes the expiry times desired. A fraction of the blobs written will contain those expiry times in
   *                    round robin order
   * @throws StoreException
   */
  private void writeDataToMeetRequiredSegmentCount(long countRequired, List<Long> expiryTimes) throws StoreException {
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
   * @return the time (in ms) at which all the deletes are valid.
   * @throws IOException
   * @throws StoreException
   */
  private long reduceValidDataSizeInLogSegments(List<String> logSegmentsToReduceFrom, long ceilingSize)
      throws IOException, StoreException {
    List<String> logSegments = new ArrayList<>(logSegmentsToReduceFrom);
    long validDataSize = getValidDataSize(logSegmentsToReduceFrom, state.time.milliseconds());
    while (validDataSize > ceilingSize) {
      assertTrue("There are no more segments to delete data from", logSegments.size() > 0);
      int selectedIdx = TestUtils.RANDOM.nextInt(logSegments.size());
      String segmentToDeleteFrom = logSegments.get(selectedIdx);
      MockId idToDelete = state.getIdToDeleteFromLogSegment(state.log.getSegment(segmentToDeleteFrom), false);
      if (idToDelete == null) {
        logSegments.remove(selectedIdx);
      } else {
        state.addDeleteEntry(idToDelete);
        validDataSize = getValidDataSize(logSegmentsToReduceFrom, state.time.milliseconds() + 1);
      }
    }
    state.advanceTime(Time.MsPerSec);
    return state.time.milliseconds();
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
    Set<MockId> idsInCompactedLogSegments = getIdsWithPutInSegments(segmentsUnderCompaction);

    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.initialize(state.index);

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }

    assertFalse("No compaction should be in progress", CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID));
    assertEquals("Swap segments should not be found", 0, compactor.getSwapSegmentsInUse().length);
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
    verifySavedBytesCount(logSegmentCountBeforeCompaction, 0);
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
   * @param checkSavedBytesReported {@code true} if the metric reporting saved bytes has to be checked. {@code false}
   *                                            otherwise.
   * @throws Exception
   */
  private void compactWithRecoveryAndVerify(Log log, DiskIOScheduler diskIOScheduler, PersistentIndex index,
      List<String> segmentsUnderCompaction, long deleteReferenceTimeMs, boolean changeExpected,
      boolean checkSavedBytesReported) throws Exception {
    // getting these from the "real" log and indexes
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    long logSegmentCountBeforeCompaction = state.index.getLogSegmentCount();
    long indexSegmentCountBeforeCompaction = state.index.getIndexSegments().size();

    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction);
    long expectedValidDataSize = getValidDataSize(segmentsUnderCompaction, deleteReferenceTimeMs);
    List<String> unaffectedSegments = getUnaffectedSegments(segmentsUnderCompaction);
    List<LogEntry> validLogEntriesInOrder = getValidLogEntriesInOrder(segmentsUnderCompaction, deleteReferenceTimeMs);
    Set<MockId> idsInCompactedLogSegments = getIdsWithPutInSegments(segmentsUnderCompaction);

    compactor = getCompactor(log, diskIOScheduler);
    compactor.initialize(index);

    try {
      compactor.compact(details, bundleReadBuffer);
      if (throwExceptionInsteadOfClose) {
        fail("Compact should have thrown exception");
      }
    } catch (RuntimeException e) {
      assertEquals("Exception not as expected", EXCEPTION_MSG, e.getMessage());
    } finally {
      assertTrue("Close was not induced by the test", closeOrExceptionInduced);
      assertTrue("There should be a compaction in progress",
          CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID));
      compactor.close(0);
    }

    // record any bytes saved because index is going to reinit the metric registry
    long savedBytesReported = metricRegistry.getCounters()
        .get(MetricRegistry.name(BlobStoreCompactor.class, "CompactionBytesReclaimedCount"))
        .getCount();
    // have to reload log since the instance changed by the old compactor is different.
    state.reloadLog(false);
    // use the "real" log, index and disk IO schedulers this time.
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    state.initIndex(null);
    compactor.initialize(state.index);
    assertEquals("Wrong number of swap segments in use",
        tempDir.list(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length, compactor.getSwapSegmentsInUse().length);
    try {
      if (CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID)) {
        compactor.resumeCompaction(bundleReadBuffer);
      }
    } finally {
      compactor.close(0);
    }

    verifyCompaction(segmentsUnderCompaction, unaffectedSegments, expectedValidDataSize, validLogEntriesInOrder,
        idsInCompactedLogSegments, deleteReferenceTimeMs);
    checkVitals(changeExpected, logSegmentSizeSumBeforeCompaction, logSegmentCountBeforeCompaction,
        indexSegmentCountBeforeCompaction);
    if (checkSavedBytesReported) {
      verifySavedBytesCount(logSegmentCountBeforeCompaction, savedBytesReported);
    }
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
      // Compaction remove some of the records in the log, so the end of the compacted log should be less than the
      // original one.
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
   * @param idsInCompactedLogSegments the ids that had PUT records in the segments that were compacted.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @throws IOException
   * @throws StoreException
   */
  private void verifyCompaction(List<String> segmentsCompacted, List<String> unaffectedSegments,
      long targetSegmentsExpectedValidSize, List<LogEntry> validLogEntriesInOrder,
      Set<MockId> idsInCompactedLogSegments, long deleteReferenceTimeMs) throws IOException, StoreException {
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
    // TODO: verify that the number of index segments in mem == on disk.
    Offset lastIndexSegmentStartOffset = state.index.getIndexSegments().lastKey();
    for (Map.Entry<Offset, IndexSegment> indexSegmentEntry : state.index.getIndexSegments().entrySet()) {
      Offset indexSegmentStartOffset = indexSegmentEntry.getKey();
      assertTrue("Index segment does not refer to any active log segments",
          allSegmentNames.contains(indexSegmentEntry.getKey().getName()));
      assertEquals("Index segment sealed state not as expected",
          !indexSegmentStartOffset.equals(lastIndexSegmentStartOffset), indexSegmentEntry.getValue().isSealed());
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
   * @param idsInCompactedLogSegments the ids that had PUT records in the segments that were compacted.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @throws IOException
   * @throws StoreException
   */
  private void verifyDataPostCompaction(Set<MockId> idsInCompactedLogSegments, long deleteReferenceTimeMs)
      throws IOException, StoreException {
    for (MockId id : state.allKeys.keySet()) {
      if (state.liveKeys.contains(id)) {
        BlobReadOptions options = state.index.getBlobReadInfo(id, EnumSet.noneOf(StoreGetOptions.class));
        checkRecord(id, options);
        options.close();
        checkIndexValue(id);
      } else if (state.expiredKeys.contains(id)) {
        boolean shouldBeCompacted =
            idsInCompactedLogSegments.contains(id) && state.isExpiredAt(id, state.time.milliseconds());
        try {
          state.index.getBlobReadInfo(id, EnumSet.noneOf(StoreGetOptions.class));
          fail("Should not be able to GET " + id);
        } catch (StoreException e) {
          StoreErrorCodes expectedErrorCode = state.deletedKeys.contains(id) ? StoreErrorCodes.ID_Deleted
              : shouldBeCompacted ? StoreErrorCodes.ID_Not_Found : StoreErrorCodes.TTL_Expired;
          assertEquals(id + " failed with error code " + e.getErrorCode(), expectedErrorCode, e.getErrorCode());
        }
        try {
          BlobReadOptions options = state.index.getBlobReadInfo(id, EnumSet.allOf(StoreGetOptions.class));
          if (shouldBeCompacted) {
            fail("Should not be able to GET " + id);
          } else {
            checkRecord(id, options);
            options.close();
            checkIndexValue(id);
          }
        } catch (StoreException e) {
          assertTrue("Blob for " + id + " should have been retrieved", shouldBeCompacted);
          StoreErrorCodes expectedErrorCode =
              state.deletedKeys.contains(id) ? StoreErrorCodes.ID_Deleted : StoreErrorCodes.ID_Not_Found;
          assertEquals(id + " failed with error code " + e.getErrorCode(), expectedErrorCode, e.getErrorCode());
        }
      } else if (state.deletedKeys.contains(id)) {
        IndexValue latestValue = state.getExpectedValue(id, false);
        boolean shouldBeAbsent = state.getExpectedValue(id, true) == null || (idsInCompactedLogSegments.contains(id)
            && latestValue.getOperationTimeInMs() < deleteReferenceTimeMs);
        try {
          state.index.getBlobReadInfo(id, EnumSet.noneOf(StoreGetOptions.class));
          fail("Should not be able to GET " + id);
        } catch (StoreException e) {
          assertEquals(id + " failed with error code " + e.getErrorCode(), StoreErrorCodes.ID_Deleted,
              e.getErrorCode());
        }
        try {
          BlobReadOptions options = state.index.getBlobReadInfo(id, EnumSet.allOf(StoreGetOptions.class));
          if (shouldBeAbsent) {
            fail("Should not be able to GET " + id);
          } else {
            checkRecord(id, options);
            options.close();
            checkIndexValue(id);
          }
        } catch (StoreException e) {
          assertTrue("Blob for " + id + " should have been retrieved", shouldBeAbsent);
          assertEquals(id + " failed with error code " + e.getErrorCode(), StoreErrorCodes.ID_Deleted,
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
              state.time.milliseconds(), getFileSpanForLogSegments(logSegmentsUnderConsideration));
      addToLogEntriesInOrder(validIndexEntries, validLogEntriesInOrder);
    }
    return validLogEntriesInOrder;
  }

  /**
   * @param logSegments the log names of the log segments that should be under the returned {@link FileSpan}
   * @return a {@link FileSpan} that covers the given {@code logSegments}
   */
  private FileSpan getFileSpanForLogSegments(List<String> logSegments) {
    LogSegment first = state.log.getSegment(logSegments.get(0));
    LogSegment last = state.log.getSegment(logSegments.get(logSegments.size() - 1));
    Offset start = new Offset(first.getName(), first.getStartOffset());
    Offset end = new Offset(last.getName(), last.getEndOffset());
    return new FileSpan(start, end);
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
          indexSegment.find(info.getStoreKey())
              .forEach(value -> indexEntries.add(new IndexEntry(info.getStoreKey(), value)));
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
      PersistentIndex.IndexEntryType entryType = PersistentIndex.IndexEntryType.PUT;
      if (entry.getValue().isDelete()) {
        entryType = PersistentIndex.IndexEntryType.DELETE;
      } else if (entry.getValue().isUndelete()) {
        entryType = PersistentIndex.IndexEntryType.UNDELETE;
      } else if (entry.getValue().isTtlUpdate()) {
        entryType = PersistentIndex.IndexEntryType.TTL_UPDATE;
      }
      logEntriesInOrder.add(new LogEntry(id, entryType, entry.getValue().getSize()));
    }
  }

  /**
   * Checks the record of an id by ensuring that the {@link BlobReadOptions} received from the store matches what is
   * expected. Also checks the data if the key is not deleted and hard delete enabled.
   * @param id the {@link MockId} whose record needs to be checked.
   * @param options the {@link BlobReadOptions} received from the {@link PersistentIndex}.
   * @throws IOException
   */
  private void checkRecord(MockId id, BlobReadOptions options) throws IOException {
    MessageReadSet readSet = new StoreMessageReadSet(Arrays.asList(options));
    IndexValue value = state.getExpectedValue(id, true);
    assertEquals("Unexpected key in BlobReadOptions", id, options.getMessageInfo().getStoreKey());
    assertEquals("Unexpected size in BlobReadOptions", value.getSize(), options.getMessageInfo().getSize());
    assertEquals("Unexpected expiresAtMs in BlobReadOptions", value.getExpiresAtMs(),
        options.getMessageInfo().getExpirationTimeInMs());
    if (!state.index.hardDeleter.enabled.get() || !state.deletedKeys.contains(id)) {
      ByteBuffer readBuf = ByteBuffer.allocate((int) value.getSize());
      ByteBufferOutputStream stream = new ByteBufferOutputStream(readBuf);
      WritableByteChannel channel = Channels.newChannel(stream);
      readSet.writeTo(0, channel, 0, value.getSize());
      byte[] expectedData = state.getExpectedData(id, true);
      assertArrayEquals("Data obtained from reset does not match original", expectedData, readBuf.array());
    }
  }

  /**
   * Checks that the {@link IndexValue} obtained from store matches what is expected.
   * @param id the {@link MockId} of the blob whose {@link IndexValue} needs to be checked.
   * @throws StoreException
   */
  private void checkIndexValue(MockId id) throws StoreException {
    IndexValue value = state.getExpectedValue(id, false);
    IndexValue valueFromStore = state.index.findKey(id);
    assertEquals("Unexpected size in IndexValue", value.getSize(), valueFromStore.getSize());
    assertEquals("Unexpected expiresAtMs in IndexValue", value.getExpiresAtMs(), valueFromStore.getExpiresAtMs());
    assertEquals("Unexpected op time in IndexValue ", value.getOperationTimeInMs(),
        valueFromStore.getOperationTimeInMs());
    assertEquals("Unexpected account ID in IndexValue", value.getAccountId(), valueFromStore.getAccountId());
    assertEquals("Unexpected container ID in IndexValue", value.getContainerId(), valueFromStore.getContainerId());
    assertEquals("Unexpected flags in IndexValue", value.getFlags(), valueFromStore.getFlags());
  }

  /**
   * Verifies that the metric reporting the number of bytes saved after compaction is correct.
   * @param logSegmentCountBeforeCompaction the number of log segments in the {@link Log} before compaction.
   * @param alreadySavedBytes the number of bytes that have already been saved but won't show up in the metric registry
   * (because it may have been re-inited).
   */
  private void verifySavedBytesCount(long logSegmentCountBeforeCompaction, long alreadySavedBytes) {
    long expectedSavedBytes =
        state.log.getSegmentCapacity() * (logSegmentCountBeforeCompaction - state.index.getLogSegmentCount());
    long savedBytesReported = alreadySavedBytes + metricRegistry.getCounters()
        .get(MetricRegistry.name(BlobStoreCompactor.class, "CompactionBytesReclaimedCount"))
        .getCount();
    assertEquals("Saved bytes reported not equal to expected", expectedSavedBytes, savedBytesReported);
  }

  // badInputTest() helpers

  /**
   * Ensures that {@link BlobStoreCompactor#compact(CompactionDetails, byte[])} fails because {@code details} is invalid.
   * @param details the invalid {@link CompactionDetails}
   * @param msg the message to print on failure if no exception is thrown.
   * @throws Exception
   */
  private void ensureArgumentFailure(CompactionDetails details, String msg) throws Exception {
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
      fail(msg);
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    } finally {
      compactor.close(0);
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
   */
  private long getInvalidationTime(long numSegmentsToWritePutRecordsTo) {
    // set some invalidation time that is far enough into the future that it cannot be affected by segment rollovers
    long possiblePutRecords =
        numSegmentsToWritePutRecordsTo * state.log.getSegmentCapacity() / CuratedLogIndexState.PUT_RECORD_SIZE + 1;
    long possibleIndexSegments = numSegmentsToWritePutRecordsTo + possiblePutRecords / state.getMaxInMemElements() + 1;
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

  // interspersedDeletedAndExpiredBlobsTest() helpers.

  /**
   * Verifies the given {@code entry} has the values for fields equal to the ones provided.
   * @param entry the {@link IndexEntry} to check.
   * @param id the expected {@link MockId}.
   * @param offset the expected offset.
   * @param size the expected size.
   * @param expiresAtMs the expected expiry time in ms.
   * @param isDeleted the expected delete state.
   * @param origMsgOffset the expected original message offset.
   */
  private void verifyIndexEntry(IndexEntry entry, MockId id, long offset, long size, long expiresAtMs,
      boolean isDeleted, long origMsgOffset) {
    assertEquals("Key not as expected", id, entry.getKey());
    IndexValue value = entry.getValue();
    assertEquals("Offset not as expected", offset, value.getOffset().getOffset());
    assertEquals("Size not as expected", size, value.getSize());
    assertEquals("ExpiresAtMs not as expected", expiresAtMs, value.getExpiresAtMs());
    assertEquals("Entry type not as expected", isDeleted, value.isDelete());
    assertEquals("Original message offset not as expected", origMsgOffset, value.getOriginalMessageOffset());
  }

  private void verifyIndexEntry(IndexEntry entry, MockId id, long offset, long size, long expiresAtMs,
      boolean isDeleted, boolean isTtlUpdate, boolean isUndelete, short lifeVersion) {
    assertEquals("Key not as expected", id, entry.getKey());
    IndexValue value = entry.getValue();
    assertEquals("Offset not as expected", offset, value.getOffset().getOffset());
    assertEquals("Size not as expected", size, value.getSize());
    assertEquals("ExpiresAtMs not as expected", expiresAtMs, value.getExpiresAtMs());
    assertEquals("Entry type not as expected", isDeleted, value.isDelete());
    assertEquals("Entry type not as expected", isUndelete, value.isUndelete());
    assertEquals("Entry type not as expected", isTtlUpdate, value.isTtlUpdate());
    assertEquals("LifeVersion mismatch", lifeVersion, value.getLifeVersion());
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
    // create another log that wraps over the same files but induces close as required.
    Log log = new InterruptionInducingLog(addSegmentCallCountToInterruptAt, dropSegmentCallCountToInterruptAt);
    compactWithRecoveryAndVerify(log, DISK_IO_SCHEDULER, state.index, segmentsUnderCompaction,
        state.time.milliseconds(), false, true);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

    // there will be changes past expiration time
    expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
    state.advanceTime(expiryTimeAndSegmentsUnderCompaction.getFirst() + Time.MsPerSec - state.time.milliseconds());
    // create another log that wraps over the same files but induces close as required.
    log = new InterruptionInducingLog(addSegmentCallCountToInterruptAt, dropSegmentCallCountToInterruptAt);
    compactWithRecoveryAndVerify(log, DISK_IO_SCHEDULER, state.index, segmentsUnderCompaction,
        state.time.milliseconds(), true, true);
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
    // create another index that wraps over the same files but induces close as required.
    PersistentIndex index = new InterruptionInducingIndex();
    compactWithRecoveryAndVerify(state.log, DISK_IO_SCHEDULER, index, segmentsUnderCompaction,
        state.time.milliseconds(), false, true);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);

    // there will be changes past expiration time
    expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
    state.advanceTime(expiryTimeAndSegmentsUnderCompaction.getFirst() + Time.MsPerSec - state.time.milliseconds());
    // create another index that wraps over the same files but induces close as required.
    index = new InterruptionInducingIndex();
    compactWithRecoveryAndVerify(state.log, DISK_IO_SCHEDULER, index, segmentsUnderCompaction,
        state.time.milliseconds(), true, true);
  }

  // interruptionDuringOrAfterIndexSegmentProcessingTest() helpers

  /**
   * Does compaction tests where compaction is interrupted (and recovered) after processing a few index segments.
   * @throws Exception
   */
  private void doInterruptionDuringOrAfterIndexSegmentProcessingTest() throws Exception {
    bundleReadBuffer = null;
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
          state.time.milliseconds(), false, true);
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
          state.time.milliseconds(), true, true);
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

  // interruptionDuringRecordCopyTest() helpers

  /**
   * Does compaction tests where compaction is interrupted (and recovered) after copying a few records from an index
   * segment.
   * @throws Exception
   */
  private void doInterruptionDuringRecordCopyTest() throws Exception {
    bundleReadBuffer = null;
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
          state.time.milliseconds(), false, true);
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
          state.time.milliseconds(), true, true);
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
    bundleReadBuffer = null;
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
          state.time.milliseconds(), true, true);
    } else {
      compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);
    }
    assertTrue("Hard delete should be running", state.index.hardDeleter.isRunning());
  }

  // ttlUpdateSpecificTest() and ttlUpdateSpecificRecoveryTest() helpers

  /**
   * Creates state required for some TTL update specific tests that need PUTs and TTL updates
   * @return
   * @throws Exception
   */
  private Pair<List<MockId>, Long> createStateWithPutAndTtlUpdate() throws Exception {
    refreshState(false, false);
    int numPuts =
        (int) ((state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE) / (2 * CuratedLogIndexState.PUT_RECORD_SIZE));
    long expiryTimeMs = getInvalidationTime(1);
    List<IndexEntry> entries = state.addPutEntries(numPuts, CuratedLogIndexState.PUT_RECORD_SIZE, expiryTimeMs);
    List<MockId> ids = new ArrayList<>(entries.size());
    for (IndexEntry entry : entries) {
      MockId id = (MockId) entry.getKey();
      state.makePermanent(id, false);
      ids.add(id);
    }
    state.advanceTime(expiryTimeMs + 1 - state.time.milliseconds());
    // add put entries so that the log segment rolls over
    writeDataToMeetRequiredSegmentCount(state.index.getLogSegmentCount() + 1, null);
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    return new Pair<>(ids, expiryTimeMs);
  }

  /**
   * Creates state required for some TTL update specific tests that need PUTs, TTL updates and DELETEs.
   * @return the time at which the PUT records that don't have TTL update records expire.
   * @throws Exception
   */
  private long createStateWithPutTtlUpdateAndDelete() throws Exception {
    // this sets up state such that there are a bunch of PUTs with TTL such that they expire immediately, TTL
    // updates for all but two PUTs and deletes for all PUTs. The key is that the first log segment contains some TTL
    // updates at least so that the the removal of the two PUTs without TTL updates will cause some TTL updates that
    // were originally in the second segment to move to the first.
    refreshState(false, false);
    // this code is trying to determine how many put records there should be (close is good enough). It aims to have
    // just enough PUT records such that a quarter of the TTL updates for these PUT records fits in the first log
    // segment along with the PUTs themselves
    // numPuts * PUT_RECORD_SIZE + numPuts/4 * TTL_UPDATE_RECORD_SIZE + HEADER_SIZE < SEGMENT_CAPACITY
    // we have to solve for numPuts
    // numPuts < (SEGMENT_CAPACITY - HEADER_SIZE) / (PUT_RECORD_SIZE + TTL_UPDATE_RECORD_SIZE/4)
    // the -1 in the below statement is make numPuts < the output of the RHS
    int numPuts = (int) Math.floorDiv(state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE,
        CuratedLogIndexState.PUT_RECORD_SIZE + (CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE) / 4) - 1;
    // this ensures that the crash test cases cause the crash in the middle of an index segment
    state.properties.setProperty("store.index.max.number.of.inmem.elements", Integer.toString(numPuts));
    // reload for new props to take effect
    state.reloadIndex(true, false);

    // insert numPuts PUT records with immediate expiry
    long expiryTimeMs = getInvalidationTime(1);
    List<IndexEntry> entries = state.addPutEntries(numPuts, CuratedLogIndexState.PUT_RECORD_SIZE, expiryTimeMs);
    // insert ttl updates for all but two of the PUTs
    for (int i = 0; i < entries.size() - 2; i++) {
      state.makePermanent((MockId) entries.get(i).getKey(), false);
    }
    // advance time and delete all the data (some of the data will get deleted at deleteTimeMs + delta).
    state.advanceTime(expiryTimeMs + 1 - state.time.milliseconds());
    for (IndexEntry entry : entries) {
      state.addDeleteEntry((MockId) entry.getKey());
    }
    // add put entries so that the log segment rolls over
    writeDataToMeetRequiredSegmentCount(state.index.getLogSegmentCount() + 1, null);
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    return expiryTimeMs;
  }

  /**
   * Creates state required for some TTL update specific tests that need TTL updates and DELETEs only.
   * @return the {@link MockId}s for which TTL updates and deletes were added.
   * @throws Exception
   */
  private Set<MockId> createStateWithTtlUpdatesAndDeletes() throws Exception {
    Set<MockId> ids = new HashSet<>();
    refreshState(false, false);
    int numEntries =
        (int) ((state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE) / (CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE
            + CuratedLogIndexState.DELETE_RECORD_SIZE));
    for (int i = 0; i < numEntries; i++) {
      MockId id = state.getUniqueId();
      state.makePermanent(id, true);
      state.addDeleteEntry(id);
      ids.add(id);
    }
    // add put entries so that the log segment rolls over
    writeDataToMeetRequiredSegmentCount(state.index.getLogSegmentCount() + 1, null);
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    return ids;
  }

  /**
   * Tests the case where close/crash creates a situation where duplicates will be detected during the search for copy
   * candidates in the srcIndex
   * @throws Exception
   */
  private void doTtlUpdateSrcDupTest() throws Exception {
    // close/crash after the first log segment is switched out
    long expiryTimeMs = createStateWithPutTtlUpdateAndDelete();
    state.advanceTime(TimeUnit.SECONDS.toMillis(1));
    List<String> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    Log log = new InterruptionInducingLog(Integer.MAX_VALUE, 1);
    compactWithRecoveryAndVerify(log, DISK_IO_SCHEDULER, state.index, segmentsUnderCompaction, expiryTimeMs, true,
        true);
  }

  /**
   * Tests the case where close/crash creates a situation where duplicates will be detected during the search for copy
   * candidates in the tgtIndex
   * @throws Exception
   */
  private void doTtlUpdateTgtDupTest() throws Exception {
    // second segment only (close/crash in the middle)
    createStateWithPutTtlUpdateAndDelete();
    state.advanceTime(TimeUnit.SECONDS.toMillis(1));
    List<String> segmentsUnderCompaction = getLogSegments(1, 1);
    Map<String, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
    DiskIOScheduler diskIOScheduler;
    if (throwExceptionInsteadOfClose) {
      diskIOScheduler = new InterruptionInducingDiskIOScheduler(2, Integer.MAX_VALUE);
    } else {
      diskIOScheduler =
          new InterruptionInducingDiskIOScheduler(Integer.MAX_VALUE, 2 * CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE);
    }
    compactWithRecoveryAndVerify(state.log, diskIOScheduler, state.index, segmentsUnderCompaction,
        state.time.milliseconds(), false, true);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);
  }

  // support class helpers

  /**
   * Throws an exception if {@link #throwExceptionBeforeOperation} is {@code true}.
   */
  private void throwExceptionIfRequired() {
    if (throwExceptionBeforeOperation) {
      closeOrExceptionInduced = true;
      throw new RuntimeException(EXCEPTION_MSG);
    }
  }

  /**
   * Interrupts the compactor.
   * 1. Throws an exception if {@link #throwExceptionInsteadOfClose} is {@code true}.
   * 2. Closes the compactor otherwise.
   */
  private void closeCompactorOrThrowException() {
    closeOrExceptionInduced = true;
    if (throwExceptionInsteadOfClose) {
      throw new RuntimeException(EXCEPTION_MSG);
    }
    try {
      compactor.close(0);
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
      if (jobType.equals(BlobStoreCompactor.INDEX_SEGMENT_READ_JOB_NAME)) {
        indexSegmentsCopied += usedSinceLastCall;
      } else if (jobType.equals(BlobStoreCompactor.COMPACTION_CLEANUP_JOB_NAME)) {
        numBytesCopied += usedSinceLastCall;
      }
      if (indexSegmentsCopied == indexSegmentCountToCutoffAt || numBytesCopied >= numBytesToCutoffAt) {
        closeCompactorOrThrowException();
      }
      return Long.MAX_VALUE;
    }
  }

  /**
   * Extension of {@link PersistentIndex} that interrupts the compaction when index commit is being executed.
   */
  private class InterruptionInducingIndex extends PersistentIndex {

    InterruptionInducingIndex() throws StoreException {
      super(tempDirStr, tempDirStr, state.scheduler, state.log,
          new StoreConfig(new VerifiableProperties(state.properties)), CuratedLogIndexState.STORE_KEY_FACTORY,
          state.recovery, state.hardDelete, CuratedLogIndexState.DISK_IO_SCHEDULER,
          new StoreMetrics(new MetricRegistry()), state.time, state.sessionId, state.incarnationId);
    }

    @Override
    void changeIndexSegments(List<File> segmentFilesToAdd, Set<Offset> segmentsToRemove) throws StoreException {
      throwExceptionIfRequired();
      super.changeIndexSegments(segmentFilesToAdd, segmentsToRemove);
      closeCompactorOrThrowException();
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
     * @throws StoreException
     */
    InterruptionInducingLog(int addSegmentCallCountToInterruptAt, int dropSegmentCallCountToInterruptAt)
        throws StoreException {
      super(tempDirStr, state.log.getCapacityInBytes(), StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
          createStoreConfig(state.log.getSegmentCapacity(), true), new StoreMetrics(new MetricRegistry()));
      // set end offsets correctly
      LogSegment original = state.log.getFirstSegment();
      while (original != null) {
        LogSegment dup = getSegment(original.getName());
        dup.setEndOffset(original.getEndOffset());
        original = state.log.getNextSegment(original);
      }
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
        closeCompactorOrThrowException();
      }
    }

    @Override
    void dropSegment(String segmentName, boolean decreaseUsedSegmentCount) throws StoreException {
      segmentsDropped++;
      if (segmentsDropped == dropSegmentCallCountToInterruptAt) {
        throwExceptionIfRequired();
      }
      super.dropSegment(segmentName, decreaseUsedSegmentCount);
      if (segmentsDropped == dropSegmentCallCountToInterruptAt) {
        closeCompactorOrThrowException();
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
    long size;

    /**
     * Create an instance with {@code id} and {@code entryType}
     * @param id the {@link MockId} of the entry.
     * @param entryType the type of the entry.
     * @param size the size of the log entry.
     */
    LogEntry(MockId id, PersistentIndex.IndexEntryType entryType, long size) {
      this.id = id;
      this.entryType = entryType;
      this.size = size;
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
      return id.equals(logEntry.id) && entryType == logEntry.entryType && size == logEntry.size;
    }

    @Override
    public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + entryType.hashCode();
      result = 31 * result + (int) size;
      return result;
    }

    @Override
    public String toString() {
      return "[id: " + id + " entryType: " + entryType + " size: " + size + "]";
    }
  }
}
