/**
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
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Tests for {@link BlobStoreStats}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class BlobStoreStatsTest {
  private static final long TEST_TIME_INTERVAL_IN_MS = CuratedLogIndexState.DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS / 2;
  private static final long BUCKET_SPAN_IN_MS = Time.MsPerSec;
  private static final long QUEUE_PROCESSOR_PERIOD_IN_SECS = 1;
  private static final StoreMetrics METRICS = new StoreMetrics("test", new MetricRegistry());
  private final Map<String, Throttler> throttlers = new HashMap<>();
  private final DiskIOScheduler diskIOScheduler = new DiskIOScheduler(throttlers);
  private final ScheduledExecutorService storeStatsScheduler = Utils.newScheduler(1, false);
  private final CuratedLogIndexState state;
  private final File tempDir;
  private final long logSegmentForecastOffsetMs;
  private final int bucketCount;
  private BlobStoreStats blobStoreStats;

  /**
   * Creates a temporary directory and sets up some test state.
   * @throws IOException
   */
  public BlobStoreStatsTest(boolean isLogSegmented) throws InterruptedException, IOException, StoreException {
    tempDir = StoreTestUtils.createTempDirectory("blobStoreStatsDir-" + UtilsTest.getRandomString(10));
    state = new CuratedLogIndexState(isLogSegmented, tempDir);
    logSegmentForecastOffsetMs = state.time.milliseconds();
    bucketCount = 2 * (int) (logSegmentForecastOffsetMs / BUCKET_SPAN_IN_MS);
    blobStoreStats =
        new BlobStoreStats(state.index, bucketCount, BUCKET_SPAN_IN_MS, logSegmentForecastOffsetMs / Time.MsPerSec,
            QUEUE_PROCESSOR_PERIOD_IN_SECS, state.time, Utils.newScheduler(1, false), diskIOScheduler, METRICS);
  }

  /**
   * Running for both segmented and non-segmented log.
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   */
  @After
  public void cleanup() throws InterruptedException, IOException, StoreException {
    state.destroy();
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Basic test to verify reported valid size information per container by BlobStoreStats.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testContainerValidDataSize() throws InterruptedException, StoreException, IOException {
    verifyAndGetContainerValidSize(state.time.milliseconds());
    // advance time
    state.advanceTime(TEST_TIME_INTERVAL_IN_MS);
    verifyAndGetContainerValidSize(state.time.milliseconds());
  }

  /**
   * Basic test to verify reported valid size information per log segment by BlobStoreStats.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testLogSegmentValidDataSize() throws InterruptedException, StoreException, IOException {
    long currentTimeInMs = state.time.milliseconds();
    for (long i = 0; i <= currentTimeInMs + TEST_TIME_INTERVAL_IN_MS; i += TEST_TIME_INTERVAL_IN_MS) {
      TimeRange timeRange = new TimeRange(i, 0L);
      verifyAndGetLogSegmentValidSize(timeRange);
    }
  }

  /**
   * Tests to verify the correctness of reported stats after new puts via the following steps:
   * 1. Verify reported stats and record the total valid size prior to adding the new puts.
   * 2. Add new puts.
   * 3. Verify reported stats and record the total valid size after new puts are added.
   * 4. Verify the delta of total valid size prior to adding the new puts and after matches with the expected delta.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testValidDataSizeAfterPuts() throws InterruptedException, StoreException, IOException {
    // advance time to the next second for deletes/expiration to take effect
    advanceTimeToNextSecond();
    long timeInMsBeforePuts = state.time.milliseconds();
    long totalLogSegmentValidSizeBeforePuts = verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsBeforePuts, 0L));
    long totalContainerValidSizeBeforePuts = verifyAndGetContainerValidSize(timeInMsBeforePuts);

    // 3 puts
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    long timeInMsAfterPuts = state.time.milliseconds();
    long totalLogSegmentValidSizeAfterPuts = verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsAfterPuts, 0L));
    long totalContainerValidSizeAfterPuts = verifyAndGetContainerValidSize(timeInMsAfterPuts);
    long expectedIncrement = 3 * CuratedLogIndexState.PUT_RECORD_SIZE;
    assertEquals("Put entries are not properly counted for log segment valid size", totalLogSegmentValidSizeAfterPuts,
        totalLogSegmentValidSizeBeforePuts + expectedIncrement);
    assertEquals("Put entries are not properly counted for container valid size", totalContainerValidSizeAfterPuts,
        totalContainerValidSizeBeforePuts + expectedIncrement);
  }

  /**
   * Tests to verify the correctness of reported stats with puts that is going to expire via the following steps:
   * 1. Verify reported stats and record the total valid size before adding the new expiring puts.
   * 2. Add new expiring and non-expiring puts.
   * 3. Verify the new puts are being reported correctly.
   * 4. Advance time to let the expiration take effect.
   * 5. Verify reported stats and record the total valid size after new puts are expired.
   * 6. Verify the reported total valid size difference before the new puts and after.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testValidDataSizeAfterExpiration() throws InterruptedException, StoreException, IOException {
    // advance time to the next second for previous deletes/expiration to take effect
    advanceTimeToNextSecond();
    long timeInMsBeforePuts = state.time.milliseconds();
    long totalLogSegmentValidSizeBeforePuts = verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsBeforePuts, 0L));
    long totalContainerValidSizeBeforePuts = verifyAndGetContainerValidSize(state.time.milliseconds());

    // 1 put with no expiry
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 3 puts that will expire in 20 seconds (note the two puts should be in the same index segment)
    long expiresAtInMs = state.time.milliseconds() + 20 * Time.MsPerSec;
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, expiresAtInMs);

    // advance time to exactly the time of expiration, all new puts should still be valid
    state.advanceTime(expiresAtInMs - state.time.milliseconds());

    long expectedDeltaAfterPut = 4 * CuratedLogIndexState.PUT_RECORD_SIZE;
    long timeInMsAfterPuts = state.time.milliseconds();
    long totalLogSegmentValidSizeAfterPuts = verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsAfterPuts, 0L));
    long totalContainerValidSizeAfterPuts = verifyAndGetContainerValidSize(timeInMsAfterPuts);
    assertEquals("Put entries with expiry are not properly counted for log segment valid size",
        totalLogSegmentValidSizeAfterPuts, totalLogSegmentValidSizeBeforePuts + expectedDeltaAfterPut);
    assertEquals("Put entries with expiry are not properly counted for container valid size",
        totalContainerValidSizeAfterPuts, totalContainerValidSizeBeforePuts + expectedDeltaAfterPut);

    // advance time to the next second for expiration to take effect
    advanceTimeToNextSecond();

    long expectedDeltaAfterExpiration = CuratedLogIndexState.PUT_RECORD_SIZE;
    long timeInMsAfterExpiration = state.time.milliseconds();
    long totalLogSegmentValidSizeAfterExpiration =
        verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsAfterExpiration, 0L));
    long totalContainerValidSizeAfterExpiration = verifyAndGetContainerValidSize(timeInMsAfterExpiration);
    assertEquals("Expired put entries are not properly counted for log segment valid size",
        totalLogSegmentValidSizeAfterExpiration, totalLogSegmentValidSizeBeforePuts + expectedDeltaAfterExpiration);
    assertEquals("Expired put entries are not properly counted for container valid size",
        totalContainerValidSizeAfterExpiration, totalContainerValidSizeBeforePuts + expectedDeltaAfterExpiration);
  }

  /**
   * Tests to verify the correctness of reported stats with after new deletes via the following steps:
   * 1. Add new puts that are going to be deleted later.
   * 2. Verify reported stats and record the total valid size before new deletes.
   * 3. Perform the deletes.
   * 4. Verify reported stats after the deletes but at a time point before the deletes are relevant.
   * 5. Verify reported stats and record the total valid size after the deletes.
   * 6. Verify the delta of total valid size prior to the new deletes and after matches with the expected delta.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testValidDataSizeAfterDeletes() throws InterruptedException, StoreException, IOException {
    int numEntries = CuratedLogIndexState.MAX_IN_MEM_ELEMENTS - state.referenceIndex.lastEntry().getValue().size()
        + CuratedLogIndexState.MAX_IN_MEM_ELEMENTS - 2;
    state.addPutEntries(numEntries, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    long timeInMsBeforeDeletes = state.time.milliseconds();
    long totalLogSegmentValidSizeBeforeDeletes =
        verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsBeforeDeletes, 0L));
    long totalContainerValidSizeBeforeDeletes = verifyAndGetContainerValidSize(timeInMsBeforeDeletes);

    // advance time to the next seconds before adding the deletes
    advanceTimeToNextSecond();
    // 2 deletes from the last index segment
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));

    long expectedDeltaBeforeDeletesRelevant = 2 * CuratedLogIndexState.DELETE_RECORD_SIZE;
    long totalLogSegmentValidSizeBeforeDeletesRelevant =
        verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsBeforeDeletes, 0L));
    long totalContainerValidSizeBeforeDeletesRelevant = verifyAndGetContainerValidSize(timeInMsBeforeDeletes);
    assertEquals("Delete entries are not properly counted for log segment valid size",
        totalLogSegmentValidSizeBeforeDeletesRelevant,
        totalLogSegmentValidSizeBeforeDeletes + expectedDeltaBeforeDeletesRelevant);
    assertEquals("Delete entries are not properly counted for container valid size",
        totalContainerValidSizeBeforeDeletesRelevant, totalContainerValidSizeBeforeDeletes);

    // advance time to the next second for deletes/expiration to take effect
    advanceTimeToNextSecond();

    long timeInMsAfterDeletes = state.time.milliseconds();
    long totalLogSegmentValidSizeAfterDeletes =
        verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsAfterDeletes, 0L));
    long totalContainerValidSizeAfterDeletes = verifyAndGetContainerValidSize(timeInMsAfterDeletes);
    long expectedLogSegmentDecrement =
        2 * (CuratedLogIndexState.PUT_RECORD_SIZE - CuratedLogIndexState.DELETE_RECORD_SIZE);
    long expectedContainerDecrement = 2 * CuratedLogIndexState.PUT_RECORD_SIZE;
    assertEquals("Delete entries are not properly counted for log segment valid size",
        totalLogSegmentValidSizeAfterDeletes, totalLogSegmentValidSizeBeforeDeletes - expectedLogSegmentDecrement);
    assertEquals("Delete entries are not properly counted for container valid size",
        totalContainerValidSizeAfterDeletes, totalContainerValidSizeBeforeDeletes - expectedContainerDecrement);
  }

  /**
   <<<<<<< HEAD
   * Test the static method that converts the quota stats in the form of a nested {@link Map} to the generic
   =======
   * Test {@link BlobStoreStats} can start and shutdown properly.
   * @throws InterruptedException
   */
  @Test
  public void testStartAndShutdown() throws InterruptedException {
    final CountDownLatch scanStartedLatch = new CountDownLatch(1);
    MockThrottler mockThrottler = new MockThrottler(scanStartedLatch, new CountDownLatch(0));
    throttlers.put(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, mockThrottler);
    blobStoreStats.start();
    // proceed only when the scan is started
    assertTrue("IndexScanner took too long to start", scanStartedLatch.await(5, TimeUnit.SECONDS));
    blobStoreStats.shutdown();
  }

  /**
   * Test {@link BlobStoreStats} can shutdown properly even if it's not started.
   * @throws InterruptedException
   */
  @Test
  public void testShutdownBeforeStart() throws InterruptedException {
    blobStoreStats.shutdown();
  }

  /**
   * Basic test to verify that the {@link BlobStoreStats} can scan the index, populate the buckets and use these buckets
   * to report stats correctly.
   * @throws StoreException
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testBucketingBasic() throws StoreException, InterruptedException, IOException {
    final CountDownLatch scanStartedLatch = new CountDownLatch(1);
    MockThrottler mockThrottler = new MockThrottler(scanStartedLatch, new CountDownLatch(0));
    throttlers.put(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, mockThrottler);
    // add 3 puts with expiry
    long expiresAtInMs = state.time.milliseconds() + 30 * Time.MsPerSec;
    List<IndexEntry> newPutEntries = state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, expiresAtInMs);
    for (IndexEntry entry : newPutEntries) {
      blobStoreStats.handleNewPut(entry.getValue());
    }
    // since new entries are added before the scan, compute the new log segment forecast start time to ensure requests
    // are within the forecast coverage
    long logSegmentForecastStartTimeMs = state.time.milliseconds() - logSegmentForecastOffsetMs;
    blobStoreStats.start();
    // proceed only when the scan is started
    assertTrue("IndexScanner took too long to start", scanStartedLatch.await(5, TimeUnit.SECONDS));
    verifyAndGetContainerValidSize(state.time.milliseconds());
    long throttleCountBeforeRequests = mockThrottler.throttleCount.get();
    // requests within the forecast coverage of the ongoing scan will wait for the scan to complete
    for (long i = logSegmentForecastStartTimeMs; i <= state.time.milliseconds() + TEST_TIME_INTERVAL_IN_MS;
        i += TEST_TIME_INTERVAL_IN_MS) {
      verifyAndGetLogSegmentValidSize(new TimeRange(i, 0L));
    }
    // advance time to let the added puts to expire
    long timeToLiveInMs = expiresAtInMs - state.time.milliseconds() < 0 ? 0 : expiresAtInMs - state.time.milliseconds();
    state.advanceTime(timeToLiveInMs + Time.MsPerSec);
    for (long i = logSegmentForecastStartTimeMs; i <= state.time.milliseconds() + TEST_TIME_INTERVAL_IN_MS;
        i += TEST_TIME_INTERVAL_IN_MS) {
      verifyAndGetLogSegmentValidSize(new TimeRange(i, 0L));
    }
    verifyAndGetContainerValidSize(state.time.milliseconds());
    assertEquals("Throttle count shouldn't have changed", throttleCountBeforeRequests,
        mockThrottler.throttleCount.get());
    blobStoreStats.shutdown();
  }

  /**
   * Test to verify new entries after a scan are properly counted to keep the current {@link ScanResults} relevant
   * before the next scan. In addition, verify that requests within the coverage are served without triggering any scan.
   * @throws StoreException
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testBucketingWithNewEntriesAfterScan() throws StoreException, InterruptedException, IOException {
    final CountDownLatch scanStartedLatch = new CountDownLatch(1);
    CountDownLatch queueProcessedLatch = new CountDownLatch(1);
    MockThrottler mockThrottler = new MockThrottler(scanStartedLatch, new CountDownLatch(0));
    throttlers.put(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, mockThrottler);
    blobStoreStats.start();
    // proceed only when the scan is started
    assertTrue("IndexScanner took too long to start", scanStartedLatch.await(5, TimeUnit.SECONDS));
    advanceTimeToNextSecond();
    verifyAndGetLogSegmentValidSize(new TimeRange(state.time.milliseconds(), 0L));
    verifyAndGetContainerValidSize(state.time.milliseconds());
    int throttleCountBeforeRequests = mockThrottler.throttleCount.get();
    // add 3 put with expiry
    long expiresAtInMs = state.time.milliseconds() + 30 * Time.MsPerSec;
    List<IndexEntry> newPutEntries = state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, expiresAtInMs);
    for (IndexEntry entry : newPutEntries) {
      blobStoreStats.handleNewPut(entry.getValue());
    }
    // delete the first new put with expiry
    MockId firstPutWithExpiry = getIdToDelete(newPutEntries.get(0).getKey());
    newDelete(firstPutWithExpiry);
    // a probe put with a latch to inform us about the state of the queue
    blobStoreStats.handleNewPut(new MockIndexValue(queueProcessedLatch, state.index.getCurrentEndOffset()));
    assertTrue("QueueProcessor took too long to process the new entries",
        queueProcessedLatch.await(5, TimeUnit.SECONDS));
    advanceTimeToNextSecond();
    verifyAndGetLogSegmentValidSize(new TimeRange(state.time.milliseconds(), 0L));
    verifyAndGetContainerValidSize(state.time.milliseconds());
    // add 3 put with no expiry
    newPutEntries = state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    for (IndexEntry entry : newPutEntries) {
      blobStoreStats.handleNewPut(entry.getValue());
    }
    // delete one of the newly added put
    newDelete(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    queueProcessedLatch = new CountDownLatch(1);
    // a probe put with a latch to inform us about the state of the queue
    blobStoreStats.handleNewPut(new MockIndexValue(queueProcessedLatch, state.index.getCurrentEndOffset()));
    assertTrue("QueueProcessor took too long to process the new entries",
        queueProcessedLatch.await(5, TimeUnit.SECONDS));
    advanceTimeToNextSecond();
    verifyAndGetLogSegmentValidSize(new TimeRange(state.time.milliseconds(), 0L));
    verifyAndGetContainerValidSize(state.time.milliseconds());
    // advance time beyond expiration of the blobs and verify no double counting for expiration and delete
    long timeToLiveInMs = expiresAtInMs - state.time.milliseconds() < 0 ? 0 : expiresAtInMs - state.time.milliseconds();
    state.advanceTime(timeToLiveInMs + Time.MsPerSec);
    verifyAndGetLogSegmentValidSize(new TimeRange(state.time.milliseconds(), 0L));
    verifyAndGetContainerValidSize(state.time.milliseconds());
    assertEquals("Throttle count shouldn't have changed", throttleCountBeforeRequests,
        mockThrottler.throttleCount.get());
    blobStoreStats.shutdown();
  }

  /**
   * Test to verify that new entries that got added while scanning are being counted properly. That is, no double
   * counting or miscounting. The behavior is verified via the following steps:
   * 1. Start the scan.
   * 2. Once the scan is started it will be put on hold and various new entries will be added.
   * 3. Resume the scan and wait for queue processor to process the new entries.
   * 4. Perform various checks to verify the reported stats.
   * @throws StoreException
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testBucketingWithNewEntriesDuringScan() throws StoreException, InterruptedException, IOException {
    final CountDownLatch scanStartedLatch = new CountDownLatch(1);
    // scan will be put on hold once it's started
    final CountDownLatch scanHoldLatch = new CountDownLatch(1);
    final CountDownLatch queueProcessedLatch = new CountDownLatch(1);
    throttlers.put(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, new MockThrottler(scanStartedLatch, scanHoldLatch));
    blobStoreStats.start();
    // proceed only when the scan is started
    assertTrue("IndexScanner took too long to start", scanStartedLatch.await(5, TimeUnit.SECONDS));
    // 1 new put that will expire in 20 seconds
    long expiresAtInMs = state.time.milliseconds() + 20 * Time.MsPerSec;
    List<IndexEntry> newPutEntries = state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, expiresAtInMs);
    // 3 new puts with no expiry
    newPutEntries.addAll(state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time));
    for (IndexEntry entry : newPutEntries) {
      blobStoreStats.handleNewPut(entry.getValue());
    }
    List<MockId> newDeletes = new ArrayList<>();
    // 1 delete from the first index segment
    newDeletes.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.firstKey()));
    // 1 delete from the last index segment
    newDeletes.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    for (MockId idToDelete : newDeletes) {
      if (idToDelete != null) {
        newDelete(idToDelete);
      }
    }
    // a probe put with a latch to inform us about the state of the queue
    blobStoreStats.handleNewPut(new MockIndexValue(queueProcessedLatch, state.index.getCurrentEndOffset()));
    scanHoldLatch.countDown();
    assertTrue("QueueProcessor took too long to process the new entries",
        queueProcessedLatch.await(5, TimeUnit.SECONDS));
    for (long i = 0; i <= state.time.milliseconds() + TEST_TIME_INTERVAL_IN_MS; i += TEST_TIME_INTERVAL_IN_MS) {
      verifyAndGetLogSegmentValidSize(new TimeRange(i, 0L));
    }
    verifyAndGetContainerValidSize(state.time.milliseconds());
    long timeToLiveInMs = expiresAtInMs - state.time.milliseconds() < 0 ? 0 : expiresAtInMs - state.time.milliseconds();
    state.advanceTime(timeToLiveInMs + Time.MsPerSec);
    advanceTimeToNextSecond();
    verifyAndGetContainerValidSize(state.time.milliseconds());
    blobStoreStats.shutdown();
  }

  /**
   * Tests to verify requests inside and outside of the forecast coverage can still be served properly while scanning.
   */
  @Test
  public void testBucketingCoverageTransition() throws InterruptedException, StoreException, IOException {
    int bucketCount = 2;
    long logSegmentForecastOffsetSecs = 0;
    blobStoreStats = new BlobStoreStats(state.index, bucketCount, BUCKET_SPAN_IN_MS, logSegmentForecastOffsetSecs,
        QUEUE_PROCESSOR_PERIOD_IN_SECS, state.time, storeStatsScheduler, diskIOScheduler, METRICS);
    CountDownLatch scanStartedLatch = new CountDownLatch(1);
    // do not hold the initial scan
    MockThrottler mockThrottler = new MockThrottler(scanStartedLatch, new CountDownLatch(0));
    throttlers.put(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, mockThrottler);
    // add a put that is going to expire in 20 seconds
    long initialScanTimeInMs = state.time.milliseconds();
    blobStoreStats.start();
    // proceed only when the scan is started
    assertTrue("IndexScanner took too long to start", scanStartedLatch.await(5, TimeUnit.SECONDS));
    verifyAndGetLogSegmentValidSize(new TimeRange(initialScanTimeInMs, 0L));
    verifyAndGetContainerValidSize(initialScanTimeInMs);
    // advance time by 20 seconds
    state.advanceTime(20 * Time.MsPerSec);
    // hold the next scan
    CountDownLatch scanHoldLatch = new CountDownLatch(1);
    scanStartedLatch = new CountDownLatch(1);
    mockThrottler.holdLatch = scanHoldLatch;
    mockThrottler.startedLatch = scanStartedLatch;
    mockThrottler.isThrottlerStarted = false;
    assertTrue("IndexScanner took too long to start", scanStartedLatch.await(5, TimeUnit.SECONDS));
    int throttleCountBeforeRequests = mockThrottler.throttleCount.get();
    verifyAndGetLogSegmentValidSize(new TimeRange(initialScanTimeInMs, 0L));
    verifyAndGetContainerValidSize(initialScanTimeInMs);
    assertEquals("Throttle count shouldn't have changed", throttleCountBeforeRequests,
        mockThrottler.throttleCount.get());
    // request something outside of the forecast coverage
    verifyAndGetLogSegmentValidSize(new TimeRange(0L, 0L));
    verifyAndGetContainerValidSize(0L);
    scanHoldLatch.countDown();
    blobStoreStats.shutdown();
  }

  /**
   * Test to verify {@link BlobStoreStats} is resolving the given {@link TimeRange} correctly and the appropriate action
   * is taken. That is, use the readily available {@link ScanResults} to answer the request or trigger a on demand scan.
   * Specifically the following cases are tested (plus cases when boundaries touch each other):
   *    [_______]  [_______]     [________]   [______]              [______]  <--- forecast range
   *  [___]      ,       [___] ,   [___]    ,         [___] , [___]           <--- given time range
   * @throws InterruptedException
   */
  @Test
  public void testTimeRangeResolutionWithStats() throws InterruptedException, StoreException {
    final CountDownLatch scanStartedLatch = new CountDownLatch(1);
    MockThrottler mockThrottler = new MockThrottler(scanStartedLatch, new CountDownLatch(0));
    throttlers.put(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, mockThrottler);
    // advance time before starting the scan to offset the log segment forecast start time from 0
    state.advanceTime(20000);
    long logSegmentForecastStartTimeMs = state.time.milliseconds() - logSegmentForecastOffsetMs;
    long logSegmentForecastEndTimeMs = bucketCount * BUCKET_SPAN_IN_MS + logSegmentForecastStartTimeMs;
    blobStoreStats.start();
    // proceed only when the scan is started
    assertTrue("IndexScanner took too long to start", scanStartedLatch.await(5, TimeUnit.SECONDS));
    // ensure the scan is complete before proceeding
    verifyAndGetContainerValidSize(state.time.milliseconds());
    int throttleCountBeforeRequests = mockThrottler.throttleCount.get();
    TimeRange timeRange = new TimeRange(logSegmentForecastStartTimeMs, Time.MsPerSec);
    assertEquals("Unexpected collection time", timeRange.getEndTimeInMs(),
        blobStoreStats.getValidDataSizeByLogSegment(timeRange).getFirst().longValue());
    timeRange = new TimeRange(logSegmentForecastEndTimeMs, Time.MsPerSec);
    assertEquals("Unexpected collection time", logSegmentForecastEndTimeMs - BUCKET_SPAN_IN_MS,
        blobStoreStats.getValidDataSizeByLogSegment(timeRange).getFirst().longValue());
    timeRange = new TimeRange((logSegmentForecastStartTimeMs + logSegmentForecastEndTimeMs) / 2, Time.MsPerSec);
    assertEquals("Unexpected collection time", timeRange.getEndTimeInMs(),
        blobStoreStats.getValidDataSizeByLogSegment(timeRange).getFirst().longValue());
    // time range end time is equal to the start of forecast range
    timeRange = new TimeRange(logSegmentForecastStartTimeMs - Time.MsPerSec, Time.MsPerSec);
    assertEquals("Unexpected collection time", timeRange.getEndTimeInMs(),
        blobStoreStats.getValidDataSizeByLogSegment(timeRange).getFirst().longValue());
    // all previous time range are inside the forecast range
    assertEquals("Throttle count shouldn't have changed", throttleCountBeforeRequests,
        mockThrottler.throttleCount.get());
    // time range start time is equal the end of forecast range (considered to be outside of forecast range)
    timeRange = new TimeRange(logSegmentForecastEndTimeMs + Time.MsPerSec, Time.MsPerSec);
    assertEquals("Unexpected collection time", timeRange.getEndTimeInMs(),
        blobStoreStats.getValidDataSizeByLogSegment(timeRange).getFirst().longValue());
    timeRange = new TimeRange(logSegmentForecastEndTimeMs + 5 * Time.MsPerSec, Time.MsPerSec);
    assertEquals("Unexpected collection time", timeRange.getEndTimeInMs(),
        blobStoreStats.getValidDataSizeByLogSegment(timeRange).getFirst().longValue());
    timeRange = new TimeRange(logSegmentForecastStartTimeMs - 5 * Time.MsPerSec, Time.MsPerSec);
    assertEquals("Unexpected collection time", timeRange.getEndTimeInMs(),
        blobStoreStats.getValidDataSizeByLogSegment(timeRange).getFirst().longValue());
    blobStoreStats.shutdown();
  }

  /**
   * Test the static method that converts the quota stats stored in a nested Map to an avro generated
   >>>>>>> Refactored bucketing for BlobStoreStats
   * {@link StatsSnapshot} object.
   */
  @Test
  public void testConvertQuotaMapToStatsSnapshot() {
    Random random = new Random();
    Map<String, Map<String, Long>> quotaMap = new HashMap<>();
    Map<String, StatsSnapshot> accountSubMap = new HashMap<>();
    long total = 0;
    for (int i = 0; i < 10; i++) {
      Map<String, StatsSnapshot> containerSubMap = new HashMap<>();
      Map<String, Long> innerQuotaMap = new HashMap<>();
      long subTotal = 0;
      for (int j = 0; j < 3; j++) {
        long randValue = random.nextInt(10000);
        subTotal += randValue;
        innerQuotaMap.put(String.valueOf(j), randValue);
        containerSubMap.put(String.valueOf(j), new StatsSnapshot(randValue, null));
      }
      total += subTotal;
      quotaMap.put(String.valueOf(i), innerQuotaMap);
      accountSubMap.put(String.valueOf(i), new StatsSnapshot(subTotal, containerSubMap));
    }
    StatsSnapshot statsSnapshot = new StatsSnapshot(total, accountSubMap);
    StatsSnapshot convertedStatsSnapshot = BlobStoreStats.convertQuotaToStatsSnapshot(quotaMap);
    assertTrue("Mismatch between the converted StatsSnapshot and expected StatsSnapshot",
        statsSnapshot.equals(convertedStatsSnapshot));
  }

  /**
   * Test the getStatsSnapshot method by verifying the returned {@link StatsSnapshot} against the original {@link Map}.
   */
  @Test
  public void testGetStatsSnapshot() throws StoreException {
    long deleteAndExpirationRefTimeInMs = state.time.milliseconds();
    Map<String, Map<String, Long>> quotaMap =
        blobStoreStats.getValidDataSizeByContainer(deleteAndExpirationRefTimeInMs);
    StatsSnapshot statsSnapshot = blobStoreStats.getStatsSnapshot(deleteAndExpirationRefTimeInMs);
    Map<String, StatsSnapshot> accountValidSizeMap = statsSnapshot.getSubMap();
    assertEquals("Mismatch on number of accounts", quotaMap.size(), accountValidSizeMap.size());
    for (Map.Entry<String, Map<String, Long>> entry : quotaMap.entrySet()) {
      Map<String, StatsSnapshot> containerValidSizeMap = accountValidSizeMap.get(entry.getKey()).getSubMap();
      Map<String, Long> innerQuotaMap = entry.getValue();
      assertEquals("Mismatch on number of containers", innerQuotaMap.size(), containerValidSizeMap.size());
      for (Map.Entry<String, Long> innerEntry : innerQuotaMap.entrySet()) {
        assertEquals("Mismatch on leaf node value", innerEntry.getValue().longValue(),
            containerValidSizeMap.get(innerEntry.getKey()).getValue());
      }
    }
  }

  /**
   * Use the given {@link StoreKey} to get its corresponding {@link MockId} and update various states in
   * {@link CuratedLogIndexState} in preparation for the delete.
   * @param storeKey the {@link StoreKey} to be deleted
   * @return the {@link MockId} to be deleted
   */
  private MockId getIdToDelete(StoreKey storeKey) {
    MockId idToDelete = null;
    for (MockId id : state.liveKeys) {
      if (id.getID().equals(storeKey.getID())) {
        idToDelete = id;
      }
    }
    if (idToDelete != null) {
      state.deletedKeys.add(idToDelete);
      state.liveKeys.remove(idToDelete);
    }
    return idToDelete;
  }

  /**
   * A helper function that deletes the PUT with the given {@link MockId} and inform {@link BlobStoreStats} about it.
   * @param idToDelete the {@link MockId} to be deleted
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  private void newDelete(MockId idToDelete) throws InterruptedException, StoreException, IOException {
    state.addDeleteEntry(idToDelete);
    Pair<IndexValue, IndexValue> putAndDeletePair = state.allKeys.get(idToDelete);
    blobStoreStats.handleNewDelete(putAndDeletePair.getSecond(), putAndDeletePair.getFirst());
  }

  /**
   * Advance the time to the next nearest second. That is, 1 sec to 2 sec or 1001 ms to 2000ms.
   */
  private void advanceTimeToNextSecond() throws InterruptedException {
    long currentTimeInMs = state.time.milliseconds();
    state.advanceTime(Time.MsPerSec - currentTimeInMs % Time.MsPerSec);
  }

  /**
   * Verify the correctness of valid data size information per container returned by BlobStoreStats and return the
   * total valid data size of all containers.
   * @param deleteAndExpirationRefTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return the total valid data size of all containers (from all serviceIds)
   */
  private long verifyAndGetContainerValidSize(long deleteAndExpirationRefTimeInMs) throws StoreException {
    Map<String, Map<String, Long>> actualContainerValidSizeMap =
        blobStoreStats.getValidDataSizeByContainer(deleteAndExpirationRefTimeInMs);
    Map<String, Map<String, Long>> expectedContainerValidSizeMap =
        getValidSizeByContainer(deleteAndExpirationRefTimeInMs);
    long totalValidSize = 0L;

    for (Map.Entry<String, Map<String, Long>> expectedContainerValidSizeEntry : expectedContainerValidSizeMap.entrySet()) {
      String serviceId = expectedContainerValidSizeEntry.getKey();
      assertTrue("Expected serviceId: " + serviceId + " not found", actualContainerValidSizeMap.containsKey(serviceId));
      Map<String, Long> innerMap = expectedContainerValidSizeEntry.getValue();
      for (Map.Entry<String, Long> innerEntry : innerMap.entrySet()) {
        String containerId = innerEntry.getKey();
        assertTrue("Expected containerId: " + containerId + " not found in serviceId: " + serviceId,
            innerMap.containsKey(containerId));
        long expectedContainerValidSize = innerEntry.getValue();
        long actualContainerValidSize = actualContainerValidSizeMap.get(serviceId).get(containerId);
        assertEquals("Valid data size mismatch for serviceId: " + serviceId + " containerId: " + containerId,
            expectedContainerValidSize, actualContainerValidSize);
        totalValidSize += expectedContainerValidSize;
      }
      assertEquals("Mismatch in number of containerIds in serviceId: " + serviceId, innerMap.size(),
          actualContainerValidSizeMap.get(serviceId).size());
    }
    assertEquals("Mismatch in number of serviceIds", expectedContainerValidSizeMap.size(),
        actualContainerValidSizeMap.size());

    return totalValidSize;
  }

  /**
   * Verify the correctness of valid data size information per log segment returned by BlobStoreStats for a given
   * {@link TimeRange} and return the total valid data size of all log segments.
   * @param timeRange the {@link TimeRange} to be used for the verification
   * @return the total valid data size of all log segments
   * @throws StoreException
   */
  private long verifyAndGetLogSegmentValidSize(TimeRange timeRange) throws StoreException {
    long expiryReferenceTimeMs = timeRange.getEndTimeInMs();
    Pair<Long, NavigableMap<String, Long>> actualLogSegmentValidSizeMap =
        blobStoreStats.getValidDataSizeByLogSegment(timeRange);
    assertTrue("Valid data size collection time should be in the range",
        timeRange.getStartTimeInMs() <= actualLogSegmentValidSizeMap.getFirst()
            && timeRange.getEndTimeInMs() >= actualLogSegmentValidSizeMap.getFirst());

    int expectedNumberOfLogSegments = 0;
    long expectedTotalLogSegmentValidSize = 0L;
    LogSegment logSegment = state.log.getFirstSegment();

    while (logSegment != null) {
      String logSegmentName = logSegment.getName();
      assertTrue("Log segment: " + logSegmentName + " not found",
          actualLogSegmentValidSizeMap.getSecond().containsKey(logSegmentName));

      long expectedLogSegmentValidSize =
          state.getValidDataSizeForLogSegment(logSegment, timeRange.getEndTimeInMs(), expiryReferenceTimeMs);
      long actualLogSegmentValidSize = actualLogSegmentValidSizeMap.getSecond().get(logSegmentName);
      assertEquals("Valid data size mismatch for log segment: " + logSegmentName, expectedLogSegmentValidSize,
          actualLogSegmentValidSize);

      expectedTotalLogSegmentValidSize += expectedLogSegmentValidSize;
      expectedNumberOfLogSegments++;
      logSegment = state.log.getNextSegment(logSegment);
    }

    assertEquals("Mismatch in number of log segments", expectedNumberOfLogSegments,
        actualLogSegmentValidSizeMap.getSecond().size());

    Pair<Long, Long> actualTotalValidSize = blobStoreStats.getValidSize(timeRange);
    assertTrue("Returned collection time should be in the range",
        timeRange.getStartTimeInMs() <= actualTotalValidSize.getFirst()
            && timeRange.getEndTimeInMs() >= actualLogSegmentValidSizeMap.getFirst());
    assertEquals("Total valid data size of all log segments mismatch", expectedTotalLogSegmentValidSize,
        actualTotalValidSize.getSecond().longValue());

    return actualTotalValidSize.getSecond();
  }

  /**
   * Go over the referenceIndex to collect valid data size information per container. The result is used for
   * verification purposes.
   * @param deleteAndExpirationRefTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return a nested {@link Map} of serviceId to containerId to valid data size
   */
  private Map<String, Map<String, Long>> getValidSizeByContainer(long deleteAndExpirationRefTimeInMs) {
    Map<String, Map<String, Long>> containerValidSizeMap = new HashMap<>();
    for (Map.Entry<Offset, TreeMap<MockId, IndexValue>> segmentEntry : state.referenceIndex.entrySet()) {
      List<IndexEntry> validEntries =
          state.getValidIndexEntriesForIndexSegment(segmentEntry.getKey(), deleteAndExpirationRefTimeInMs,
              deleteAndExpirationRefTimeInMs);
      for (IndexEntry indexEntry : validEntries) {
        IndexValue indexValue = indexEntry.getValue();
        if (!indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
          updateNestedMapHelper(containerValidSizeMap, String.valueOf(indexValue.getServiceId()),
              String.valueOf(indexValue.getContainerId()), indexValue.getSize());
        }
      }
    }
    return containerValidSizeMap;
  }

  /**
   * Helper function to update nested map data structure.
   * @param nestedMap nested {@link Map} to be updated
   * @param firstKey of the nested map
   * @param secondKey of the nested map
   * @param value the value to be added at the corresponding entry
   */
  private void updateNestedMapHelper(Map<String, Map<String, Long>> nestedMap, String firstKey, String secondKey,
      Long value) {
    if (!nestedMap.containsKey(firstKey)) {
      nestedMap.put(firstKey, new HashMap<String, Long>());
    }
    Map<String, Long> innerMap = nestedMap.get(firstKey);
    Long newValue = innerMap.containsKey(secondKey) ? innerMap.get(secondKey) + value : value;
    innerMap.put(secondKey, newValue);
  }

  /**
   * Mock {@link Throttler} with latches and a counter to track and control various states during a scan.
   */
  private class MockThrottler extends Throttler {
    final AtomicInteger throttleCount = new AtomicInteger(0);
    volatile boolean isThrottlerStarted = false;
    CountDownLatch startedLatch;
    CountDownLatch holdLatch;

    MockThrottler(CountDownLatch startedLatch, CountDownLatch holdLatch) {
      super(0, 0, true, new MockTime());
      this.startedLatch = startedLatch;
      this.holdLatch = holdLatch;
    }

    @Override
    public void maybeThrottle(double observed) throws InterruptedException {
      throttleCount.incrementAndGet();
      if (!isThrottlerStarted) {
        isThrottlerStarted = true;
        startedLatch.countDown();
        assertTrue("IndexScanner is held for too long", holdLatch.await(3, TimeUnit.SECONDS));
      }
    }
  }

  /**
   * Mock {@link IndexValue} with a latch to act as a probe to inform us about the state of the newEntryQueue in
   * {@link BlobStoreStats}.
   */
  private class MockIndexValue extends IndexValue {
    private final CountDownLatch latch;

    MockIndexValue(CountDownLatch latch, Offset offset) {
      super(0, offset, Utils.Infinite_Time);
      this.latch = latch;
    }

    @Override
    Offset getOffset() {
      latch.countDown();
      return super.getOffset();
    }
  }
}
