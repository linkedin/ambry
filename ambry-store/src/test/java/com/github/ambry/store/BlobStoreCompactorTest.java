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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.store.CuratedLogIndexState.*;
import static com.github.ambry.store.StoreTestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;


/**
 * Tests for {@link BlobStoreCompactor}.
 */
@RunWith(Parameterized.class)
public class BlobStoreCompactorTest {
  private static final Logger logger = LoggerFactory.getLogger(BlobStoreCompactorTest.class);

  private static final String STORE_ID = "compactor_example_store";
  private static final DiskIOScheduler DISK_IO_SCHEDULER = new DiskIOScheduler(null);
  private static final String EXCEPTION_MSG = TestUtils.getRandomString(10);

  private final File tempDir;
  private final String tempDirStr;
  private final boolean doDirectIO;
  private final boolean directIOWithBuffer;
  private final boolean withUndelete;
  private final boolean purgeDeleteTombstone;
  private final StoreConfig config;
  private final Time time = new MockTime();
  private static final String COMPACT_POLICY_INFO_FILE_NAME_V2 = "compactionPolicyInfoV2.json";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private CuratedLogIndexState state = null;
  private BlobStoreCompactor compactor = null;
  private AccountService accountService = null;

  // indicates whether any of InterruptionInducers induced the close/crash.
  private boolean closeOrExceptionInduced = false;
  // for InterruptionInducingLog and InterruptionInducingIndex, an exception is thrown after the operation
  // if throwExceptionBeforeOperation is not true.
  private boolean throwExceptionInsteadOfClose = false;
  // not applicable to the InterruptionInducingDiskIOScheduler. Throws in InterruptionInducingLog and
  // InterruptionInducingIndex irrespective of the value of throwExceptionInsteadOfClose
  private boolean throwExceptionBeforeOperation = false;

  private MetricRegistry metricRegistry;

  private byte[] bundleReadBuffer = new byte[((int) PUT_RECORD_SIZE * 2 + 1)];

  List<String> peerReplicaMountPath = null;

  /**
   * Running for both direct IO compactor and general IO compactor.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    //@formatter:off
    return Arrays.asList(
        new Object[][]{
              {true, true, true, false},
              {false, false, true, false},
              {false, true, false, false},
              {true, true, true, true}
        });
    //@formatter:on
  }

  /**
   * Creates a temporary directory for the store.
   * @throws Exception
   */
  public BlobStoreCompactorTest(boolean doDirectIO, boolean withUndelete, boolean purgeDeleteTombstone,
      boolean directIOWithBuffer) throws Exception {
    tempDir = StoreTestUtils.createTempDirectory("compactorDir-" + TestUtils.getRandomString(10));
    tempDirStr = tempDir.getAbsolutePath();
    config = new StoreConfig(new VerifiableProperties(new Properties()));
    this.doDirectIO = doDirectIO;
    this.withUndelete = withUndelete;
    this.purgeDeleteTombstone = purgeDeleteTombstone;
    if (doDirectIO) {
      assumeTrue(Utils.isLinux());
    }
    this.directIOWithBuffer = directIOWithBuffer;
    accountService = Mockito.mock(AccountService.class);
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws Exception
   */
  @After
  public void cleanup() throws Exception {
    if (peerReplicaMountPath != null) {
      for (String path : peerReplicaMountPath) {
        FileUtils.deleteDirectory(new File(path));
      }
    }

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
    refreshState(false, true, false);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);
    compactor.close(0);
  }

  /**
   * Tests closing without initialization.
   * @throws Exception
   */
  @Test
  public void closeWithoutInitTest() throws Exception {
    refreshState(false, true, false);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.close(0);
  }

  /**
   * Tests attempt to use the service without initializing the service.
   * @throws Exception
   */
  @Test
  public void useServiceWithoutInitTest() throws Exception {
    refreshState(false, true, false);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    LogSegmentName firstSegmentName = state.log.getFirstSegment().getName();
    CompactionDetails details =
        new CompactionDetails(state.time.milliseconds(), Collections.singletonList(firstSegmentName), null);

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
    refreshState(false, true, false);
    // compaction range contains a log segment that is still in the journal
    LogSegmentName firstLogSegmentName = state.referenceIndex.firstKey().getName();
    LogSegmentName secondLogSegmentName = state.log.getNextSegment(state.log.getSegment(firstLogSegmentName)).getName();
    LogSegmentName lastLogSegmentName = state.referenceIndex.lastKey().getName();
    CompactionDetails details = new CompactionDetails(state.time.milliseconds() + Time.MsPerSec,
        Arrays.asList(firstLogSegmentName, lastLogSegmentName), null);
    ensureArgumentFailure(details, "Should have failed because compaction range contains offsets still in the journal");

    // compaction range contains segments in the wrong order
    details = new CompactionDetails(state.time.milliseconds() + Time.MsPerSec,
        Arrays.asList(secondLogSegmentName, firstLogSegmentName), null);
    ensureArgumentFailure(details, "Should have failed because segments are in the wrong order");

    // compaction contains segments that don't exist
    details = new CompactionDetails(0, Collections.singletonList(lastLogSegmentName.getNextPositionName()), null);
    ensureArgumentFailure(details, "Should have failed because compaction range contains offsets still in the journal");

    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity();
    writeDataToMeetRequiredSegmentCount(requiredCount, null);
    LogSegmentName thirdLogSegmentName = state.log.getNextSegment(state.log.getSegment(secondLogSegmentName)).getName();
    // compaction range is not consecutive
    details = new CompactionDetails(state.time.milliseconds() + Time.MsPerSec,
        Arrays.asList(firstLogSegmentName, thirdLogSegmentName), null);
    ensureArgumentFailure(details, "Should have failed because segments are not consecutive");
  }

  @Test
  public void statsBasedCompactionStrategyWithInvalidLogSegment() throws Exception {
    assumeTrue(!withUndelete);
    refreshState(false, true, false);
    // Current log segment is setup like this:
    // three log segment: 0_0, 1_0, 2_0
    // Now set the log segments so that we have:
    // 3_0 doesn't have any valid index values (all expired put)
    // 4_0 has only one valid index value (the rest are expired put)
    // 5_0 has data so 4_0 won't be in the journal
    // This setup would make sure that:
    // 3_0 has value 0 in the result from BlobStoreStats
    // 1_0, 2_0, 4_0 would be the best candidate to compact if we ignore 3_0
    long requiredCount = state.index.getLogSegmentCount();
    long requiredBytes = requiredCount * state.log.getSegmentCapacity();
    long numPuts = (requiredBytes - state.index.getLogUsedCapacity()) / PUT_RECORD_SIZE;
    state.addPutEntries((int) numPuts, PUT_RECORD_SIZE, Utils.Infinite_Time);
    requiredBytes = (requiredCount + 1) * state.log.getSegmentCapacity();
    numPuts = (requiredBytes - state.index.getLogUsedCapacity()) / PUT_RECORD_SIZE;
    state.addPutEntries((int) numPuts, PUT_RECORD_SIZE, 0);
    requiredBytes = (requiredCount + 2) * state.log.getSegmentCapacity();
    numPuts = (requiredBytes - state.index.getLogUsedCapacity()) / PUT_RECORD_SIZE - 1;
    state.addPutEntries((int) numPuts, PUT_RECORD_SIZE, 0L);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    requiredBytes = (requiredCount + 3) * state.log.getSegmentCapacity();
    numPuts = (requiredBytes - state.index.getLogUsedCapacity()) / PUT_RECORD_SIZE;
    state.addPutEntries((int) numPuts, PUT_RECORD_SIZE, Utils.Infinite_Time);

    state.time.setCurrentMilliseconds(System.currentTimeMillis());
    Properties properties = new Properties();
    properties.setProperty("store.min.used.capacity.to.trigger.compaction.in.percentage", "1");
    StoreConfig storeConfig = new StoreConfig(new VerifiableProperties(properties));
    StatsBasedCompactionPolicy policy = new StatsBasedCompactionPolicy(storeConfig, state.time);
    ScheduledExecutorService scheduler = Utils.newScheduler(1, true);

    BlobStoreStats stats =
        new BlobStoreStats("", state.index, 0, Time.MsPerSec, 0, 100, Time.SecsPerMin, false, purgeDeleteTombstone,
            state.time, scheduler, scheduler, DISK_IO_SCHEDULER, new StoreMetrics(new MetricRegistry()), 1, false);
    BlobStoreStats spyStats = Mockito.spy(stats);
    Mockito.doReturn(PUT_RECORD_SIZE).when(spyStats).getMaxBlobSize();
    CompactionDetails details =
        policy.getCompactionDetails(state.log.getCapacityInBytes(), state.index.getLogUsedCapacity(),
            state.log.getSegmentCapacity(), LogSegment.HEADER_SIZE, state.index.getLogSegmentsNotInJournal(), spyStats,
            "/tmp");
    List<LogSegmentName> logSegmentNames = details.getLogSegmentsUnderCompaction();
    assertEquals(1, logSegmentNames.size());
    assertEquals("3" + BlobStore.SEPARATOR + "0", logSegmentNames.get(0).toString());
  }

  /**
   * Test the cases where the whole log segment is removed by the compaction, before and after compaction index segment
   * offsets has to be correct.
   * @throws Exception
   */
  @Test
  public void emptyOneLogSegmentInCompactionTest() throws Exception {
    refreshState(false, false, false);
    // Set up log segment like this
    // 0_0 doesn't have any valid values (all expired put), will compact 0_0 by itself.
    // 1_0 is full of PUT, some of them are going to be compacted.
    // 2_0 doesn't have any valid values (all expired put), will compact 1_0 and 2_0 together
    // 3_0 is full of valid PUTs
    // 4_0 is doesn't have any valid values (all expired put), will compact 4_0 by itself.

    int numPuts = (int) ((state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE) / (PUT_RECORD_SIZE));
    state.addPutEntries(numPuts, PUT_RECORD_SIZE, 0);
    assertEquals(1, state.index.getLogSegmentCount());
    state.addPutEntries(numPuts - 3, PUT_RECORD_SIZE, Utils.Infinite_Time);
    state.addPutEntries(3, PUT_RECORD_SIZE, 0);
    assertEquals(2, state.index.getLogSegmentCount());
    state.addPutEntries(numPuts, PUT_RECORD_SIZE, 0);
    assertEquals(3, state.index.getLogSegmentCount());
    state.addPutEntries(numPuts, PUT_RECORD_SIZE, Utils.Infinite_Time);
    assertEquals(4, state.index.getLogSegmentCount());
    state.addPutEntries(numPuts, PUT_RECORD_SIZE, 0);
    assertEquals(5, state.index.getLogSegmentCount());
    writeDataToMeetRequiredSegmentCount(6, Collections.singletonList(Utils.Infinite_Time));

    state.time.setCurrentMilliseconds(System.currentTimeMillis());
    state.reloadIndex(true, false);

    // Compact 0_0, the target would be empty, and there is no index segment before the target, the before and after index
    // segment offset map would be empty in compaction log.
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 1);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);

    CompactionDetails details = new CompactionDetails(time.milliseconds(), segmentsUnderCompaction, null);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }

    CompactionLog latestLog = getLatestCompactionLog();
    assertEquals(0, latestLog.getBeforeAndAfterIndexSegmentOffsets().size());

    state.reloadIndex(true, false);
    // 0_0 is gone, now 1_0 is the first log segment
    segmentsUnderCompaction = getLogSegments(0, 2);
    details = new CompactionDetails(time.milliseconds(), segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    state.reloadIndex(true, false);
    latestLog = getLatestCompactionLog();
    Offset lastIndexSegmentOffsetForTarget =
        state.index.getIndexSegments().floorKey(new Offset(LogSegmentName.fromPositionAndGeneration(3, 0), 0));
    assertEquals(LogSegmentName.fromPositionAndGeneration(1, 1), lastIndexSegmentOffsetForTarget.getName());
    for (Map.Entry<Offset, Offset> entry : latestLog.getBeforeAndAfterIndexSegmentOffsets().entrySet()) {
      if (entry.getKey().getName().equals(LogSegmentName.fromPositionAndGeneration(2, 0))) {
        assertEquals(lastIndexSegmentOffsetForTarget, entry.getValue());
      }
    }

    state.reloadIndex(true, false);
    // 0_0 and 2_0 are gone, now it's 1_1, 3_0, 4_0
    segmentsUnderCompaction = getLogSegments(2, 1);
    details = new CompactionDetails(time.milliseconds(), segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    state.reloadIndex(true, false);
    latestLog = getLatestCompactionLog();
    Offset lastIndexSegmentOffsetFromSource =
        state.index.getIndexSegments().floorKey(new Offset(LogSegmentName.fromPositionAndGeneration(4, 0), 0));
    assertEquals(LogSegmentName.fromPositionAndGeneration(3, 0), lastIndexSegmentOffsetFromSource.getName());
    for (Map.Entry<Offset, Offset> entry : latestLog.getBeforeAndAfterIndexSegmentOffsets().entrySet()) {
      assertEquals(lastIndexSegmentOffsetFromSource, entry.getValue());
    }
  }

  /**
   * Tests to make sure that {@link BlobStoreCompactor#compact(CompactionDetails, byte[])} fails when a compaction is
   * already in progress.
   * @throws Exception
   */
  @Test
  public void compactWithCompactionInProgressTest() throws Exception {
    refreshState(false, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    CompactionDetails details = new CompactionDetails(0, segmentsUnderCompaction, null);

    // create a compaction log in order to mimic a compaction being in progress
    try (CompactionLog cLog = new CompactionLog(tempDirStr, STORE_ID, state.time, details, config)) {
      compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
      compactor.initialize(state.index);
      compactor.compact(details, bundleReadBuffer);
      fail("compact() should have failed because a compaction is already in progress");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    } finally {
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
    refreshState(false, true, false);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
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

  @Test
  public void getIndexSegmentDetailsTest() throws Exception {
    // Create 0_1 log and 0_10 log and their index segment files, use getIndexSegmentDetails to load all the index segment
    // files for 0_1, making sure that 0_10 log's index segment files won't be included in the final result.
    refreshState(false, false, false);
    LogSegmentName name01 = LogSegmentName.fromPositionAndGeneration(0, 1);
    LogSegmentName name010 = LogSegmentName.fromPositionAndGeneration(0, 10);
    Files.createFile(Paths.get(tempDirStr, name01.toFilename()));
    Files.createFile(Paths.get(tempDirStr, name010.toFilename()));

    // Create 3 index segment files for each log segment

    for (LogSegmentName logSegmentName : new LogSegmentName[]{name01, name010}) {
      for (long off : new long[]{18, 1000, 2000}) {
        Offset offset = new Offset(logSegmentName, off);
        Files.createFile(Paths.get(tempDirStr,
            IndexSegment.generateIndexSegmentFilenamePrefix(offset) + IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX));
      }
    }
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    SortedMap<Offset, File> details = compactor.getIndexSegmentDetails(name01);
    assertEquals(3, details.size());
  }

  /**
   * A basic test for compaction that selects the first two log segments and compacts them into one log segment.
   * @throws Exception
   */
  @Test
  public void basicTest() throws Exception {
    refreshState(false, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    long deleteReferenceTimeMs = reduceValidDataSizeInLogSegments(segmentsUnderCompaction,
        state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE);
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, true);
  }

  /**
   * Tests that permanent delete tombstone can be compacted once all peer tokens are past its position.
   * @throws Exception
   */
  @Test
  public void deleteTombstoneCleanupTest() throws Exception {
    assumeTrue(purgeDeleteTombstone);
    refreshState(false, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    CompactionDetails details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    List<MockReplicaId> localAndPeerReplicas = generateLocalAndPeerReplicas();
    ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
    MockIdFactory keyFactory = new MockIdFactory();
    RemoteTokenTracker tokenTracker =
        new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    // Generate tokens for peer replicas and make sure they are both past position of 1st delete tombstone and haven't
    // reached position of 2nd tombstone.
    MockId tombstone1 = state.permanentDeleteTombstones.get(0);
    MockId tombstone2 = state.permanentDeleteTombstones.get(1);
    IndexValue deleteIndexValue1 = state.index.findKey(tombstone1);
    IndexValue deleteIndexValue2 = state.index.findKey(tombstone2);
    IndexSegment indexSegment1 = state.index.getIndexSegments().floorEntry(deleteIndexValue1.getOffset()).getValue();
    IndexSegment indexSegment2 = state.index.getIndexSegments().floorEntry(deleteIndexValue2.getOffset()).getValue();
    // find a key that is behind tombstone1 and the other key that falls between tombstone1 and tombstone2
    IndexSegment segmentBehindSegment1 =
        state.index.getIndexSegments().higherEntry(indexSegment1.getStartOffset()).getValue();
    MockId keyInToken1 = (MockId) segmentBehindSegment1.iterator().next().getKey();
    MockId keyInToken2 = (MockId) indexSegment2.iterator().next().getKey();
    StoreFindToken peerToken1 =
        new StoreFindToken(keyInToken1, segmentBehindSegment1.getStartOffset(), state.sessionId, state.incarnationId,
            segmentBehindSegment1.getResetKey(), segmentBehindSegment1.getResetKeyType(),
            segmentBehindSegment1.getResetKeyLifeVersion());
    StoreFindToken peerToken2 =
        new StoreFindToken(keyInToken2, indexSegment2.getStartOffset(), state.sessionId, state.incarnationId,
            indexSegment2.getResetKey(), indexSegment2.getResetKeyType(), indexSegment2.getResetKeyLifeVersion());
    // update token associated with peer replica
    MockReplicaId peerReplica1 = localAndPeerReplicas.get(1);
    MockReplicaId peerReplica2 = localAndPeerReplicas.get(2);
    tokenTracker.updateTokenFromPeerReplica(peerToken1, peerReplica1.getDataNodeId().getHostname(),
        peerReplica1.getReplicaPath());
    tokenTracker.updateTokenFromPeerReplica(peerToken2, peerReplica2.getDataNodeId().getHostname(),
        peerReplica2.getReplicaPath());
    // initiate compaction
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, tokenTracker, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    tokenTracker.close();

    // the first delete tombstone should be compacted
    assertNull("Delete tombstone should be compacted", state.index.findKey(tombstone1));
    // the second delete tombstone should exist
    assertNotNull("Delete tombstone should be present as at least one token hasn't reached its position",
        state.index.findKey(tombstone2));
  }

  /**
   * Test finding key with old file span. The log segment associated with start offset in file span has been compacted.
   * @throws Exception
   */
  @Test
  public void findKeyWithOldFileSpan() throws Exception {
    assumeTrue(purgeDeleteTombstone);
    refreshState(false, true, false);
    FileSpan oldFileSpan = new FileSpan(state.index.getStartOffset(), state.index.getCurrentEndOffset());
    MockId tombstone1 = state.permanentDeleteTombstones.get(0);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    CompactionDetails details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // find the key by using old file span (the 0_0 log segment has been compacted to 0_1). The index should use current
    // first segment if there is no log segment found less than or equal to start offset in old file span.
    IndexValue indexValue = state.index.findKey(tombstone1, oldFileSpan);
    assertNotNull("The key should exist", indexValue);
  }

  /**
   * Tests compacting delete tombstone with both invalid and journal based tokens.
   * @throws Exception
   */
  @Test
  public void compactDeleteTombstoneTwiceTest() throws Exception {
    assumeTrue(purgeDeleteTombstone);
    refreshState(false, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    CompactionDetails details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    List<MockReplicaId> localAndPeerReplicas = generateLocalAndPeerReplicas();
    ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
    MockIdFactory keyFactory = new MockIdFactory();
    RemoteTokenTracker tokenTracker =
        new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    MockId tombstone1 = state.permanentDeleteTombstones.get(0);
    MockId tombstone2 = state.permanentDeleteTombstones.get(1);
    IndexValue deleteIndexValue2 = state.index.findKey(tombstone2);
    IndexSegment indexSegment2 = state.index.getIndexSegments().floorEntry(deleteIndexValue2.getOffset()).getValue();
    MockId keyInToken = (MockId) indexSegment2.iterator().next().getKey();

    UUID invalidIncarnationId;
    do {
      invalidIncarnationId = UUID.randomUUID();
    } while (invalidIncarnationId.equals(state.incarnationId));
    // invalid token with other incarnation id
    StoreFindToken invalidToken =
        new StoreFindToken(keyInToken, indexSegment2.getStartOffset(), state.sessionId, invalidIncarnationId,
            indexSegment2.getResetKey(), indexSegment2.getResetKeyType(), indexSegment2.getResetKeyLifeVersion());
    StoreFindToken peerToken2 =
        new StoreFindToken(keyInToken, indexSegment2.getStartOffset(), state.sessionId, state.incarnationId,
            indexSegment2.getResetKey(), indexSegment2.getResetKeyType(), indexSegment2.getResetKeyLifeVersion());
    MockReplicaId peerReplica1 = localAndPeerReplicas.get(1);
    MockReplicaId peerReplica2 = localAndPeerReplicas.get(2);
    tokenTracker.updateTokenFromPeerReplica(invalidToken, peerReplica1.getDataNodeId().getHostname(),
        peerReplica1.getReplicaPath());
    tokenTracker.updateTokenFromPeerReplica(peerToken2, peerReplica2.getDataNodeId().getHostname(),
        peerReplica2.getReplicaPath());
    // initiate compaction
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, tokenTracker, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // both tombstones should exist
    assertNotNull("Delete tombstone should be present", state.index.findKey(tombstone1));
    assertNotNull("Delete tombstone should be present", state.index.findKey(tombstone2));

    // update remote token tracker with journal based tokens and compact again.
    IndexSegment lastIndexSegment = state.index.getIndexSegments().lastEntry().getValue();
    peerToken2 = new StoreFindToken(lastIndexSegment.getStartOffset(), state.sessionId, state.incarnationId, true,
        lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(), lastIndexSegment.getResetKeyLifeVersion());
    tokenTracker.updateTokenFromPeerReplica(peerToken2, peerReplica1.getDataNodeId().getHostname(),
        peerReplica1.getReplicaPath());
    tokenTracker.updateTokenFromPeerReplica(peerToken2, peerReplica2.getDataNodeId().getHostname(),
        peerReplica2.getReplicaPath());
    // initiate compaction
    segmentsUnderCompaction = getLogSegments(0, 2);
    details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, tokenTracker, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    tokenTracker.close();

    // both tombstones should be compacted
    assertNull("Delete tombstone should be compacted", state.index.findKey(tombstone1));
    assertNull("Delete tombstone should be compacted", state.index.findKey(tombstone2));
  }

  /**
   * Tests RemoteTokenTracker persist the remote tokens in different case.
   * @throws Exception
   */
  @Test
  public void remoteTokenPersistTest() throws Exception {
    assumeTrue(purgeDeleteTombstone);
    refreshState(false, true, false);
    List<MockReplicaId> localAndPeerReplicas = generateLocalAndPeerReplicas();
    MockIdFactory keyFactory = new MockIdFactory();
    ScheduledExecutorService scheduler = Utils.newScheduler(1, true);

    // Prepare for the token to update
    // Generate tokens for peer replicas and make sure they are both past position of 1st delete tombstone and haven't
    // reached position of 2nd tombstone.
    MockId tombstone1 = state.permanentDeleteTombstones.get(0);
    MockId tombstone2 = state.permanentDeleteTombstones.get(1);
    IndexValue deleteIndexValue1 = state.index.findKey(tombstone1);
    IndexValue deleteIndexValue2 = state.index.findKey(tombstone2);
    IndexSegment indexSegment1 = state.index.getIndexSegments().floorEntry(deleteIndexValue1.getOffset()).getValue();
    IndexSegment indexSegment2 = state.index.getIndexSegments().floorEntry(deleteIndexValue2.getOffset()).getValue();
    MockId keyInToken1 = (MockId) indexSegment1.iterator().next().getKey();
    MockId keyInToken2 = (MockId) indexSegment2.iterator().next().getKey();
    StoreFindToken peerToken1 =
        new StoreFindToken(keyInToken1, indexSegment1.getStartOffset(), state.sessionId, state.incarnationId,
            indexSegment1.getResetKey(), indexSegment1.getResetKeyType(), indexSegment1.getResetKeyLifeVersion());
    StoreFindToken peerToken2 =
        new StoreFindToken(keyInToken2, indexSegment2.getStartOffset(), state.sessionId, state.incarnationId,
            indexSegment2.getResetKey(), indexSegment2.getResetKeyType(), indexSegment2.getResetKeyLifeVersion());
    // update token associated with peer replica
    MockReplicaId peerReplica1 = localAndPeerReplicas.get(1);
    MockReplicaId peerReplica2 = localAndPeerReplicas.get(2);

    // Case 1: Test token persist file doesn't exist
    RemoteTokenTracker tokenTracker =
        new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> notPersistedToken = tokenTracker.getPeerReplicaAndToken();
    assertEquals(2, notPersistedToken.size());
    for (Map.Entry<String, Pair<Long, FindToken>> entry : notPersistedToken.entrySet()) {
      assertEquals(FindTokenType.Uninitialized, entry.getValue().getSecond().getType());
    }
    assertEquals(true, tokenTracker.persistToken());    // persist the token

    // Case 2: Although the file exist, no meaningful token content. The token timestamp should be the same as before
    tokenTracker = new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> persistedEmptyToken = tokenTracker.getPeerReplicaAndToken();
    assertEquals(2, persistedEmptyToken.size());
    for (Map.Entry<String, Pair<Long, FindToken>> entry : persistedEmptyToken.entrySet()) {
      Pair<Long, FindToken> before = notPersistedToken.get(entry.getKey());
      assertEquals(entry.getValue().getFirst(), before.getFirst());
      assertEquals(FindTokenType.Uninitialized, entry.getValue().getSecond().getType());
    }
    assertEquals(notPersistedToken, persistedEmptyToken);
    // advance time
    state.time.sleep(10);
    // update peerReplica1's token
    tokenTracker.updateTokenFromPeerReplica(peerToken1, peerReplica1.getDataNodeId().getHostname(),
        peerReplica1.getReplicaPath());
    assertEquals(true, tokenTracker.persistToken());

    // Case 3: peerReplica1 should have the valid token
    tokenTracker = new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> oneValidToken = tokenTracker.getPeerReplicaAndToken();
    assertEquals(2, oneValidToken.size());
    for (Map.Entry<String, Pair<Long, FindToken>> entry : oneValidToken.entrySet()) {
      Pair<Long, FindToken> before = persistedEmptyToken.get(entry.getKey());
      if (entry.getKey().startsWith(peerReplica1.getDataNodeId().getHostname())) {
        // token is updated
        assertTrue(entry.getValue().getFirst() > before.getFirst());
        assertEquals(entry.getValue().getSecond(), peerToken1);
      } else {
        assertEquals(entry.getValue().getFirst(), before.getFirst());
        assertEquals(FindTokenType.Uninitialized, entry.getValue().getSecond().getType());
      }
    }
    state.time.sleep(10);
    // update peerReplica2's token as well
    tokenTracker.updateTokenFromPeerReplica(peerToken2, peerReplica2.getDataNodeId().getHostname(),
        peerReplica2.getReplicaPath());
    assertEquals(true, tokenTracker.persistToken());

    // Case 4: both tokens are updated
    tokenTracker = new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> twoValidTokens = tokenTracker.getPeerReplicaAndToken();
    assertEquals(2, twoValidTokens.size());
    for (Map.Entry<String, Pair<Long, FindToken>> entry : twoValidTokens.entrySet()) {
      Pair<Long, FindToken> before = oneValidToken.get(entry.getKey());
      if (entry.getKey().startsWith(peerReplica2.getDataNodeId().getHostname())) {
        // token is updated
        assertTrue(entry.getValue().getFirst() > before.getFirst());
        assertEquals(entry.getValue().getSecond(), peerToken2);
      } else {
        assertEquals(entry.getValue().getFirst(), before.getFirst());
        assertEquals(entry.getValue().getSecond(), peerToken1);
      }
    }
    // corrupt the persist file
    String filePath = tokenTracker.getPersistFilePath();
    File file = new File(filePath);
    FileOutputStream fileStream = new FileOutputStream(file);
    DataOutputStream writer = new DataOutputStream(fileStream);
    // version part.
    writer.writeShort(765);
    fileStream.getChannel().force(true);
    writer.close();

    // Case 5: Corrupted the file
    long currentTime = state.time.milliseconds();
    tokenTracker = new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> corrupted = tokenTracker.getPeerReplicaAndToken();
    assertEquals(2, corrupted.size());
    for (Map.Entry<String, Pair<Long, FindToken>> entry : corrupted.entrySet()) {
      assertEquals(FindTokenType.Uninitialized, entry.getValue().getSecond().getType());
      assertTrue(entry.getValue().getFirst() >= currentTime);
    }
    assertEquals(true, tokenTracker.persistToken());    // persist the token

    // Case 6: The file is back to normal.
    tokenTracker = new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> newToken = tokenTracker.getPeerReplicaAndToken();
    assertEquals(2, newToken.size());
    for (Map.Entry<String, Pair<Long, FindToken>> entry : newToken.entrySet()) {
      Pair<Long, FindToken> before = corrupted.get(entry.getKey());
      assertEquals(FindTokenType.Uninitialized, entry.getValue().getSecond().getType());
      assertEquals(before.getFirst(), entry.getValue().getFirst());       // same time stamp
    }
    assertEquals(true, tokenTracker.persistToken());

    // Case 7: Remove one replica
    MockReplicaId modifiedLocalReplica = localAndPeerReplicas.get(0);
    modifiedLocalReplica.setPeerReplicas(Collections.singletonList(peerReplica1));
    tokenTracker = new RemoteTokenTracker(modifiedLocalReplica, scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> onePeerOnlyToken = tokenTracker.getPeerReplicaAndToken();
    assertEquals(1, onePeerOnlyToken.size()); // only have one entry
    for (Map.Entry<String, Pair<Long, FindToken>> entry : onePeerOnlyToken.entrySet()) {
      Pair<Long, FindToken> before = newToken.get(entry.getKey());
      assertEquals(FindTokenType.Uninitialized, entry.getValue().getSecond().getType());
      assertEquals(before.getFirst(), entry.getValue().getFirst()); // same time stamp
      assertTrue(entry.getKey().startsWith(peerReplica1.getDataNodeId().getHostname()));
    }
    assertEquals(true, tokenTracker.persistToken());
  }

  /**
   * Tests tokens are persisted and
   * permanent delete tombstone can be compacted once all peer tokens are past its position.
   * @throws Exception
   */
  @Test
  public void deleteTombstoneTokenPersistTest() throws Exception {
    assumeTrue(purgeDeleteTombstone);
    refreshState(false, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    CompactionDetails details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    List<MockReplicaId> localAndPeerReplicas = generateLocalAndPeerReplicas();
    ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
    MockIdFactory keyFactory = new MockIdFactory();
    RemoteTokenTracker tokenTracker =
        new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    tokenTracker.close(); // stop the background persist. will explicitly call persistToken later.
    // Generate tokens for peer replicas and make sure they are both past position of 1st delete tombstone and haven't
    // reached position of 2nd tombstone.
    MockId tombstone1 = state.permanentDeleteTombstones.get(0);
    MockId tombstone2 = state.permanentDeleteTombstones.get(1);
    IndexValue deleteIndexValue1 = state.index.findKey(tombstone1);
    IndexValue deleteIndexValue2 = state.index.findKey(tombstone2);
    IndexSegment indexSegment1 = state.index.getIndexSegments().floorEntry(deleteIndexValue1.getOffset()).getValue();
    IndexSegment indexSegment2 = state.index.getIndexSegments().floorEntry(deleteIndexValue2.getOffset()).getValue();
    // find a key that is behind tombstone1 and the other key that falls between tombstone1 and tombstone2
    IndexSegment segmentBehindSegment1 =
        state.index.getIndexSegments().higherEntry(indexSegment1.getStartOffset()).getValue();
    MockId keyInToken1 = (MockId) segmentBehindSegment1.iterator().next().getKey();
    MockId keyInToken2 = (MockId) indexSegment2.iterator().next().getKey();
    StoreFindToken peerToken1 =
        new StoreFindToken(keyInToken1, segmentBehindSegment1.getStartOffset(), state.sessionId, state.incarnationId,
            segmentBehindSegment1.getResetKey(), segmentBehindSegment1.getResetKeyType(),
            segmentBehindSegment1.getResetKeyLifeVersion());
    StoreFindToken peerToken2 =
        new StoreFindToken(keyInToken2, indexSegment2.getStartOffset(), state.sessionId, state.incarnationId,
            indexSegment2.getResetKey(), indexSegment2.getResetKeyType(), indexSegment2.getResetKeyLifeVersion());
    // update token associated with peer replica
    MockReplicaId peerReplica1 = localAndPeerReplicas.get(1);
    MockReplicaId peerReplica2 = localAndPeerReplicas.get(2);
    tokenTracker.updateTokenFromPeerReplica(peerToken1, peerReplica1.getDataNodeId().getHostname(),
        peerReplica1.getReplicaPath());
    tokenTracker.updateTokenFromPeerReplica(peerToken2, peerReplica2.getDataNodeId().getHostname(),
        peerReplica2.getReplicaPath());
    Map<String, Pair<Long, FindToken>> tokenBefore = tokenTracker.getPeerReplicaAndToken();
    tokenTracker.persistToken();

    // start a new RemoteTokenTracker. It'll pick up the persisted remote tokens.
    tokenTracker = new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> tokenAfter = tokenTracker.getPeerReplicaAndToken();

    // test if the file is not there.
    assertEquals(tokenBefore, tokenAfter);
    // initiate compaction
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, tokenTracker, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }

    // the first delete tombstone should be compacted
    assertNull("Delete tombstone should be compacted", state.index.findKey(tombstone1));
    // the second delete tombstone should exist
    assertNotNull("Delete tombstone should be present as at least one token hasn't reached its position",
        state.index.findKey(tombstone2));
  }

  /**
   * Test the case where peers went offline for a long time and they should be ignored when checking if a delete tombstone
   * is still valid
   * @throws Exception
   */
  @Test
  public void deleteTombstoneIgnoreOfflinePeers() throws Exception {
    assumeTrue(purgeDeleteTombstone);
    refreshState(false, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    CompactionDetails details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    List<MockReplicaId> localAndPeerReplicas = generateLocalAndPeerReplicas();
    ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
    MockIdFactory keyFactory = new MockIdFactory();
    RemoteTokenTracker tokenTracker =
        new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    tokenTracker.close(); // stop the background persist. will explicitly call persistToken later.
    // Generate token that is past position of 1st delete tombstone and haven't reached position of 2nd tombstone.
    MockId tombstone1 = state.permanentDeleteTombstones.get(0);
    MockId tombstone2 = state.permanentDeleteTombstones.get(1);
    IndexValue deleteIndexValue1 = state.index.findKey(tombstone1);
    IndexSegment indexSegment1 = state.index.getIndexSegments().floorEntry(deleteIndexValue1.getOffset()).getValue();
    IndexSegment segmentBehindSegment1 =
        state.index.getIndexSegments().higherEntry(indexSegment1.getStartOffset()).getValue();
    MockId keyInToken = (MockId) segmentBehindSegment1.iterator().next().getKey();
    // We have two tokens now, first token is an uninitialized token, second token is the one point to somewhere between
    // first tombstone and second tombstone.
    StoreFindToken peerToken1 = new StoreFindToken();
    StoreFindToken peerToken2 =
        new StoreFindToken(keyInToken, segmentBehindSegment1.getStartOffset(), state.sessionId, state.incarnationId,
            segmentBehindSegment1.getResetKey(), segmentBehindSegment1.getResetKeyType(),
            segmentBehindSegment1.getResetKeyLifeVersion());
    // update token associated with peer replica
    MockReplicaId peerReplica1 = localAndPeerReplicas.get(1);
    MockReplicaId peerReplica2 = localAndPeerReplicas.get(2);
    tokenTracker.updateTokenFromPeerReplica(peerToken1, peerReplica1.getDataNodeId().getHostname(),
        peerReplica1.getReplicaPath());
    tokenTracker.updateTokenFromPeerReplica(peerToken2, peerReplica2.getDataNodeId().getHostname(),
        peerReplica2.getReplicaPath());
    tokenTracker.persistToken();
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, tokenTracker, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }

    // both delete tombstones should be not compacted
    assertNotNull(state.index.findKey(tombstone1));
    assertNotNull(state.index.findKey(tombstone2));

    // Now move the timestamp forward for 7+1 days.
    final int offlineReplicasDays = 7;
    state.time.sleep(TimeUnit.DAYS.toMillis(offlineReplicasDays + 1));
    // Update the timestamp for second peer
    deleteIndexValue1 = state.index.findKey(tombstone1);
    indexSegment1 = state.index.getIndexSegments().floorEntry(deleteIndexValue1.getOffset()).getValue();
    segmentBehindSegment1 = state.index.getIndexSegments().higherEntry(indexSegment1.getStartOffset()).getValue();
    keyInToken = (MockId) segmentBehindSegment1.iterator().next().getKey();
    peerToken2 =
        new StoreFindToken(keyInToken, segmentBehindSegment1.getStartOffset(), state.sessionId, state.incarnationId,
            segmentBehindSegment1.getResetKey(), segmentBehindSegment1.getResetKeyType(),
            segmentBehindSegment1.getResetKeyLifeVersion());
    tokenTracker.updateTokenFromPeerReplica(peerToken2, peerReplica2.getDataNodeId().getHostname(),
        peerReplica2.getReplicaPath());
    segmentsUnderCompaction = getLogSegments(0, 2);
    details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, tokenTracker, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // both delete tombstones should be not compacted since the configuration to ignore offline replicas is not turned on
    assertNotNull(state.index.findKey(tombstone1));
    assertNotNull(state.index.findKey(tombstone2));

    // Now turn on the configuration
    deleteIndexValue1 = state.index.findKey(tombstone1);
    indexSegment1 = state.index.getIndexSegments().floorEntry(deleteIndexValue1.getOffset()).getValue();
    segmentBehindSegment1 = state.index.getIndexSegments().higherEntry(indexSegment1.getStartOffset()).getValue();
    keyInToken = (MockId) segmentBehindSegment1.iterator().next().getKey();
    peerToken2 =
        new StoreFindToken(keyInToken, segmentBehindSegment1.getStartOffset(), state.sessionId, state.incarnationId,
            segmentBehindSegment1.getResetKey(), segmentBehindSegment1.getResetKeyType(),
            segmentBehindSegment1.getResetKeyLifeVersion());
    tokenTracker.updateTokenFromPeerReplica(peerToken2, peerReplica2.getDataNodeId().getHostname(),
        peerReplica2.getReplicaPath());
    state.properties.setProperty(StoreConfig.storeCompactionIgnorePeersUnavailableForDaysName,
        String.valueOf(offlineReplicasDays));
    segmentsUnderCompaction = getLogSegments(0, 2);
    details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, tokenTracker, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // first replica should be compacted
    assertNull("First delete tombstone " + tombstone1 + " should be compacted", state.index.findKey(tombstone1));
    assertNotNull(state.index.findKey(tombstone2));

    // Sleep for another 7 days so peer2 would become offline as well
    state.time.sleep(TimeUnit.DAYS.toMillis(offlineReplicasDays + 1));
    segmentsUnderCompaction = getLogSegments(0, 2);
    details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, tokenTracker, false);
    compactor.initialize(state.index);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // first replica should be compacted
    assertNull("First delete tombstone " + tombstone1 + " should be compacted", state.index.findKey(tombstone1));
    assertNull(state.index.findKey(tombstone2));
  }

  /**
   * Get the store token behind the tombstone
   * @return the store token behind the tombstone
   */
  private StoreFindToken getTombstoneToken(int index) throws Exception {
    if (index >= state.permanentDeleteTombstones.size()) {
      return null;
    }

    MockId tombstone = state.permanentDeleteTombstones.get(index);
    IndexValue deleteIndexValue = state.index.findKey(tombstone);
    IndexSegment indexSegment = state.index.getIndexSegments().floorEntry(deleteIndexValue.getOffset()).getValue();
    // find a key that is behind tombstone
    IndexSegment segmentBehindSegment =
        state.index.getIndexSegments().higherEntry(indexSegment.getStartOffset()).getValue();
    MockId keyInToken1 = (MockId) segmentBehindSegment.iterator().next().getKey();
    StoreFindToken peerToken =
        new StoreFindToken(keyInToken1, segmentBehindSegment.getStartOffset(), state.sessionId, state.incarnationId,
            segmentBehindSegment.getResetKey(), segmentBehindSegment.getResetKeyType(),
            segmentBehindSegment.getResetKeyLifeVersion());
    return peerToken;
  }

  /**
   * Test the remote token persistor is disabled by default
   * @throws Exception
   */
  // Right now we always enable remote token persistor.
  @Ignore
  @Test
  public void testRemoteTokenPersistIsDisabled() throws Exception {
    assumeTrue(purgeDeleteTombstone);
    refreshState(false, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    CompactionDetails details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    List<MockReplicaId> localAndPeerReplicas = generateLocalAndPeerReplicas();
    ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
    MockIdFactory keyFactory = new MockIdFactory();
    RemoteTokenTracker tokenTracker =
        new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    // Try to start the persistor. But by default, storePersistRemoteTokenIntervalInSeconds is zero and won't start it.
    tokenTracker.start(config.storePersistRemoteTokenIntervalInSeconds);
    Map<String, Pair<Long, FindToken>> emptyTokens = tokenTracker.getPeerReplicaAndToken();

    // Generate one token
    StoreFindToken tombstoneToken = getTombstoneToken(0);
    // update token associated with peer replicas
    tokenTracker.updateTokenFromPeerReplica(tombstoneToken, localAndPeerReplicas.get(1).getDataNodeId().getHostname(),
        localAndPeerReplicas.get(1).getReplicaPath());
    tokenTracker.updateTokenFromPeerReplica(tombstoneToken, localAndPeerReplicas.get(2).getDataNodeId().getHostname(),
        localAndPeerReplicas.get(2).getReplicaPath());
    Map<String, Pair<Long, FindToken>> validTokens = tokenTracker.getPeerReplicaAndToken();
    tokenTracker.close();

    // start a new RemoteTokenTracker. It'll be empty tokens since we didn't persist it in the last run.
    tokenTracker = new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> tokenAfter = tokenTracker.getPeerReplicaAndToken();

    // check the tokens
    for (Map.Entry<String, Pair<Long, FindToken>> entry : tokenAfter.entrySet()) {
      Pair<Long, FindToken> afterToken = entry.getValue();
      Pair<Long, FindToken> emptyToken = emptyTokens.get(entry.getKey());
      Pair<Long, FindToken> validToken = validTokens.get(entry.getKey());

      assertEquals(FindTokenType.Uninitialized, afterToken.getSecond().getType());
      assertEquals(FindTokenType.Uninitialized, emptyToken.getSecond().getType());
      assertEquals(tombstoneToken, validToken.getSecond());
      assertTrue(validToken.getFirst() >= emptyToken.getFirst());
      assertTrue(afterToken.getFirst() >= validToken.getFirst());
    }
  }

  /**
   * Test the remote token persist is enabled by the StoreConfig
   * @throws Exception
   */
  @Test
  public void testRemoteTokenPersistIsEnabled() throws Exception {
    assumeTrue(purgeDeleteTombstone);
    refreshState(false, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    List<MockReplicaId> localAndPeerReplicas = generateLocalAndPeerReplicas();
    ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
    MockIdFactory keyFactory = new MockIdFactory();
    RemoteTokenTracker tokenTracker =
        new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Properties properties = new Properties();
    int intervalInSeconds = 5;
    properties.setProperty(StoreConfig.storePersistRemoteTokenIntervalInSecondsName,
        Integer.toString(intervalInSeconds));
    StoreConfig storeConfig = new StoreConfig(new VerifiableProperties(properties));

    // Starting the persistor with the storeConfig
    tokenTracker.start(storeConfig.storePersistRemoteTokenIntervalInSeconds);
    Map<String, Pair<Long, FindToken>> emptyTokens = tokenTracker.getPeerReplicaAndToken();

    // Generate one token
    StoreFindToken tombstoneToken = getTombstoneToken(0);
    // update token associated with peer replicas
    tokenTracker.updateTokenFromPeerReplica(tombstoneToken, localAndPeerReplicas.get(1).getDataNodeId().getHostname(),
        localAndPeerReplicas.get(1).getReplicaPath());
    tokenTracker.updateTokenFromPeerReplica(tombstoneToken, localAndPeerReplicas.get(2).getDataNodeId().getHostname(),
        localAndPeerReplicas.get(2).getReplicaPath());
    Map<String, Pair<Long, FindToken>> validTokens = tokenTracker.getPeerReplicaAndToken();
    Thread.sleep(intervalInSeconds * 1000);
    tokenTracker.close();

    // start a new RemoteTokenTracker. It'll pick up the persisted remote tokens.
    tokenTracker = new RemoteTokenTracker(localAndPeerReplicas.get(0), scheduler, keyFactory, state.time);
    Map<String, Pair<Long, FindToken>> tokenAfter = tokenTracker.getPeerReplicaAndToken();

    // check the tokens
    for (Map.Entry<String, Pair<Long, FindToken>> entry : tokenAfter.entrySet()) {
      Pair<Long, FindToken> afterToken = entry.getValue();
      Pair<Long, FindToken> emptyToken = emptyTokens.get(entry.getKey());
      Pair<Long, FindToken> validToken = validTokens.get(entry.getKey());

      assertEquals(FindTokenType.Uninitialized, emptyToken.getSecond().getType());
      assertEquals(tombstoneToken, validToken.getSecond());
      assertEquals(tombstoneToken, afterToken.getSecond());
      assertTrue(emptyToken.getFirst() <= validToken.getFirst());
      assertEquals(validToken.getFirst(), afterToken.getFirst());
    }
  }

  /**
   * A test similar to basicTest but doesn't use bundleReadBuffer.
   * @throws Exception
   */
  @Test
  public void basicTestNoBundleReadBuffer() throws Exception {
    bundleReadBuffer = null;
    refreshState(false, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
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
    refreshState(false, false, false);
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
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, false);
  }

  /**
   * Compacts the whole log (except the last log segment) and a changed size is expected i.e. there is some invalid
   * data.
   * @throws Exception
   */
  @Test
  public void compactWholeLogWithChangeExpectedTest() throws Exception {
    refreshState(false, true, false);
    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity() - 2;
    writeDataToMeetRequiredSegmentCount(requiredCount,
        Arrays.asList(state.time.milliseconds() / 2, state.time.milliseconds(), state.time.milliseconds() * 2));
    // do some random deleting.
    int deleteCount =
        Math.min(state.liveKeys.size() / 3, (int) (1.8 * state.log.getSegmentCapacity() / DELETE_RECORD_SIZE));
    List<MockId> allLiveKeys = new ArrayList<>(state.liveKeys);
    for (int i = 0; i < deleteCount; i++) {
      MockId idToDelete = allLiveKeys.remove(TestUtils.RANDOM.nextInt(allLiveKeys.size()));
      state.addDeleteEntry(idToDelete);
    }
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    long deleteReferenceTimeMs = state.time.milliseconds();
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, true);
  }

  /**
   * Compacts the whole log multiple times with some data compacted each time.
   * @throws Exception
   */
  @Test
  public void compactWholeLogMultipleTimesTest() throws Exception {
    refreshState(false, true, false);
    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity() - 3;
    long expiryTimeMs = getInvalidationTime(requiredCount);
    List<Long> expiryTimesMs = Arrays.asList(state.time.milliseconds() / 2, expiryTimeMs, expiryTimeMs * 2);
    writeDataToMeetRequiredSegmentCount(requiredCount, expiryTimesMs);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    Set<MockId> compactedDeletes = new HashSet<>();
    Set<MockId> idsInCompactedLogSegments = getIdsWithPutInSegments(segmentsUnderCompaction);

    for (long setTimeMs : expiryTimesMs) {
      if (state.time.milliseconds() < setTimeMs + Time.MsPerSec) {
        state.advanceTime(setTimeMs + Time.MsPerSec - state.time.milliseconds());
      }
      long deleteReferenceTimeMs = state.time.milliseconds();
      getCurrentBlobIdsFromWholeIndex(state.index, compactedDeletes, purgeDeleteTombstone);
      long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
      CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);

      compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
      compactor.initialize(state.index);
      long logSegmentsBeforeCompaction = state.index.getLogSegmentCount();
      try {
        compactor.compact(details, bundleReadBuffer);
      } finally {
        compactor.close(0);
      }
      assertFalse("Sum of size of log segments did not change after compaction",
          logSegmentSizeSumBeforeCompaction == getSumOfLogSegmentEndOffsets());
      verifyDataPostCompaction(idsInCompactedLogSegments, compactedDeletes, deleteReferenceTimeMs);
      state.reloadLog(true);
      verifyDataPostCompaction(idsInCompactedLogSegments, compactedDeletes, deleteReferenceTimeMs);
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

  @Test
  public void testDuplicatePuts() throws Exception {
    testDuplicatePutsHelper();
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
    Pair<Long, List<LogSegmentName>> deleteTimeAndSegmentsUnderCompaction = setupStateWithDeletedBlobsAtSpecificTime();
    List<LogSegmentName> segmentsUnderCompaction = deleteTimeAndSegmentsUnderCompaction.getSecond();
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
        getValidDataSize(segmentsUnderCompaction, deleteReferenceTimeMs, purgeDeleteTombstone));
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
    Pair<Long, List<LogSegmentName>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    Map<LogSegmentName, Long> oldSegmentNamesAndEndOffsets =
        getEndOffsets(expiryTimeAndSegmentsUnderCompaction.getSecond());
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
    Pair<Long, List<LogSegmentName>> deleteTimeAndSegmentsUnderCompaction = setupStateWithDeletedBlobsAtSpecificTime();
    long deleteReferenceTimeMs = deleteTimeAndSegmentsUnderCompaction.getFirst() - Time.MsPerSec;
    Map<LogSegmentName, Long> oldSegmentNamesAndEndOffsets =
        getEndOffsets(deleteTimeAndSegmentsUnderCompaction.getSecond());
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
    Pair<Long, List<LogSegmentName>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
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
    refreshState(false, false, false);
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(5));
    state.initIndex(null);

    int numFinalSegmentsCount = 3;
    long expiryTimeMs = getInvalidationTime(numFinalSegmentsCount + 1);
    // fill up one segment.
    writeDataToMeetRequiredSegmentCount(1, null);

    // IS 1.1 starts
    // 1. Put entry that contains a delete entry in the same index segment and is not counted as deleted.
    // won't be cleaned up.
    IndexEntry entry = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    assertEquals("There should have been a new log segment created", 2, state.index.getLogSegmentCount());
    LogSegmentName logSegmentName = entry.getValue().getOffset().getName();
    MockId delUnexpPutSameIdxSegId = (MockId) entry.getKey();
    state.addDeleteEntry(delUnexpPutSameIdxSegId);

    // 2. Put entry that has expired and contains a delete entry in the same index segment. Does not count as deleted
    // but is expired.
    // will be cleaned up, but delete record remains
    MockId delExpPutSameIdxSegId = (MockId) state.addPutEntries(1, PUT_RECORD_SIZE, 0).get(0).getKey();
    state.addDeleteEntry(delExpPutSameIdxSegId);

    // 3. Put entry that will be deleted.
    // won't be cleaned up.
    MockId willBeDelPut = (MockId) state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0).getKey();
    // IS 1.1 ends

    // roll over. IS 1.2 starts
    // 4. Put entry that has expired.
    // will be cleaned up.
    state.addPutEntries(1, PUT_RECORD_SIZE, 0).get(0).getKey();

    // 5. Put entry with an expiry time  that is not expired and has a delete entry in the same index segment and is not
    // counted as deleted or expired. In the compacted log, the put entry will be in one index segment and the delete
    // in another
    // won't be cleaned up.
    MockId delUnexpPutDiffIdxSegId = (MockId) state.addPutEntries(1, PUT_RECORD_SIZE, expiryTimeMs).get(0).getKey();
    state.addDeleteEntry(delUnexpPutDiffIdxSegId);

    // 6. Put entry that will expire (but is valid right now).
    // won't be cleaned up.
    MockId willExpPut = (MockId) state.addPutEntries(1, PUT_RECORD_SIZE, expiryTimeMs).get(0).getKey();

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
    List<LogSegmentName> segmentsUnderCompaction = Collections.singletonList(logSegmentName);
    long endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();
    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);
    long logSegmentCountBeforeCompaction = state.index.getLogSegmentCount();
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }

    LogSegmentName compactedLogSegmentName = logSegmentName.getNextGenerationName();
    LogSegment compactedLogSegment = state.log.getSegment(compactedLogSegmentName);
    long cleanedUpSize = 2 * PUT_RECORD_SIZE;
    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());

    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE);
    // get the first index segment that refers to the compacted segment
    IndexSegment indexSegment =
        state.index.getIndexSegments().get(new Offset(compactedLogSegmentName, compactedLogSegment.getStartOffset()));
    List<IndexEntry> indexEntries = new ArrayList<>();
    assertTrue("Should have got some index entries",
        indexSegment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), true, false));
    assertEquals("There should be 5 index entries returned", 4, indexEntries.size());
    indexEntries.sort(PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);

    long logSegmentStartOffset = compactedLogSegment.getStartOffset();
    long currentExpectedOffset = logSegmentStartOffset + PUT_RECORD_SIZE;
    // first index entry should be a delete and it should have an original message offset
    verifyIndexEntry(indexEntries.get(0), delUnexpPutSameIdxSegId, currentExpectedOffset, DELETE_RECORD_SIZE,
        Utils.Infinite_Time, true, logSegmentStartOffset);
    currentExpectedOffset += DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(1), delExpPutSameIdxSegId, currentExpectedOffset, DELETE_RECORD_SIZE, 0, true,
        IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET);
    currentExpectedOffset += DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(2), willBeDelPut, currentExpectedOffset, PUT_RECORD_SIZE, Utils.Infinite_Time,
        false, currentExpectedOffset);
    long willBeDelOffset = currentExpectedOffset;
    currentExpectedOffset += PUT_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(3), delUnexpPutDiffIdxSegId, currentExpectedOffset, PUT_RECORD_SIZE, expiryTimeMs,
        false, currentExpectedOffset);
    currentExpectedOffset += PUT_RECORD_SIZE;

    // get the second index segment
    indexSegment = state.index.getIndexSegments().higherEntry(indexSegment.getStartOffset()).getValue();
    indexEntries.clear();
    assertTrue("Should have got some index entries",
        indexSegment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), true, false));
    assertEquals("There should be 5 index entries returned", 5, indexEntries.size());
    indexEntries.sort(PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), delUnexpPutDiffIdxSegId, currentExpectedOffset, DELETE_RECORD_SIZE,
        expiryTimeMs, true, currentExpectedOffset - PUT_RECORD_SIZE);
    currentExpectedOffset += DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(1), willExpPut, currentExpectedOffset, PUT_RECORD_SIZE, expiryTimeMs, false,
        currentExpectedOffset);
    currentExpectedOffset += PUT_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(2), willBeDelPut, currentExpectedOffset, DELETE_RECORD_SIZE, Utils.Infinite_Time,
        true, willBeDelOffset);
    currentExpectedOffset += DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(3), idFromAnotherSegment, currentExpectedOffset, DELETE_RECORD_SIZE,
        Utils.Infinite_Time, true, IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET);
    currentExpectedOffset += DELETE_RECORD_SIZE;
    verifyIndexEntry(indexEntries.get(4), uniqueId, currentExpectedOffset, DELETE_RECORD_SIZE, expiryTimeMs, true,
        IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET);

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
   * cycle of compaction (tests the case where compaction finishes in {@code BlobStoreCompactor.fixStateIfRequired()}.
   * @throws Exception
   */
  @Test
  public void interruptionDuringLastCommitTest() throws Exception {
    // keep hard delete enabled
    refreshState(true, true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
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
    // NOTE: There is no need to add tests for cases here that are already covered by  This test
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

    refreshState(false, false, false);
    state.properties.setProperty("store.index.max.number.of.inmem.elements", "5");
    state.reloadIndex(true, false);
    long notExpiredMs = state.time.milliseconds() + TimeUnit.SECONDS.toMillis(Short.MAX_VALUE);
    long expiredMs = state.time.milliseconds();

    // LS (Log Segment) 0
    // IS (Index Segment) 0.1
    IndexEntry p1 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    IndexEntry p2 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p3 = state.addPutEntries(1, PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p5d1 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    state.addDeleteEntry((MockId) p5d1.getKey());
    // IS 0.2
    IndexEntry p5d2 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p7d1 = state.addPutEntries(1, PUT_RECORD_SIZE, expiredMs).get(0);
    state.addDeleteEntry((MockId) p7d1.getKey());
    IndexEntry p9d1 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p9d1.getKey());
    // IS 0.3
    IndexEntry p7d2 = state.addPutEntries(1, PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p9d2 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    IndexEntry p5d3 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p7d3 = state.addPutEntries(1, PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p9d3 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
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
    otherPuts.addAll(state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time));
    otherPuts.addAll(state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time));
    // this is where we cut off the time for compaction (retention time)
    long deleteReferenceTimeMs = state.time.milliseconds() + TimeUnit.SECONDS.toMillis(1);
    // IS 1.2
    IndexEntry p4d1 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    state.addDeleteEntry((MockId) p4d1.getKey());
    IndexEntry p6d1 = state.addPutEntries(1, PUT_RECORD_SIZE, expiredMs).get(0);
    state.addDeleteEntry((MockId) p6d1.getKey());
    IndexEntry p4d2 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    // IS 1.3
    IndexEntry p8d1 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p8d1.getKey());
    state.addDeleteEntry((MockId) p4d2.getKey());
    IndexEntry p6d2 = state.addPutEntries(1, PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p8d2 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    // IS 1.4
    state.addDeleteEntry((MockId) p6d2.getKey());
    state.addDeleteEntry((MockId) p8d2.getKey());
    IndexEntry p4d3 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p6d3 = state.addPutEntries(1, PUT_RECORD_SIZE, expiredMs).get(0);
    IndexEntry p8d3 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
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
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 2);
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);

    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
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
        assertEquals(id + " failed with error code " + e.getErrorCode(), StoreErrorCodes.IDDeleted, e.getErrorCode());
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
   * Tests compaction on a log that contains a PUT record which the corresponding container marked as INACTIVE/DELETE_IN_PROGRESS
   * @throws Exception
   */
  @Test
  public void containerDeletionTest() throws Exception {
    refreshState(false, false, false);

    state.properties.setProperty("store.container.deletion.enabled", "true");
    state.properties.setProperty("store.index.max.number.of.inmem.elements", "5");
    state.initIndex(null);
    long notExpiredMs = state.time.milliseconds() + TimeUnit.SECONDS.toMillis(Short.MAX_VALUE);
    // LS (Log Segment) 0
    // IS (Index Segment) 0.1
    // p1 DeleteInProgress
    // p2 Inactive
    IndexEntry p1 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p2 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);

    // IS (Index Segment) 0.2
    // p3 DeleteInProgress
    // p4 Inactive
    IndexEntry p3 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    IndexEntry p4 = state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);
    long lastRecSize = state.log.getSegmentCapacity() - state.index.getCurrentEndOffset().getOffset();
    state.addPutEntries(1, lastRecSize, notExpiredMs).get(0);

    // LS 1
    state.addPutEntries(1, PUT_RECORD_SIZE, notExpiredMs).get(0);

    // Make sure we have two log segments
    writeDataToMeetRequiredSegmentCount(2, null);

    //add to be deleted index entries into indexEntriesDeleteInProgress list.
    List<IndexEntry> indexEntriesDeleteInProgress = new ArrayList<>();
    indexEntriesDeleteInProgress.add(p1);
    indexEntriesDeleteInProgress.add(p3);
    //add invalid index entries into indexEntriesInvalid list.
    List<IndexEntry> indexEntriesInactive = new ArrayList<>();
    indexEntriesInactive.add(p2);
    indexEntriesInactive.add(p4);

    // Mock indexEntries' corresponding container as INACTIVE/DELETE_IN_PROGRESS.
    Set<Container> deleteInProgressSet = new HashSet<>();
    Set<MockId> deletedInProgressKeys = new HashSet<>();
    Set<Container> InactiveSet = new HashSet<>();
    Set<MockId> InactiveKeys = new HashSet<>();
    long cleanedUpSize = 0;

    for (IndexEntry indexEntry : indexEntriesDeleteInProgress) {
      Container container = Mockito.mock(Container.class);
      Mockito.when(container.getParentAccountId()).thenReturn(indexEntry.getValue().getAccountId());
      Mockito.when(container.getId()).thenReturn(indexEntry.getValue().getContainerId());
      Mockito.when(container.getDeleteTriggerTime())
          .thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(config.storeContainerDeletionRetentionDays));
      deleteInProgressSet.add(container);
      deletedInProgressKeys.add((MockId) indexEntry.getKey());
      cleanedUpSize += indexEntry.getValue().getSize();
    }

    for (IndexEntry indexEntry : indexEntriesInactive) {
      Container container = Mockito.mock(Container.class);
      Mockito.when(container.getParentAccountId()).thenReturn(indexEntry.getValue().getAccountId());
      Mockito.when(container.getId()).thenReturn(indexEntry.getValue().getContainerId());
      InactiveSet.add(container);
      InactiveKeys.add((MockId) indexEntry.getKey());
      cleanedUpSize += indexEntry.getValue().getSize();
    }

    Mockito.when(accountService.getContainersByStatus(Container.ContainerStatus.DELETE_IN_PROGRESS))
        .thenReturn(deleteInProgressSet);
    Mockito.when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(InactiveSet);

    // get everything except the last log segment entries out of the journal
    state.reloadIndex(true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 1);
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    CompactionDetails details = new CompactionDetails(notExpiredMs, segmentsUnderCompaction, null);

    LogSegmentName logSegmentName = p1.getValue().getOffset().getName();
    LogSegmentName compactedLogSegmentName = logSegmentName.getNextGenerationName();
    long endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();

    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);
    int indexSegmentCountBeforeCompaction = state.index.getIndexSegments().size();

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }

    assertFalse("Sum of size of log segments did not change after compaction",
        logSegmentSizeSumBeforeCompaction == getSumOfLogSegmentEndOffsets());

    // inactive/deleted indexEntries should not be found
    for (MockId deletedKey : deletedInProgressKeys) {
      assertNull("There should be no record of " + deletedKey, state.index.findKey(deletedKey));
    }

    for (MockId inactiveKey : InactiveKeys) {
      assertNull("There should be no record of " + inactiveKey, state.index.findKey(inactiveKey));
    }

    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());

    int indexSegmentDiff = 1;
    assertEquals("Index Segment not as expected after compaction", indexSegmentCountBeforeCompaction - indexSegmentDiff,
        state.index.getIndexSegments().size());
  }

  /**
   * Tests compaction on a log that contains a PUT record that has no corresponding entry in the index (this can happen
   * due to recovery corner cases - refer to recovery code in PersistentIndex.java).
   * @throws Exception
   */
  @Test
  public void orphanedPutRecordsTest() throws Exception {
    refreshState(false, false, false);
    // write a PUT record that will be "lost"
    MockId orphanedId = (MockId) state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0).getKey();
    // add a delete entry for  orphanedId
    state.addDeleteEntry(orphanedId);
    // get the index value and "lose" the PUT record. This works because we get a reference to the value in the index.
    IndexValue value = state.index.findKey(orphanedId);
    value.clearOriginalMessageOffset();
    // add a put entry that spans the rest of the log segment
    long lastRecSize = state.log.getSegmentCapacity() - state.index.getCurrentEndOffset().getOffset();
    state.addPutEntries(1, lastRecSize, Utils.Infinite_Time);
    // add an entry so that a new log segment is created
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    state.reloadIndex(true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 1);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);

    // make sure the delete still exists
    checkIndexValue(orphanedId);
    // the first log segment should not contain the the PUT that had no index entry
    LogSegment firstLogSegment = state.log.getFirstSegment();
    long size = firstLogSegment.getEndOffset() - firstLogSegment.getStartOffset();
    assertEquals("Segment size not as expected", lastRecSize + DELETE_RECORD_SIZE, size);
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
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
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
      if (segment.getLogSegmentName().getGeneration() == 0) {
        break;
      }
      segment.getIndexEntriesSince(null, condition, indexEntries, currentTotalSize, false, false);
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
   * Tests when the PUT, DELETE AND UNDELETE of the same key are in the same index segment.
   * @throws Exception
   */
  @Test
  public void undeleteSameIndexSegmentTest_NoTtlUpdate() throws Exception {
    assumeTrue(withUndelete);
    refreshState(false, false, false);
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(5));
    state.initIndex(null);

    // Log Segment 0
    long expirationTime = state.time.milliseconds() + 200 * Time.MsPerSec;
    // Index Segment 0.1 P D U -> P U
    IndexEntry p1 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p1.getKey());
    state.addUndeleteEntry((MockId) p1.getKey());
    state.addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.2 P(expired) D U -> []
    IndexEntry p2 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.addDeleteEntry((MockId) p2.getKey());
    state.addUndeleteEntry((MockId) p2.getKey());
    state.addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.3 P D U D U -> P U
    IndexEntry p3 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p3.getKey());
    state.addUndeleteEntry((MockId) p3.getKey());
    state.addDeleteEntry((MockId) p3.getKey());
    state.addUndeleteEntry((MockId) p3.getKey());

    // Index Segment 0.4 P(expired) D U D U -> []
    IndexEntry p4 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.addDeleteEntry((MockId) p4.getKey());
    state.addUndeleteEntry((MockId) p4.getKey());
    state.addDeleteEntry((MockId) p4.getKey());
    state.addUndeleteEntry((MockId) p4.getKey());

    // Index Segment 0.5 P D D U -> P U
    IndexEntry p5 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p5.getKey());
    state.addDeleteEntry((MockId) p5.getKey(), null, (short) 3);
    state.addUndeleteEntry((MockId) p5.getKey());
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.6 P D U D -> D
    IndexEntry p6 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p6.getKey());
    state.addUndeleteEntry((MockId) p6.getKey());
    state.addDeleteEntry((MockId) p6.getKey());
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.7 P(expired) D U D -> D
    IndexEntry p7 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.addDeleteEntry((MockId) p7.getKey());
    state.addUndeleteEntry((MockId) p7.getKey());
    state.addDeleteEntry((MockId) p7.getKey());
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.8 P D D -> D
    IndexEntry p8 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p8.getKey());
    state.addDeleteEntry((MockId) p8.getKey(), null, (short) 3);
    state.addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.9 P U -> P U
    IndexEntry p9 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addUndeleteEntry((MockId) p9.getKey(), (short) 2);
    state.addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);

    writeDataToMeetRequiredSegmentCount(1, null);
    // This is the end of the first log segment

    // Log segment 1
    // Index Segment 1.1 P(expired) U -> []
    IndexEntry p11 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.addUndeleteEntry((MockId) p11.getKey(), (short) 2);
    state.addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 1.2 P D U U -> P U
    IndexEntry p12 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p12.getKey());
    state.addUndeleteEntry((MockId) p12.getKey());
    state.addUndeleteEntry((MockId) p12.getKey(), (short) 2);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 1.3 P2 -> P2
    IndexEntry p13 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time, (short) 2).get(0);
    state.addPutEntries(4, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Make sure we have 3 log segments
    writeDataToMeetRequiredSegmentCount(3, null);
    state.reloadIndex(true, false);

    final LogSegmentName firstLogSegmentName = p1.getValue().getOffset().getName();
    TreeSet<Offset> firstLogSegmentIndexOffsets = state.index.getIndexSegments()
        .keySet()
        .stream()
        .filter(offset -> offset.getName().equals(firstLogSegmentName))
        .collect(Collectors.toCollection(TreeSet::new));
    final LogSegmentName secondLogSegmentName = firstLogSegmentName.getNextPositionName();
    TreeSet<Offset> secondLogSegmentIndexOffsets = state.index.getIndexSegments()
        .keySet()
        .stream()
        .filter(offset -> offset.getName().equals(secondLogSegmentName))
        .collect(Collectors.toCollection(TreeSet::new));

    LogSegmentName logSegmentName = p1.getValue().getOffset().getName();
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 1);
    state.advanceTime(expirationTime - state.time.milliseconds() + 1000);
    long deleteReferenceTimeMs = state.time.milliseconds();
    long endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();
    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // Before compaction, the records in the log are
    // P1 D1 U1 P P| P2 D2 U2 P P| P3 D3 U3 D3 U3| P4 D4 U4 D4 U4| P5 D5 D5 U5 P| P6 D6 U6 D6 P| P7 D7 U7 D7 P |P8 D8 D8 P P| P9 U9 P P P
    // After compaction, the records in the log are
    // P1 U1 P P P| P P3 U3 P5 U5| P D6 P D7 P| D8 P P P9 U9| P P P

    long ds = DELETE_RECORD_SIZE;
    long ps = PUT_RECORD_SIZE;
    long us = UNDELETE_RECORD_SIZE;
    long ts = TTL_UPDATE_RECORD_SIZE;
    long cleanedUpSize =
        ds + (ps + ds + us) + (2 * ds + us) + (2 * ds + 2 * us + ps) + 2 * ds + (ps + ds + us) + (ps + ds + us) + (ps
            + ds) + 0;
    LogSegmentName compactedLogSegmentName = logSegmentName.getNextGenerationName();
    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());

    CompactionLog cLog = getLatestCompactionLog();
    Map<Offset, Offset> indexSegmentOffsets = cLog.getBeforeAndAfterIndexSegmentOffsets();
    Map<Offset, Offset> expectedIndexSegmentOffsets = new HashMap<>();

    LogSegment compactedLogSegment = state.log.getSegment(compactedLogSegmentName);
    ConcurrentNavigableMap<Offset, IndexSegment> indexSegments = state.index.getIndexSegments();
    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE);
    long currentExpectedOffset = compactedLogSegment.getStartOffset();

    // Get the entries in the first segment
    IndexSegment segment = indexSegments.get(new Offset(compactedLogSegmentName, compactedLogSegment.getStartOffset()));
    List<IndexEntry> indexEntries = new ArrayList<>();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p1.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(1), (MockId) p1.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 1);
    currentExpectedOffset += us + 3 * ps; // skip three puts

    Offset before = firstLogSegmentIndexOffsets.first();
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 1 -> after 1
    before = firstLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 2 -> after 1

    // Get the entries in the second segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += ps; // skip one put
    verifyIndexEntry(indexEntries.get(1), (MockId) p3.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(2), (MockId) p3.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 2);
    currentExpectedOffset += us;
    verifyIndexEntry(indexEntries.get(3), (MockId) p5.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(4), (MockId) p5.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 4);
    currentExpectedOffset += us;

    before = firstLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 3 -> after 2
    before = firstLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 4 -> after 2
    before = firstLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 5 -> after 2

    // Get the entries in the third segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(1), (MockId) p6.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        false, false, (short) 1);
    currentExpectedOffset += ds + ps; // skip one put
    verifyIndexEntry(indexEntries.get(3), (MockId) p7.getKey(), currentExpectedOffset, ds, expirationTime, true, false,
        false, (short) 1);
    currentExpectedOffset += ds + ps; // skip one puts

    before = firstLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 6 -> after 3
    before = firstLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 7 -> after 3
    before = firstLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 8 -> after 3
    // when copying 8th index segment, the third index segment after compaction didn't roll over yet.

    // Get the entries in the fourth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p8.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        false, false, (short) 3);
    currentExpectedOffset += ds + 2 * ps;
    verifyIndexEntry(indexEntries.get(3), (MockId) p9.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(4), (MockId) p9.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 2);

    before = firstLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 9 -> after 4

    for (Map.Entry<Offset, Offset> expectedEntry : expectedIndexSegmentOffsets.entrySet()) {
      Offset expectedBefore = expectedEntry.getKey();
      Offset expectedAfter = expectedEntry.getValue();
      Offset obtainedAfter = indexSegmentOffsets.get(expectedBefore);
      assertEquals("Mismatch for offset " + expectedBefore + " " + firstLogSegmentIndexOffsets, expectedAfter,
          obtainedAfter);
    }
    expectedIndexSegmentOffsets.clear();

    // no clean shutdown file should exist
    assertFalse("Clean shutdown file not deleted",
        new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
    // there should be no temp files
    assertEquals("There are some temp log segments", 0,
        tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);

    state.reloadIndex(true, false);
    logSegmentName = p11.getValue().getOffset().getName();
    segmentsUnderCompaction = Collections.singletonList(logSegmentName);
    deleteReferenceTimeMs = state.time.milliseconds();
    endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();
    details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);

    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // Before compaction, the records in the log are
    // P11 U11 P P P| P12 D12 U12 U12 P | P13(2) P P P P
    // After compaction, the records in the log are
    // P P P P12 U12| P P13(2)
    cleanedUpSize = (ps + us) + (ds + us);
    compactedLogSegmentName = logSegmentName.getNextGenerationName();
    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());

    cLog = getLatestCompactionLog();
    indexSegmentOffsets = cLog.getBeforeAndAfterIndexSegmentOffsets();

    compactedLogSegment = state.log.getSegment(compactedLogSegmentName);
    indexSegments = state.index.getIndexSegments()
        .subMap(new Offset(compactedLogSegmentName, 0), true,
            new Offset(compactedLogSegmentName.getNextPositionName(), 0), false);
    condition = new FindEntriesCondition(Long.MAX_VALUE);
    currentExpectedOffset = compactedLogSegment.getStartOffset();

    // Get the entries in the first segment
    segment = indexSegments.get(new Offset(compactedLogSegmentName, compactedLogSegment.getStartOffset()));
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += 3 * ps; // skip three puts
    verifyIndexEntry(indexEntries.get(3), (MockId) p12.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(4), (MockId) p12.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 2);
    currentExpectedOffset += us;

    before = secondLogSegmentIndexOffsets.first();
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 1 -> after 1
    before = secondLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 2 -> after 1

    // Get the entries in the second segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += ps; // skip one put
    verifyIndexEntry(indexEntries.get(1), (MockId) p13.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 2);

    before = secondLogSegmentIndexOffsets.higher(before);
    expectedIndexSegmentOffsets.put(before, segment.getStartOffset()); // before 3 -> after 2

    for (Map.Entry<Offset, Offset> expectedEntry : expectedIndexSegmentOffsets.entrySet()) {
      Offset expectedBefore = expectedEntry.getKey();
      Offset expectedAfter = expectedEntry.getValue();
      Offset obtainedAfter = indexSegmentOffsets.get(expectedBefore);
      assertEquals("Mismatch for offset " + expectedBefore + " " + secondLogSegmentIndexOffsets, expectedAfter,
          obtainedAfter);
    }
    expectedIndexSegmentOffsets.clear();

    // no clean shutdown file should exist
    assertFalse("Clean shutdown file not deleted",
        new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
    // there should be no temp files
    assertEquals("There are some temp log segments", 0,
        tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);
  }

  /**
   * Tests when the PUT, DELETE, TTL_UPDATE AND UNDELETE of the same key are in the same index segment.
   * @throws Exception
   */
  @Test
  public void undeleteSameIndexSegmentTest_WithTtlUpdate() throws Exception {
    assumeTrue(withUndelete);
    refreshState(false, false, false);
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(5));
    state.initIndex(null);

    // Log Segment 0
    long expirationTime = state.time.milliseconds() + 500 * Time.MsPerSec;
    // Index Segment 0.1 P T D U -> P T U
    IndexEntry p1 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.makePermanent((MockId) p1.getKey(), false);
    state.addDeleteEntry((MockId) p1.getKey());
    state.addUndeleteEntry((MockId) p1.getKey());
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.2 P(expired) T D U -> P T U
    IndexEntry p2 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.makePermanent((MockId) p2.getKey(), false);
    state.addDeleteEntry((MockId) p2.getKey());
    state.addUndeleteEntry((MockId) p2.getKey());
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.3 P D U T -> P U T
    IndexEntry p3 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p3.getKey());
    state.addUndeleteEntry((MockId) p3.getKey());
    state.makePermanent((MockId) p3.getKey(), false);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.4 P(expired) D U T -> P U T
    IndexEntry p4 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.addDeleteEntry((MockId) p4.getKey());
    state.addUndeleteEntry((MockId) p4.getKey());
    state.makePermanent((MockId) p4.getKey(), false);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.5 P D U T D -> D
    IndexEntry p5 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p5.getKey());
    state.addUndeleteEntry((MockId) p5.getKey());
    state.makePermanent((MockId) p5.getKey(), false);
    state.addDeleteEntry((MockId) p5.getKey());

    // Index Segment 0.6 P(expired) D U T D -> D
    IndexEntry p6 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.addDeleteEntry((MockId) p6.getKey());
    state.addUndeleteEntry((MockId) p6.getKey());
    state.makePermanent((MockId) p6.getKey(), false);
    state.addDeleteEntry((MockId) p6.getKey());

    // Index Segment 0.7 P T -> P T
    IndexEntry p7 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.makePermanent((MockId) p7.getKey(), false);
    state.addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.8 P(expired) T -> P T
    IndexEntry p8 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.makePermanent((MockId) p8.getKey(), false);
    state.addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.9 P U U T -> P U T
    IndexEntry p9 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addUndeleteEntry((MockId) p9.getKey(), (short) 1);
    state.addUndeleteEntry((MockId) p9.getKey(), (short) 2);
    state.makePermanent((MockId) p9.getKey(), false);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    writeDataToMeetRequiredSegmentCount(1, null);
    // This is the end of the first log segment

    // Log segment 1
    // Index Segment 1.1 P(expired) U U T -> P U T
    IndexEntry p11 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.addUndeleteEntry((MockId) p11.getKey(), (short) 1);
    state.addUndeleteEntry((MockId) p11.getKey(), (short) 2);
    state.makePermanent((MockId) p11.getKey(), false);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 1.2 P T U U -> P T U
    IndexEntry p12 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.makePermanent((MockId) p12.getKey(), false);
    state.addUndeleteEntry((MockId) p12.getKey(), (short) 1);
    state.addUndeleteEntry((MockId) p12.getKey(), (short) 2);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 1.3 P(expired) T U U -> P T U
    IndexEntry p13 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.makePermanent((MockId) p13.getKey(), false);
    state.addUndeleteEntry((MockId) p13.getKey(), (short) 1);
    state.addUndeleteEntry((MockId) p13.getKey(), (short) 2);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 1.4 P T D D -> D
    IndexEntry p14 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.makePermanent((MockId) p14.getKey(), false);
    state.addDeleteEntry((MockId) p14.getKey());
    state.addDeleteEntry((MockId) p14.getKey(), null, (short) 1);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 1.5 P(expired) T D D -> D
    IndexEntry p15 = state.addPutEntries(1, PUT_RECORD_SIZE, expirationTime).get(0);
    state.makePermanent((MockId) p15.getKey(), false);
    state.addDeleteEntry((MockId) p15.getKey());
    state.addDeleteEntry((MockId) p15.getKey(), null, (short) 1);
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 1.6 T -> [], Put already compacted
    MockId p16id = state.getUniqueId();
    state.makePermanent(p16id, true);
    state.addPutEntries(4, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Make sure we have 3 log segments
    writeDataToMeetRequiredSegmentCount(3, null);
    state.reloadIndex(true, false);

    LogSegmentName logSegmentName = p1.getValue().getOffset().getName();
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 1);
    state.advanceTime(expirationTime - state.time.milliseconds() + 1000);
    long deleteReferenceTimeMs = state.time.milliseconds();
    long endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();
    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // Before compaction, the records in the log are
    // P1 T1 D1 U1 P| P2 T2 D2 U2 P| P3 D3 U3 T3 P| P4 D4 U4 T4 P| P5 D5 U5 T5 D5| P6 D6 U6 T6 D6| P7 T7 P P P| P8 T8 P P P| P9 U9 U9 T9 P
    // After compaction, the records in the log are
    // P1 T1 U1 P P2| T2 U2 P P3 U3| T3 P P4 U4 T4| P D5 D6 P7 T7| P P P P8 T8| P P P P9 U9| T9

    long ds = DELETE_RECORD_SIZE;
    long ps = PUT_RECORD_SIZE;
    long us = UNDELETE_RECORD_SIZE;
    long ts = TTL_UPDATE_RECORD_SIZE;
    long cleanedUpSize = ds + ds + ds + ds + (ps + ds + us + ts) + (ps + ds + us + ts) + 0 + 0 + us;
    LogSegmentName compactedLogSegmentName = logSegmentName.getNextGenerationName();
    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());

    LogSegment compactedLogSegment = state.log.getSegment(compactedLogSegmentName);
    ConcurrentNavigableMap<Offset, IndexSegment> indexSegments = state.index.getIndexSegments();
    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE);
    long currentExpectedOffset = compactedLogSegment.getStartOffset();

    // Get the entries in the first segment
    IndexSegment segment = indexSegments.get(new Offset(compactedLogSegmentName, compactedLogSegment.getStartOffset()));
    List<IndexEntry> indexEntries = new ArrayList<>();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p1.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(1), (MockId) p1.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 0);
    currentExpectedOffset += ts;
    verifyIndexEntry(indexEntries.get(2), (MockId) p1.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        true, true, (short) 1);
    currentExpectedOffset += us + ps; // skip one put
    verifyIndexEntry(indexEntries.get(4), (MockId) p2.getKey(), currentExpectedOffset, ps, expirationTime, false, false,
        false, (short) 0);
    currentExpectedOffset += ps;

    // Get the entries in the second segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p2.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 0);
    currentExpectedOffset += ts;
    verifyIndexEntry(indexEntries.get(1), (MockId) p2.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        true, true, (short) 1);
    currentExpectedOffset += us + ps; // skip one put
    verifyIndexEntry(indexEntries.get(3), (MockId) p3.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(4), (MockId) p3.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 1);
    currentExpectedOffset += us;

    // Get the entries in the third segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p3.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 1);
    currentExpectedOffset += ts + ps; // skip one put
    verifyIndexEntry(indexEntries.get(2), (MockId) p4.getKey(), currentExpectedOffset, ps, expirationTime, false, false,
        false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(3), (MockId) p4.getKey(), currentExpectedOffset, us, expirationTime, false, false,
        true, (short) 1);
    currentExpectedOffset += us;
    verifyIndexEntry(indexEntries.get(4), (MockId) p4.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 1);
    currentExpectedOffset += ts;

    // Get the entries in the fourth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += ps; // skip one put
    verifyIndexEntry(indexEntries.get(1), (MockId) p5.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        true, false, (short) 1);
    currentExpectedOffset += ds;
    verifyIndexEntry(indexEntries.get(2), (MockId) p6.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        true, false, (short) 1);
    currentExpectedOffset += ds;
    verifyIndexEntry(indexEntries.get(3), (MockId) p7.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(4), (MockId) p7.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 0);
    currentExpectedOffset += ts;

    // Get the entries in the fourth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += 3 * ps; // skip three puts
    verifyIndexEntry(indexEntries.get(3), (MockId) p8.getKey(), currentExpectedOffset, ps, expirationTime, false, false,
        false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(4), (MockId) p8.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 0);
    currentExpectedOffset += ts;

    // Get the entries in the fifth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += 3 * ps; // skip three puts
    verifyIndexEntry(indexEntries.get(3), (MockId) p9.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(4), (MockId) p9.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        false, true, (short) 2);
    currentExpectedOffset += us;

    // Get the entries in the sixth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p9.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 2);

    // no clean shutdown file should exist
    assertFalse("Clean shutdown file not deleted",
        new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
    // there should be no temp files
    assertEquals("There are some temp log segments", 0,
        tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);

    state.reloadIndex(true, false);
    logSegmentName = p11.getValue().getOffset().getName();
    segmentsUnderCompaction = Collections.singletonList(logSegmentName);
    deleteReferenceTimeMs = state.time.milliseconds();
    endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();
    details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);

    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // Before compaction, the records in the log are
    // P11 U11 U11 T11 P| P12 T12 U12 U12 P| P13 T13 U13 U13 P| P14 T14 D14 D14 P| P15 T15 D15 D15 P | T16 P P P P
    // After compaction, the records in the log are
    // P11 U11 T11 P P12| T12 U12 P P13 T13| U13 P D14 P D15| P P P P P
    cleanedUpSize = us + us + us + (ps + ts + ds) + (ps + ts + ds) + ts;
    compactedLogSegmentName = logSegmentName.getNextGenerationName();
    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());

    compactedLogSegment = state.log.getSegment(compactedLogSegmentName);
    indexSegments = state.index.getIndexSegments()
        .subMap(new Offset(compactedLogSegmentName, 0), true,
            new Offset(compactedLogSegmentName.getNextPositionName(), 0), false);
    condition = new FindEntriesCondition(Long.MAX_VALUE);
    currentExpectedOffset = compactedLogSegment.getStartOffset();

    // Get the entries in the first segment
    segment = indexSegments.get(new Offset(compactedLogSegmentName, compactedLogSegment.getStartOffset()));
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p11.getKey(), currentExpectedOffset, ps, expirationTime, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(1), (MockId) p11.getKey(), currentExpectedOffset, us, expirationTime, false,
        false, true, (short) 2);
    currentExpectedOffset += us;
    verifyIndexEntry(indexEntries.get(2), (MockId) p11.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 2);
    currentExpectedOffset += ts + ps; // skip one put
    verifyIndexEntry(indexEntries.get(4), (MockId) p12.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;

    // Get the entries in the second segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p12.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 0);
    currentExpectedOffset += ts;
    verifyIndexEntry(indexEntries.get(1), (MockId) p12.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        true, true, (short) 2);
    currentExpectedOffset += us + ps; // skip one put
    verifyIndexEntry(indexEntries.get(3), (MockId) p13.getKey(), currentExpectedOffset, ps, expirationTime, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(4), (MockId) p13.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 0);
    currentExpectedOffset += ts;

    // Get the entries in the third segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p13.getKey(), currentExpectedOffset, us, Utils.Infinite_Time, false,
        true, true, (short) 2);
    currentExpectedOffset += us + ps; // skip one put
    verifyIndexEntry(indexEntries.get(2), (MockId) p14.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        true, false, (short) 1);
    currentExpectedOffset += us + ps; // skip one put
    verifyIndexEntry(indexEntries.get(4), (MockId) p15.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        true, false, (short) 1);
    currentExpectedOffset += ds;

    // no clean shutdown file should exist
    assertFalse("Clean shutdown file not deleted",
        new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
    // there should be no temp files
    assertEquals("There are some temp log segments", 0,
        tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);
  }

  /**
   * Test when the PUT, DELETE, TTL_UPDATE and UNDELETE of the same key are in the same index segment and the final state
   * is delete but it's still within the retention date.
   * @throws Exception
   */
  @Test
  public void undeleteSameIndexSegmentTest_DeleteNotInEffect() throws Exception {
    assumeTrue(withUndelete);
    refreshState(false, false, false);
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(5));
    state.initIndex(null);

    // Log Segment 0
    // Index Segment 0.1 P D D -> P D
    IndexEntry p1 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p1.getKey());
    state.addDeleteEntry((MockId) p1.getKey(), null, (short) 1);
    state.addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.2 P T D U D -> P T D
    IndexEntry p2 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.makePermanent((MockId) p2.getKey(), false);
    state.addDeleteEntry((MockId) p2.getKey());
    state.addUndeleteEntry((MockId) p2.getKey());
    state.addDeleteEntry((MockId) p2.getKey());

    // Index Segment 0.3 P U U D -> P D
    IndexEntry p3 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addUndeleteEntry((MockId) p3.getKey(), (short) 1);
    state.addUndeleteEntry((MockId) p3.getKey(), (short) 2);
    state.addDeleteEntry((MockId) p3.getKey());
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    writeDataToMeetRequiredSegmentCount(2, null);
    state.reloadIndex(true, false);

    LogSegmentName logSegmentName = p1.getValue().getOffset().getName();
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 1);
    long endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();
    // Use 0 as delete reference time so all the deletes are still with in the retention date
    CompactionDetails details = new CompactionDetails(0, segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // Before compaction, the records in the log are
    // P1 D1 D1 P P| P2 T2 D2 U2 D| P3 U3 U3 D3 P
    // After compaction, the records in the log are
    // P1 D1 P P P2| T2 D2 P3 D3 P

    long ds = DELETE_RECORD_SIZE;
    long ps = PUT_RECORD_SIZE;
    long us = UNDELETE_RECORD_SIZE;
    long ts = TTL_UPDATE_RECORD_SIZE;
    long cleanedUpSize = ds + (ds + us) + 2 * us;
    LogSegmentName compactedLogSegmentName = logSegmentName.getNextGenerationName();
    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());

    LogSegment compactedLogSegment = state.log.getSegment(compactedLogSegmentName);
    ConcurrentNavigableMap<Offset, IndexSegment> indexSegments = state.index.getIndexSegments();
    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE);
    long currentExpectedOffset = compactedLogSegment.getStartOffset();

    // Get the entries in the first segment
    IndexSegment segment = indexSegments.get(new Offset(compactedLogSegmentName, compactedLogSegment.getStartOffset()));
    List<IndexEntry> indexEntries = new ArrayList<>();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p1.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(1), (MockId) p1.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        false, false, (short) 1);
    currentExpectedOffset += ds + 2 * ps; // skip two puts
    verifyIndexEntry(indexEntries.get(4), (MockId) p2.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;

    // Get the entries in the second segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), (MockId) p2.getKey(), currentExpectedOffset, ts, Utils.Infinite_Time, false,
        true, false, (short) 0);
    currentExpectedOffset += ts;
    verifyIndexEntry(indexEntries.get(1), (MockId) p2.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        true, false, (short) 1);
    currentExpectedOffset += ds;
    verifyIndexEntry(indexEntries.get(2), (MockId) p3.getKey(), currentExpectedOffset, ps, Utils.Infinite_Time, false,
        false, false, (short) 0);
    currentExpectedOffset += ps;
    verifyIndexEntry(indexEntries.get(3), (MockId) p3.getKey(), currentExpectedOffset, ds, Utils.Infinite_Time, true,
        false, false, (short) 2);

    // no clean shutdown file should exist
    assertFalse("Clean shutdown file not deleted",
        new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
    // there should be no temp files
    assertEquals("There are some temp log segments", 0,
        tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);
  }

  /**
   * Test when there is no PUT in the beginning of the blob history and all the records for the same key are in the same
   * index segment. It's possible that PUT record is compacted in a previous compaction cycle.
   * @throws Exception
   */
  @Test
  public void undeleteSameIndexSegmentTest_NoPut() throws Exception {
    assumeTrue(withUndelete);
    refreshState(false, false, false);
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(5));
    state.initIndex(null);

    // Log Segment 0
    // Index Segment 0.1 P P P P P
    state.addPutEntries(5, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.2 D -> D
    MockId p2 = state.getUniqueId();
    state.addDeleteEntry(p2, new MessageInfo(p2, DELETE_RECORD_SIZE, true, false, p2.getAccountId(), p2.getAccountId(),
        state.time.milliseconds()), (short) 0);
    state.addPutEntries(4, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.3 U -> U
    MockId p3 = state.getUniqueId();
    state.addUndeleteEntry(p3,
        new MessageInfo(p3, UNDELETE_RECORD_SIZE, true, false, p3.getAccountId(), p3.getAccountId(),
            state.time.milliseconds()), (short) 1);
    state.addPutEntries(4, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.4 T, U -> T, U
    MockId p4 = state.getUniqueId();
    state.makePermanent(p4, true);
    state.addUndeleteEntry(p4,
        new MessageInfo(p4, UNDELETE_RECORD_SIZE, true, false, p4.getAccountId(), p4.getAccountId(),
            state.time.milliseconds()), (short) 1);
    state.addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.5 T, D -> D
    MockId p5 = state.getUniqueId();
    state.makePermanent(p5, true);
    state.addDeleteEntry(p5, null, (short) 0);
    state.addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.6 D, U -> U
    MockId p6 = state.getUniqueId();
    state.addDeleteEntry(p6, new MessageInfo(p6, DELETE_RECORD_SIZE, true, false, p6.getAccountId(), p6.getAccountId(),
        state.time.milliseconds()), (short) 0);
    state.addUndeleteEntry(p6,
        new MessageInfo(p6, UNDELETE_RECORD_SIZE, true, false, p6.getAccountId(), p6.getAccountId(),
            state.time.milliseconds()), (short) 1);
    state.addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.7 U, D -> D
    MockId p7 = state.getUniqueId();
    state.addUndeleteEntry(p7,
        new MessageInfo(p7, UNDELETE_RECORD_SIZE, true, false, p7.getAccountId(), p7.getAccountId(),
            state.time.milliseconds()), (short) 1);
    state.addDeleteEntry(p7, null, (short) 1);
    state.addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.8 T, D, U -> T, U
    MockId p8 = state.getUniqueId();
    state.makePermanent(p8, true);
    state.addDeleteEntry(p8, null, (short) 0);
    state.addUndeleteEntry(p8,
        new MessageInfo(p8, UNDELETE_RECORD_SIZE, true, false, p8.getAccountId(), p8.getAccountId(),
            state.time.milliseconds()), (short) 1);
    state.addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // Index Segment 0.9 D, U, T -> U, T
    MockId p9 = state.getUniqueId();
    state.addDeleteEntry(p9, new MessageInfo(p9, DELETE_RECORD_SIZE, true, false, p9.getAccountId(), p9.getAccountId(),
        state.time.milliseconds()), (short) 0);
    state.addUndeleteEntry(p9,
        new MessageInfo(p9, UNDELETE_RECORD_SIZE, true, false, p9.getAccountId(), p9.getAccountId(),
            state.time.milliseconds()), (short) 1);
    state.makePermanent(p9, false);
    state.addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);

    writeDataToMeetRequiredSegmentCount(2, null);
    state.reloadIndex(true, false);

    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 1);
    LogSegmentName logSegmentName = segmentsUnderCompaction.get(0);
    long endOffsetOfSegmentBeforeCompaction = state.log.getSegment(logSegmentName).getEndOffset();
    CompactionDetails details = new CompactionDetails(state.time.milliseconds(), segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // Before compaction, the records in the log are
    // P P P P P| D2 P P P P| U3 P P P P| T4 U4 P P P| T5 D5 P P P| D6 U6 P P P| U7 D7 P P P| T8 D8 U8 P P| D9 U9 T9 P P
    // After compaction, the records in the log are
    // P P P P P| D2 P P P P| U3 P P P P| T4 U4 P P P| D5 P P P U6| P P P D7 P| P P T8 U8 P| P U9 T9 P P

    long ds = DELETE_RECORD_SIZE;
    long ps = PUT_RECORD_SIZE;
    long us = UNDELETE_RECORD_SIZE;
    long ts = TTL_UPDATE_RECORD_SIZE;
    long cleanedUpSize = 0 + 0 + 0 + 0 + ts + ds + us + ds + ds;
    LogSegmentName compactedLogSegmentName = logSegmentName.getNextGenerationName();
    assertEquals("End offset of log segment not as expected after compaction",
        endOffsetOfSegmentBeforeCompaction - cleanedUpSize,
        state.log.getSegment(compactedLogSegmentName).getEndOffset());

    LogSegment compactedLogSegment = state.log.getSegment(compactedLogSegmentName);
    ConcurrentNavigableMap<Offset, IndexSegment> indexSegments = state.index.getIndexSegments();
    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE);
    long currentExpectedOffset = compactedLogSegment.getStartOffset();

    // Get the entries in the first segment
    IndexSegment segment = indexSegments.get(new Offset(compactedLogSegmentName, compactedLogSegment.getStartOffset()));
    List<IndexEntry> indexEntries = new ArrayList<>();
    currentExpectedOffset += 5 * ps; // skip five puts

    // Get the entries in the second segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), p2, currentExpectedOffset, ds, Utils.Infinite_Time, true, false, false,
        (short) 0);
    currentExpectedOffset += ds + 4 * ps; // skip four puts

    // Get the entries in the third segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), p3, currentExpectedOffset, us, Utils.Infinite_Time, false, false, true,
        (short) 1);
    currentExpectedOffset += us + 4 * ps; // skip four puts

    // Get the entries in the fourth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), p4, currentExpectedOffset, ts, Utils.Infinite_Time, false, true, false,
        (short) 0);
    currentExpectedOffset += ts;
    verifyIndexEntry(indexEntries.get(1), p4, currentExpectedOffset, us, Utils.Infinite_Time, false, true, true,
        (short) 1);
    currentExpectedOffset += us + 3 * ps; // skip three puts

    // Get the entries in the fifth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    verifyIndexEntry(indexEntries.get(0), p5, currentExpectedOffset, ds, Utils.Infinite_Time, true, true, false,
        (short) 0);
    currentExpectedOffset += ds + 3 * ps; // skip three puts
    verifyIndexEntry(indexEntries.get(4), p6, currentExpectedOffset, us, Utils.Infinite_Time, false, false, true,
        (short) 1);
    currentExpectedOffset += us;

    // Get the entries in the sixth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += 3 * ps; // skip three puts
    verifyIndexEntry(indexEntries.get(3), p7, currentExpectedOffset, ds, Utils.Infinite_Time, true, false, false,
        (short) 1);
    currentExpectedOffset += ds + ps; // skip one put

    // Get the entries in the seventh segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += 2 * ps; // skip two puts
    verifyIndexEntry(indexEntries.get(2), p8, currentExpectedOffset, ts, Utils.Infinite_Time, false, true, false,
        (short) 0);
    currentExpectedOffset += ts;
    verifyIndexEntry(indexEntries.get(3), p8, currentExpectedOffset, us, Utils.Infinite_Time, false, true, true,
        (short) 1);
    currentExpectedOffset += us + ps; // skip one put

    // Get the entries in the eighth segment
    segment = indexSegments.higherEntry(segment.getStartOffset()).getValue();
    indexEntries.clear();
    assertEquals("LogSegment name mismatch", compactedLogSegmentName, segment.getStartOffset().getName());
    segment.getIndexEntriesSince(null, condition, indexEntries, new AtomicLong(0), false, false);
    Collections.sort(indexEntries, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    currentExpectedOffset += ps; // skip one put
    verifyIndexEntry(indexEntries.get(1), p9, currentExpectedOffset, us, Utils.Infinite_Time, false, false, true,
        (short) 1);
    currentExpectedOffset += us;
    verifyIndexEntry(indexEntries.get(2), p9, currentExpectedOffset, ts, Utils.Infinite_Time, false, true, false,
        (short) 1);

    // no clean shutdown file should exist
    assertFalse("Clean shutdown file not deleted",
        new File(tempDirStr, BlobStoreCompactor.TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME).exists());
    // there should be no temp files
    assertEquals("There are some temp log segments", 0,
        tempDir.listFiles(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length);
  }

  /**
   * Tests when the PUT DELETE and UNDELETE are located in different log segments and the undelete is the final state.
   * @throws Exception
   */
  @Test
  public void undeleteCrossLogSegmentTest_UndeleteAsFinal() throws Exception {
    assumeTrue(withUndelete);
    refreshState(false, false, false);
    int numPuts = (int) Math.floorDiv(state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE,
        PUT_RECORD_SIZE + (DELETE_RECORD_SIZE) / 2) - 1;
    List<IndexEntry> entries = state.addPutEntries(numPuts, PUT_RECORD_SIZE, Utils.Infinite_Time);
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
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);

    // For all the keys, in entries, there should only have two index value left, one Put, one Undelete.
    for (IndexEntry entry : entries) {
      List<IndexValue> values = state.index.findAllIndexValuesForKey(entry.getKey(), null);
      assertNotNull(values);
      assertEquals(2, values.size());
      IndexValue undelete = values.get(0);
      IndexValue put = values.get(1);
      assertTrue(undelete.isUndelete());
      assertFalse(undelete.isDelete());
      assertEquals((short) 1, undelete.getLifeVersion());
      IndexValue expected = entry.getValue();
      assertEquals(expected.isPut(), put.isPut());
      assertEquals(expected.getLifeVersion(), put.getLifeVersion());
      assertEquals(expected.getAccountId(), put.getAccountId());
      assertEquals(expected.getContainerId(), put.getContainerId());
      assertEquals(expected.getSize(), put.getSize());
      assertEquals(expected.getExpiresAtMs(), put.getExpiresAtMs());
      assertEquals(expected.getOperationTimeInMs(), put.getOperationTimeInMs());
    }
  }

  /**
   * Tests when the PUT DELETE and UNDELETE are located in different log segments and the delete is the final state.
   * @throws Exception
   */
  @Test
  public void undeleteCrossLogSegmentTest_DeleteAsFinal() throws Exception {
    assumeTrue(withUndelete);
    for (long deleteReferenceTime : new long[]{-1, 0}) {
      refreshState(false, false, false);
      int numPuts = (int) Math.floorDiv(state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE,
          PUT_RECORD_SIZE + (DELETE_RECORD_SIZE) / 2) - 1;
      List<IndexEntry> entries = state.addPutEntries(numPuts, PUT_RECORD_SIZE, Utils.Infinite_Time);
      for (IndexEntry entry : entries) {
        state.addDeleteEntry((MockId) entry.getKey());
      }
      for (IndexEntry entry : entries) {
        state.addUndeleteEntry((MockId) entry.getKey());
      }
      writeDataToMeetRequiredSegmentCount(state.index.getLogSegmentCount() + 1, null);
      for (IndexEntry entry : entries) {
        state.addDeleteEntry((MockId) entry.getKey());
      }
      writeDataToMeetRequiredSegmentCount(state.index.getLogSegmentCount() + 1, null);
      state.reloadIndex(true, false);

      List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
      long realDeleteReferenceTime = deleteReferenceTime < 0 ? state.time.milliseconds() : deleteReferenceTime;
      compactAndVerify(segmentsUnderCompaction, realDeleteReferenceTime, true);

      for (IndexEntry entry : entries) {
        List<IndexValue> values = state.index.findAllIndexValuesForKey(entry.getKey(), null);
        assertNotNull(values);
        IndexValue delete = values.get(0);
        assertFalse(delete.isUndelete());
        assertTrue(delete.isDelete());
        assertEquals((short) 1, delete.getLifeVersion());
        if (deleteReferenceTime < 0) {
          assertEquals(1, values.size());
        } else {
          assertEquals(2, values.size());
          IndexValue put = values.get(1);
          IndexValue expected = entry.getValue();
          assertEquals(expected.isPut(), put.isPut());
          assertEquals(expected.getLifeVersion(), put.getLifeVersion());
          assertEquals(expected.getAccountId(), put.getAccountId());
          assertEquals(expected.getContainerId(), put.getContainerId());
          assertEquals(expected.getSize(), put.getSize());
          assertEquals(expected.getExpiresAtMs(), put.getExpiresAtMs());
          assertEquals(expected.getOperationTimeInMs(), put.getOperationTimeInMs());
        }
      }
    }
  }

  /**
   * Test the case when there are multiple deletes in the log.
   * @throws Exception
   */
  @Test
  public void deleteWithOlderVersionTest() throws Exception {
    assumeTrue(withUndelete);
    // 1. Compact all the puts and deletes with delete reference time being 0
    List<IndexEntry> entries = createDeleteWithOlderVersion();
    int numPuts = entries.size();
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    long realBytesBeforeCompaction =
        logSegmentSizeSumBeforeCompaction - LogSegment.HEADER_SIZE * state.index.getLogSegmentCount();
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, 0, true);
    long compactedBytes = numPuts * (DELETE_RECORD_SIZE + UNDELETE_RECORD_SIZE);
    long logSegmentSizeSumAfterCompaction = getSumOfLogSegmentEndOffsets();
    long realBytesAfterCompaction =
        logSegmentSizeSumAfterCompaction - LogSegment.HEADER_SIZE * state.index.getLogSegmentCount();
    assertEquals(compactedBytes, realBytesBeforeCompaction - realBytesAfterCompaction);

    for (IndexEntry entry : entries) {
      List<IndexValue> values = state.index.findAllIndexValuesForKey(entry.getKey(), null);
      assertNotNull(values);
      assertEquals(2, values.size());
      assertEquals((short) 1, values.get(0).getLifeVersion());
      assertTrue(values.get(0).isDelete());
      assertTrue(values.get(1).isPut());
    }

    // 2. Compact all the puts and deletes with delete reference time being now.
    entries = createDeleteWithOlderVersion();
    numPuts = entries.size();
    logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    realBytesBeforeCompaction =
        logSegmentSizeSumBeforeCompaction - LogSegment.HEADER_SIZE * state.index.getLogSegmentCount();
    segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);
    compactedBytes = numPuts * (DELETE_RECORD_SIZE + UNDELETE_RECORD_SIZE + PUT_RECORD_SIZE);
    logSegmentSizeSumAfterCompaction = getSumOfLogSegmentEndOffsets();
    realBytesAfterCompaction =
        logSegmentSizeSumAfterCompaction - LogSegment.HEADER_SIZE * state.index.getLogSegmentCount();
    assertEquals(compactedBytes, realBytesBeforeCompaction - realBytesAfterCompaction);

    for (IndexEntry entry : entries) {
      List<IndexValue> values = state.index.findAllIndexValuesForKey(entry.getKey(), null);
      assertNotNull(values);
      assertEquals(1, values.size());
      assertEquals((short) 1, values.get(0).getLifeVersion());
      assertTrue(values.get(0).isDelete());
    }

    // 3. Compact only the deletes, with delete reference time being now.
    entries = createDeleteWithOlderVersion();
    numPuts = entries.size();
    logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    realBytesBeforeCompaction =
        logSegmentSizeSumBeforeCompaction - LogSegment.HEADER_SIZE * state.index.getLogSegmentCount();
    segmentsUnderCompaction = getLogSegments(1, state.index.getLogSegmentCount() - 2);
    compactAndVerify(segmentsUnderCompaction, state.time.milliseconds(), true);
    // there is one put at the beginning of the second log segment.
    compactedBytes = numPuts * (DELETE_RECORD_SIZE + UNDELETE_RECORD_SIZE) + PUT_RECORD_SIZE;
    logSegmentSizeSumAfterCompaction = getSumOfLogSegmentEndOffsets();
    realBytesAfterCompaction =
        logSegmentSizeSumAfterCompaction - LogSegment.HEADER_SIZE * state.index.getLogSegmentCount();
    assertEquals(compactedBytes, realBytesBeforeCompaction - realBytesAfterCompaction);
  }

  /**
   * Test when log has P, D, U, T, U, but T and second U is in a separate segment under compaction
   */
  @Test
  public void undeleteTargetIndexOnlyHasTtlUpdateTest() throws Exception {
    assumeTrue(withUndelete);
    refreshState(false, false, false);
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(5));
    state.initIndex(null);

    // Insert P, D, U in the first log segment
    long p1Expiration = state.time.milliseconds() + 1000 * Time.MsPerSec;
    IndexEntry p1 = state.addPutEntries(1, PUT_RECORD_SIZE, p1Expiration).get(0);
    state.addDeleteEntry((MockId) p1.getKey());
    state.addUndeleteEntry((MockId) p1.getKey());

    writeDataToMeetRequiredSegmentCount(1, null);
    // Now we are at second segment
    // Randomly insert a new put
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    state.makePermanent((MockId) p1.getKey(), false);
    state.addUndeleteEntry((MockId) p1.getKey(), (short) 2);

    writeDataToMeetRequiredSegmentCount(3, null);

    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(1, 1);
    state.advanceTime(p1Expiration - state.time.milliseconds() + 1000);
    long deleteReferenceTimeMs = state.time.milliseconds();
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, false);

    state.reloadIndex(true, false);
    List<IndexValue> values = state.index.findAllIndexValuesForKey(p1.getKey(), null);
    assertNotNull(values);
    assertEquals(5, values.size());
    assertTrue(values.get(0).isUndelete());
    assertTrue(values.get(0).isTtlUpdate());
    assertEquals(Utils.Infinite_Time, values.get(0).getExpiresAtMs());
    assertEquals((short) 2, values.get(0).getLifeVersion());
    assertTrue(values.get(1).isTtlUpdate());
    assertEquals(Utils.Infinite_Time, values.get(1).getExpiresAtMs());
    assertEquals((short) 1, values.get(1).getLifeVersion());
  }

  /**
   * Test when log has P, D, U, D, but P and first D is in a separate segment under compaction.
   * @throws Exception
   */
  @Test
  public void undeletePutCompactedTest() throws Exception {
    assumeTrue(withUndelete);
    refreshState(false, false, false);
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(5));
    state.initIndex(null);

    // Insert P, D in the first log segment
    IndexEntry p1 = state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    state.addDeleteEntry((MockId) p1.getKey());

    writeDataToMeetRequiredSegmentCount(1, null);
    // Now we are at second segment
    // Randomly insert a new put
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    state.addUndeleteEntry((MockId) p1.getKey());
    state.addDeleteEntry((MockId) p1.getKey());

    writeDataToMeetRequiredSegmentCount(3, null);

    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, 1);
    long deleteReferenceTimeMs = state.time.milliseconds() + 1000;
    compactAndVerify(segmentsUnderCompaction, deleteReferenceTimeMs, true);

    state.reloadIndex(true, false);
    List<IndexValue> values = state.index.findAllIndexValuesForKey(p1.getKey(), null);
    assertNotNull(values);
    assertEquals(2, values.size());
    assertTrue(values.get(0).isDelete());
    assertEquals((short) 1, values.get(0).getLifeVersion());
    assertTrue(values.get(1).isUndelete());
    assertEquals((short) 1, values.get(1).getLifeVersion());
  }

  /**
   * Tests some recovery scenarios related to UNDELETE records in particular
   * @throws Exception
   */
  @Test
  public void undeleteSpecificRecoveryTest() throws Exception {
    assumeTrue(withUndelete);
    bundleReadBuffer = null;
    // close testing
    doUndeleteSrcDupTest();
    doUndeleteTgtDupTest();
    // crash testing
    throwExceptionInsteadOfClose = true;
    doUndeleteSrcDupTest();
    doUndeleteTgtDupTest();
  }

  /**
   * Tests for {@link BlobStoreCompactor#getDesiredSpeedPerSecond(double, int)}.
   * @throws Exception
   */
  @Test
  public void testGetDesiredSpeedPerSecond() throws Exception {
    refreshState(false, true, false);
    int storeCompactionOperationsBytesPerSec = 1000;
    int storeCompactionMinOperationBytesPerSec = 500;
    double storeCompactionOperationsAdjustK = 1.5;
    int storeDiskIoReservoirTimeWindowMs = 200;
    state.properties.put("store.compaction.operations.bytes.per.sec",
        Integer.toString(storeCompactionOperationsBytesPerSec));
    state.properties.put("store.compaction.min.operations.bytes.per.sec",
        Integer.toString(storeCompactionMinOperationBytesPerSec));
    state.properties.put("store.compaction.operations.adjust.k", Double.toString(storeCompactionOperationsAdjustK));
    state.properties.put("store.disk.io.reservoir.time.window.ms", Integer.toString(storeDiskIoReservoirTimeWindowMs));
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);
    int latencyThreshold = 20;

    // Test getDesiredSpeedPerSecond
    assertEquals("Incorrect speed", storeCompactionOperationsBytesPerSec,
        compactor.getDesiredSpeedPerSecond(latencyThreshold, latencyThreshold));
    assertEquals("Incorrect speed", storeCompactionOperationsBytesPerSec,
        compactor.getDesiredSpeedPerSecond(latencyThreshold - 1, latencyThreshold));

    int actualLatency = latencyThreshold * 2;
    int expectedSpeed = (int) (storeCompactionMinOperationBytesPerSec
        + (storeCompactionOperationsBytesPerSec - storeCompactionMinOperationBytesPerSec) * latencyThreshold
        / actualLatency / storeCompactionOperationsAdjustK);
    assertEquals("Incorrect speed", expectedSpeed,
        compactor.getDesiredSpeedPerSecond(latencyThreshold * 2, latencyThreshold));

    // // Test histogram and getDesiredSpeedPerSecond
    Histogram histogram = new Histogram(new SlidingTimeWindowArrayReservoir(1000, TimeUnit.MILLISECONDS));
    double p95 = histogram.getSnapshot().get95thPercentile();
    assertEquals("Histogram without data should return 0.0", 0.0, p95, 0.001);
    assertEquals("Incorrect speed", storeCompactionOperationsBytesPerSec,
        compactor.getDesiredSpeedPerSecond(p95, latencyThreshold));

    histogram.update(100);
    p95 = histogram.getSnapshot().get95thPercentile();
    assertEquals("Histogram without data should return 0.0", 100.0, p95, 0.001);
    expectedSpeed = (int) (storeCompactionMinOperationBytesPerSec
        + (storeCompactionOperationsBytesPerSec - storeCompactionMinOperationBytesPerSec) * latencyThreshold / p95
        / storeCompactionOperationsAdjustK);
    assertEquals("Incorrect speed", expectedSpeed, compactor.getDesiredSpeedPerSecond(p95, latencyThreshold));
  }

  // helpers

  // general

  /**
   * Generate local replica and two peer replicas.
   * @return a list of replicas (first one is local replica, others are remote peer replicas)
   */
  private List<MockReplicaId> generateLocalAndPeerReplicas() {
    Port port = new Port(6667, PortType.PLAINTEXT);
    // Use temp folder to create the mount point. /mnt/u001 may not exist or accessible.
    peerReplicaMountPath = Arrays.asList(tempDir.getAbsolutePath() + "/u001", tempDir.getAbsolutePath() + "/u002",
        tempDir.getAbsolutePath() + "/u003");

    // generate two peer replicas
    MockDataNodeId peerNode1 =
        new MockDataNodeId("node1_host", Collections.singletonList(port), peerReplicaMountPath, null);
    MockDataNodeId peerNode2 =
        new MockDataNodeId("node2_host", Collections.singletonList(port), peerReplicaMountPath, null);
    MockDataNodeId localNode =
        new MockDataNodeId("local_host", Collections.singletonList(port), peerReplicaMountPath, null);
    MockPartitionId mockPartitionId = new MockPartitionId(101L, MockClusterMap.DEFAULT_PARTITION_CLASS);

    // create mount point for the local replica. MockReplicaId constructor will create replica folder under it.
    for (String path : peerReplicaMountPath) {
      File mountFile = new File(path);
      mountFile.mkdir();
      mountFile.deleteOnExit();
    }
    MockReplicaId peerReplica1 = new MockReplicaId(port.getPort(), mockPartitionId, peerNode1, 0);
    MockReplicaId peerReplica2 = new MockReplicaId(port.getPort(), mockPartitionId, peerNode2, 1);
    MockReplicaId localReplica = new MockReplicaId(port.getPort(), mockPartitionId, localNode, 2);
    localReplica.setPeerReplicas(Arrays.asList(peerReplica1, peerReplica2));
    return Arrays.asList(localReplica, peerReplica1, peerReplica2);
  }

  private void testDuplicatePutsHelper() throws Exception {
    // Construct a blob store, where there is a duplicate PUT indexValue in different log segment.
    refreshState(false, false, false);
    writeDataToMeetRequiredSegmentCount(1, Collections.singletonList(Utils.Infinite_Time));
    // we know all the IndexEntries are PUT entries.
    TreeMap<MockId, TreeSet<IndexValue>> lastEntry = state.referenceIndex.lastEntry().getValue();
    MockId id = lastEntry.firstKey();
    IndexValue value = lastEntry.get(id).first();
    byte[] bytes = state.logOrder.get(value.getOffset()).getSecond().buffer;

    // Delete some PUT entries, so we know the first log segment will be compacted
    for (MockId toDelete : state.referenceIndex.firstEntry().getValue().keySet()) {
      state.addDeleteEntry(toDelete);
    }
    state.forceAddPutEntry(id, value, bytes);
    writeDataToMeetRequiredSegmentCount(3, Collections.singletonList(Utils.Infinite_Time));

    // Now compact the blobstore, it will fail because of duplicate puts
    state.reloadIndex(true, false);
    long deleteReferenceTimeMs = state.time.milliseconds();
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);

    long duplicateCount = compactor.getTgtMetrics().compactionTargetIndexDuplicateOnNonRecoveryCount.getCount();
    try {
      compactor.compact(details, bundleReadBuffer);
      fail("Should fail with duplicate put");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.UnknownError, e.getErrorCode());
      assertTrue(e.getMessage().contains("duplicate PUT"));
    }
    compactor.close(0);
  }

  /**
   * Refreshes the state by destroying any existing state and creating a new one.
   * @param hardDeleteEnabled {@code true} if hard delete needs to be enabled. {@code false} otherwise.
   * @param initState {@code true} if some test state has to be created. {@code false} otherwise.
   * @param enableAutoCloseLastLogSegment
   * @throws Exception
   */
  private void refreshState(boolean hardDeleteEnabled, boolean initState, boolean enableAutoCloseLastLogSegment)
      throws Exception {
    if (state != null) {
      state.destroy();
    }
    state = new CuratedLogIndexState(true, tempDir, hardDeleteEnabled, initState, true, withUndelete, false,
        enableAutoCloseLastLogSegment);
  }

  /**
   * Gets an instance of {@link BlobStoreCompactor}.
   * @param log the {@link Log} instance to use.
   * @param ioScheduler the {@link DiskIOScheduler} instance to use.
   * @param remoteTokenTracker the {@link RemoteTokenTracker} instance to use.
   * @param enableAutoCloseLastLogSegment
   * @return an instance of {@link BlobStoreCompactor}.
   * @throws IOException
   * @throws StoreException
   */
  private BlobStoreCompactor getCompactor(Log log, DiskIOScheduler ioScheduler, RemoteTokenTracker remoteTokenTracker,
      boolean enableAutoCloseLastLogSegment) throws IOException, StoreException {
    closeOrExceptionInduced = false;
    state.properties.put("store.compaction.enable.direct.io", Boolean.toString(doDirectIO));
    if (directIOWithBuffer) {
      state.properties.put(StoreConfig.storeCompactionDirectIOBufferSizeName, String.valueOf(4 * 1024 * 1024));
    }
    if (withUndelete) {
      state.properties.put("store.compaction.filter", "IndexSegmentValidEntryWithUndelete");
    }
    state.properties.put("store.auto.close.last.log.segment.enabled", Boolean.toString(enableAutoCloseLastLogSegment));
    if (enableAutoCloseLastLogSegment) {
      state.properties.put("store.compaction.policy.factory", "com.github.ambry.store.HybridCompactionPolicyFactory");
      state.properties.put("store.container.deletion.enabled", Boolean.toString(enableAutoCloseLastLogSegment));
    }
    state.properties.put("store.compaction.purge.delete.tombstone", Boolean.toString(purgeDeleteTombstone));
    StoreConfig config = new StoreConfig(new VerifiableProperties(state.properties));
    metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    BlobStoreCompactor compactor =
        new BlobStoreCompactor(tempDirStr, STORE_ID, STORE_KEY_FACTORY, config, metrics, metrics, ioScheduler,
            StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR, log, state.time, state.sessionId, state.incarnationId,
            accountService, remoteTokenTracker, null);
    compactor.enablePersistIndexSegmentOffsets();
    return compactor;
  }

  /**
   * Gets {@code count} number of log segments starting at position {@code startPos} (inclusive and indexed on 0).
   * <p/>
   * For e.g., to get the first two log segments, the call would be getLogSegments(0, 2).
   * @param startPos the position of the first log segment required (inclusive and indexed on 0).
   * @param count number of log segments require.
   * @return {@code count} number of log segments starting at position {@code startPos} (inclusive and indexed on 0).
   */
  private List<LogSegmentName> getLogSegments(long startPos, long count) {
    List<LogSegmentName> segmentsUnderCompaction = new ArrayList<>();
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
  private Set<MockId> getIdsWithPutInSegments(List<LogSegmentName> logSegmentNames) {
    Set<MockId> idsInCompactedLogSegments = new HashSet<>();
    for (LogSegmentName segmentName : logSegmentNames) {
      idsInCompactedLogSegments.addAll(state.getIdsWithPutsInLogSegment(state.log.getSegment(segmentName)));
    }
    return idsInCompactedLogSegments;
  }

  /**
   * Gets the sum of size of valid data in {@code logSegments} at {@code deleteReferenceTimeMs}.
   * @param logSegments the names of log segments whose sum of size of valid data is required.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @param invalidateExpiredDelete whether to invalidate expired delete.
   * @return the sum of size of valid data in {@code logSegments} at {@code deleteReferenceTimeMs}.
   */
  private long getValidDataSize(List<LogSegmentName> logSegments, long deleteReferenceTimeMs,
      boolean invalidateExpiredDelete) {
    long size = 0;
    Pair<Set<MockId>, Set<MockId>> expiredDeletes = new Pair<>(new HashSet<>(), new HashSet<>());
    for (LogSegmentName segment : logSegments) {
      long logSegmentValidSize =
          state.getValidDataSizeForLogSegment(state.log.getSegment(segment), deleteReferenceTimeMs,
              state.time.milliseconds(), getFileSpanForLogSegments(logSegments),
              new DeleteTombstoneStats.Builder(state.time), expiredDeletes, invalidateExpiredDelete);
      size += logSegmentValidSize;
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
    while (state.index.getLogUsedCapacity() + PUT_RECORD_SIZE <= capacityLimit) {
      long expiresAtMs = Utils.Infinite_Time;
      if (expiryTimes != null && blobsPut % 4 == 0) {
        expiresAtMs = expiryTimes.get(expiredBlobsCount % expiryTimes.size());
        expiredBlobsCount++;
      }
      state.addPutEntries(1, PUT_RECORD_SIZE, expiresAtMs);
      blobsPut++;
    }
  }

  /**
   * Write data so that the log contains {@code countRequired} number of log segments. If a non {@code null} for auto close log segment test.
   * @param countRequired the number of log segments required.
   * @throws StoreException
   */
  private void writeDataToMeetRequiredSegmentCountForAutoCloseLogSegmentTest(long countRequired) throws StoreException {
    long capacityLimit = countRequired * state.log.getSegmentCapacity();
    int blobsPut = 0;
    while (state.index.getLogUsedCapacity() + PUT_RECORD_SIZE <= capacityLimit) {
      long expiresAtMs = Utils.Infinite_Time;
      state.addPutEntries(1, PUT_RECORD_SIZE, expiresAtMs);
      blobsPut++;
    }
    logger.info("The number of blobs put in the log: {}.", blobsPut);
  }

  /**
   * Reduces the valid data size in {@code logSegmentsToReduceFrom} to <= {@code ceilingSize} by deleting keys in these
   * segments.
   * @param logSegmentsToReduceFrom the names of the log segments to reduce the data from.
   * @param ceilingSize the maximum allowed valid size in these log segments.
   * @return the time (in ms) at which all the deletes are valid.
   * @throws StoreException
   */
  private long reduceValidDataSizeInLogSegments(List<LogSegmentName> logSegmentsToReduceFrom, long ceilingSize)
      throws StoreException {
    List<LogSegmentName> logSegments = new ArrayList<>(logSegmentsToReduceFrom);
    long validDataSize = getValidDataSize(logSegmentsToReduceFrom, state.time.milliseconds(), purgeDeleteTombstone);
    while (validDataSize > ceilingSize) {
      assertTrue("There are no more segments to delete data from", logSegments.size() > 0);
      int selectedIdx = TestUtils.RANDOM.nextInt(logSegments.size());
      LogSegmentName segmentToDeleteFrom = logSegments.get(selectedIdx);
      MockId idToDelete = state.getIdToDeleteFromLogSegment(state.log.getSegment(segmentToDeleteFrom), false);
      if (idToDelete == null) {
        logSegments.remove(selectedIdx);
      } else {
        state.addDeleteEntry(idToDelete);
        validDataSize = getValidDataSize(logSegmentsToReduceFrom, state.time.milliseconds() + 1, purgeDeleteTombstone);
      }
    }
    state.advanceTime(Time.MsPerSec);
    return state.time.milliseconds();
  }

  /**
   * Compacts the {@code segmentsUnderCompaction} and verifies sanity of store and data.
   * @param segmentsUnderCompaction the names of the log segments under compaction.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @param changeExpected {@code true} if compaction will cause a change in size of data. {@code false} otherwise.
   * @throws Exception
   */
  private void compactAndVerifyForContainerDeletion(List<LogSegmentName> segmentsUnderCompaction,
      long deleteReferenceTimeMs, boolean changeExpected) throws Exception {
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    long logSegmentCountBeforeCompaction = state.index.getLogSegmentCount();
    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);
    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      if (changeExpected) {
        compactor.close(0);
      }
    }
    if (state.log.autoCloseLastLogSegmentIfQualified()) {
      //refresh journal.
      state.index.journal.cleanUpJournal();
    }
    if (!changeExpected) {
      assertEquals("Journal size should be cleaned up after last log segment closed", 0,
          state.index.journal.getCurrentNumberOfEntries());
    }
    long logSegmentSizeAfterCompaction = getSumOfLogSegmentEndOffsets();
    long logSegmentCountAfterCompaction = state.index.getLogSegmentCount();
    if (changeExpected) {
      assertTrue("Last index segment should be compacted",
          logSegmentSizeSumBeforeCompaction - logSegmentSizeAfterCompaction > 0);
      assertEquals("Log Segment count should be same due to no new index entry mapping to the last log segment.",
          logSegmentCountBeforeCompaction, logSegmentCountAfterCompaction);
    }
  }

  private void backUpCompactionPolicyInfo(String tempDir, CompactionPolicySwitchInfo compactionPolicySwitchInfo) {
    if (tempDir != null) {
      File tempFile = new File(tempDir, COMPACT_POLICY_INFO_FILE_NAME_V2 + ".temp");
      try {
        tempFile.createNewFile();
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(tempFile, compactionPolicySwitchInfo);
        tempFile.renameTo(new File(tempDir, COMPACT_POLICY_INFO_FILE_NAME_V2));
      } catch (IOException e) {
        logger.error("Exception while store compaction policy info for local report. Output file path - {}",
            tempFile.getAbsolutePath(), e);
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
  private void compactAndVerify(List<LogSegmentName> segmentsUnderCompaction, long deleteReferenceTimeMs,
      boolean changeExpected) throws Exception {
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    long logSegmentCountBeforeCompaction = state.index.getLogSegmentCount();
    long indexSegmentCountBeforeCompaction = state.index.getIndexSegments().size();

    Set<Offset> indexSegmentOffsetsUnderCompaction = state.index.getIndexSegments()
        .keySet()
        .stream()
        .filter(offset -> segmentsUnderCompaction.contains(offset.getName()))
        .collect(Collectors.toSet());

    ScheduledExecutorService scheduler = Utils.newScheduler(1, true);
    BlobStoreStats stats =
        new BlobStoreStats("", state.index, 0, Time.MsPerSec, 0, 100, Time.SecsPerMin, false, purgeDeleteTombstone,
            state.time, scheduler, scheduler, DISK_IO_SCHEDULER, new StoreMetrics(new MetricRegistry()), 1, false);
    NavigableMap<LogSegmentName, Long> validDataSizeFromBlobStoreStats =
        stats.getValidDataSizeByLogSegment(new TimeRange(deleteReferenceTimeMs, 0L),
            getFileSpanForLogSegments(segmentsUnderCompaction)).getSecond();
    scheduler.shutdown();
    long totalSizeAfterCompactionFromBlobStoreStats =
        segmentsUnderCompaction.stream().mapToLong(validDataSizeFromBlobStoreStats::get).sum();

    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);
    long expectedValidDataSize = getValidDataSize(segmentsUnderCompaction, deleteReferenceTimeMs, purgeDeleteTombstone);
    assertEquals("Valid size from blob store should be the same as compacted size", expectedValidDataSize,
        totalSizeAfterCompactionFromBlobStoreStats);
    List<LogSegmentName> unaffectedSegments = getUnaffectedSegments(segmentsUnderCompaction);
    Pair<Set<MockId>, Set<MockId>> expiredDeletes = new Pair<>(new HashSet<>(), new HashSet<>());
    List<LogEntry> validLogEntriesInOrder =
        getValidLogEntriesInOrder(segmentsUnderCompaction, deleteReferenceTimeMs, expiredDeletes, purgeDeleteTombstone);
    Set<MockId> idsInCompactedLogSegments = getIdsWithPutInSegments(segmentsUnderCompaction);
    // "compactedDeletes" are those tombstones that should be compacted in single run (if no exception occurs);
    // "deletesWithPuts" are those tombstones temporarily with PUTs but may be eligible to be compacted in subsequent cycle
    Set<MockId> compactedDeletes = expiredDeletes.getFirst();
    Set<MockId> deletesWithPuts = expiredDeletes.getSecond();

    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    compactor.initialize(state.index);

    try {
      compactor.compact(details, bundleReadBuffer);
    } finally {
      compactor.close(0);
    }
    // First, verify the index segment offset map is good
    verifyIndexSegmentOffsetsBeforeAndAfterCompaction(indexSegmentOffsetsUnderCompaction);

    Set<MockId> remainingBlobIds = getCurrentBlobIdsFromWholeIndex(state.index, null, purgeDeleteTombstone);
    // since this method aims to verify success compaction case, we only need to account for some deletes with PUTs are
    // compacted in the multi-cycle compaction (i.e. PUT is 1st log segment and gets compacted in 1st cycle. DELETE is
    // in 2nd log segment and the 2nd cycle compaction may compact the DELETE as well because source index is updated
    // when switching out the 1st log segment and original PUT is not found in source index)
    deletesWithPuts.removeAll(remainingBlobIds);
    expectedValidDataSize -= deletesWithPuts.size() * DELETE_RECORD_SIZE;
    compactedDeletes.addAll(deletesWithPuts);
    // remove these deletes from valid log entries (if any)
    validLogEntriesInOrder = deletesWithPuts.isEmpty() ? validLogEntriesInOrder : validLogEntriesInOrder.stream()
        .filter(logEntry -> !deletesWithPuts.contains(logEntry.getId()))
        .collect(Collectors.toList());

    assertFalse("No compaction should be in progress", CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID));
    assertEquals("Swap segments should not be found", 0, compactor.getSwapSegmentsInUse().length);
    long logSegmentSizeAfterCompaction = getSumOfLogSegmentEndOffsets();
    long logSegmentCountAfterCompaction = state.index.getLogSegmentCount();
    long indexSegmentCountAfterCompaction = state.index.getIndexSegments().size();
    verifyCompaction(segmentsUnderCompaction, unaffectedSegments, expectedValidDataSize, validLogEntriesInOrder,
        idsInCompactedLogSegments, deleteReferenceTimeMs, compactedDeletes);

    state.reloadLog(true);
    verifyCompaction(segmentsUnderCompaction, unaffectedSegments, expectedValidDataSize, validLogEntriesInOrder,
        idsInCompactedLogSegments, deleteReferenceTimeMs, compactedDeletes);

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
      List<LogSegmentName> segmentsUnderCompaction, long deleteReferenceTimeMs, boolean changeExpected,
      boolean checkSavedBytesReported) throws Exception {
    // getting these from the "real" log and indexes
    long logSegmentSizeSumBeforeCompaction = getSumOfLogSegmentEndOffsets();
    long logSegmentCountBeforeCompaction = state.index.getLogSegmentCount();
    long indexSegmentCountBeforeCompaction = state.index.getIndexSegments().size();
    List<LogSegmentName> allLogSegmentsBeforeCompaction = log.getAllLogSegmentNames();

    Set<Offset> indexSegmentOffsetsUnderCompaction = state.index.getIndexSegments()
        .keySet()
        .stream()
        .filter(offset -> segmentsUnderCompaction.contains(offset.getName()))
        .collect(Collectors.toSet());

    CompactionDetails details = new CompactionDetails(deleteReferenceTimeMs, segmentsUnderCompaction, null);
    long expectedValidDataSize = getValidDataSize(segmentsUnderCompaction, deleteReferenceTimeMs, purgeDeleteTombstone);
    List<LogSegmentName> unaffectedSegments = getUnaffectedSegments(segmentsUnderCompaction);
    // "expiredDeletes" has two parts: 1. expired delete without PUT; 2. expired delete with PUT.
    Pair<Set<MockId>, Set<MockId>> expiredDeletes = new Pair<>(new HashSet<>(), new HashSet<>());
    // get valid log entries including deletes as a backup (in case exception occurred in the middle of compaction)
    List<LogEntry> validLogEntriesInOrder =
        getValidLogEntriesInOrder(segmentsUnderCompaction, deleteReferenceTimeMs, expiredDeletes, purgeDeleteTombstone);
    Set<MockId> compactedDeletes = expiredDeletes.getFirst();
    Set<MockId> deletesWithPuts = expiredDeletes.getSecond();
    Set<MockId> idsInCompactedLogSegments = getIdsWithPutInSegments(segmentsUnderCompaction);
    compactor = getCompactor(log, diskIOScheduler, null, false);
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
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
    // Create a compactor before loading index. Since this compactor would remove the old log segment so the old index segments
    // won't be loaded. This is the same order as the BlobStore initialize the compactor and index.
    state.initIndex(null);
    compactor.initialize(state.index);
    assertEquals("Wrong number of swap segments in use",
        tempDir.list(BlobStoreCompactor.TEMP_LOG_SEGMENTS_FILTER).length, compactor.getSwapSegmentsInUse().length);

    // rebuild compaction history
    CompactionLog.processCompactionLogs(tempDirStr, STORE_ID, STORE_KEY_FACTORY, time, state.config, cLog -> {
      state.index.updateBeforeAndAfterCompactionIndexSegmentOffsets(cLog.getStartTime(),
          cLog.getBeforeAndAfterIndexSegmentOffsetsForCompletedCycles(), false);
      return false;
    });
    state.index.sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets();
    // For some reason this compaction is interrupted, we have to check if there is a new log segment added.
    List<LogSegmentName> allLogSegmentsAfterCompaction = log.getAllLogSegmentNames();
    List<LogSegmentName> newLogSegments = allLogSegmentsAfterCompaction.stream()
        .filter(name -> !allLogSegmentsBeforeCompaction.contains(name))
        .collect(Collectors.toList());

    if (newLogSegments.size() != 0) {
      // If we have new log segments, then the before offsets belong to the segments under compaction
      // and the after offsets belong to the new log segments.
      Map<Offset, Offset> indexSegmentOffsets = state.index.getBeforeAndAfterCompactionIndexSegmentOffsets();
      assertTrue(indexSegmentOffsets.size() > 0);
      for (Map.Entry<Offset, Offset> entry : indexSegmentOffsets.entrySet()) {
        assertTrue(segmentsUnderCompaction.contains(entry.getKey().getName()));
        assertTrue(newLogSegments.contains(entry.getValue().getName()));
      }
    }

    try {
      if (CompactionLog.isCompactionInProgress(tempDirStr, STORE_ID)) {
        compactor.resumeCompaction(bundleReadBuffer);
      }
    } finally {
      compactor.close(0);
    }

    verifyIndexSegmentOffsetsBeforeAndAfterCompaction(indexSegmentOffsetsUnderCompaction);
    Set<MockId> remainingBlobIds = getCurrentBlobIdsFromWholeIndex(state.index, null, purgeDeleteTombstone);
    // find those deletes initially had PUTs but were compacted in the subsequent compaction cycle.
    deletesWithPuts.removeAll(remainingBlobIds);

    // remove expired deletes that were compacted in second round of compaction (i.e. resume compaction after shutdown/crash)
    expectedValidDataSize -= deletesWithPuts.size() * DELETE_RECORD_SIZE;
    // some deletes had PUTs before compaction and were considered valid. After first cycle of compaction, the original
    // PUT is compacted (i.e. PUT and DELETE are in different log segments), and the delete may be eligible to be
    // compacted in the second cycle. If this did happen, remove such deletes from valid log entries.
    validLogEntriesInOrder = validLogEntriesInOrder.stream()
        .filter(logEntry -> !deletesWithPuts.contains(logEntry.getId()))
        .collect(Collectors.toList());
    // add deletes (initially with PUTs) that were cleaned up to the "compactedDeletes" set
    compactedDeletes.addAll(deletesWithPuts);

    // if those deletes (which supposed to be compacted) are still present, use valid log entries with deletes
    verifyCompaction(segmentsUnderCompaction, unaffectedSegments, expectedValidDataSize, validLogEntriesInOrder,
        idsInCompactedLogSegments, deleteReferenceTimeMs, compactedDeletes);
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
  private List<LogSegmentName> getUnaffectedSegments(List<LogSegmentName> segmentsUnderCompaction) {
    List<LogSegmentName> unaffectedSegments = new ArrayList<>();
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

  private CompactionLog getLatestCompactionLog() throws IOException {
    AtomicReference<CompactionLog> latestLogRef = new AtomicReference<>(null);
    CompactionLog.processCompactionLogs(tempDirStr, STORE_ID, STORE_KEY_FACTORY, state.time, state.config, cLog -> {
      latestLogRef.set(cLog);
      return false;
    });
    CompactionLog latestLog = latestLogRef.get();
    assertNotNull(latestLog);
    return latestLog;
  }

  private void verifyIndexSegmentOffsetsBeforeAndAfterCompaction(Set<Offset> indexSegmentOffsetsUnderCompaction)
      throws Exception {
    CompactionLog latestLog = getLatestCompactionLog();
    NavigableMap<Offset, Offset> indexSegmentOffsets = latestLog.getBeforeAndAfterIndexSegmentOffsets();
    Map<Offset, Offset> indexSegmentOffsetsFromIndex = state.index.getBeforeAndAfterCompactionIndexSegmentOffsets();
    assertEquals(indexSegmentOffsetsFromIndex, indexSegmentOffsets);
    Map<Long, Set<Offset>> compactionTimestampToOffsets = state.index.getCompactionTimestampToIndexSegmentOffsets();
    assertEquals(1, compactionTimestampToOffsets.size());
    assertTrue(compactionTimestampToOffsets.containsKey(latestLog.getStartTime()));
    assertEquals(compactionTimestampToOffsets.get(latestLog.getStartTime()), indexSegmentOffsets.keySet());
    assertEquals(indexSegmentOffsetsUnderCompaction, indexSegmentOffsets.keySet());
    Set<Long> positionsUnderCompaction = indexSegmentOffsetsUnderCompaction.stream()
        .map(offset -> offset.getName().getPosition())
        .collect(Collectors.toSet());
    Map<LogSegmentName, TreeSet<Offset>> indexSegmentOffsetsAfterCompaction =
        new TreeSet<>(state.index.getIndexSegments().keySet()).stream()
            .filter(offset -> positionsUnderCompaction.contains(offset.getName().getPosition()))
            .collect(Collectors.groupingBy(offset -> offset.getName(), Collectors.toCollection(TreeSet::new)));
    // After compaction, we would have several new index segments. The values of map indexSegmentOffsets should either
    // equal to the new index segment offsets, or a subset of it(excluding the last index segment).
    Map<LogSegmentName, Set<Offset>> values = new TreeSet<>(indexSegmentOffsets.values()).stream()
        .collect(Collectors.groupingBy(offset -> offset.getName(), Collectors.toSet()));
    for (Map.Entry<LogSegmentName, TreeSet<Offset>> entry : indexSegmentOffsetsAfterCompaction.entrySet()) {
      TreeSet<Offset> afterCompaction = entry.getValue();
      Set<Offset> offsets = values.get(entry.getKey());
      if (offsets == null) {
        assertEquals(1, afterCompaction.size());
      } else {
        if (offsets.size() == afterCompaction.size()) {
          assertEquals(afterCompaction, offsets);
        } else {
          afterCompaction.remove(afterCompaction.last());
          assertEquals(afterCompaction, offsets);
        }
      }
    }

    Offset prevAfterCompactionOffset = null;
    for (Map.Entry<Offset, Offset> entry : indexSegmentOffsets.entrySet()) {
      Offset curr = entry.getValue();
      if (prevAfterCompactionOffset != null) {
        assertFalse(curr.compareTo(prevAfterCompactionOffset) < 0);
      }
      prevAfterCompactionOffset = curr;
    }
  }

  /**
   * Verifies compaction by checking the integrity of the store and data.
   * Calls {@link #verifyStorePostCompaction(List, List, long, List)} and {@link #verifyDataPostCompaction(Set, Set, long)}.
   * @param segmentsCompacted the names of the log segments that were compacted.
   * @param unaffectedSegments the names of the log segments that should have been unaffected.
   * @param targetSegmentsExpectedValidSize the expected valid size of all the new segments that were created due to
   *                                        compaction.
   * @param validLogEntriesInOrder the log entries for valid data before compaction in order. The new segments should
   *                               have all of these entries and in the same order.
   * @param idsInCompactedLogSegments the ids that had PUT records in the segments that were compacted.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @param compactedDeletes a set of delete tombstone ids that should be cleaned up in compacted segments.
   * @throws IOException
   * @throws StoreException
   */
  private void verifyCompaction(List<LogSegmentName> segmentsCompacted, List<LogSegmentName> unaffectedSegments,
      long targetSegmentsExpectedValidSize, List<LogEntry> validLogEntriesInOrder,
      Set<MockId> idsInCompactedLogSegments, long deleteReferenceTimeMs, Set<MockId> compactedDeletes)
      throws IOException, StoreException {
    verifyStorePostCompaction(segmentsCompacted, unaffectedSegments, targetSegmentsExpectedValidSize,
        validLogEntriesInOrder);
    verifyDataPostCompaction(idsInCompactedLogSegments, compactedDeletes, deleteReferenceTimeMs);
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
   * @throws StoreException
   */
  private void verifyStorePostCompaction(List<LogSegmentName> segmentsCompacted,
      List<LogSegmentName> unaffectedSegments, long targetSegmentsExpectedValidSize,
      List<LogEntry> validLogEntriesInOrder) throws StoreException {
    long highestGeneration = 0;
    for (LogSegmentName segmentCompacted : segmentsCompacted) {
      highestGeneration = Math.max(highestGeneration, segmentCompacted.getGeneration());
    }
    highestGeneration++;
    long lowestPosition = segmentsCompacted.get(0).getPosition();
    long highestPosition = segmentsCompacted.get(segmentsCompacted.size() - 1).getPosition();
    long sizeOfTargetSegments = 0;
    Set<Long> positionsEncountered = new HashSet<>();
    Set<LogSegmentName> allSegmentNames = new HashSet<>();
    List<LogSegmentName> nonTargetSegmentNames = new ArrayList<>();
    List<LogSegmentName> targetSegmentNames = new ArrayList<>();
    LogSegment segment = state.log.getFirstSegment();
    while (segment != null) {
      LogSegmentName segmentName = segment.getName();
      allSegmentNames.add(segmentName);
      assertFalse("Segment " + segmentName + " should not exist", segmentsCompacted.contains(segmentName));
      long pos = segmentName.getPosition();
      assertFalse("Encountered two log segments with the same position", positionsEncountered.contains(pos));
      positionsEncountered.add(pos);
      long gen = segmentName.getGeneration();
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
   * @param compactedDeletes a set of delete tombstone ids that should be cleaned up in compacted segments.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @throws IOException
   * @throws StoreException
   */
  private void verifyDataPostCompaction(Set<MockId> idsInCompactedLogSegments, Set<MockId> compactedDeletes,
      long deleteReferenceTimeMs) throws IOException, StoreException {
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
          StoreErrorCodes expectedErrorCode = compactedDeletes.contains(id) ? StoreErrorCodes.IDNotFound
              : state.deletedKeys.contains(id) ? StoreErrorCodes.IDDeleted
                  : shouldBeCompacted ? StoreErrorCodes.IDNotFound : StoreErrorCodes.TTLExpired;
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
          StoreErrorCodes expectedErrorCode = compactedDeletes.contains(id) ? StoreErrorCodes.IDNotFound
              : state.deletedKeys.contains(id) ? StoreErrorCodes.IDDeleted : StoreErrorCodes.IDNotFound;
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
          StoreErrorCodes expectedErrorCode =
              compactedDeletes.contains(id) ? StoreErrorCodes.IDNotFound : StoreErrorCodes.IDDeleted;
          if (expectedErrorCode != e.getErrorCode()) {
            assertEquals(id + " failed with error code " + e.getErrorCode(), expectedErrorCode, e.getErrorCode());
          }
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
          StoreErrorCodes expectedErrorCode =
              compactedDeletes.contains(id) ? StoreErrorCodes.IDNotFound : StoreErrorCodes.IDDeleted;
          assertEquals(id + " failed with error code " + e.getErrorCode(), expectedErrorCode, e.getErrorCode());
        }
      }
    }
  }

  /**
   * Gets all the valid log entries in {@code logSegmentsUnderConsideration} in order of their occurrence in the log.
   * @param logSegmentsUnderConsideration the log segments whose log entries are required.
   * @param deleteReferenceTimeMs the reference time in ms to use to decide whether deletes are valid.
   * @param expiredDeletes a pair of two sets of deletes. The first one contains expired delete tombstones (with no
   *                      associated PUT); the second one includes expired deletes currently with PUTs ahead of them.
   * @param invalidateExpiredDelete whether to invalidate expired delete.
   * @return the valid log entries in {@code logSegmentsUnderConsideration} in order of their occurrence in the log.
   */
  private List<LogEntry> getValidLogEntriesInOrder(List<LogSegmentName> logSegmentsUnderConsideration,
      long deleteReferenceTimeMs, Pair<Set<MockId>, Set<MockId>> expiredDeletes, boolean invalidateExpiredDelete) {
    List<LogEntry> validLogEntriesInOrder = new ArrayList<>();
    for (LogSegmentName logSegment : logSegmentsUnderConsideration) {
      List<IndexEntry> validIndexEntries =
          state.getValidIndexEntriesForLogSegment(state.log.getSegment(logSegment), deleteReferenceTimeMs,
              state.time.milliseconds(), getFileSpanForLogSegments(logSegmentsUnderConsideration),
              new DeleteTombstoneStats.Builder(state.time), expiredDeletes, invalidateExpiredDelete);
      addToLogEntriesInOrder(validIndexEntries, validLogEntriesInOrder);
    }
    return validLogEntriesInOrder;
  }

  /**
   * Find the current blob ids (no matter what status it is) from given {@link PersistentIndex}.
   * @param index the {@link PersistentIndex} to search from.
   * @param removableDeletes a removable deletes set to populate (the delete tombstones will be added to it).
   * @param checkRemovableDeletes whether to check removable deletes. If {@code true}, the removableDeletes set should
   *                              be populated.
   * @return a set of current blob ids in given {@link PersistentIndex}
   */
  private Set<MockId> getCurrentBlobIdsFromWholeIndex(PersistentIndex index, Set<MockId> removableDeletes,
      boolean checkRemovableDeletes) {
    Set<MockId> seenPuts = new HashSet<>();
    Set<MockId> allBlobIds = new HashSet<>();
    for (IndexSegment indexSegment : index.getIndexSegments().values()) {
      Iterator<IndexEntry> iterator = indexSegment.iterator();
      while (iterator.hasNext()) {
        IndexEntry indexEntry = iterator.next();
        MockId key = (MockId) indexEntry.getKey();
        IndexValue indexValue = indexEntry.getValue();
        allBlobIds.add(key);
        if (indexValue.isDelete()) {
          if (!seenPuts.contains(key)) {
            // Here we don't check delete version based on the assumption that the solitary delete (no PUT associated)
            // should be final delete. Deletes with lower version (if any) should have been compacted already.
            if (removableDeletes != null && checkRemovableDeletes && indexValue.getExpiresAtMs() != Utils.Infinite_Time
                && indexValue.getExpiresAtMs() < state.time.milliseconds()) {
              removableDeletes.add(key);
            }
          }
        } else if (indexValue.isPut()) {
          seenPuts.add(key);
        }
      }
    }
    return allBlobIds;
  }

  /**
   * @param logSegments the log names of the log segments that should be under the returned {@link FileSpan}
   * @return a {@link FileSpan} that covers the given {@code logSegments}
   */
  private FileSpan getFileSpanForLogSegments(List<LogSegmentName> logSegments) {
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
   * @throws StoreException
   */
  private List<LogEntry> getLogEntriesInOrder(List<LogSegmentName> logSegmentsUnderConsideration)
      throws StoreException {
    List<LogEntry> logEntriesInOrder = new ArrayList<>();
    NavigableMap<Offset, IndexSegment> indexSegments = state.index.getIndexSegments();
    for (LogSegmentName logSegmentName : logSegmentsUnderConsideration) {
      LogSegment logSegment = state.log.getSegment(logSegmentName);
      Offset indexSegmentStartOffset = new Offset(logSegmentName, logSegment.getStartOffset());
      while (indexSegmentStartOffset != null && indexSegmentStartOffset.getName().equals(logSegmentName)) {
        IndexSegment indexSegment = indexSegments.get(indexSegmentStartOffset);
        List<MessageInfo> infos = new ArrayList<>();
        indexSegment.getEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), infos, new AtomicLong(0), false);
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
      logEntriesInOrder.add(new LogEntry(id, entry.getValue().getIndexValueType(), entry.getValue().getSize()));
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
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, false);
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
  private Pair<Long, List<LogSegmentName>> setupStateWithExpiredBlobsAtSpecificTime() throws Exception {
    refreshState(false, true, false);
    // we want at least 3 log segments that are outside the journal and with no invalid records
    long extraSegmentCountRequired = 4;
    // send +1 to account for half filled segments from curated log-index state.
    long expiryTimeMs = getInvalidationTime(extraSegmentCountRequired + 1);

    long currentLogSegmentCount = state.index.getLogSegmentCount();
    writeDataToMeetRequiredSegmentCount(currentLogSegmentCount + extraSegmentCountRequired,
        Collections.singletonList(expiryTimeMs));
    List<LogSegmentName> segmentsUnderCompaction =
        getLogSegments(currentLogSegmentCount, extraSegmentCountRequired - 1);
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
  private Pair<Long, List<LogSegmentName>> setupStateWithDeletedBlobsAtSpecificTime() throws Exception {
    refreshState(false, true, false);
    // we want at least 3 log segments that are outside the journal and with no invalid records
    long extraSegmentCountRequired = 4;
    // send +1 to account for half filled segments from curated log-index state.
    long deleteTimeMs = getInvalidationTime(extraSegmentCountRequired + 1);

    long currentLogSegmentCount = state.index.getLogSegmentCount();
    writeDataToMeetRequiredSegmentCount(currentLogSegmentCount + extraSegmentCountRequired, null);

    // advance time and delete some data (some of the data will get deleted at deleteTimeMs + delta).
    state.advanceTime(deleteTimeMs - state.time.milliseconds());
    List<LogSegmentName> segmentsUnderCompaction =
        getLogSegments(currentLogSegmentCount, extraSegmentCountRequired - 1);
    // we need enough data to fill two log segments after compaction
    // to allow for non alignment of log segment boundaries, reduce valid size required by the put record size
    long validSizeRequired = 2 * (state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE - PUT_RECORD_SIZE);
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
    long possiblePutRecords = numSegmentsToWritePutRecordsTo * state.log.getSegmentCapacity() / PUT_RECORD_SIZE + 1;
    long possibleIndexSegments = numSegmentsToWritePutRecordsTo + possiblePutRecords / state.getMaxInMemElements() + 1;
    long invalidationTimeMs = state.time.milliseconds() + possibleIndexSegments * DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS;
    // round up to the next second
    return (invalidationTimeMs / Time.MsPerSec + 1) * Time.MsPerSec;
  }

  /**
   * Gets the end offsets of all the {@code logSegmentNames}.
   * @param logSegmentNames the names of the log segments whose
   * @return a map of log segment names to end offsets.
   */
  private Map<LogSegmentName, Long> getEndOffsets(List<LogSegmentName> logSegmentNames) {
    Map<LogSegmentName, Long> oldSegmentNamesAndEndOffsets = new HashMap<>();
    for (LogSegmentName segmentUnderCompaction : logSegmentNames) {
      oldSegmentNamesAndEndOffsets.put(segmentUnderCompaction,
          state.log.getSegment(segmentUnderCompaction).getEndOffset());
    }
    return oldSegmentNamesAndEndOffsets;
  }

  /**
   * Verifies that the new log segments created by compacting the old log segments have the same end offsets.
   * @param oldSegmentNamesAndEndOffsets the names and end offsets of the old log segments.
   */
  private void verifyNoChangeInEndOffsets(Map<LogSegmentName, Long> oldSegmentNamesAndEndOffsets) {
    for (Map.Entry<LogSegmentName, Long> nameAndEndOffset : oldSegmentNamesAndEndOffsets.entrySet()) {
      // there should be a segment with the higher gen
      LogSegmentName oldSegmentName = nameAndEndOffset.getKey();
      assertNull("Old segment should not exist", state.log.getSegment(oldSegmentName));
      LogSegmentName newSegmentName = oldSegmentName.getNextGenerationName();
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
   * @param dropSegmentCallCountToInterruptAt the number calls to {@link Log#dropSegment(LogSegmentName, boolean)} allowed
   *                                          before interruption.
   * @throws Exception
   */
  private void doTestWithInterruptionInducingLog(int addSegmentCallCountToInterruptAt,
      int dropSegmentCallCountToInterruptAt) throws Exception {
    // no change before expiry time
    Pair<Long, List<LogSegmentName>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    List<LogSegmentName> segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
    Map<LogSegmentName, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
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
    Pair<Long, List<LogSegmentName>> expiryTimeAndSegmentsUnderCompaction = setupStateWithExpiredBlobsAtSpecificTime();
    List<LogSegmentName> segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
    Map<LogSegmentName, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
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
      Pair<Long, List<LogSegmentName>> expiryTimeAndSegmentsUnderCompaction =
          setupStateWithExpiredBlobsAtSpecificTime();
      List<LogSegmentName> segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
      Map<LogSegmentName, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
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
  private List<Offset> getIndexSegmentStartOffsetsForLogSegments(List<LogSegmentName> logSegmentNames) {
    List<Offset> offsets = new ArrayList<>();
    for (LogSegmentName logSegmentName : logSegmentNames) {
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
    for (long interruptAt : Arrays.asList(PUT_RECORD_SIZE, -2 * PUT_RECORD_SIZE)) {
      // no change before expiry time
      Pair<Long, List<LogSegmentName>> expiryTimeAndSegmentsUnderCompaction =
          setupStateWithExpiredBlobsAtSpecificTime();
      List<LogSegmentName> segmentsUnderCompaction = expiryTimeAndSegmentsUnderCompaction.getSecond();
      Map<LogSegmentName, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
      // if negative, set crash count starting from the end
      long countToInterruptAt = interruptAt;
      if (countToInterruptAt < 0) {
        // while copying each index segment, the bytes copied for the last record is not reported to the diskIOScheduler
        long unreported = getIndexSegmentStartOffsetsForLogSegments(segmentsUnderCompaction).size() * PUT_RECORD_SIZE;
        countToInterruptAt +=
            getValidDataSize(segmentsUnderCompaction, state.time.milliseconds(), purgeDeleteTombstone) - unreported;
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
        long unreported = getIndexSegmentStartOffsetsForLogSegments(segmentsUnderCompaction).size() * PUT_RECORD_SIZE;
        countToInterruptAt +=
            getValidDataSize(segmentsUnderCompaction, state.time.milliseconds(), purgeDeleteTombstone) - unreported;
      }
      diskIOScheduler = new InterruptionInducingDiskIOScheduler(Integer.MAX_VALUE, countToInterruptAt);
      compactWithRecoveryAndVerify(state.log, diskIOScheduler, state.index, segmentsUnderCompaction,
          state.time.milliseconds(), true, true);
    }
  }

  @Test
  public void testCloseLastLogSegmentIfQualified() throws Exception {
    //create first active log segment
    refreshState(false, false, true);
    //leave five log segment space for auto close last log segment purpose
    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity() - 5;
    writeDataToMeetRequiredSegmentCountForAutoCloseLogSegmentTest(requiredCount);

    //delete blobs in last index segment.
    ConcurrentSkipListMap<Offset, IndexSegment> AllIndexSegments = state.index.getIndexSegments();
    IndexSegment indexSegment = AllIndexSegments.floorEntry(state.log.getEndOffset()).getValue();
    ListIterator<IndexEntry> segmentListIter = indexSegment.listIterator(indexSegment.size());

    Set<Container> deleteInProgressSet = new HashSet<>();
    int cnt = 0;
    while (segmentListIter.hasNext()) {
      IndexEntry indexEntry = segmentListIter.next();
      MockId mockId = (MockId) indexEntry.getKey();
      Container container = new ContainerBuilder(mockId.getContainerId(), "containerName" + cnt++,
          Container.ContainerStatus.DELETE_IN_PROGRESS, null, mockId.getAccountId()).build();
      deleteInProgressSet.add(container);
    }

    Mockito.when(accountService.getContainersByStatus(Container.ContainerStatus.DELETE_IN_PROGRESS))
        .thenReturn(deleteInProgressSet);

    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    // Prepare the compactionPolicyInfo before compaction.
    CompactionPolicySwitchInfo compactionPolicySwitchInfo =
        new CompactionPolicySwitchInfo(System.currentTimeMillis(), true);
    backUpCompactionPolicyInfo(tempDir.toString(), compactionPolicySwitchInfo);
    //instantiate compactor.
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, true);
    compactor.initialize(state.index);

    compactAndVerifyForContainerDeletion(segmentsUnderCompaction, state.time.milliseconds(), false);
    //increment the counter
    compactionPolicySwitchInfo.setNextRoundIsCompactAllPolicy(false);
    // reload index to make sure journal is on only the latest log segment
    backUpCompactionPolicyInfo(tempDir.toString(), compactionPolicySwitchInfo);
    segmentsUnderCompaction = getLogSegments(0,
        state.log.getCapacityInBytes() / state.log.getSegmentCapacity() - state.log.getRemainingUnallocatedSegments()
            - 1);

    compactAndVerifyForContainerDeletion(segmentsUnderCompaction, state.time.milliseconds(), true);

    // Edge case test: if the last log segment is empty, no need to auto close it.
    int beforeAutoCloseLogSegmentsCnt = state.log.getLogSegmentCount();
    compactionPolicySwitchInfo = new CompactionPolicySwitchInfo(System.currentTimeMillis(), true);
    backUpCompactionPolicyInfo(tempDir.toString(), compactionPolicySwitchInfo);
    compactor = getCompactor(state.log, DISK_IO_SCHEDULER, null, true);
    compactor.initialize(state.index);
    if (state.log.autoCloseLastLogSegmentIfQualified()) {
      //refresh journal.
      state.index.journal.cleanUpJournal();
    }
    int afterAutoCloseLogSegmentsCnt = state.log.getLogSegmentCount();
    assertEquals("No segments should be created since last log segment is empty", beforeAutoCloseLogSegmentsCnt,
        afterAutoCloseLogSegmentsCnt);

    //make sure new data will be added into the last log segment
    long logSegmentCountBeforeNewDataAddIntoAutoClosedLogSegment = state.index.getLogSegmentCount();
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    long logSegmentCountAfterNewDataAddIntoAutoClosedLogSegment = state.index.getLogSegmentCount();
    assertEquals("Log Segment count should be increased after some data has been added to the last log segment.",
        logSegmentCountBeforeNewDataAddIntoAutoClosedLogSegment,
        logSegmentCountAfterNewDataAddIntoAutoClosedLogSegment - 1);
    assertEquals("Last index segment should belongs to auto closed log segment",
        state.index.getIndexSegments().lastEntry().getValue().getLogSegmentName(),
        state.log.getLastSegment().getName());

    //close the last log segment again.
    beforeAutoCloseLogSegmentsCnt = state.log.getLogSegmentCount();
    if (state.log.autoCloseLastLogSegmentIfQualified()) {
      //refresh journal.
      state.index.journal.cleanUpJournal();
    }
    afterAutoCloseLogSegmentsCnt = state.log.getLogSegmentCount();
    assertEquals("One log segment should be created since last log segment is not empty", beforeAutoCloseLogSegmentsCnt,
        afterAutoCloseLogSegmentsCnt - 1);

    //make sure new data will be added into the last log segment
    logSegmentCountBeforeNewDataAddIntoAutoClosedLogSegment = state.index.getLogSegmentCount();
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    logSegmentCountAfterNewDataAddIntoAutoClosedLogSegment = state.index.getLogSegmentCount();
    assertEquals("Log Segment count should be increased after some data has been added to the last log segment.",
        logSegmentCountBeforeNewDataAddIntoAutoClosedLogSegment,
        logSegmentCountAfterNewDataAddIntoAutoClosedLogSegment - 1);
    assertEquals("Last index segment should belongs to auto closed log segment",
        state.index.getIndexSegments().lastEntry().getValue().getLogSegmentName(),
        state.log.getLastSegment().getName());

    // check flow after deployment
    state.reloadLog(true);
  }

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
    refreshState(true, true, false);
    long requiredCount = state.log.getCapacityInBytes() / state.log.getSegmentCapacity() - 2;
    writeDataToMeetRequiredSegmentCount(requiredCount, null);
    // do some random deleting.
    int deleteCount =
        Math.min(state.liveKeys.size() / 3, (int) (1.8 * state.log.getSegmentCapacity() / DELETE_RECORD_SIZE));
    List<MockId> allLiveKeys = new ArrayList<>(state.liveKeys);
    for (int i = 0; i < deleteCount; i++) {
      MockId idToDelete = allLiveKeys.remove(TestUtils.RANDOM.nextInt(allLiveKeys.size()));
      state.addDeleteEntry(idToDelete);
    }
    // reload index to make sure journal is on only the latest log segment
    state.reloadIndex(true, false);
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
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
   * @return a pair of ids of newly added PUTs and expiry time associated with them.
   * @throws Exception
   */
  private Pair<List<MockId>, Long> createStateWithPutAndTtlUpdate() throws Exception {
    refreshState(false, false, false);
    int numPuts = (int) ((state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE) / (2 * PUT_RECORD_SIZE));
    long expiryTimeMs = getInvalidationTime(1);
    List<IndexEntry> entries = state.addPutEntries(numPuts, PUT_RECORD_SIZE, expiryTimeMs);
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
    refreshState(false, false, false);
    // this code is trying to determine how many put records there should be (close is good enough). It aims to have
    // just enough PUT records such that a quarter of the TTL updates for these PUT records fits in the first log
    // segment along with the PUTs themselves
    // numPuts * PUT_RECORD_SIZE + numPuts/4 * TTL_UPDATE_RECORD_SIZE + HEADER_SIZE < SEGMENT_CAPACITY
    // we have to solve for numPuts
    // numPuts < (SEGMENT_CAPACITY - HEADER_SIZE) / (PUT_RECORD_SIZE + TTL_UPDATE_RECORD_SIZE/4)
    // the -1 in the below statement is make numPuts < the output of the RHS
    int numPuts = (int) Math.floorDiv(state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE,
        PUT_RECORD_SIZE + (TTL_UPDATE_RECORD_SIZE) / 4) - 1;
    // this ensures that the crash test cases cause the crash in the middle of an index segment
    state.properties.setProperty("store.index.max.number.of.inmem.elements", Integer.toString(numPuts));
    // reload for new props to take effect
    state.reloadIndex(true, false);

    // insert numPuts PUT records with immediate expiry
    long expiryTimeMs = getInvalidationTime(1);
    List<IndexEntry> entries = state.addPutEntries(numPuts, PUT_RECORD_SIZE, expiryTimeMs);
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
    refreshState(false, false, false);
    int numEntries = (int) ((state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE) / (TTL_UPDATE_RECORD_SIZE
        + DELETE_RECORD_SIZE));
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
    // close/crash after the first log segment is switched out, this is mocked by closing compactor immediately after
    // dropping first log segment
    long expiryTimeMs = createStateWithPutTtlUpdateAndDelete();
    state.advanceTime(TimeUnit.SECONDS.toMillis(1));
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
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
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(1, 1);
    Map<LogSegmentName, Long> oldSegmentNamesAndEndOffsets = getEndOffsets(segmentsUnderCompaction);
    DiskIOScheduler diskIOScheduler;
    if (throwExceptionInsteadOfClose) {
      diskIOScheduler = new InterruptionInducingDiskIOScheduler(2, Integer.MAX_VALUE);
    } else {
      diskIOScheduler = new InterruptionInducingDiskIOScheduler(Integer.MAX_VALUE, 2 * TTL_UPDATE_RECORD_SIZE);
    }
    compactWithRecoveryAndVerify(state.log, diskIOScheduler, state.index, segmentsUnderCompaction,
        state.time.milliseconds(), false, true);
    verifyNoChangeInEndOffsets(oldSegmentNamesAndEndOffsets);
  }

  /**
   * Tests the case where close/crash creates a situation where duplicates, including undeletes, will be detected during
   * the search for copy candidates in the srcIndex
   * @throws Exception
   */
  private void doUndeleteSrcDupTest() throws Exception {
    List<MockId> ids = createLogSegmentWithDuplicateOfUndelete();
    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(0, state.index.getLogSegmentCount() - 1);
    Log log = new InterruptionInducingLog(Integer.MAX_VALUE, 1);
    compactWithRecoveryAndVerify(log, DISK_IO_SCHEDULER, state.index, segmentsUnderCompaction,
        state.time.milliseconds(), true, true);

    MockId pu = ids.get(0);
    MockId pd = ids.get(1);
    MockId pt = ids.get(2);
    List<IndexValue> values = state.index.findAllIndexValuesForKey(pu, null);
    assertNotNull(values);
    assertEquals(3, values.size());
    assertTrue(values.get(0).isUndelete());
    assertEquals(1, values.get(0).getLifeVersion());
    assertTrue(values.get(1).isTtlUpdate());
    assertTrue(values.get(2).isPut());
    values = state.index.findAllIndexValuesForKey(pd, null);
    assertNotNull(values);
    assertEquals(1, values.size());
    assertTrue(values.get(0).isDelete());
    values = state.index.findAllIndexValuesForKey(pt, null);
    assertNotNull(values);
    assertEquals(2, values.size());
    assertTrue(values.get(0).isTtlUpdate());
    assertTrue(values.get(1).isPut());
  }

  /**
   * Tests the case where close/crash creates a situation where duplicates, includeing undeletes, will be detected during
   * the search for copy candidates in the tgtIndex
   * @throws Exception
   */
  private void doUndeleteTgtDupTest() throws Exception {
    List<MockId> ids = createLogSegmentWithDuplicateOfUndelete();
    MockId pu = ids.get(0);
    MockId pd = ids.get(1);
    MockId pt = ids.get(2);

    List<LogSegmentName> segmentsUnderCompaction = getLogSegments(1, 1);
    DiskIOScheduler diskIOScheduler;
    if (throwExceptionInsteadOfClose) {
      // pu + pd + pt should span two segment, give it another one for margin.
      diskIOScheduler = new InterruptionInducingDiskIOScheduler(3, Integer.MAX_VALUE);
    } else {
      // For pu, it's 1 PUT + 1 TTL_UPDATE + 1 UNDELETE
      // For pd, it's 1 DELETE
      // For pt, it's 1 PUT + 1 TTL_UPDATE
      long totalBytes = 2 * PUT_RECORD_SIZE + 2 * TTL_UPDATE_RECORD_SIZE + UNDELETE_RECORD_SIZE + DELETE_RECORD_SIZE;
      totalBytes += 2 * PUT_RECORD_SIZE; // Give another put for margin.
      diskIOScheduler = new InterruptionInducingDiskIOScheduler(Integer.MAX_VALUE, totalBytes);
    }
    Log log = new InterruptionInducingLog(Integer.MAX_VALUE, 1);
    compactWithRecoveryAndVerify(state.log, diskIOScheduler, state.index, segmentsUnderCompaction,
        state.time.milliseconds(), true, true);

    List<IndexValue> values = state.index.findAllIndexValuesForKey(pu, null);
    assertNotNull(values);
    assertEquals(3, values.size());
    assertTrue(values.get(0).isUndelete());
    assertEquals(1, values.get(0).getLifeVersion());
    assertTrue(values.get(1).isTtlUpdate());
    assertTrue(values.get(2).isPut());
    values = state.index.findAllIndexValuesForKey(pd, null);
    assertNotNull(values);
    assertEquals(1, values.size());
    assertTrue(values.get(0).isDelete());
    values = state.index.findAllIndexValuesForKey(pt, null);
    assertNotNull(values);
    assertEquals(2, values.size());
    assertTrue(values.get(0).isTtlUpdate());
    assertTrue(values.get(1).isPut());
  }

  private List<IndexEntry> createDeleteWithOlderVersion() throws Exception {
    refreshState(false, false, false);
    int numPuts = (int) ((state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE) / PUT_RECORD_SIZE + 1);
    List<IndexEntry> entries = state.addPutEntries(numPuts, PUT_RECORD_SIZE, Utils.Infinite_Time);
    assertEquals(2, state.index.getLogSegmentCount());
    for (IndexEntry entry : entries) {
      state.addDeleteEntry((MockId) entry.getKey());
      state.addUndeleteEntry((MockId) entry.getKey());
    }
    // Now delete it again
    for (IndexEntry entry : entries) {
      state.addDeleteEntry((MockId) entry.getKey());
    }
    // Here we have four records for earch id, P0, D0, U1, D1. Compact them with 0 being delete reference time, it should
    // be compacted to P0, D1.
    writeDataToMeetRequiredSegmentCount(state.index.getLogSegmentCount() + 1, null);
    return entries;
  }

  private List<MockId> createLogSegmentWithDuplicateOfUndelete() throws Exception {
    refreshState(false, false, false);
    int numPuts = (int) ((state.log.getSegmentCapacity() - LogSegment.HEADER_SIZE) / (PUT_RECORD_SIZE));
    List<IndexEntry> entries = state.addPutEntries(numPuts, PUT_RECORD_SIZE, Utils.Infinite_Time);
    // We will construct log such as
    // First log segment will be all PUTs, and some of them will be compacted by adding DELETEs in the end
    // Second log segment will contain certain records, such that the records will be compacted and fitted in a single
    // compacted segment with first log segment. And certain records will be inserted to make sure it meets some corner
    // cases, which wil be commented below.
    assertEquals(1, state.index.getLogSegmentCount());

    int numRecordToCompact = 0;
    MockId pu = (MockId) state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0).getKey();
    state.makePermanent(pu, false);
    state.addDeleteEntry(pu);
    state.addUndeleteEntry(pu);
    numRecordToCompact += 3;
    // LifeVersion in source index > candidate.
    // Here we have a P0, T0, D0 and U1. These records will be compacted to P0, T1, U1. When it recovers, the U1 will be in
    // source index, and P0, T0, U1 will be tested alreadyExists against U1. (D0 is not valid).

    MockId pd = (MockId) state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0).getKey();
    state.makePermanent(pd, false);
    state.addDeleteEntry(pd);
    numRecordToCompact += 1;
    // LifeVersion in source index = candidate. Delete is final
    // Here we have a P0, T0 and D0. These records will be compacted to D0. When it recovers, the D0 will be in the source
    // index and P0 and T0 will be tested alreadyExists against D0.

    MockId pt = (MockId) state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0).getKey();
    state.makePermanent(pt, false);
    numRecordToCompact += 2;
    // LifeVersion in source index = candidate. TtlUpdate is final
    // Here we have a P0, T0. These records will be both copied over. When it recovers, the T0 will be in the source
    // index and P0 and T0 will be tested alreadyExists against T0.

    // LifeVersion in source index < candidate is too common that it's already been tested in other test functions.
    writeDataToMeetRequiredSegmentCount(2, null);

    assertTrue(numRecordToCompact < numPuts);
    for (int i = 0; i < numRecordToCompact; i++) {
      state.addDeleteEntry((MockId) entries.get(i).getKey());
    }
    writeDataToMeetRequiredSegmentCount(4, null);
    return Arrays.asList(pu, pd, pt);
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
          new StoreConfig(new VerifiableProperties(state.properties)), STORE_KEY_FACTORY, state.recovery,
          state.hardDelete, DISK_IO_SCHEDULER, new StoreMetrics(new MetricRegistry()), state.time, state.sessionId,
          state.incarnationId);
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
   * {@link #addSegment(LogSegment, boolean)} or {@link #dropSegment(LogSegmentName, boolean)} have been made.
   */
  private class InterruptionInducingLog extends Log {
    private final int addSegmentCallCountToInterruptAt;
    private final int dropSegmentCallCountToInterruptAt;

    private int segmentsAdded = 0;
    private int segmentsDropped = 0;

    /**
     * Creates an instance of InterruptionInducingLog.
     * @param addSegmentCallCountToInterruptAt number of allowed calls to {@link #addSegment(LogSegment, boolean)}.
     * @param dropSegmentCallCountToInterruptAt number of allowed calls to {@link #dropSegment(LogSegmentName, boolean)}.
     * @throws StoreException
     */
    InterruptionInducingLog(int addSegmentCallCountToInterruptAt, int dropSegmentCallCountToInterruptAt)
        throws StoreException {
      super(tempDirStr, state.log.getCapacityInBytes(), StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
          createStoreConfig(state.log.getSegmentCapacity(), true), new StoreMetrics(new MetricRegistry()), null);
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
    void dropSegment(LogSegmentName segmentName, boolean decreaseUsedSegmentCount) throws StoreException {
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

    public MockId getId() {
      return id;
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
