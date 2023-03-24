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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.store.StoreFindToken.*;
import static org.junit.Assert.*;


/**
 * Tests for {@link CompactionLog}.
 */
public class CompactionLogTest {
  private static final StoreKeyFactory STORE_KEY_FACTORY;

  static {
    try {
      STORE_KEY_FACTORY = new MockIdFactory();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  // Variables that represent the folder where the data resides
  private final File tempDir;
  private final String tempDirStr;
  private final StoreConfig config;
  // the time instance that will be used in the index
  private final MockTime time = new MockTime();
  private final Set<LogSegmentName> generatedSegmentNames = new HashSet<>();

  /**
   * Creates a temporary directory for the compaction log file.
   * @throws IOException
   */
  public CompactionLogTest() throws IOException {
    tempDir = StoreTestUtils.createTempDirectory("storeDir-" + TestUtils.getRandomString(10));
    tempDirStr = tempDir.getAbsolutePath();
    config = new StoreConfig(new VerifiableProperties(new Properties()));
  }

  /**
   * Deletes the temporary directory.
   * @throws IOException
   */
  @After
  public void cleanup() throws IOException {
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Tests the use of {@link CompactionLog} when it is used without closing b/w operations and compaction cycles.
   * @throws IOException
   */
  @Test
  public void iterationWithoutReloadTest() throws IOException {
    String storeName = "store";
    List<CompactionDetails> detailsList = getCompactionDetailsList(5);
    CompactionDetails combined = combineListOfDetails(detailsList);
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, combined, config);
    assertTrue("Compaction should be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    int currentIdx = 0;
    Iterator<CompactionDetails> detailsIterator = detailsList.iterator();
    CompactionDetails currDetails = detailsIterator.next();
    while (currDetails != null) {
      detailsIterator.remove();
      assertEquals("CurrentIdx not as expected", currentIdx, cLog.getCurrentIdx());
      verifyEquality(combined, cLog.getCompactionDetails());
      assertEquals("Should be in the PREPARE phase", CompactionLog.Phase.PREPARE, cLog.getCompactionPhase());
      cLog.markCopyStart();
      assertEquals("Should be in the COPY phase", CompactionLog.Phase.COPY, cLog.getCompactionPhase());
      Offset offset = new Offset(LogSegmentName.generateFirstSegmentName(true),
          Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE));
      cLog.setStartOffsetOfLastIndexSegmentForDeleteCheck(offset);
      assertEquals("Offset that was set was not the one returned", offset,
          cLog.getStartOffsetOfLastIndexSegmentForDeleteCheck());
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentName.generateFirstSegmentName(true), 0),
              new UUID(1, 1), new UUID(1, 1), null, null, UNINITIALIZED_RESET_KEY_VERSION);
      cLog.setSafeToken(safeToken);
      assertEquals("Returned token not the same as the one that was set", safeToken, cLog.getSafeToken());
      assertTrue(cLog.isIndexSegmentOffsetsPersisted());
      int sizeBefore = cLog.getBeforeAndAfterIndexSegmentOffsets().size();
      Pair<Offset, Offset> pair = addOneIndexSegmentOffsetPair(cLog);
      cLog.addBeforeAndAfterIndexSegmentOffsetPair(pair.getFirst(), pair.getSecond());
      assertEquals(sizeBefore + 1, cLog.getBeforeAndAfterIndexSegmentOffsets().size());
      try {
        LogSegmentName logSegmentName = StoreTestUtils.getRandomLogSegmentName(generatedSegmentNames);
        Offset before = new Offset(logSegmentName, LogSegment.HEADER_SIZE);
        Offset after = new Offset(logSegmentName.getNextGenerationName(), LogSegment.HEADER_SIZE);
        cLog.addBeforeAndAfterIndexSegmentOffsetPair(before, after);
        fail("Offset " + before + " doesn't belong to under compaction log segment, should fail");
      } catch (Exception e) {
        // Expect to see exception
      }
      CompactionDetails nextDetails = detailsIterator.hasNext() ? detailsIterator.next() : null;
      if (nextDetails != null) {
        cLog.splitCurrentCycle(nextDetails.getLogSegmentsUnderCompaction().get(0));
        verifyEquality(currDetails, cLog.getCompactionDetails());
      }
      cLog.markCommitStart();
      assertEquals("Should be in the SWITCH phase", CompactionLog.Phase.COMMIT, cLog.getCompactionPhase());
      cLog.markCleanupStart();
      assertEquals("Should be in the CLEANUP phase", CompactionLog.Phase.CLEANUP, cLog.getCompactionPhase());
      cLog.markCycleComplete();
      currentIdx++;
      currDetails = nextDetails;
      if (nextDetails != null) {
        combined = combineListOfDetails(detailsList);
      }
    }
    assertEquals("CurrentIdx not as expected", -1, cLog.getCurrentIdx());
    assertEquals("Should be in the DONE phase", CompactionLog.Phase.DONE, cLog.getCompactionPhase());
    cLog.close();
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
  }

  /**
   * Tests the use of {@link CompactionLog} when it is closed and reloaded b/w operations and compaction cycles.
   * @throws IOException
   */
  @Test
  public void iterationWithReloadTest() throws IOException {
    String storeName = "store";
    List<CompactionDetails> detailsList = getCompactionDetailsList(5);
    CompactionDetails combined = combineListOfDetails(detailsList);
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, combined, config);
    assertTrue("Compaction should should be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    int currentIdx = 0;
    Iterator<CompactionDetails> detailsIterator = detailsList.iterator();
    CompactionDetails currDetails = detailsIterator.next();
    while (currDetails != null) {
      detailsIterator.remove();
      assertEquals("CurrentIdx not as expected", currentIdx, cLog.getCurrentIdx());

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      verifyEquality(combined, cLog.getCompactionDetails());
      assertEquals("Should be in the PREPARE phase", CompactionLog.Phase.PREPARE, cLog.getCompactionPhase());
      cLog.markCopyStart();

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      assertEquals("Should be in the COPY phase", CompactionLog.Phase.COPY, cLog.getCompactionPhase());
      Offset offset = new Offset(LogSegmentName.generateFirstSegmentName(true),
          Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE));
      cLog.setStartOffsetOfLastIndexSegmentForDeleteCheck(offset);

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      assertEquals("Offset that was set was not the one returned", offset,
          cLog.getStartOffsetOfLastIndexSegmentForDeleteCheck());
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentName.generateFirstSegmentName(true), 0),
              new UUID(1, 1), new UUID(1, 1), null, null, UNINITIALIZED_RESET_KEY_VERSION);
      cLog.setSafeToken(safeToken);

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      assertEquals("Returned token not the same as the one that was set", safeToken, cLog.getSafeToken());
      assertTrue(cLog.isIndexSegmentOffsetsPersisted());
      int sizeBefore = cLog.getBeforeAndAfterIndexSegmentOffsets().size();
      Pair<Offset, Offset> pair = addOneIndexSegmentOffsetPair(cLog);
      assertEquals(sizeBefore + 1, cLog.getBeforeAndAfterIndexSegmentOffsets().size());
      cLog.addBeforeAndAfterIndexSegmentOffsetPair(pair.getFirst(), pair.getSecond());
      assertEquals(sizeBefore + 1, cLog.getBeforeAndAfterIndexSegmentOffsets().size());
      try {
        LogSegmentName logSegmentName = StoreTestUtils.getRandomLogSegmentName(generatedSegmentNames);
        Offset before = new Offset(logSegmentName, LogSegment.HEADER_SIZE);
        Offset after = new Offset(logSegmentName.getNextGenerationName(), LogSegment.HEADER_SIZE);
        cLog.addBeforeAndAfterIndexSegmentOffsetPair(before, after);
        fail("Offset " + before + " doesn't belong to under compaction log seegment, should fail");
      } catch (Exception e) {
        // Expect to see exception
      }

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      assertTrue(cLog.isIndexSegmentOffsetsPersisted());
      assertEquals(sizeBefore + 1, cLog.getBeforeAndAfterIndexSegmentOffsets().size());
      CompactionDetails nextDetails = detailsIterator.hasNext() ? detailsIterator.next() : null;
      if (nextDetails != null) {
        cLog.splitCurrentCycle(nextDetails.getLogSegmentsUnderCompaction().get(0));
        verifyEquality(currDetails, cLog.getCompactionDetails());
        cLog.close();
        cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
        verifyEquality(currDetails, cLog.getCompactionDetails());
      }
      cLog.markCommitStart();

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      assertEquals("Should be in the SWITCH phase", CompactionLog.Phase.COMMIT, cLog.getCompactionPhase());
      cLog.markCleanupStart();

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      assertEquals("Should be in the CLEANUP phase", CompactionLog.Phase.CLEANUP, cLog.getCompactionPhase());
      cLog.markCycleComplete();

      currentIdx++;
      currDetails = nextDetails;
      if (nextDetails != null) {
        combined = combineListOfDetails(detailsList);
      }
    }
    assertEquals("CurrentIdx not as expected", -1, cLog.getCurrentIdx());
    assertEquals("Should be in the DONE phase", CompactionLog.Phase.DONE, cLog.getCompactionPhase());
    cLog.close();
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
  }

  /**
   * Tests phase transition order is enforced during phase transitions
   */
  @Test
  public void phaseTransitionEnforcementTest() throws IOException {
    String storeName = "store";
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, getCompactionDetails(), config);

    assertEquals("Should be in the PREPARE phase", CompactionLog.Phase.PREPARE, cLog.getCompactionPhase());
    checkTransitionFailure(cLog, CompactionLog.Phase.COMMIT, CompactionLog.Phase.CLEANUP, CompactionLog.Phase.DONE);

    cLog.markCopyStart();
    assertEquals("Should be in the COPY phase", CompactionLog.Phase.COPY, cLog.getCompactionPhase());
    checkTransitionFailure(cLog, CompactionLog.Phase.COPY, CompactionLog.Phase.CLEANUP, CompactionLog.Phase.DONE);

    cLog.markCommitStart();
    assertEquals("Should be in the SWITCH phase", CompactionLog.Phase.COMMIT, cLog.getCompactionPhase());
    checkTransitionFailure(cLog, CompactionLog.Phase.COPY, CompactionLog.Phase.COMMIT, CompactionLog.Phase.DONE);

    cLog.markCleanupStart();
    assertEquals("Should be in the CLEANUP phase", CompactionLog.Phase.CLEANUP, cLog.getCompactionPhase());
    checkTransitionFailure(cLog, CompactionLog.Phase.COPY, CompactionLog.Phase.COMMIT, CompactionLog.Phase.CLEANUP);

    cLog.markCycleComplete();
    assertEquals("Should be in the DONE phase", CompactionLog.Phase.DONE, cLog.getCompactionPhase());
    checkTransitionFailure(cLog, CompactionLog.Phase.COPY, CompactionLog.Phase.COMMIT, CompactionLog.Phase.CLEANUP,
        CompactionLog.Phase.DONE);
    cLog.close();
  }

  /**
   * Tests for construction of {@link CompactionLog} with bad arguments/state.
   * @throws IOException
   */
  @Test
  public void constructionBadArgsTest() throws IOException {
    String storeName = "store";
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, getCompactionDetails(), config);
    cLog.close();
    // log file already exists
    try {
      new CompactionLog(tempDirStr, storeName, time, getCompactionDetails(), config);
      fail("Construction should have failed because compaction log file already exists");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // make sure file disappears
    cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
    cLog.markCopyStart();
    cLog.markCommitStart();
    cLog.markCleanupStart();
    cLog.markCycleComplete();
    cLog.close();
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));

    // log file does not exist
    try {
      new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      fail("Construction should have failed because compaction log file does not exist");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests the reading of version 0
   * @throws IOException
   */
  @Test
  public void versionZeroReadTest() throws IOException {
    String storeName = "store";
    long referenceTimeMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    CompactionDetails details = getCompactionDetails(referenceTimeMs);
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, CompactionLog.VERSION_0, time, details, config);
    cLog.close();
    cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
    verifyEquality(details, cLog.getCompactionDetails());
    assertEquals("Current Idx not as expected", 0, cLog.getCurrentIdx());
  }

  /**
   * Tests the reading of version 1
   * @throws IOException
   */
  @Test
  public void versionOneReadTest() throws IOException {
    String storeName = "store";
    long referenceTimeMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    CompactionDetails details = getCompactionDetails(referenceTimeMs);
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, CompactionLog.VERSION_1, time, details, config);
    LogSegmentName logSegmentName = details.getLogSegmentsUnderCompaction().get(0);
    Offset before = new Offset(logSegmentName, LogSegment.HEADER_SIZE);
    Offset after = new Offset(logSegmentName.getNextGenerationName(), LogSegment.HEADER_SIZE);
    cLog.addBeforeAndAfterIndexSegmentOffsetPair(before, after);
    Assert.assertFalse(cLog.isIndexSegmentOffsetsPersisted());
    Assert.assertEquals(0, cLog.getBeforeAndAfterIndexSegmentOffsets().size());
    cLog.close();
    // Adding index segment offset pair shouldn't change any of the version 1 compaction log
    cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
    Assert.assertFalse(cLog.isIndexSegmentOffsetsPersisted());
    Assert.assertEquals(0, cLog.getBeforeAndAfterIndexSegmentOffsets().size());
    verifyEquality(details, cLog.getCompactionDetails());
    assertEquals("Current Idx not as expected", 0, cLog.getCurrentIdx());
  }

  /**
   * Testing the methods to get the before and after index segment maps
   * @throws IOException
   */
  @Test
  public void testIndexSegmentOffsetsMethods() throws IOException {
    String storeName = "store";
    CompactionDetails details = getCompactionDetails(0);
    List<LogSegmentName> segmentsUnderCompaction = details.getLogSegmentsUnderCompaction();
    segmentsUnderCompaction.sort(LogSegmentName::compareTo);
    details = new CompactionDetails(0, segmentsUnderCompaction, null);

    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, details, config);
    for (LogSegmentName name : segmentsUnderCompaction) {
      addOneIndexSegmentOffsetPair(name, cLog);
    }
    NavigableMap<Offset, Offset> indexSegmentOffsets = cLog.getBeforeAndAfterIndexSegmentOffsets();
    for (LogSegmentName name : segmentsUnderCompaction) {
      Offset before = new Offset(name, LogSegment.HEADER_SIZE);
      Offset expectedAfter = new Offset(name.getNextGenerationName(), LogSegment.HEADER_SIZE);
      Assert.assertEquals(expectedAfter, indexSegmentOffsets.get(before));
    }
    SortedMap<Offset, Offset> indexSegmentOffsetsCompleted =
        cLog.getBeforeAndAfterIndexSegmentOffsetsForCompletedCycles();
    // There is no completedCycle yet
    Assert.assertEquals(0, indexSegmentOffsetsCompleted.size());
    // All log segments are under compaction at current cycle.
    SortedMap<Offset, Offset> indexSegmentOffsetsCurrent = cLog.getBeforeAndAfterIndexSegmentOffsetsForCurrentCycle();
    Assert.assertEquals(segmentsUnderCompaction.size(), indexSegmentOffsetsCurrent.size());

    for (int i = 0; i < segmentsUnderCompaction.size(); i++) {
      LogSegmentName name = segmentsUnderCompaction.get(i);
      cLog.markCopyStart();
      if (i != segmentsUnderCompaction.size() - 1) {
        cLog.splitCurrentCycle(segmentsUnderCompaction.get(i + 1));
      }
      cLog.markCommitStart();
      cLog.markCleanupStart();

      indexSegmentOffsetsCurrent = cLog.getBeforeAndAfterIndexSegmentOffsetsForCurrentCycle();
      Assert.assertEquals(1, indexSegmentOffsetsCurrent.size());
      Assert.assertEquals(new Offset(name, LogSegment.HEADER_SIZE),
          indexSegmentOffsetsCurrent.keySet().iterator().next());

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);

      // Before completing this cycle, get the
      indexSegmentOffsetsCompleted = cLog.getBeforeAndAfterIndexSegmentOffsetsForCompletedCycles();
      Assert.assertEquals(i, indexSegmentOffsetsCompleted.size());
      for (int idx = 0; idx < i; idx++) {
        LogSegmentName sname = segmentsUnderCompaction.get(idx);
        Assert.assertTrue(indexSegmentOffsetsCompleted.containsKey(new Offset(sname, LogSegment.HEADER_SIZE)));
      }
      cLog.markCycleComplete();
      indexSegmentOffsetsCompleted = cLog.getBeforeAndAfterIndexSegmentOffsetsForCompletedCycles();
      Assert.assertEquals(i + 1, indexSegmentOffsetsCompleted.size());
      for (int idx = 0; idx < i + 1; idx++) {
        LogSegmentName sname = segmentsUnderCompaction.get(idx);
        Assert.assertTrue(indexSegmentOffsetsCompleted.containsKey(new Offset(sname, LogSegment.HEADER_SIZE)));
      }

      cLog.close();
      if (i != segmentsUnderCompaction.size() - 1) {
        cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      } else {
        AtomicReference<CompactionLog> compactionLogRef = new AtomicReference<>();
        CompactionLog.processCompactionLogs(tempDirStr, storeName, STORE_KEY_FACTORY, time, config, compactionLog -> {
          compactionLogRef.set(compactionLog);
          return false;
        });
        cLog = compactionLogRef.get();
      }
      indexSegmentOffsetsCompleted = cLog.getBeforeAndAfterIndexSegmentOffsetsForCompletedCycles();
      Assert.assertEquals(i + 1, indexSegmentOffsetsCompleted.size());
      for (int idx = 0; idx < i + 1; idx++) {
        LogSegmentName sname = segmentsUnderCompaction.get(idx);
        Assert.assertTrue(indexSegmentOffsetsCompleted.containsKey(new Offset(sname, LogSegment.HEADER_SIZE)));
      }
    }
  }

  /**
   * Test {@link CompactionLog#processCompactionLogs}.
   * @throws Exception
   */
  @Test
  public void testProcessCompactionLogs() throws Exception {
    // First create some compaction logs
    time.setCurrentMilliseconds(System.currentTimeMillis());
    String storeName = "store";
    List<CompactionDetails> detailsList = getCompactionDetailsList(10);
    List<Long> expectedStartTimes = new ArrayList<>();
    Map<Offset, Offset> expectedIndexSegmentOffsets = new HashMap<>();
    for (CompactionDetails details : detailsList) {
      CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, details, config);
      Pair<Offset, Offset> pair = addOneIndexSegmentOffsetPair(cLog);
      expectedIndexSegmentOffsets.put(pair.getFirst(), pair.getSecond());
      expectedStartTimes.add(0, time.milliseconds());
      cLog.markCopyStart();
      cLog.markCommitStart();
      cLog.markCleanupStart();
      cLog.markCycleComplete();
      cLog.close();
      Assert.assertFalse(CompactionLog.isCompactionInProgress(tempDirStr, storeName));
      long sleepTime = 3 * Integer.MAX_VALUE;
      time.sleep(sleepTime);
    }

    CompactionDetails details = getCompactionDetailsList(1).get(0);
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, details, config);
    Pair<Offset, Offset> pair = addOneIndexSegmentOffsetPair(cLog);
    expectedIndexSegmentOffsets.put(pair.getFirst(), pair.getSecond());
    expectedStartTimes.add(0, time.milliseconds());
    cLog.close();
    Assert.assertTrue(CompactionLog.isCompactionInProgress(tempDirStr, storeName));

    // Now we have 10 completed compaction logs and one in progress compaction log
    List<Long> obtainedStartTimes = new ArrayList<>();
    Map<Offset, Offset> obtainedIndexSegmentOffsets = new HashMap<>();
    CompactionLog.processCompactionLogs(tempDirStr, storeName, STORE_KEY_FACTORY, time, config, log -> {
      obtainedStartTimes.add(log.getStartTime());
      obtainedIndexSegmentOffsets.putAll(log.getBeforeAndAfterIndexSegmentOffsets());
      return true;
    });

    Assert.assertEquals(11, obtainedStartTimes.size());
    Assert.assertEquals(11, obtainedIndexSegmentOffsets.size());

    Assert.assertEquals(expectedIndexSegmentOffsets, obtainedIndexSegmentOffsets);
    Assert.assertEquals(expectedStartTimes, obtainedStartTimes);
  }

  @Test
  public void testCleanupCompactionLogFiles() throws Exception {
    // First create some compaction logs
    time.setCurrentMilliseconds(System.currentTimeMillis());
    long latestTs = 0;
    long timeGap = 10 * 1000;
    List<Long> startTimes = new ArrayList<>();
    String storeName = "store";
    List<CompactionDetails> detailsList = getCompactionDetailsList(10);
    for (CompactionDetails details : detailsList) {
      CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, details, config);
      Pair<Offset, Offset> pair = addOneIndexSegmentOffsetPair(cLog);
      cLog.markCopyStart();
      cLog.markCommitStart();
      cLog.markCleanupStart();
      cLog.markCycleComplete();
      cLog.close();
      latestTs = cLog.getStartTime();
      startTimes.add(0, latestTs);
      Assert.assertFalse(CompactionLog.isCompactionInProgress(tempDirStr, storeName));
      time.sleep(timeGap);
    }

    // Add in progress compaction log
    CompactionDetails details = getCompactionDetailsList(1).get(0);
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, details, config);
    startTimes.add(0, cLog.getStartTime());
    cLog.close();
    Assert.assertTrue(CompactionLog.isCompactionInProgress(tempDirStr, storeName));

    long cutoffTime = latestTs - 1 - 3 * timeGap;
    CompactionLog.cleanupCompactionLogs(tempDirStr, storeName, cutoffTime);
    // The cutoff time would cover latest completed compaction logs and another 3 completed compaction logs and in progress
    // compaction logs, so we should have 5 compaction logs
    startTimes = startTimes.subList(0, 5);

    AtomicInteger counter = new AtomicInteger();
    List<Long> obtainedStartTimes = new ArrayList<>();
    CompactionLog.processCompactionLogs(tempDirStr, storeName, STORE_KEY_FACTORY, time, config, log -> {
      counter.addAndGet(1);
      obtainedStartTimes.add(log.getStartTime());
      return true;
    });
    Assert.assertEquals(5, obtainedStartTimes.size());
    Assert.assertEquals(startTimes, obtainedStartTimes);
  }

  // helpers
  // general

  /**
   * @return a {@link CompactionDetails} with random data.
   */
  private CompactionDetails getCompactionDetails() {
    long referenceTime = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    return getCompactionDetails(referenceTime);
  }

  /**
   * A {@link CompactionDetails} with random data.
   * @param referenceTime the reference time to use to generate the {@link CompactionDetails}
   * @return {@link CompactionDetails} with random data.
   */
  private CompactionDetails getCompactionDetails(long referenceTime) {
    int segmentCount = TestUtils.RANDOM.nextInt(10) + 1;
    List<LogSegmentName> segmentsUnderCompaction = new ArrayList<>();
    for (int j = 0; j < segmentCount; j++) {
      LogSegmentName segmentName = StoreTestUtils.getRandomLogSegmentName(generatedSegmentNames);
      generatedSegmentNames.add(segmentName);
      segmentsUnderCompaction.add(segmentName);
    }
    return new CompactionDetails(referenceTime, segmentsUnderCompaction, null);
  }

  /**
   * Verifies that two {@link CompactionDetails} instances are equal.
   * @param original the expected {@link CompactionDetails}.
   * @param toCheck the {@link CompactionDetails} that needs to be checked.
   */
  private void verifyEquality(CompactionDetails original, CompactionDetails toCheck) {
    assertEquals("Reference time does not match", original.getReferenceTimeMs(), toCheck.getReferenceTimeMs());
    assertEquals("Segments under compaction don't match", original.getLogSegmentsUnderCompaction(),
        toCheck.getLogSegmentsUnderCompaction());
  }

  // iteration tests helpers

  /**
   * Gets a {@link List<CompactionDetails>} of size {@code size} with random {@link CompactionDetails}.
   * @param size the number of {@link CompactionDetails} required.
   * @return a {@link List<CompactionDetails>} of size {@code size} with random {@link CompactionDetails}.
   */
  private List<CompactionDetails> getCompactionDetailsList(int size) {
    List<CompactionDetails> detailsList = new ArrayList<>();
    long referenceTime = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    for (int i = 0; i < size; i++) {
      CompactionDetails details = getCompactionDetails(referenceTime);
      detailsList.add(details);
    }
    return detailsList;
  }

  /**
   * Combines a {@link List<CompactionDetails>} into one {@link CompactionDetails} instance. Uses the reference time
   * of the first entry in the list.
   * @param detailsList the {@link List<CompactionDetails>} to combine.
   * @return combines {@code detailsList} into a single {@link CompactionDetails}.
   */
  private CompactionDetails combineListOfDetails(List<CompactionDetails> detailsList) {
    List<LogSegmentName> allSegmentNames = new ArrayList<>();
    for (CompactionDetails details : detailsList) {
      allSegmentNames.addAll(details.getLogSegmentsUnderCompaction());
    }
    return new CompactionDetails(detailsList.get(0).getReferenceTimeMs(), allSegmentNames, null);
  }

  // phaseTransitionEnforcementTest() helpers

  /**
   * Checks that transitions to phases in {@code transitionToFailures} fails.
   * @param cLog the {@link CompactionLog} being used.
   * @param transitionToFailures the list of phases, transition to which should fail.
   */
  private void checkTransitionFailure(CompactionLog cLog, CompactionLog.Phase... transitionToFailures) {
    for (CompactionLog.Phase transitionToFailure : transitionToFailures) {
      try {
        switch (transitionToFailure) {
          case COPY:
            cLog.markCopyStart();
            break;
          case COMMIT:
            cLog.markCommitStart();
            break;
          case CLEANUP:
            cLog.markCleanupStart();
            break;
          case DONE:
            cLog.markCycleComplete();
            break;
          default:
            throw new IllegalArgumentException("Unknown Phase: " + transitionToFailure);
        }
        fail("Transition should have failed");
      } catch (IllegalStateException e) {
        // expected. Nothing to do.
      }
    }

    if (!cLog.getCompactionPhase().equals(CompactionLog.Phase.COPY)) {
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentName.generateFirstSegmentName(true), 0),
              new UUID(1, 1), new UUID(1, 1), null, null, UNINITIALIZED_RESET_KEY_VERSION);
      try {
        cLog.setSafeToken(safeToken);
        fail("Setting safe token should have failed");
      } catch (IllegalStateException e) {
        // expected. Nothing to do.
      }
    }
  }

  private Pair<Offset, Offset> addOneIndexSegmentOffsetPair(CompactionLog cLog) {
    LogSegmentName logSegmentName = cLog.getCompactionDetails().getLogSegmentsUnderCompaction().get(0);
    return addOneIndexSegmentOffsetPair(logSegmentName, cLog);
  }

  private Pair<Offset, Offset> addOneIndexSegmentOffsetPair(LogSegmentName logSegmentName, CompactionLog cLog) {
    Offset before = new Offset(logSegmentName, LogSegment.HEADER_SIZE);
    Offset after = new Offset(logSegmentName.getNextGenerationName(), LogSegment.HEADER_SIZE);
    cLog.addBeforeAndAfterIndexSegmentOffsetPair(before, after);
    return new Pair<>(before, after);
  }
}
