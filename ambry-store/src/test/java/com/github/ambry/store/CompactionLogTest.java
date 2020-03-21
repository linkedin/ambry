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
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.junit.After;
import org.junit.Test;

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
  private final Time time = new MockTime();
  private final Set<String> generatedSegmentNames = new HashSet<>();

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
      Offset offset = new Offset(LogSegmentNameHelper.generateFirstSegmentName(true),
          Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE));
      cLog.setStartOffsetOfLastIndexSegmentForDeleteCheck(offset);
      assertEquals("Offset that was set was not the one returned", offset,
          cLog.getStartOffsetOfLastIndexSegmentForDeleteCheck());
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentNameHelper.generateFirstSegmentName(true), 0),
              new UUID(1, 1), new UUID(1, 1));
      cLog.setSafeToken(safeToken);
      assertEquals("Returned token not the same as the one that was set", safeToken, cLog.getSafeToken());
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
      Offset offset = new Offset(LogSegmentNameHelper.generateFirstSegmentName(true),
          Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE));
      cLog.setStartOffsetOfLastIndexSegmentForDeleteCheck(offset);

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      assertEquals("Offset that was set was not the one returned", offset,
          cLog.getStartOffsetOfLastIndexSegmentForDeleteCheck());
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentNameHelper.generateFirstSegmentName(true), 0),
              new UUID(1, 1), new UUID(1, 1));
      cLog.setSafeToken(safeToken);

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
      assertEquals("Returned token not the same as the one that was set", safeToken, cLog.getSafeToken());
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
   * Tests the reading of versions older than the current versions.
   * @throws IOException
   */
  @Test
  public void oldVersionsReadTest() throws IOException {
    String storeName = "store";
    long startTimeMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    long referenceTimeMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    CompactionDetails details = getCompactionDetails(referenceTimeMs);
    for (int i = 0; i < CompactionLog.CURRENT_VERSION; i++) {
      File file = new File(tempDir, storeName + CompactionLog.COMPACTION_LOG_SUFFIX);
      switch (i) {
        case CompactionLog.VERSION_0:
          try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            CrcOutputStream crcOutputStream = new CrcOutputStream(fileOutputStream);
            DataOutputStream stream = new DataOutputStream(crcOutputStream);
            stream.writeShort(i);
            stream.writeLong(startTimeMs);
            stream.writeInt(0);
            stream.writeInt(1);
            stream.write(new CompactionLog.CycleLog(details).toBytes());
            stream.writeLong(crcOutputStream.getValue());
            fileOutputStream.getChannel().force(true);
          }
          CompactionLog cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time, config);
          verifyEquality(details, cLog.getCompactionDetails());
          assertEquals("Current Idx not as expected", 0, cLog.getCurrentIdx());
          break;
        default:
          throw new IllegalStateException("No serialization implementation for version: " + i);
      }
    }
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
    List<String> segmentsUnderCompaction = new ArrayList<>();
    for (int j = 0; j < segmentCount; j++) {
      String segmentName;
      do {
        segmentName = TestUtils.getRandomString(10);
      } while (generatedSegmentNames.contains(segmentName));
      generatedSegmentNames.add(segmentName);
      segmentsUnderCompaction.add(segmentName);
    }
    return new CompactionDetails(referenceTime, segmentsUnderCompaction);
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
    List<String> allSegmentNames = new ArrayList<>();
    for (CompactionDetails details : detailsList) {
      allSegmentNames.addAll(details.getLogSegmentsUnderCompaction());
    }
    return new CompactionDetails(detailsList.get(0).getReferenceTimeMs(), allSegmentNames);
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
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentNameHelper.generateFirstSegmentName(true), 0),
              new UUID(1, 1), new UUID(1, 1));
      try {
        cLog.setSafeToken(safeToken);
        fail("Setting safe token should have failed");
      } catch (IllegalStateException e) {
        // expected. Nothing to do.
      }
    }
  }
}
