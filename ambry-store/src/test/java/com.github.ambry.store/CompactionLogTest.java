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

import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
  // the time instance that will be used in the index
  private final Time time = new MockTime();

  /**
   * Creates a temporary directory for the compaction log file.
   * @throws IOException
   */
  public CompactionLogTest() throws IOException {
    tempDir = StoreTestUtils.createTempDirectory("storeDir-" + UtilsTest.getRandomString(10));
    tempDirStr = tempDir.getAbsolutePath();
  }

  /**
   * Deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   */
  @After
  public void cleanup() throws InterruptedException, IOException, StoreException {
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Tests the use of {@link CompactionLog} when it is used without closing b/w operations and compaction cycles.
   * @throws IOException
   */
  @Test
  public void iterationWithoutReloadTest() throws IOException {
    String storeName = "store";
    List<CompactionDetails> detailsList = getCompactionDetails(5);
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, detailsList);
    assertTrue("Compaction should be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    int currentIdx = 0;
    for (CompactionDetails details : detailsList) {
      assertEquals("CurrentIdx not as expected", currentIdx, cLog.getCurrentIdx());
      verifyEquality(details, cLog.getCompactionDetails());
      assertEquals("Should be in the PREPARE phase", CompactionLog.Phase.PREPARE, cLog.getCompactionPhase());
      cLog.markCopyStart();
      assertEquals("Should be in the COPY phase", CompactionLog.Phase.COPY, cLog.getCompactionPhase());
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentNameHelper.generateFirstSegmentName(true), 0),
              new UUID(1, 1), new UUID(1, 1));
      cLog.setSafeToken(safeToken);
      assertEquals("Returned token not the same as the one that was set", safeToken, cLog.getSafeToken());
      cLog.markCommitStart();
      assertEquals("Should be in the SWITCH phase", CompactionLog.Phase.COMMIT, cLog.getCompactionPhase());
      cLog.markCleanupStart();
      assertEquals("Should be in the CLEANUP phase", CompactionLog.Phase.CLEANUP, cLog.getCompactionPhase());
      cLog.markCycleComplete();
      currentIdx++;
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
    List<CompactionDetails> detailsList = getCompactionDetails(5);
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, detailsList);
    assertTrue("Compaction should should be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    int currentIdx = 0;
    for (CompactionDetails details : detailsList) {
      assertEquals("CurrentIdx not as expected", currentIdx, cLog.getCurrentIdx());

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      verifyEquality(details, cLog.getCompactionDetails());
      assertEquals("Should be in the PREPARE phase", CompactionLog.Phase.PREPARE, cLog.getCompactionPhase());
      cLog.markCopyStart();

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      assertEquals("Should be in the COPY phase", CompactionLog.Phase.COPY, cLog.getCompactionPhase());
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentNameHelper.generateFirstSegmentName(true), 0),
              new UUID(1, 1), new UUID(1, 1));
      cLog.setSafeToken(safeToken);

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      assertEquals("Returned token not the same as the one that was set", safeToken, cLog.getSafeToken());
      cLog.markCommitStart();

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      assertEquals("Should be in the SWITCH phase", CompactionLog.Phase.COMMIT, cLog.getCompactionPhase());
      cLog.markCleanupStart();

      cLog.close();
      cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      assertEquals("Should be in the CLEANUP phase", CompactionLog.Phase.CLEANUP, cLog.getCompactionPhase());
      cLog.markCycleComplete();

      currentIdx++;
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
    List<CompactionDetails> detailsList = getCompactionDetails(1);
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, detailsList);

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
    CompactionLog cLog = new CompactionLog(tempDirStr, storeName, time, getCompactionDetails(1));
    cLog.close();
    // log file already exists
    try {
      new CompactionLog(tempDirStr, storeName, time, getCompactionDetails(1));
      fail("Construction should have failed because compaction log file already exists");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // make sure file disappears
    cLog = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
    cLog.markCopyStart();
    cLog.markCommitStart();
    cLog.markCleanupStart();
    cLog.markCycleComplete();
    cLog.close();
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));

    // log file does not exist
    try {
      new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      fail("Construction should have failed because compaction log file does not exist");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  // helpers
  // general

  /**
   * Gets a {@link List<CompactionDetails>} of size {@code size} with random {@link CompactionDetails}.
   * @param size the number of {@link CompactionDetails} required.
   * @return a {@link List<CompactionDetails>} of size {@code size} with random {@link CompactionDetails}.
   */
  private List<CompactionDetails> getCompactionDetails(int size) {
    List<CompactionDetails> details = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      long referenceTime = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      int segmentCount = TestUtils.RANDOM.nextInt(10) + 1;
      List<String> segmentsUnderCompaction = new ArrayList<>();
      for (int j = 0; j < segmentCount; j++) {
        segmentsUnderCompaction.add(UtilsTest.getRandomString(10));
      }
      String extraSegmentName = i % 2 == 0 ? null : UtilsTest.getRandomString(10);
      int swapSpaceCount = TestUtils.RANDOM.nextInt(10) + 1;
      details.add(new CompactionDetails(referenceTime, segmentsUnderCompaction, extraSegmentName, swapSpaceCount));
    }
    return details;
  }

  /**
   * Verifies that two {@link CompactionDetails} instances are equal.
   * @param original the expected {@link CompactionDetails}.
   * @param toCheck the {@link CompactionDetails} that needs to be checked.
   */
  private void verifyEquality(CompactionDetails original, CompactionDetails toCheck) {
    assertEquals("Reference time does not match", original.getReferenceTime(), toCheck.getReferenceTime());
    assertEquals("Segments under compaction don't match", original.getLogSegmentsUnderCompaction(),
        toCheck.getLogSegmentsUnderCompaction());
    assertEquals("Extra segment name does not match", original.getExtraSegmentName(), toCheck.getExtraSegmentName());
    assertEquals("Swap space count does not match", original.getSwapSpaceCount(), toCheck.getSwapSpaceCount());
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
