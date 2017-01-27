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
    CompactionLog log = new CompactionLog(tempDirStr, storeName, time, detailsList);
    assertTrue("Compaction should be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    for (CompactionDetails details : detailsList) {
      verifyEquality(details, log.getCompactionDetails());
      assertEquals("Should be in the PREPARE phase", CompactionLog.Phase.PREPARE, log.getCompactionPhase());
      log.markCopyStart();
      assertEquals("Should be in the COPY phase", CompactionLog.Phase.COPY, log.getCompactionPhase());
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentNameHelper.generateFirstSegmentName(5), 0),
              new UUID(1, 1), new UUID(1, 1));
      log.setSafeToken(safeToken);
      assertEquals("Returned token not the same as the one that was set", safeToken, log.getSafeToken());
      log.markSwitchStart();
      assertEquals("Should be in the SWITCH phase", CompactionLog.Phase.SWITCH, log.getCompactionPhase());
      log.markCleanupStart();
      assertEquals("Should be in the CLEANUP phase", CompactionLog.Phase.CLEANUP, log.getCompactionPhase());
      log.markCycleComplete();
    }
    assertEquals("Should be in the DONE phase", CompactionLog.Phase.DONE, log.getCompactionPhase());
    log.close();
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
    CompactionLog log = new CompactionLog(tempDirStr, storeName, time, detailsList);
    assertTrue("Compaction should should be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
    for (CompactionDetails details : detailsList) {
      log.close();
      log = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      verifyEquality(details, log.getCompactionDetails());
      assertEquals("Should be in the PREPARE phase", CompactionLog.Phase.PREPARE, log.getCompactionPhase());
      log.markCopyStart();

      log.close();
      log = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      assertEquals("Should be in the COPY phase", CompactionLog.Phase.COPY, log.getCompactionPhase());
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentNameHelper.generateFirstSegmentName(5), 0),
              new UUID(1, 1), new UUID(1, 1));
      log.setSafeToken(safeToken);

      log.close();
      log = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      assertEquals("Returned token not the same as the one that was set", safeToken, log.getSafeToken());
      log.markSwitchStart();

      log.close();
      log = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      assertEquals("Should be in the SWITCH phase", CompactionLog.Phase.SWITCH, log.getCompactionPhase());
      log.markCleanupStart();

      log.close();
      log = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      assertEquals("Should be in the CLEANUP phase", CompactionLog.Phase.CLEANUP, log.getCompactionPhase());
      log.markCycleComplete();
    }
    assertEquals("Should be in the DONE phase", CompactionLog.Phase.DONE, log.getCompactionPhase());
    log.close();
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));
  }

  /**
   * Tests phase transition order is enforced during phase transitions
   */
  @Test
  public void phaseTransitionEnforcementTest() throws IOException {
    String storeName = "store";
    List<CompactionDetails> detailsList = getCompactionDetails(1);
    CompactionLog log = new CompactionLog(tempDirStr, storeName, time, detailsList);

    assertEquals("Should be in the PREPARE phase", CompactionLog.Phase.PREPARE, log.getCompactionPhase());
    checkTransitionFailure(log, CompactionLog.Phase.SWITCH, CompactionLog.Phase.CLEANUP, CompactionLog.Phase.DONE);

    log.markCopyStart();
    assertEquals("Should be in the COPY phase", CompactionLog.Phase.COPY, log.getCompactionPhase());
    checkTransitionFailure(log, CompactionLog.Phase.COPY, CompactionLog.Phase.CLEANUP, CompactionLog.Phase.DONE);

    log.markSwitchStart();
    assertEquals("Should be in the SWITCH phase", CompactionLog.Phase.SWITCH, log.getCompactionPhase());
    checkTransitionFailure(log, CompactionLog.Phase.COPY, CompactionLog.Phase.SWITCH, CompactionLog.Phase.DONE);

    log.markCleanupStart();
    assertEquals("Should be in the CLEANUP phase", CompactionLog.Phase.CLEANUP, log.getCompactionPhase());
    checkTransitionFailure(log, CompactionLog.Phase.COPY, CompactionLog.Phase.SWITCH, CompactionLog.Phase.CLEANUP);

    log.markCycleComplete();
    assertEquals("Should be in the DONE phase", CompactionLog.Phase.DONE, log.getCompactionPhase());
    checkTransitionFailure(log, CompactionLog.Phase.COPY, CompactionLog.Phase.SWITCH, CompactionLog.Phase.CLEANUP,
        CompactionLog.Phase.DONE);
    log.close();
  }

  /**
   * Tests for construction of {@link CompactionLog} with bad arguments/state.
   * @throws IOException
   */
  @Test
  public void constructionBadArgsTest() throws IOException {
    String storeName = "store";
    CompactionLog log = new CompactionLog(tempDirStr, storeName, time, getCompactionDetails(1));
    log.close();
    // log file already exists
    try {
      new CompactionLog(tempDirStr, storeName, time, getCompactionDetails(1));
      fail("Construction should have failed because log file already exists");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // make sure file disappears
    log = new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
    log.markCopyStart();
    log.markSwitchStart();
    log.markCleanupStart();
    log.markCycleComplete();
    log.close();
    assertFalse("Compaction should not be in progress", CompactionLog.isCompactionInProgress(tempDirStr, storeName));

    // log file does not exist
    try {
      new CompactionLog(tempDirStr, storeName, STORE_KEY_FACTORY, time);
      fail("Construction should have failed because log file does not exist");
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
   * @param log the {@link CompactionLog} being used.
   * @param transitionToFailures the list of phases, transition to which should fail.
   */
  private void checkTransitionFailure(CompactionLog log, CompactionLog.Phase... transitionToFailures) {
    for (CompactionLog.Phase transitionToFailure : transitionToFailures) {
      try {
        switch (transitionToFailure) {
          case COPY:
            log.markCopyStart();
            break;
          case SWITCH:
            log.markSwitchStart();
            break;
          case CLEANUP:
            log.markCleanupStart();
            break;
          case DONE:
            log.markCycleComplete();
            break;
          default:
            throw new IllegalArgumentException("Unknown Phase: " + transitionToFailure);
        }
        fail("Transition should have failed");
      } catch (IllegalStateException e) {
        // expected. Nothing to do.
      }
    }

    if (!log.getCompactionPhase().equals(CompactionLog.Phase.COPY)) {
      StoreFindToken safeToken =
          new StoreFindToken(new MockId("dummy"), new Offset(LogSegmentNameHelper.generateFirstSegmentName(5), 0),
              new UUID(1, 1), new UUID(1, 1));
      try {
        log.setSafeToken(safeToken);
        fail("Setting safe token should have failed");
      } catch (IllegalStateException e) {
        // expected. Nothing to do.
      }
    }
  }
}
