/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link BootstrapSessionManager}.
 */
@RunWith(MockitoJUnitRunner.class)
public class BootstrapSessionTest {
  /**
   * Mocked {@link DiskManager#controlCompactionForBlobStore} to control compaction for the blob store.
   */
  @Mock
  private BiFunction<PartitionId, Boolean, Boolean> mockCompactionControl;

  /**
   * Mocked {@link PartitionId} to be used in the tests.
   */
  @Mock
  private PartitionId mockPartitionId;

  /**
   * Mocked {@link DiskManagerConfig} to be used in the tests.
   */
  private DiskManagerConfig diskManagerConfig;

  private BootstrapSessionManager bootstrapSessionManager;

  /**
   * Default timer value for deferred compaction in test context.
   */
  private final int diskManagerDeferredCompactionDefaultTimerTimeoutMilliseconds = 300; // 0.3 seconds

  /**
   * Default timer value for total-timer-since-compaction-was-disabled in test context.
   */
  private final int diskManagerDeferredCompactionTotalTimerTimeoutMilliseconds = 1000; // 1 second

  /**
   * Setup method to initialize the {@link BootstrapSessionManager} and mock objects.
   */
  @Before
  public void setup() {
    when(mockCompactionControl.apply(any(), anyBoolean())).thenReturn(true);
    when(mockPartitionId.getId()).thenReturn(100L);

    Properties properties = new Properties();
    properties.put("disk.manager.deferred.compaction.default.timer.timeout.milliseconds",
        String.valueOf(diskManagerDeferredCompactionDefaultTimerTimeoutMilliseconds));
    properties.put("disk.manager.deferred.compaction.total.timer.timeout.milliseconds",
        String.valueOf(diskManagerDeferredCompactionTotalTimerTimeoutMilliseconds));
    diskManagerConfig = new DiskManagerConfig(new VerifiableProperties(properties));

    bootstrapSessionManager = new BootstrapSessionManager(diskManagerConfig, mockCompactionControl);
    bootstrapSessionManager.enable();
  }

  /**
   * Adds a bootstrap session. Test that the session is added correctly and that the compaction control is called.
   */
  @Test
  public void testAddAndStartBootstrapSession() {
    bootstrapSessionManager.addAndStartBootstrapSession(mockPartitionId, "snapshot1", "node1");

    BootstrapSession session = bootstrapSessionManager.getBootstrapSession(mockPartitionId, "node1");
    assertNotNull(session);

    assertEquals("snapshot1", session.getSnapShotId());
    verify(mockCompactionControl).apply(mockPartitionId, false);
  }

  /**
   * Adds & removes a bootstrap session and checks that the session is removed correctly.
   */
  @Test
  public void testRemoveBootstrapSession() {
    bootstrapSessionManager.addAndStartBootstrapSession(mockPartitionId, "snapshot2", "node2");
    bootstrapSessionManager.removeBootstrapSession(mockPartitionId, "node2");

    assertNull(bootstrapSessionManager.getBootstrapSession(mockPartitionId, "node2"));
  }

  /**
   * Clears all sessions and checks that the sessions are removed correctly.
   */
  @Test
  public void testClearAllSessions() {
    bootstrapSessionManager.addAndStartBootstrapSession(mockPartitionId, "snap1", "nodeX");
    bootstrapSessionManager.clearAllSessions();

    assertNull(bootstrapSessionManager.getBootstrapSession(mockPartitionId, "nodeX"));
  }

  /**
   *
   */
  @Test
  public void testEnableCompactionOnTimerExpiryHandler() throws InterruptedException {
    // Arrange
    AtomicBoolean compactionEnabled = new AtomicBoolean(false);
    BootstrapSessionManager testBootstrapSessionManager = new BootstrapSessionManager(diskManagerConfig, (partitionId1, enable) -> {
      if (enable) {
        compactionEnabled.set(true);
      }
      return true;
    });
    testBootstrapSessionManager.enable();

    // Act
    testBootstrapSessionManager.addAndStartBootstrapSession(mockPartitionId, "snapshot4", "nodeZ");

    // Wait for time with buffer (50% extra time considering the default timer in test context is configured to be 300ms.
    // Less than 50% buffer could end up making this test flaky due to GC or other jvm delays)
    long waitTimeMs = (long) (diskManagerDeferredCompactionDefaultTimerTimeoutMilliseconds * 1.5);
    Thread.sleep(waitTimeMs);

    assertTrue("Compaction should be re-enabled after deferral timer expiry", compactionEnabled.get());
    assertNull("Bootstrap session should be cleaned up after timer expiry",
        testBootstrapSessionManager.getBootstrapSession(mockPartitionId, "nodeZ"));
  }
}
