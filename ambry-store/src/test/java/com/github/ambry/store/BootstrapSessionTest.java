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

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link BootstrapSession}.
 */
@RunWith(MockitoJUnitRunner.class)
public class BootstrapSessionTest extends BootstrapSessionManagerTest {

  /**
   * Test that {@link BootstrapSession#stop} cancels the timers before they trigger.
   */
  @Test
  public void testStopCancelsTimersBeforeTrigger() throws InterruptedException {
    // Arrange
    AtomicBoolean handlerCalled = new AtomicBoolean(false);
    BootstrapSession session = new BootstrapSession(mockPartitionId, "snapshot1", "node1",
        diskManagerConfig, (nodeId, partitionId) -> handlerCalled.set(true), (nodeId, partitionId) -> handlerCalled.set(true));

    // Act
    session.start();
    session.stop();
    Thread.sleep(150); // The default timer in test context is configured to be 300ms. Let time pass, timers shouldn't fire.

    // Assert
    assertFalse("Timer handler should not have been called after stop()", handlerCalled.get());
  }

  /**
   * Test that the default timer can be extended and that it delays the handler.
   */
  @Test
  public void testExtendDeferralTimerDelaysHandler() throws InterruptedException {
    // Arrange
    AtomicBoolean handlerCalled = new AtomicBoolean(false);
    BootstrapSession session = new BootstrapSession(mockPartitionId, "snapshot1", "node1",
        diskManagerConfig, (nodeId, partitionId) -> handlerCalled.set(true), (nodeId, partitionId) -> handlerCalled.set(true));
    session.start();

    // Sleep less than the original delay (300 ms)
    Thread.sleep(250);

    // Act
    // Extend the deferral timer
    session.extendDeferralTimer(500);

    // Wait long enough to exceed original delay, but not the extended delay
    // If Extend functionality is not working, this will trigger the handler as we have waited 350ms (250 + 100)
    // after starting the session and this is more than the default timer of 300ms.
    Thread.sleep(100);

    // Assert
    // Should not have triggered yet
    assertFalse(handlerCalled.get());

    // Wait more to exceed the extended delay
    // We have waited 600ms (100 + 250) after extending the session and this is more than the extended timer of 500ms.
    Thread.sleep(500);

    assertTrue(handlerCalled.get());
  }

  /**
   * Test that the total timer expires even if the deferral timer is extended.
   */
  @Test
  public void testTotalTimerExpiresEvenIfDeferralTimerIsExtended() throws InterruptedException {
    // Arrange
    AtomicBoolean deferralHandlerCalled = new AtomicBoolean(false);
    AtomicBoolean totalHandlerCalled = new AtomicBoolean(false);

    BootstrapSession session = new BootstrapSession(mockPartitionId, "snapshot1", "node1",
        diskManagerConfig, (nodeId, partitionId) -> deferralHandlerCalled.set(true), (nodeId, partitionId) -> totalHandlerCalled.set(true));
    session.start();

    // Act
    // Repeatedly extend the deferral timer before it expires
    for (int i = 0; i < 3; i++) {
      Thread.sleep(250);  // less than deferralTimeout
      session.extendDeferralTimer(300);  // push it further each time
    }

    // Wait a bit longer to allow total timer to expire
    Thread.sleep(300);  // 4 * 250 + 300 = ~1300ms > 1000ms

    // Assert
    // The deferral timer should not have fired as it was restarted multiple times
    assertFalse(deferralHandlerCalled.get());

    // The total timer should have fired once, despite extensions
    assertTrue(totalHandlerCalled.get());
  }
}
