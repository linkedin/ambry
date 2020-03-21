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
package com.github.ambry.clustermap;

import com.github.ambry.utils.MockTime;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for the {@link FixedBackoffResourceStatePolicy}
 */
public class FixedBackoffResourceStatePolicyTest {
  private final FixedBackoffResourceStatePolicy policy;
  private final MockTime time;
  private final Resource resource;
  private static final int FAILURE_COUNT_THRESHOLD = 3;
  private static final int RETRY_BACKOFF_MS = 10;

  public FixedBackoffResourceStatePolicyTest() {
    // Ensure that the values for FAILURE_COUNT_THRESHOLD and RETRY_BACKOFF_MS are valid for the tests.
    assertTrue("Test initialization error, FAILURE_COUNT_THRESHOLD is too low", FAILURE_COUNT_THRESHOLD >= 2);
    assertTrue("Test initialization error, RETRY_BACKOFF_MS is too low", RETRY_BACKOFF_MS >= 2);
    time = new MockTime();
    resource = () -> null;
    policy = new FixedBackoffResourceStatePolicy(resource, false, FAILURE_COUNT_THRESHOLD, RETRY_BACKOFF_MS, time);
  }

  /**
   * This should be called at base state - when the resource is unconditionally up with no outstanding errors.
   */
  private void initiateAndVerifyResourceGoesDownExactlyAtThresholdFailures() {
    for (int i = 0; i < FAILURE_COUNT_THRESHOLD; i++) {
      assertFalse(policy.isDown());
      assertFalse(policy.isHardDown());
      policy.onError();
    }
    assertTrue(policy.isDown());
    assertFalse(policy.isHardDown());
  }

  /**
   * Tests to validate that the thresholds and retries are serving exactly their intended purposes.
   */
  @Test
  public void testThresholdsAndRetries() {
    // Verify that the resource becomes down exactly after receiving 3 errors.
    initiateAndVerifyResourceGoesDownExactlyAtThresholdFailures();

    // Verify that the resource will stay down until 10 ms have passed.
    time.sleep(RETRY_BACKOFF_MS / 2);
    assertTrue(policy.isDown());
    time.sleep(RETRY_BACKOFF_MS - RETRY_BACKOFF_MS / 2);
    assertFalse(policy.isDown());

    // At this time the resource is conditionally up. Verify that a single error should bring it back down.
    policy.onError();
    assertTrue(policy.isDown());

    // A single success should bring a down resource unconditionally up.
    policy.onSuccess();
    // Verify that the resource is unconditionally up - a single error should not bring it down.
    initiateAndVerifyResourceGoesDownExactlyAtThresholdFailures();

    time.sleep(RETRY_BACKOFF_MS);
    assertFalse(policy.isDown());
    // Verify that a conditionally up resource becomes unconditionally up after receiving a single success.
    policy.onSuccess();
    initiateAndVerifyResourceGoesDownExactlyAtThresholdFailures();
  }

  /**
   * Tests that hard events are honored in the way expected.
   */
  @Test
  public void testHardDownandHardUp() {
    // Verify that once the resource is hard down, it stays down indefinitely unless an explicit event occurs.
    assertFalse(policy.isDown());
    policy.onHardDown();
    assertTrue(policy.isDown());
    time.sleep(RETRY_BACKOFF_MS * 2);
    assertTrue(policy.isDown());
    assertTrue(policy.isHardDown());

    // A single success should not bring it back up, as the resource is hard down.
    policy.onSuccess();
    assertTrue(policy.isDown());
    assertTrue(policy.isHardDown());

    // A single hard up should bring it back up.
    policy.onHardUp();
    assertFalse(policy.isDown());
    assertFalse(policy.isHardDown());

    // Verify that a hard up resource goes down immediately if onHardDown() is called.
    policy.onHardDown();
    assertTrue(policy.isDown());
    assertTrue(policy.isHardDown());

    policy.onHardUp();
    assertFalse(policy.isDown());
    assertFalse(policy.isHardDown());

    // Verify that a hard up resource goes down as part of normal errors when thresholds are reached.
    initiateAndVerifyResourceGoesDownExactlyAtThresholdFailures();
  }
}

