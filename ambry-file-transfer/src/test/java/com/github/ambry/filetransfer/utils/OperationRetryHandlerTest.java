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
package com.github.ambry.filetransfer.utils;


import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.filetransfer.handler.FileCopyHandlerConfig;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link OperationRetryHandler}.
 */
@RunWith(MockitoJUnitRunner.class)
public class OperationRetryHandlerTest {
  private OperationRetryHandler retryHandler;

  @Before
  public void setup() {
    FileCopyBasedReplicationConfig fileCopyHandlerConfig = new FileCopyBasedReplicationConfig(new VerifiableProperties(new Properties()));
    retryHandler = new OperationRetryHandler(fileCopyHandlerConfig);
  }

  /**
   * Test that am operation is executed until it succeeds for a maximum of 3 attempts.
   */
  @Test
  public void testRetryUntilSuccess() throws Exception {
    AtomicInteger attemptCounter = new AtomicInteger(0);

    // Arrange: Simulate 2 failures and 1 success
    OperationRetryHandler.RetryableOperation<String> operation = () -> {
      if (attemptCounter.incrementAndGet() < 3) {
        throw new IOException("Simulated failure");
      }
      return "Success";
    };

    // Act: Execute the operation
    String result = retryHandler.executeWithRetry(operation, "testRetryUntilSuccess");

    // Assert: The operation should succeed after 3 retry attempts
    assertEquals("Success", result);
    assertEquals(3, attemptCounter.get()); // 2 failures + 1 success
  }

  /**
   * Test that an operation is failed after exceeding the maximum number of retries.
   */
  @Test
  public void testRetriesExceedingLimit() {
    AtomicInteger attemptCounter = new AtomicInteger(0);

    // Arrange: Simulate a persistent failure
    OperationRetryHandler.RetryableOperation<String> failingOperation = () -> {
      attemptCounter.incrementAndGet();
      throw new RuntimeException("Persistent failure");
    };

    // Act: Execute the operation
    try {
      retryHandler.executeWithRetry(failingOperation, "testRetriesExceedingLimit");
      fail("Expected IOException");
    } catch (Exception e) {
      assertEquals("Persistent failure", e.getMessage());
    }

    // Assert: The operation should fail after 3 retry attempts
    assertEquals(3, attemptCounter.get()); // 3 retries + 1 initial attempt
  }
}
