/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network;

import com.github.ambry.utils.MockTime;
import java.io.InputStream;
import org.junit.Test;

import static org.junit.Assert.*;


public class FifoNetworkRequestQueueTest {
  private final int timeout = 100;
  private final int capacity = 10;

  @Test
  public void testOffer() throws InterruptedException {
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(timeout, mockTime, capacity);
    MockRequest mockRequest = new MockRequest(0);
    requestQueue.offer(mockRequest);
    assertEquals("Mismatch in size of the queue", 1, requestQueue.size());
    assertEquals("Mismatch in request de-queued", mockRequest, requestQueue.take());
    assertEquals("Mismatch in size of the queue", 0, requestQueue.size());
  }

  @Test
  public void testExpiry() throws InterruptedException {
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(timeout, mockTime, capacity);
    MockRequest firstRequest = new MockRequest(0);
    requestQueue.offer(firstRequest);
    mockTime.sleep(timeout + 1);
    MockRequest secondRequest = new MockRequest(0);
    requestQueue.offer(secondRequest);
    // 1. Verify 1st request is expired
    NetworkRequest receivedRequest = requestQueue.take();
    assertEquals("Mismatch in request queued", firstRequest, receivedRequest);
    assertTrue("Request should be expired", requestQueue.isExpired(receivedRequest));
    // 2. Verify 2nd request is valid
    receivedRequest = requestQueue.take();
    assertEquals("Mismatch in request queued", secondRequest, receivedRequest);
    assertTrue("Request should not be expired", requestQueue.isExpired(receivedRequest));
  }

  @Test
  public void testCapacity() throws InterruptedException {
    MockTime mockTime = new MockTime();
    // Keep capacity as 2
    int capacity = 2;
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(timeout, mockTime, capacity);
    // Verify 2 requests are able to be queued
    assertTrue("Queuing should be successful", requestQueue.offer(new MockRequest(0)));
    assertTrue("Queuing should be successful", requestQueue.offer(new MockRequest(0)));
    // Verify 3rd request fails to be queued
    assertFalse("Queuing should fail", requestQueue.offer(new MockRequest(0)));
  }

  /**
   * Implementation of {@link NetworkRequest} to help with tests.
   */
  private static class MockRequest implements NetworkRequest {

    private final long startTime;

    /**
     * Constructs a {@link MockRequest}.
     * @param startTime creation time of the request.
     */
    public MockRequest(long startTime) {
      this.startTime = startTime;
    }

    @Override
    public InputStream getInputStream() {
      return null;
    }

    @Override
    public long getStartTimeInMs() {
      return startTime;
    }
  }
}
