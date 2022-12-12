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
  private final int capacity = 10;
  private final int timeout = 100;

  @Test
  public void testOffer() {
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(capacity, timeout, mockTime);
    // Test that offer() returns false once queue is full.
    for (int i = 0; i < capacity; i++) {
      assertTrue("Unable to queue request", requestQueue.offer(new MockRequest(0)));
    }
    assertFalse("Queue should have full", requestQueue.offer(new MockRequest(0)));
  }

  @Test
  public void testTake() throws InterruptedException {
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(capacity, timeout, mockTime);

    // Test de-queuing a request
    MockRequest mockRequest = new MockRequest(0);
    requestQueue.offer(mockRequest);
    NetworkRequestBundle requestBundle = requestQueue.take();
    assertEquals("Mismatch in request queued", mockRequest, requestBundle.getRequestToServe());

    // Test case where request is valid
    requestQueue.offer(mockRequest);
    mockTime.sleep(timeout - 1);
    requestBundle = requestQueue.take();
    assertEquals("Requests to serve must not be empty", mockRequest, requestBundle.getRequestToServe());

    // Test case where request is expired
    requestQueue.offer(new MockRequest(mockTime.milliseconds()));
    mockTime.sleep(timeout + 1);
    requestBundle = requestQueue.take();
    assertNull("Requests to serve must be empty", requestBundle.getRequestToServe());
    assertEquals("There should be one expired request", 1, requestBundle.getRequestsToDrop().size());

    // Test case where there is one expired request and one valid request
    MockRequest expiredRequest = new MockRequest(mockTime.milliseconds());
    requestQueue.offer(expiredRequest);
    mockTime.sleep(timeout + 1);
    MockRequest validRequest = new MockRequest(mockTime.milliseconds());
    mockTime.sleep(timeout - 1);
    requestQueue.offer(validRequest);
    requestBundle = requestQueue.take();
    assertEquals("There should be expired requests", expiredRequest,
        requestBundle.getRequestsToDrop().iterator().next());
    assertEquals("There should be valid requests", validRequest, requestBundle.getRequestToServe());
  }

  @Test
  public void testSize() {
    int size = 5;
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(capacity, timeout, new MockTime());
    for (int i = 0; i < size; i++) {
      assertTrue("Unable to queue request", requestQueue.offer(new MockRequest(0)));
    }
    assertEquals("Mismatch in size of queue", size, requestQueue.size());
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
