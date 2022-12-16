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
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;


public class FifoNetworkRequestQueueTest {
  private final int capacity = 10;
  private final int timeout = 100;

  @Test
  public void testPut() throws InterruptedException {
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(capacity, timeout, mockTime);
    // Test that queue holds elements once it is full.
    int overflow = 5;
    for (int i = 0; i < capacity + overflow; i++) {
      requestQueue.put(new MockRequest(0));
    }
    List<NetworkRequest> droppedRequests = requestQueue.getDroppedRequests();
    assertEquals("Mismatch in number of dropped requests", overflow, droppedRequests.size());
  }

  @Test
  public void testTake() throws InterruptedException {
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(capacity, timeout, mockTime);

    // Test de-queuing a request
    MockRequest sentRequest = new MockRequest(0);
    requestQueue.put(sentRequest);
    NetworkRequest receivedRequest = requestQueue.take();
    assertEquals("Mismatch in request queued", sentRequest, receivedRequest);

    // Test case where request is valid
    requestQueue.put(sentRequest);
    mockTime.sleep(timeout - 1);
    receivedRequest = requestQueue.take();
    assertEquals("Requests to serve must not be empty", sentRequest, receivedRequest);

    // Test case where there is one expired request and one valid request
    MockRequest expiredRequest = new MockRequest(mockTime.milliseconds());
    requestQueue.put(expiredRequest);
    mockTime.sleep(timeout + 1);
    MockRequest validRequest = new MockRequest(mockTime.milliseconds());
    mockTime.sleep(timeout - 1);
    requestQueue.put(validRequest);
    receivedRequest = requestQueue.take();
    List<NetworkRequest> droppedRequests = requestQueue.getDroppedRequests();
    assertEquals("Mismatch in dropped requests", expiredRequest, droppedRequests.iterator().next());
    assertEquals("Mismatch in valid requests", validRequest, receivedRequest);
  }

  @Test
  public void testSize() throws InterruptedException {
    int size = 5;
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(capacity, timeout, new MockTime());
    for (int i = 0; i < size; i++) {
      requestQueue.put(new MockRequest(0));
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
