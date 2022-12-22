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
  private final int timeout = 100;

  @Test
  public void testOffer() throws InterruptedException {
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(timeout, mockTime);
    int numRequests = 5;
    for (int i = 0; i < numRequests; i++) {
      requestQueue.offer(new MockRequest(0));
    }
    // No requests should have been moved to dropped queue since we didn't have any timeout.
    assertEquals("Mismatch in number of active requests", numRequests, requestQueue.numActiveRequests());
    assertEquals("Mismatch in number of dropped requests", 0, requestQueue.numDroppedRequests());
  }

  @Test
  public void testTake() throws InterruptedException {
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(timeout, mockTime);

    // Test de-queuing a request
    MockRequest sentRequest = new MockRequest(0);
    requestQueue.offer(sentRequest);
    NetworkRequest receivedRequest = requestQueue.take();
    assertEquals("Mismatch in request queued", sentRequest, receivedRequest);

    // Test case where request is valid
    requestQueue.offer(sentRequest);
    mockTime.sleep(timeout - 1);
    receivedRequest = requestQueue.take();
    assertEquals("Requests to serve must not be empty", sentRequest, receivedRequest);

    // Test case where there is one expired request and one valid request
    MockRequest expiredRequest = new MockRequest(mockTime.milliseconds());
    requestQueue.offer(expiredRequest);
    mockTime.sleep(timeout + 1);
    MockRequest validRequest = new MockRequest(mockTime.milliseconds());
    requestQueue.offer(validRequest);

    receivedRequest = requestQueue.take();
    List<NetworkRequest> droppedRequests = requestQueue.getDroppedRequests();
    assertEquals("Mismatch in dropped requests", expiredRequest, droppedRequests.iterator().next());
    assertEquals("Mismatch in valid requests", validRequest, receivedRequest);
  }

  @Test
  public void testSize() throws InterruptedException {
    int size = 5;
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(timeout, new MockTime());
    for (int i = 0; i < size; i++) {
      requestQueue.offer(new MockRequest(0));
    }
    assertEquals("Mismatch in size of queue", size, requestQueue.numActiveRequests());
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
