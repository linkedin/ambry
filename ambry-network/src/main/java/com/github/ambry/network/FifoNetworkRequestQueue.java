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

import com.github.ambry.server.EmptyRequest;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A FIFO queue to hold network requests. It Internally, it maintains two queues, a bounded blocking queue to hold immediate
 * set of next requests that can be handled by Ambry and unbounded queue to hold overflow requests.
 */
public class FifoNetworkRequestQueue implements NetworkRequestQueue {
  private static final Logger logger = LoggerFactory.getLogger(FifoNetworkRequestQueue.class);
  private final int timeout;
  private final Time time;
  // Queue to hold requests for serving.
  private final BlockingQueue<NetworkRequest> queue;
  // Queue to hold requests which have expired or couldn't be en-queued in serving queue due to capacity constraints.
  // This is a unbounded queue. We should make sure we always have a consumer thread reading from it.
  private final BlockingQueue<NetworkRequest> droppedRequestsQueue;

  FifoNetworkRequestQueue(int capacity, int timeout, Time time) {
    this.timeout = timeout;
    this.time = time;
    queue = new ArrayBlockingQueue<>(capacity);
    droppedRequestsQueue = new LinkedBlockingQueue<>();
  }

  /**
   * Inserts the element into queue.
   * @param request element to be inserted.
   */
  @Override
  public void put(NetworkRequest request) throws InterruptedException {
    if (request.equals(EmptyRequest.getInstance())) {
      // This is a internal generated request used to signal shutdown. Add it to both queues (waiting if necessary) so
      // that the consumer threads can shutdown gracefully.
      logger.info("Received empty request which signals server shutdown. Adding it to queue");
      queue.put(request);
      droppedRequestsQueue.put(request);
      return;
    }
    if (!queue.offer(request)) {
      logger.warn("Request queue is full, adding incoming request {} to dropped list", request);
      droppedRequestsQueue.offer(request);
    }
  }

  @Override
  public NetworkRequest take() throws InterruptedException {
    while (true) {
      // If there are no requests in the queue, wait until a new request comes.
      NetworkRequest nextRequest = queue.take();
      // If the request expired, add it to dropped request queue and continue.
      if (needToDrop(nextRequest)) {
        logger.warn("Request expired in queue, adding request {} to dropped list", nextRequest);
        droppedRequestsQueue.offer(nextRequest);
        continue;
      }
      return nextRequest;
    }
  }

  @Override
  public List<NetworkRequest> getDroppedRequests() throws InterruptedException {
    List<NetworkRequest> droppedRequests = new ArrayList<>();
    NetworkRequest droppedRequest;
    while ((droppedRequest = droppedRequestsQueue.poll()) != null) {
      droppedRequests.add(droppedRequest);
    }
    if (droppedRequests.isEmpty()) {
      // If there are no dropped requests in the queue, wait until there is one.
      droppedRequest = droppedRequestsQueue.take();
      droppedRequests.add(droppedRequest);
    }
    return droppedRequests;
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public void close() {
    queue.forEach(NetworkRequest::release);
    queue.clear();
  }

  @Override
  public String toString() {
    return queue.toString();
  }

  private boolean needToDrop(NetworkRequest request) {
    if (request.equals(EmptyRequest.getInstance())) {
      // This is a internal generated request used to signal shutdown. Don't consider it as expired.
      return false;
    }
    return time.milliseconds() - request.getStartTimeInMs() > timeout;
  }
}
