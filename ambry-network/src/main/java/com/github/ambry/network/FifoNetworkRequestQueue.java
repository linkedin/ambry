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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * A FIFO queue to hold network requests. It Internally, it maintains two queues, a queue to hold active requests and
 * another one to hold timed out requests. Both are unbounded queues but since we have {@code RequestHandler} and
 * {@code RequestDropper thread} reading from them continuously, they shouldn't grow in indefinitely.
 */
public class FifoNetworkRequestQueue implements NetworkRequestQueue {
  private final int timeout;
  private final Time time;
  // Queue to hold active requests. If a request times out in the queue, it would be moved to droppedRequestsQueue. This
  // would prevent the queue from growing unbounded.
  private final BlockingQueue<NetworkRequest> queue = new LinkedBlockingQueue<>();
  // Queue to hold timed out requests. The requests in this queue would be continuously picked up by 'request-dropper'
  // thread. This would prevent the queue from growing unbounded.
  private final BlockingQueue<NetworkRequest> droppedRequestsQueue = new LinkedBlockingQueue<>();

  FifoNetworkRequestQueue(int timeout, Time time) {
    this.timeout = timeout;
    this.time = time;
  }

  /**
   * Inserts the element into queue.
   * @param request element to be inserted.
   */
  @Override
  public void offer(NetworkRequest request) throws InterruptedException {
    queue.offer(request);
    if (request.equals(EmptyRequest.getInstance())) {
      // This is a internal generated request used to signal shutdown. Add it to dropped requests queue as well so that
      // request-dropper thread shuts down gracefully.
      droppedRequestsQueue.offer(request);
    }
  }

  @Override
  public NetworkRequest take() throws InterruptedException {
    while (true) {
      // If there are no requests in the queue, wait until a new request comes.
      NetworkRequest nextRequest = queue.take();
      // If the request expired, add it to dropped requests queue and continue.
      if (needToDrop(nextRequest)) {
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
  public int numActiveRequests() {
    return queue.size();
  }

  @Override
  public int numDroppedRequests() {
    return droppedRequestsQueue.size();
  }

  @Override
  public void close() {
    queue.forEach(NetworkRequest::release);
    droppedRequestsQueue.forEach(NetworkRequest::release);
    queue.clear();
    droppedRequestsQueue.clear();
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
