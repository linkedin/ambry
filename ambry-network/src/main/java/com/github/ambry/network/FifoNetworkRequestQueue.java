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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * A FIFO queue to hold network requests.
 */
public class FifoNetworkRequestQueue implements NetworkRequestQueue {
  private final int timeout;
  private final Time time;
  private final BlockingQueue<NetworkRequest> queue;

  /**
   * @param timeout for the requests waiting in the incoming request queue.
   * @param time system {@link Time}
   * @param capacity capacity of the queue holding incoming requests.
   */
  FifoNetworkRequestQueue(int timeout, Time time, int capacity) {
    this.timeout = timeout;
    this.time = time;
    this.queue = new LinkedBlockingQueue<>(capacity);
  }

  @Override
  public boolean offer(NetworkRequest request) throws InterruptedException {
    return queue.offer(request);
  }

  @Override
  public NetworkRequest take() throws InterruptedException {
    return queue.take();
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

  @Override
  public boolean isExpired(NetworkRequest request) {
    if (request.equals(EmptyRequest.getInstance())) {
      // This is an internal generated request used to signal shutdown. Don't consider it as expired.
      return false;
    }
    return time.milliseconds() - request.getStartTimeInMs() > timeout;
  }
}
