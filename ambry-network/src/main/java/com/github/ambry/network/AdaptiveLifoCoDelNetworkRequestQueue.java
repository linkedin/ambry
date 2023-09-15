/*
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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Adaptive LIFO blocking queue utilizing CoDel algorithm to prevent queue overloading. This queue is based on
 * (and borrows most of its code from) {@code AdaptiveLifoCoDelCallQueue} in HBase.
 *
 * @see <a href="http://queue.acm.org/detail.cfm?id=2839461">Fail at Scale paper</a>
 * @see <a href="https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/ipc/AdaptiveLifoCoDelCallQueue.java">HBase implementation</a>
 */
public class AdaptiveLifoCoDelNetworkRequestQueue implements NetworkRequestQueue {
  private static final Logger logger = LoggerFactory.getLogger(AdaptiveLifoCoDelNetworkRequestQueue.class);
  private final Time time;
  private final LinkedBlockingDeque<NetworkRequest> deque;

  // If queue is full more than this size, we switch to LIFO mode.
  private final int lifoThreshold;
  // Both are in milliseconds
  private final int coDelTargetDelayMs;
  private final int coDelIntervalMs;
  // minimal delay observed during the interval
  private volatile long minDelayMs;
  // the moment when current interval ends
  private volatile long intervalTimeMs;
  // switch to ensure only one thread does interval cutoffs
  private final AtomicBoolean resetDelay = new AtomicBoolean(true);
  // if we're in this mode, "long" calls are getting dropped
  private final AtomicBoolean isOverloaded = new AtomicBoolean(false);
  // Variable used to track changing of queue modes from FIFO to LIFO and vice versa.
  private final AtomicBoolean lifoMode = new AtomicBoolean(false);
  // Variable used to track if emptyRequestReceived was called on server. If emptyRequestReceived is received, this
  // indicates server is shutting down. Since request handler threads are waiting on this request, switch to LIFO mode.
  private final boolean emptyRequestReceived = false;

  /**
   * @param lifoThreshold      the fraction of capacity used at which to switch the queue from FIFO to LIFO mode.
   * @param coDelTargetDelayMs the target delay in ms to use for the controlled delay algorithm.
   * @param coDelIntervalMs    the target interval in ms to use for the controlled delay algorithm.
   * @param time               {@link Time} instance.
   * @param capacity           the capacity of the queue
   */
  AdaptiveLifoCoDelNetworkRequestQueue(int lifoThreshold, int coDelTargetDelayMs, int coDelIntervalMs, Time time,
      int capacity) {
    this.coDelTargetDelayMs = coDelTargetDelayMs;
    this.coDelIntervalMs = coDelIntervalMs;
    this.lifoThreshold = lifoThreshold;
    this.time = time;
    intervalTimeMs = time.milliseconds();
    deque = new LinkedBlockingDeque<>(capacity);
  }

  @Override
  public boolean offer(NetworkRequest request) throws InterruptedException {
    return deque.offer(request);
  }

  /**
   * @return the head of the queue if operating in LIFO mode. Else, returns the tail of the queue.
   */
  @Override
  public NetworkRequest take() throws InterruptedException {
    NetworkRequest nextRequest;
    if (useLifoMode()) {
      nextRequest = deque.pollLast();
    } else {
      nextRequest = deque.pollFirst();
    }
    if (nextRequest == null) {
      // If there are no requests in the queue, wait until a new request comes.
      nextRequest = deque.take();
    }
    return nextRequest;
  }

  @Override
  public int size() {
    return deque.size();
  }

  @Override
  public void close() {
    deque.forEach(NetworkRequest::release);
    deque.clear();
  }

  @Override
  public String toString() {
    return deque.toString();
  }

  /**
   * Based on queue size, decide whether the queue should operate in LIFO or FIFO mode.
   * @return {@code true} if the queue should operate in LIFO mode.
   */
  private boolean useLifoMode() {
    if (emptyRequestReceived) {
      // If emptyRequestReceived is received, this indicates server is shutting down. Since request handler threads are
      // waiting on this request, switch to LIFO mode.
      return true;
    }
    boolean localLifoMode = deque.size() > lifoThreshold;
    if (localLifoMode && lifoMode.compareAndSet(false, true)) {
      // Mode changed to LIFO
      logger.info("Request queue operating is LIFO mode");
    } else if (!localLifoMode && lifoMode.compareAndSet(true, false)) {
      // Mode changed to FIFO
      logger.info("Request queue operating is FIFO mode");
    }
    return localLifoMode;
  }

  /**
   * Controlled delay logic is implemented here.
   * @param request to validate
   * @return {@code true} if this request needs to be dropped to reduce overload based on request creation timestamp and
   *         internal queue state (deemed overloaded).
   */
  @Override
  public boolean isExpired(NetworkRequest request) {
    if (request.equals(EmptyRequest.getInstance())) {
      // This is a internal generated request used to signal shutdown. Don't consider it as expired.
      return false;
    }
    long currentTimeMs = time.milliseconds();
    long requestDelayMs = currentTimeMs - request.getStartTimeInMs();

    long localMinDelayMs = this.minDelayMs;

    // Try and determine if we should reset
    // the delay time and determine overload
    if (currentTimeMs > intervalTimeMs && resetDelay.compareAndSet(false, true)) {
      intervalTimeMs = currentTimeMs + coDelIntervalMs;

      isOverloaded.set(localMinDelayMs > coDelTargetDelayMs);
    }

    // If it looks like we should reset the delay
    // time do it only once on one thread
    if (resetDelay.compareAndSet(true, false)) {
      minDelayMs = requestDelayMs;
      return false;
    } else if (requestDelayMs < localMinDelayMs) {
      minDelayMs = requestDelayMs;
    }

    return isOverloaded.get() && requestDelayMs > 2 * coDelTargetDelayMs;
  }
}
