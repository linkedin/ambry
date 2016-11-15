/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.SystemTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * FixedBackoffResourceStatePolicy marks a resource as unavailable for retryBackoff milliseconds if the number of
 * consecutive errors the resource encountered is greater than failureCountThreshold.
 */
class FixedBackoffResourceStatePolicy implements ResourceStatePolicy {
  private final Object resource;
  private final boolean hardDown;
  private final AtomicInteger failureCount;
  private final int failureCountThreshold;
  private final long retryBackoffMs;
  private AtomicLong downUntil;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public FixedBackoffResourceStatePolicy(Object resource, boolean hardDown, int failureCountThreshold,
      long retryBackoffMs) {
    this.resource = resource;
    this.hardDown = hardDown;
    this.failureCountThreshold = failureCountThreshold;
    this.retryBackoffMs = retryBackoffMs;
    this.downUntil = new AtomicLong(0);
    this.failureCount = new AtomicInteger(0);
  }

  /*
   * On an error, if the failureCount is greater than the threshold, mark the node as down.
   */
  @Override
  public void onError() {
    int count = failureCount.incrementAndGet();
    if (count >= failureCountThreshold) {
      if (count == failureCountThreshold) {
        logger.error("Resource " + resource + " has gone down");
      }
      logger.trace("Resource {} remains in down state at time {}; adding downtime of {} ms", resource,
          SystemTime.getInstance().milliseconds(), retryBackoffMs);
      downUntil.set(SystemTime.getInstance().milliseconds() + retryBackoffMs);
    }
  }

  /*
   * A single response resets the count.
   */
  @Override
  public void onSuccess() {
    if (failureCount.getAndSet(0) >= failureCountThreshold) {
      logger.info("Resource " + resource + " is back up");
    }
  }

  /*
   * If the number of failures are above the threshold, the resource will be counted as down unless downUntil is in
   * the past.
   * Note how failureCount is not reset to 0 here. This is so that the node is marked as down if the first request after
   * marking a node back up, also times out. We only reset failureCount on an actual response, so down nodes get a
   * 'chance' to prove they are back up every retryBackoffMs - they do not get 'fully up' status until they are actually
   * responsive.
   */
  @Override
  public boolean isDown() {
    boolean down = false;
    if (hardDown) {
      down = true;
    } else if (failureCount.get() >= failureCountThreshold) {
      if (SystemTime.getInstance().milliseconds() < downUntil.get()) {
        down = true;
      }
    }

    if (down) {
      logger.trace(
          "Resource {} is down; failureCount: {}; failureCountThreshold: {}; remaining time: {}; hard down: {}",
          resource, failureCount.get(), failureCountThreshold,
          downUntil.get() - SystemTime.getInstance().milliseconds(), hardDown);
    } else {
      logger.trace("Resource {} is not down; failureCount: {}; failureCountThreshold: {}", resource, failureCount.get(),
          failureCountThreshold);
    }
    return down;
  }

  @Override
  public boolean isHardDown() {
    return hardDown;
  }
}
