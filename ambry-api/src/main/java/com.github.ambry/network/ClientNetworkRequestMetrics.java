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
package com.github.ambry.network;

import com.codahale.metrics.Histogram;


/**
 * Tracks a set of metrics for a network request by a Client
 */
public class ClientNetworkRequestMetrics extends NetworkSendMetrics {
  private final Histogram requestQueueTime;
  private final Histogram requestTotalTime;
  private long timeSpentTillNow;

  public ClientNetworkRequestMetrics(Histogram requestQueueTime, Histogram requestSendTime, Histogram requestTotalTime,
      long timeSpentTillNow) {
    super(requestSendTime);
    this.requestQueueTime = requestQueueTime;
    this.requestTotalTime = requestTotalTime;
    this.timeSpentTillNow = timeSpentTillNow;
  }

  /**
   * Updates the time spent by the request in the queue before being sent out
   * @param value the time spent by the request in the queue
   */
  public void updateQueueTime(long value) {
    requestQueueTime.update(value);
    timeSpentTillNow += value;
  }

  /**
   * Updates few metrics once the send completes
   * @param value the time spent by the request to be completely sent out
   */
  public void updateSendTime(long value) {
    super.updateSendTime(value);
    timeSpentTillNow += value;
    requestTotalTime.update(timeSpentTillNow);
  }
}
