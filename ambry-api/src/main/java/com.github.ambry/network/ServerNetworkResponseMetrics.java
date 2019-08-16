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
 * Tracks a set of metrics for a network response by a Server
 */
public class ServerNetworkResponseMetrics {

  private final Histogram responseQueueTime;
  private final Histogram responseSendTime;
  private final Histogram responseTotalTime;
  private final Histogram responseSendTimeBySize;
  private final Histogram responseTotalTimeBySize;
  private long timeSpentTillNow;

  public ServerNetworkResponseMetrics(Histogram responseQueueTime, Histogram responseSendTime,
      Histogram responseTotalTime, Histogram responseSendTimeBySize, Histogram responseTotalTimeBySize,
      long timeSpentTillNow) {
    this.responseQueueTime = responseQueueTime;
    this.responseSendTime = responseSendTime;
    this.responseTotalTime = responseTotalTime;
    this.responseSendTimeBySize = responseSendTimeBySize;
    this.responseTotalTimeBySize = responseTotalTimeBySize;
    this.timeSpentTillNow = timeSpentTillNow;
  }

  /**
   * Updates the time spent by the response in the queue before being sent out
   * @param value the time spent by the response in the queue before being sent out
   */
  public void updateQueueTime(long value) {
    responseQueueTime.update(value);
    timeSpentTillNow += value;
  }

  /**
   * Updates few metrics when send completes
   * @param value the time spent by the response to be completely sent
   */
  public void updateSendTime(long value) {
    responseSendTime.update(value);
    if (responseSendTimeBySize != null) {
      responseSendTimeBySize.update(value);
    }
    timeSpentTillNow += value;
    responseTotalTime.update(timeSpentTillNow);
    if (responseTotalTimeBySize != null) {
      responseTotalTimeBySize.update(timeSpentTillNow);
    }
  }
}
