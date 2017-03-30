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
public class ServerNetworkResponseMetrics extends NetworkSendMetrics {

  private final Histogram responseQueueTime;
  private final Histogram responseTotalTime;
  private final Histogram responseSendTimeBySize;
  private final Histogram responseTotalTimeBySize;
  private long timeSpentTillNow;
  private final long blobSize;
  private final Histogram responseTotalTimePer100Kb;

  public ServerNetworkResponseMetrics(Histogram responseQueueTime, Histogram responseSendTime,
      Histogram responseTotalTime, Histogram responseSendTimeBySize, Histogram responseTotalTimeBySize,
      long timeSpentTillNow, long blobSize, Histogram responseTotalTimePer100Kb) {
    super(responseSendTime);
    this.responseQueueTime = responseQueueTime;
    this.responseTotalTime = responseTotalTime;
    this.responseSendTimeBySize = responseSendTimeBySize;
    this.responseTotalTimeBySize = responseTotalTimeBySize;
    this.timeSpentTillNow = timeSpentTillNow;
    this.blobSize = blobSize;
    this.responseTotalTimePer100Kb = responseTotalTimePer100Kb;
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
  @Override
  public void updateSendTime(long value) {
    super.updateSendTime(value);
    if (responseSendTimeBySize != null) {
      responseSendTimeBySize.update(value);
    }
    timeSpentTillNow += value;
    responseTotalTime.update(timeSpentTillNow);
    if (responseTotalTimeBySize != null) {
      responseTotalTimeBySize.update(timeSpentTillNow);
    }
    if (responseTotalTimePer100Kb != null && blobSize > 100 * 1024) {
      responseTotalTimePer100Kb.update(timeSpentTillNow / (100 * 1024));
    }
  }
}
