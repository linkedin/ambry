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
 * Tracks a set of metrics for a {@link NetworkSend} by a Server
 */
public class ServerNetworkSendMetrics extends NetworkSendMetrics {

  private Histogram requestOrResponseSendTimeBySize;
  private Histogram requestOrResponseTotalTimeBySize;

  public ServerNetworkSendMetrics(Histogram requestOrResponseQueueTime, Histogram requestOrResponseSendTime,
      Histogram requestOrResponseTotalTime, Histogram requestOrResponseSendTimeBySize,
      Histogram requestOrResponseTotalTimeBySize, long timeSpentTillNow) {
    super(requestOrResponseQueueTime, requestOrResponseSendTime, requestOrResponseTotalTime, timeSpentTillNow);
    this.requestOrResponseSendTimeBySize = requestOrResponseSendTimeBySize;
    this.requestOrResponseTotalTimeBySize = requestOrResponseTotalTimeBySize;
  }

  /**
   * Updates the time spent by response to be completely sent
   * @param value the time spent by the request or response to be completely sent
   */
  @Override
  public void updateSendTime(long value) {
    super.updateSendTime(value);
    if (requestOrResponseSendTimeBySize != null) {
      requestOrResponseSendTimeBySize.update(value);
    }
    if (requestOrResponseTotalTimeBySize != null) {
      requestOrResponseTotalTimeBySize.update(timeSpentTillNow);
    }
  }
}
