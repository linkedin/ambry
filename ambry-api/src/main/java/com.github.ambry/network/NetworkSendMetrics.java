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
 * Tracks send time metrics for a {@link NetworkSend}
 */
public class NetworkSendMetrics {
  private final Histogram sendTime;

  public NetworkSendMetrics(Histogram sendTime) {
    this.sendTime = sendTime;
  }

  /**
   * Updates sendTime when the networkSend has been sent out completely.
   * @param value the time spent by the request or response to be sent out completely
   */
  public void updateSendTime(long value) {
    sendTime.update(value);
  }
}
