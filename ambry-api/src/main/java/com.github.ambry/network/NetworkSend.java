/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;


/**
 * Contains the destination information and bytes to send
 */
public class NetworkSend {
  /**
   * The Id of the connection to which the bytes need to be sent
   */
  private final String connectionId;

  /**
   * The bytes to be sent over the connection
   */
  private final Send payload;

  /**
   * The start time of this send
   */
  private final long sendStartTimeInMs;

  private final NetworkRequestMetrics metrics;

  public NetworkSend(String connectionId, Send payload, NetworkRequestMetrics metrics, Time time) {
    this.connectionId = connectionId;
    this.payload = payload;
    this.sendStartTimeInMs = time.milliseconds();
    this.metrics = metrics;
  }

  public long getSendStartTimeInMs() {
    return sendStartTimeInMs;
  }

  public String getConnectionId() {
    return connectionId;
  }

  public Send getPayload() {
    return payload;
  }

  public void onSendComplete() {
    if (metrics != null) {
      metrics.updateResponseSendTime(SystemTime.getInstance().milliseconds() - sendStartTimeInMs);
    }
  }
}
