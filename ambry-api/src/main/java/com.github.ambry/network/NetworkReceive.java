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

import com.github.ambry.utils.Time;


/**
 * Contains the information about the initial send and the response bytes from the destination
 */
public class NetworkReceive {
  /**
   * The connection Id which is used for this request
   */
  private final String connectionId;
  /**
   * The bytes received from the destination
   */
  private final BoundedNettyByteBufReceive receivedBytes;

  /**
   * The start time of when the receive started
   */
  private final long receiveStartTimeInMs;

  public NetworkReceive(String connectionId, BoundedNettyByteBufReceive receivedBytes, Time time) {
    this.connectionId = connectionId;
    this.receivedBytes = receivedBytes;
    this.receiveStartTimeInMs = time.milliseconds();
  }

  public String getConnectionId() {
    return connectionId;
  }

  public BoundedNettyByteBufReceive getReceivedBytes() {
    return receivedBytes;
  }

  public long getReceiveStartTimeInMs() {
    return receiveStartTimeInMs;
  }
}
