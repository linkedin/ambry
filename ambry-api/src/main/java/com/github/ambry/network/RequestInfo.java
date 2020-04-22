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

import com.github.ambry.clustermap.ReplicaId;


/**
 * A class that consists of a request to be sent over the network in the form of {@link Send}, and a destination for it
 * in the form of a host and a {@link Port}.
 */
public class RequestInfo {
  private final String host;
  private final Port port;
  private final SendWithCorrelationId request;
  private final ReplicaId replicaId;
  private long streamSendTime = -1;
  private long streamHeaderFrameReceiveTime = -1;
  public int responseFramesCount = 0;

  /**
   * Construct a RequestInfo with the given parameters
   * @param host the host to which the data is meant for
   * @param port the port on the host to which the data is meant for
   * @param request the data to be sent.
   * @param replicaId the {@link ReplicaId} associated with this request
   */
  public RequestInfo(String host, Port port, SendWithCorrelationId request, ReplicaId replicaId) {
    this.host = host;
    this.port = port;
    this.request = request;
    this.replicaId = replicaId;
  }

  /**
   * @return the host of the destination for the data associated with this object.
   */
  public String getHost() {
    return host;
  }

  /**
   * @return the {@link Port} of the destination for the data associated with this object.
   */
  public Port getPort() {
    return port;
  }

  /**
   * @return the request in the form of {@link Send} associated with this object.
   */
  public SendWithCorrelationId getRequest() {
    return request;
  }

  /**
   * @return the {@link ReplicaId} associated with this request.
   */
  public ReplicaId getReplicaId() {
    return replicaId;
  }

  public long getStreamHeaderFrameReceiveTime() {
    return streamHeaderFrameReceiveTime;
  }

  public void setStreamHeaderFrameReceiveTime(long streamHeaderFrameReceiveTime) {
    this.streamHeaderFrameReceiveTime = streamHeaderFrameReceiveTime;
  }

  public long getStreamSendTime() {
    return streamSendTime;
  }

  public void setStreamSendTime(long streamSendTime) {
    this.streamSendTime = streamSendTime;
  }

  @Override
  public String toString() {
    return "RequestInfo{" + "host='" + host + '\'' + ", port=" + port + ", request=" + request + ", replicaId="
        + replicaId + '}';
  }
}
