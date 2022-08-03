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
import com.github.ambry.quota.Chargeable;


/**
 * A class that consists of a request to be sent over the network in the form of {@link Send}, and a destination for it
 * in the form of a host and a {@link Port}.
 */
public class RequestInfo {
  private final String host;
  private final Port port;
  private final SendWithCorrelationId request;
  private final ReplicaId replicaId;
  private final Chargeable chargeable;
  private final long requestCreateTime;
  private long requestEnqueueTime = -1;
  private long requestSendTime = -1;
  private long responseHeaderReceiveTime = -1;
  // Determines the time to wait for response once it is sent to server
  private long networkTimeOutMs;
  // Determines the time to wait for response once it is created. It is possible that request remains in router queues
  // due to insufficient quota. This time out helps the request to avoid being stuck in queues for a long time and
  // also enables to send response to client in a bounded time.
  private final long finalTimeOutMs;
  public int responseFramesCount = 0;

  /**
   * Construct a RequestInfo with the given parameters
   * @param host the host to which the data is meant for
   * @param port the port on the host to which the data is meant for
   * @param request the data to be sent.
   * @param replicaId the {@link ReplicaId} associated with this request.
   * @param chargeable the {@link Chargeable} associated with this request.
   * @param creationTime the creation time of this request in msec.
   * @param networkTimeOutMs the time in msec to wait for response from server.
   * @param finalTimeOutMs the overall wait time for a request in router.
   */
  public RequestInfo(String host, Port port, SendWithCorrelationId request, ReplicaId replicaId, Chargeable chargeable,
      long creationTime, long networkTimeOutMs, long finalTimeOutMs) {
    this.host = host;
    this.port = port;
    this.request = request;
    this.replicaId = replicaId;
    this.chargeable = chargeable;
    this.requestCreateTime = creationTime;
    this.networkTimeOutMs = networkTimeOutMs;
    this.finalTimeOutMs = finalTimeOutMs;
  }

  /**
   * Construct a RequestInfo with the given parameters. This is used only for tests.
   * @param host the host to which the data is meant for
   * @param port the port on the host to which the data is meant for
   * @param request the data to be sent.
   * @param replicaId the {@link ReplicaId} associated with this request.
   * @param chargeable the {@link Chargeable} associated with this request.
   */
  public RequestInfo(String host, Port port, SendWithCorrelationId request, ReplicaId replicaId,
      Chargeable chargeable) {
    this(host, port, request, replicaId, chargeable, System.currentTimeMillis(), -1, -1);
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
   * @return the {@link Chargeable} associated with this request.
   */
  public Chargeable getChargeable() {
    return chargeable;
  }

  /**
   * @return the {@link ReplicaId} associated with this request.
   */
  public ReplicaId getReplicaId() {
    return replicaId;
  }

  /**
   * @return time in msec at which this request was created.
   */
  public long getRequestCreateTime() {
    return requestCreateTime;
  }

  /**
   * Set the time at which request was received and enqueued at network layer.
   * @param requestEnqueueTime time in msec.
   */
  public void setRequestEnqueueTime(long requestEnqueueTime) {
    this.requestEnqueueTime = requestEnqueueTime;
  }

  /**
   * @return the time in msec at which request was received and enqueued at network layer. If it is -1, it means that
   * network layer didn't receive this request yet from router.
   */
  public long getRequestEnqueueTime() {
    return requestEnqueueTime;
  }

  /**
   * Set the time in msec at which request was sent out.
   * @param requestSendTime in msec.
   */
  public void setRequestSendTime(long requestSendTime) {
    this.requestSendTime = requestSendTime;
  }

  /**
   * @return the time in msec at which request was sent out. If it is -1, it means that request is not yet sent out of
   * network layer.
   */
  public long getRequestSendTime() {
    return requestSendTime;
  }

  /**
   * Set the time in msec at which response header is received. For HTTP2, this is the time at which first response
   * header frame is received.
   * @param responseHeaderReceiveTime in msec.
   */
  public void setResponseHeaderReceiveTime(long responseHeaderReceiveTime) {
    this.responseHeaderReceiveTime = responseHeaderReceiveTime;
  }

  /**
   * @return the time in msec at which response header is received. For HTTP2, this is the time at which first response
   * header frame is received. If it is -1, it means that response is not yet received.
   */
  public long getResponseHeaderReceiveTime() {
    return responseHeaderReceiveTime;
  }

  /**
   * @return {@code True} if request has been received by network layer.
   */
  public boolean isRequestReceivedByNetworkLayer() {
    return requestEnqueueTime != -1;
  }

  @Override
  public String toString() {
    return "RequestInfo{" + "host='" + host + '\'' + ", port=" + port + ", request=" + request + ", replicaId="
        + replicaId + '}';
  }

  /**
   * @return the time to wait for response once this request is sent to server.
   */
  public long getNetworkTimeOutMs() {
    return networkTimeOutMs;
  }

  /**
   * Increases the time to wait for response from server.
   * @param delta time in milliseconds.
   */
  public void incrementNetworkTimeOutMs(long delta) {
    networkTimeOutMs += delta;
  }

  /**
   * @return the overall wait time for this request in router.
   */
  public long getFinalTimeOutMs() {
    return finalTimeOutMs;
  }
}
