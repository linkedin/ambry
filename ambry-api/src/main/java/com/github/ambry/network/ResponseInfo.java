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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.utils.AbstractByteBufHolder;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;


/**
 * The response from a {@link NetworkClient} comes in the form of an object of this class.
 * This class consists of the request associated with this response, along with either a non-null exception if there
 * was an error sending the request or a non-null ByteBuffer containing the successful response received for this
 * request. Also, this class contains {@link DataNodeId} to which the request is issued.
 */
public class ResponseInfo extends AbstractByteBufHolder<ResponseInfo> {
  private final RequestInfo requestInfo;
  private final NetworkClientErrorCode error;
  private final DataNodeId dataNode;
  /**
   * Response received from network in the form of serialized bytes.
   */
  private ByteBuf content;
  /**
   * Response received within the same process in the form of {@code Send} java object.
   */
  private Send response;

  /**
   * Constructs a ResponseInfo with the given parameters.
   * @param requestInfo the {@link RequestInfo} associated with this response.
   * @param error the error encountered in sending this request, if there is any.
   * @param content the response received for this request.
   */
  public ResponseInfo(RequestInfo requestInfo, NetworkClientErrorCode error, ByteBuf content) {
    this(requestInfo, error, content, requestInfo == null ? null : requestInfo.getReplicaId().getDataNodeId());
  }

  /**
   * Constructs a ResponseInfo with the given parameters.
   * @param requestInfo the {@link RequestInfo} associated with this response.
   * @param error the error encountered in sending this request, if there is any.
   * @param content the response received for this request.
   * @param dataNode the {@link DataNodeId} of this request.
   */
  public ResponseInfo(RequestInfo requestInfo, NetworkClientErrorCode error, ByteBuf content, DataNodeId dataNode) {
    this.requestInfo = requestInfo;
    this.error = error;
    this.content = content;
    this.dataNode = dataNode;
  }

  /**
   * Constructs a ResponseInfo with the given parameters. This is used when responses are received in the same process
   * via {@code LocalNetworkClient} in the form of {@link Send} objects instead of deserialized bytes.
   * @param requestInfo the {@link RequestInfo} associated with this response.
   * @param error the error encountered in sending this request, if there is any.
   * @param dataNode the {@link DataNodeId} of this request.
   * @param response response received in the form of {@link Send} implementation. This is used when to send and receive
   *                responses in the same process by using local queues.
   */
  public ResponseInfo(RequestInfo requestInfo, NetworkClientErrorCode error, DataNodeId dataNode, Send response) {
    this.requestInfo = requestInfo;
    this.error = error;
    this.content = null;
    this.dataNode = dataNode;
    this.response = response;
  }

  /**
   * @return the {@link RequestInfo} associated with this response.
   */
  public RequestInfo getRequestInfo() {
    return requestInfo;
  }

  /**
   * @return the error encountered in sending this request.
   */
  public NetworkClientErrorCode getError() {
    return error;
  }

  /**
   * @return the {@link DataNodeId} with which the response is associated.
   */
  public DataNodeId getDataNode() {
    return dataNode;
  }

  /**
   * @return response in the form of {@link Send} implementation. This is used to send and receive responses in the same
   * process using local queues.
   */
  public Send getResponse() {
    return response;
  }

  @Override
  public String toString() {
    return "ResponseInfo{requestInfo=" + requestInfo + ", error=" + error + ", response=" + content + ", dataNode="
        + dataNode + '}';
  }

  @Override
  public ByteBuf content() {
    return content;
  }

  @Override
  public ResponseInfo replace(ByteBuf content) {
    return new ResponseInfo(requestInfo, error, content, dataNode);
  }

  /**
   * Override the release method since we also need to release {@link ResponseInfo#response} in case it is not null.
   * @return
   */
  @Override
  public boolean release() {

    if (response != null) {
      ReferenceCountUtil.safeRelease(response);
      response = null;
    }

    if (content != null) {
      ReferenceCountUtil.safeRelease(content);
      content = null;
    }
    return false;
  }
}
