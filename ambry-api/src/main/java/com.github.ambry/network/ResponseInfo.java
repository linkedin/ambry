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
import io.netty.util.ReferenceCountUtil;


/**
 * The response from a {@link NetworkClient} comes in the form of an object of this class.
 * This class consists of the request associated with this response, along with either a non-null exception if there
 * was an error sending the request or a non-null ByteBuffer containing the successful response received for this
 * request. Also, this class contains {@link DataNodeId} to which the request is issued.
 */
public class ResponseInfo {
  private final RequestInfo requestInfo;
  private final NetworkClientErrorCode error;
  private final Object response;
  private final DataNodeId dataNode;

  /**
   * Constructs a ResponseInfo with the given parameters.
   * @param requestInfo the {@link RequestInfo} associated with this response.
   * @param error the error encountered in sending this request, if there is any.
   * @param response the response received for this request.
   */
  public ResponseInfo(RequestInfo requestInfo, NetworkClientErrorCode error, Object response) {
    this(requestInfo, error, response, requestInfo == null ? null : requestInfo.getReplicaId().getDataNodeId());
  }

  public ResponseInfo(RequestInfo requestInfo, NetworkClientErrorCode error, Object response, DataNodeId dataNode) {
    this.requestInfo = requestInfo;
    this.error = error;
    this.response = response;
    this.dataNode = dataNode;
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
   * @return the response received for this request.
   */
  public Object getResponse() {
    return response;
  }

  /**
   * Increase the reference count of underlying response.
   */
  public void retain() {
    if (response != null) {
      ReferenceCountUtil.retain(response);
    }
  }

  /**
   * Decrease the reference count of underlying response.
   */
  public void release() {
    if (response != null) {
      ReferenceCountUtil.release(response);
    }
  }

  /**
   * @return the {@link DataNodeId} with which the response is associated.
   */
  public DataNodeId getDataNode() {
    return dataNode;
  }

  @Override
  public String toString() {
    return "ResponseInfo{" + "requestInfo=" + requestInfo + ", error=" + error + ", response=" + response
        + ", dataNode=" + dataNode + '}';
  }
}
