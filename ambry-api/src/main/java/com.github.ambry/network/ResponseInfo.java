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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;


/**
 * The response from a {@link NetworkClient} comes in the form of an object of this class.
 * This class consists of the request associated with this response, along with either a non-null exception if there
 * was an error sending the request or a non-null ByteBuffer containing the successful response received for this
 * request. Also, this class contains {@link DataNodeId} to which the request is issued.
 */
public class ResponseInfo implements ByteBufHolder {
  private final RequestInfo requestInfo;
  private final NetworkClientErrorCode error;
  private final DataNodeId dataNode;
  private ByteBuf content;

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

  @Override
  public String toString() {
    return "ResponseInfo{" + "requestInfo=" + requestInfo + ", error=" + error + ", response=" + content + ", dataNode="
        + dataNode + '}';
  }

  @Override
  public ByteBuf content() {
    return content;
  }

  @Override
  public ResponseInfo copy() {
    return replace(content().copy());
  }

  @Override
  public ResponseInfo duplicate() {
    return replace(content().duplicate());
  }

  @Override
  public ResponseInfo retainedDuplicate() {
    return replace(content().retainedDuplicate());
  }

  @Override
  public ResponseInfo replace(ByteBuf content) {
    ResponseInfo info = new ResponseInfo(requestInfo, error, content, dataNode);
    return info;
  }

  @Override
  public int refCnt() {
    return content.refCnt();
  }

  @Override
  public ResponseInfo retain() {
    content.retain();
    return this;
  }

  @Override
  public ResponseInfo retain(int increment) {
    content.retain(increment);
    return this;
  }

  @Override
  public ResponseInfo touch() {
    if (content != null) {
      content.touch();
    }
    return this;
  }

  @Override
  public ResponseInfo touch(Object hint) {
    if (content != null) {
      content.touch(hint);
    }
    return this;
  }

  @Override
  public boolean release() {
    if (content != null) {
      return content.release();
    } else {
      return false;
    }
  }

  @Override
  public boolean release(int decrement) {
    if (content != null) {
      return content.release(decrement);
    } else {
      return false;
    }
  }
}
