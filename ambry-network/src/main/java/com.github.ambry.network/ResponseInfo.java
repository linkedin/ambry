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

import java.nio.ByteBuffer;


/**
 * The response from a {@link NetworkClient} comes in the form of an object of this class.
 * This class consists of the request associated with this response, along with either a non-null exception if there
 * was an error sending the request or a non-null ByteBuffer containing the successful response received for this
 * request.
 */
public class ResponseInfo {
  private final RequestInfo requestInfo;
  private final NetworkClientErrorCode error;
  private final ByteBuffer response;

  /**
   * Constructs a ResponseInfo with the given parameters.
   * @param requestInfo the {@link RequestInfo} associated with this response.
   * @param error the error encountered in sending this request, if there is any.
   * @param response the response received for this request.
   */
  public ResponseInfo(RequestInfo requestInfo, NetworkClientErrorCode error, ByteBuffer response) {
    this.requestInfo = requestInfo;
    this.error = error;
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
   * @return the response received for this request.
   */
  public ByteBuffer getResponse() {
    return response;
  }
}
