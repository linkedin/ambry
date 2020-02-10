/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A wrapper class at the network layer for NettyRequest based RestRequest.
 */
public class NettyServerRequest implements NetworkRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyServerRequest.class);
  private final InputStream inputStream;
  private final RestResponseChannel restResponseChannel;
  private final RestRequest restRequest;
  private final long startTimeInMs;

  public NettyServerRequest(RestRequest restRequest, RestResponseChannel restResponseChannel, InputStream inputStream) {
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.inputStream = inputStream;
    this.startTimeInMs = SystemTime.getInstance().milliseconds();
  }

  @Override
  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  @Override
  public void release() {
    try {
      inputStream.close();
    } catch (IOException e) {
      LOGGER.warn("Exception closing request's InputStream", e);
    }
  }

  public RestRequest getRestRequest() {
    return restRequest;
  }

  public RestResponseChannel getRestResponseChannel() {
    return restResponseChannel;
  }
}

