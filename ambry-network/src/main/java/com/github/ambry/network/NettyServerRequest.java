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
import com.github.ambry.utils.AbstractByteBufHolder;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.ByteBuf;
import java.io.InputStream;


/**
 * A wrapper class at the network layer for NettyRequest based RestRequest.
 */
public class NettyServerRequest extends AbstractByteBufHolder<NettyServerRequest> implements NetworkRequest {
  private final InputStream inputStream;
  private final RestResponseChannel restResponseChannel;
  private final RestRequest restRequest;
  private final long startTimeInMs;
  private final ByteBuf content;

  public NettyServerRequest(RestRequest restRequest, RestResponseChannel restResponseChannel, ByteBuf content) {
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.content = content;
    this.inputStream = new NettyByteBufDataInputStream(content);
    this.startTimeInMs = System.currentTimeMillis();
  }

  @Override
  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  public RestRequest getRestRequest() {
    return restRequest;
  }

  public RestResponseChannel getRestResponseChannel() {
    return restResponseChannel;
  }

  @Override
  public ByteBuf content() {
    return content;
  }

  @Override
  public NettyServerRequest replace(ByteBuf content) {
    return new NettyServerRequest(getRestRequest(), getRestResponseChannel(), content);
  }
}

