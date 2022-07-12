/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network.http2;

import com.github.ambry.commons.Callback;
import com.github.ambry.network.Send;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.utils.AbstractByteBufHolder;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;


public class HttpResponseSendAdapter extends AbstractByteBufHolder<HttpResponseSendAdapter> implements Send {

  private final FullHttpResponse response;

  public HttpResponseSendAdapter(FullHttpResponse response) {
    this.response = response;
  }

  HttpHeaders getHeaders() {
    return response.headers();
  }

  HttpResponseStatus getStatus() {
    return response.status();
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    return 0;
  }

  @Override
  public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {
    Send.super.writeTo(channel, callback);
  }

  @Override
  public boolean isSendComplete() {
    return false;
  }

  @Override
  public long sizeInBytes() {
    return 0;
  }

  @Override
  public ByteBuf content() {
    return response.content();
  }

  @Override
  public HttpResponseSendAdapter replace(ByteBuf content) {
    return null;
  }
}
