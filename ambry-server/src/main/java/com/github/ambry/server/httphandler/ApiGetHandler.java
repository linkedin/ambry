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
package com.github.ambry.server.httphandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Http Handler to return all the supported endpoints.
 */
public class ApiGetHandler implements Handler {
  private static final Logger logger = LoggerFactory.getLogger(ApiGetHandler.class);
  private final Map<String, String> descriptions = new HashMap<>();
  private final ByteBuffer jsonSerializedDescriptions;

  public ApiGetHandler() {
    descriptions.put("POST:/", "handle frontend and replication requests");
    descriptions.put("GET:/api", "return all supported endpoints");

    ByteBuffer serialized = null;
    try {
      String content = new ObjectMapper().writeValueAsString(descriptions);
      byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
      serialized = ByteBuffer.allocate(contentBytes.length);
      serialized.put(contentBytes);
      serialized.flip();
    } catch (Exception e) {
      logger.error("Failed to serialize description map to json", e);
    }
    jsonSerializedDescriptions = serialized;
  }

  @Override
  public FullHttpResponse handle(FullHttpRequest request, FullHttpResponse defaultResponse) {
    String acceptedEncoding = request.headers().get(HttpHeaderNames.ACCEPT_ENCODING);
    if (acceptedEncoding != null && !acceptedEncoding.isEmpty() && !acceptedEncoding.toLowerCase()
        .contains("application/json")) {
      defaultResponse.setStatus(HttpResponseStatus.BAD_REQUEST);
      return defaultResponse;
    }
    defaultResponse.headers().set(HttpHeaderNames.CONTENT_ENCODING, "application/json");
    defaultResponse.headers()
        .set(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(jsonSerializedDescriptions.remaining()));
    return defaultResponse.replace(Unpooled.wrappedBuffer(jsonSerializedDescriptions.duplicate()));
  }

  @Override
  public HttpMethod supportedMethod() {
    return HttpMethod.GET;
  }

  @Override
  public boolean match(FullHttpRequest request) {
    return request.uri().equals("/api");
  }
}
