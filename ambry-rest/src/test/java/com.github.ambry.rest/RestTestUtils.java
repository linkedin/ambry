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
package com.github.ambry.rest;

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.CopyingAsyncWritableChannel;
import com.github.ambry.router.ReadableStreamChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import org.json.JSONObject;


/**
 * Some common utilities used for tests in rest package
 */
public class RestTestUtils {

  /**
   * Creates a {@link HttpRequest} with the given parameters.
   * @param httpMethod the {@link HttpMethod} required.
   * @param uri the URI to hit.
   * @return a {@link HttpRequest} with the given parameters.
   */
  public static HttpRequest createRequest(HttpMethod httpMethod, String uri, HttpHeaders headers) {
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri);
    if (headers != null) {
      httpRequest.headers().set(headers);
    }
    return httpRequest;
  }

  /**
   * Converts the content in {@code httpContent} to a human readable string.
   * @param httpContent the {@link HttpContent} whose content needs to be converted to a human readable string.
   * @return content that is inside {@code httpContent} as a human readable string.
   * @throws IOException
   */
  public static String getContentString(HttpContent httpContent) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    httpContent.content().readBytes(out, httpContent.content().readableBytes());
    return out.toString(StandardCharsets.UTF_8.name());
  }

  /**
   * Build the range header value from a {@link ByteRange}
   * @param range the {@link ByteRange} representing the range
   * @return the range header value corresponding to {@code range}.
   */
  public static String getRangeHeaderString(ByteRange range) {
    switch (range.getType()) {
      case LAST_N_BYTES:
        return "bytes=-" + range.getLastNBytes();
      case FROM_START_OFFSET:
        return "bytes=" + range.getStartOffset() + "-";
      default:
        return "bytes=" + range.getStartOffset() + "-" + range.getEndOffset();
    }
  }

  /**
   * @return an {@link SSLFactory} for use in rest unit tests.
   */
  static SSLFactory getTestSSLFactory() {
    try {
      File trustStoreFile = File.createTempFile("truststore", ".jks");
      trustStoreFile.deleteOnExit();
      return new SSLFactory(
          new SSLConfig(TestSSLUtils.createSslProps("", SSLFactory.Mode.SERVER, trustStoreFile, "frontend")));
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Reads the response received from the {@code channel} and decodes it into a {@link JSONObject}.
   * @param channel the {@link ReadableStreamChannel} that contains the response
   * @return the response decoded into a {@link JSONObject}.
   * @throws Exception
   */
  public static JSONObject getJsonizedResponseBody(ReadableStreamChannel channel) throws Exception {
    CopyingAsyncWritableChannel asyncWritableChannel = new CopyingAsyncWritableChannel((int) channel.getSize());
    channel.readInto(asyncWritableChannel, null).get();
    return new JSONObject(new String(asyncWritableChannel.getData()));
  }
}
