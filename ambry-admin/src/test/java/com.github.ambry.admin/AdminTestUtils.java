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
package com.github.ambry.admin;

import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.router.CopyingAsyncWritableChannel;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Utility functions for Admin tests.
 */
public class AdminTestUtils {

  /**
   * Method to easily create {@link RestRequest} objects containing a specific request.
   * @param restMethod the {@link RestMethod} desired.
   * @param uri string representation of the desired URI.
   * @param headers any associated headers as a {@link JSONObject}.
   * @param contents the content that accompanies the request.
   * @return A {@link RestRequest} object that defines the request required by the input.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  static RestRequest createRestRequest(RestMethod restMethod, String uri, JSONObject headers, List<ByteBuffer> contents)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod);
    request.put(MockRestRequest.URI_KEY, uri);
    if (headers != null) {
      request.put(MockRestRequest.HEADERS_KEY, headers);
    }
    return new MockRestRequest(request, contents);
  }

  /**
   * Reads the response received from the {@code channel} and decodes it into a {@link JSONObject}.
   * @param channel the {@link ReadableStreamChannel} that contains the response
   * @return the response decoded into a {@link JSONObject}.
   * @throws Exception
   */
  static JSONObject getJsonizedResponseBody(ReadableStreamChannel channel) throws Exception {
    CopyingAsyncWritableChannel asyncWritableChannel = new CopyingAsyncWritableChannel((int) channel.getSize());
    channel.readInto(asyncWritableChannel, null).get();
    return new JSONObject(new String(asyncWritableChannel.getData()));
  }
}
