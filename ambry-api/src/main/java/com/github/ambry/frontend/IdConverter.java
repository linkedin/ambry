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
package com.github.ambry.frontend;

import com.github.ambry.commons.CallbackUtils;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.commons.Callback;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;


/**
 * This is a service that can be used to convert IDs across different formats.
 * </p>
 * Typical usage will be to add extensions, tailor IDs for different use-cases, encrypt/obfuscate IDs or get name
 * mappings.
 */
public interface IdConverter extends Closeable {

  /**
   * Converts an ID.
   * @param restRequest {@link RestRequest} representing the request.
   * @param input the ID that needs to be converted.
   * @param callback the {@link Callback} to invoke once the converted ID is available. Can be null.
   * @return a {@link Future} that will eventually contain the converted ID.
   */
  Future<String> convert(RestRequest restRequest, String input, Callback<String> callback);

  /**
   * Converts an ID.
   * @param restRequest {@link RestRequest} representing the request.
   * @param input the ID that needs to be converted.
   * @return a {@link CompletableFuture} that will eventually contain the converted ID.
   */
  default CompletableFuture<String> convert(RestRequest restRequest, String input) {
    CompletableFuture<String> future = new CompletableFuture<>();
    convert(restRequest, input, CallbackUtils.fromCompletableFuture(future));
    return future;
  }

  /**
   * Converts an ID.
   * @param restRequest {@link RestRequest} representing the request.
   * @param input the ID that needs to be converted.
   * @param blobInfo the {@link BlobInfo} for an uploaded blob. Can be null for non-upload use cases.
   * @param callback the {@link Callback} to invoke once the converted ID is available. Can be null.
   * @return a {@link Future} that will eventually contain the converted ID.
   */
  default Future<String> convert(RestRequest restRequest, String input, BlobInfo blobInfo, Callback<String> callback) {
    return convert(restRequest, input, callback);
  }

  /**
   * Converts an ID.
   * @param restRequest {@link RestRequest} representing the request.
   * @param input the ID that needs to be converted.
   * @param blobInfo the {@link BlobInfo} for an uploaded blob. Can be null for non-upload use cases.
   * @return a {@link CompletableFuture} that will eventually contain the converted ID.
   */
  default CompletableFuture<String> convert(RestRequest restRequest, String input, BlobInfo blobInfo) {
    return convert(restRequest, input);
  }
}
