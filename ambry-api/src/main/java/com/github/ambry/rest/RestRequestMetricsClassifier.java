/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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

/**
 * Classifies request-size semantics for transport metrics.
 * <p/>
 * Implementations are called synchronously after request termination is observed, either on the network event loop or
 * a response-processing thread. They must be thread-safe, must not block or mutate the request, and classification must
 * depend only on request metadata established before handling.
 */
public interface RestRequestMetricsClassifier {
  /**
   * Describes what the size declared by one HTTP request represents.
   */
  enum RequestSizeCategory {
    WHOLE_BLOB,
    CHUNK,
    MULTIPART_PART,
    OTHER
  }

  /**
   * @param restRequest the request to classify.
   * @return the request's size category. Must not return {@code null}.
   */
  RequestSizeCategory classifyRequestSize(RestRequest restRequest);
}
