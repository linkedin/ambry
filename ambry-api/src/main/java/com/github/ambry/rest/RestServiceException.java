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

import java.util.Collections;
import java.util.Map;


/**
 * Exceptions thrown by different layers of the RESTful frontend. All exceptions are accompanied by a
 * {@link RestServiceErrorCode}.
 */
public class RestServiceException extends Exception {
  private final RestServiceErrorCode error;
  private final boolean includeExceptionMessageInResponse;
  private final boolean includeExceptionHeadersInResponse;
  private final Map<String, String> exceptionHeadersMap;

  /**
   * @param message the exception message.
   * @param error the {@link RestServiceErrorCode}.
   */
  public RestServiceException(String message, RestServiceErrorCode error) {
    this(message, error, false, false, Collections.emptyMap());
  }

  /**
   * @param message the exception message.
   * @param e the exception cause.
   * @param error the {@link RestServiceErrorCode}.
   */
  public RestServiceException(String message, Throwable e, RestServiceErrorCode error) {
    super(message, e);
    this.error = error;
    includeExceptionMessageInResponse = false;
    includeExceptionHeadersInResponse = false;
    exceptionHeadersMap = Collections.emptyMap();
  }

  /**
   * @param e the exception cause.
   * @param error the {@link RestServiceErrorCode}.
   */
  public RestServiceException(Throwable e, RestServiceErrorCode error) {
    super(e);
    this.error = error;
    includeExceptionMessageInResponse = false;
    includeExceptionHeadersInResponse = false;
    exceptionHeadersMap = Collections.emptyMap();
  }

  /**
   * @param message the exception message.
   * @param error the {@link RestServiceErrorCode}.
   * @param includeExceptionMessageInResponse {@code true} to hint that the exception message should be returned to the
   *                                          client as a response header.
   * @param includeExceptionHeadersInResponse {@code true} to hint the exception metadata should be included in the
   *                                                       response headers.
   * @param exceptionHeadersMap {@link Map} of exception metadata containing response headers as keys and corresponding values.
   */
  public RestServiceException(String message, RestServiceErrorCode error, boolean includeExceptionMessageInResponse,
      boolean includeExceptionHeadersInResponse, Map<String, String> exceptionHeadersMap) {
    super(message);
    this.error = error;
    this.includeExceptionMessageInResponse = includeExceptionMessageInResponse;
    this.includeExceptionHeadersInResponse = includeExceptionHeadersInResponse;
    if (includeExceptionHeadersInResponse) {
      this.exceptionHeadersMap = Collections.unmodifiableMap(exceptionHeadersMap);
    } else {
      this.exceptionHeadersMap = Collections.emptyMap();
    }
  }

  public RestServiceErrorCode getErrorCode() {
    return error;
  }

  public boolean shouldIncludeExceptionMessageInResponse() {
    return includeExceptionMessageInResponse;
  }

  public boolean shouldIncludeExceptionMetadataInResponse() {
    return includeExceptionHeadersInResponse;
  }

  public Map<String, String> getExceptionHeadersMap() {
    return exceptionHeadersMap;
  }
}
