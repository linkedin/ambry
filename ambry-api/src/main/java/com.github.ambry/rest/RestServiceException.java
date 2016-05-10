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

/**
 * Exceptions thrown by different layers of the RESTful frontend. All exceptions are accompanied by a
 * {@link RestServiceErrorCode}.
 */
public class RestServiceException extends Exception {
  private final RestServiceErrorCode error;

  public RestServiceException(String message, RestServiceErrorCode error) {
    super(message);
    this.error = error;
  }

  public RestServiceException(String message, Throwable e, RestServiceErrorCode error) {
    super(message, e);
    this.error = error;
  }

  public RestServiceException(Throwable e, RestServiceErrorCode error) {
    super(e);
    this.error = error;
  }

  public RestServiceErrorCode getErrorCode() {
    return this.error;
  }
}
