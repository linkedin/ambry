/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.commons.Callback;
import com.github.ambry.router.FutureResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * An implementation of {@link RestResponseChannel} that is a no-op on all operations.
 */
public class NoOpResponseChannel implements RestResponseChannel{
  private ResponseStatus responseStatus = ResponseStatus.Ok;
  private final Map<String, Object> headers = new HashMap<>();

  /**
   * Constructor of {@link NoOpResponseChannel}
   */
  public NoOpResponseChannel() {}

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    return new FutureResult<>();
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void close() throws IOException {
    //no op
  }

  @Override
  public void onResponseComplete(Exception exception) {
    //no op
  }

  @Override
  public void setStatus(ResponseStatus status) throws RestServiceException {
    responseStatus = status;
  }

  @Override
  public ResponseStatus getStatus() {
    return responseStatus;
  }

  @Override
  public void setHeader(String headerName, Object headerValue) {
    headers.put(headerName, headerValue);
  }

  @Override
  public Object getHeader(String headerName) {
    return headers.get(headerName);
  }
}
