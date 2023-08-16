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
import com.github.ambry.router.AsyncWritableChannel;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.Future;
import javax.net.ssl.SSLSession;

import static com.github.ambry.rest.RestMethod.*;


/**
 * Wrap the rest request to support delete after put request.
 */
public class WrappedRestRequest implements RestRequest {
  private final RestRequest restRequest;

  public WrappedRestRequest(RestRequest restRequest) {
    this.restRequest = restRequest;
  }

  @Override
  public String getUri() {
    return restRequest.getUri();
  }

  @Override
  public String getPath() {
    return restRequest.getPath();
  }

  @Override
  public RestMethod getRestMethod() {
    return DELETE;
  }

  @Override
  public void setRestMethod(RestMethod restMethod) {
    restRequest.setRestMethod(restMethod);
  }

  @Override
  public Map<String, Object> getArgs() {
    return restRequest.getArgs();
  }

  @Override
  public Object setArg(String key, Object value) {
    return restRequest.setArg(key, value);
  }

  @Override
  public SSLSession getSSLSession() {
    return restRequest.getSSLSession();
  }

  @Override
  public void prepare() throws RestServiceException {
    restRequest.prepare();
  }

  @Override
  public boolean isOpen() {
    return restRequest.isOpen();
  }

  @Override
  public void close() throws IOException {
    restRequest.close();
  }

  @Override
  public RestRequestMetricsTracker getMetricsTracker() {
    return restRequest.getMetricsTracker();
  }

  @Override
  public String toString() {
    return restRequest.toString();
  }

  @Override
  public long getSize() {
    return restRequest.getSize();
  }

  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    return restRequest.readInto(asyncWritableChannel, callback);
  }

  @Override
  public void setDigestAlgorithm(String digestAlgorithm) throws NoSuchAlgorithmException {
    restRequest.setDigestAlgorithm(digestAlgorithm);
  }

  @Override
  public byte[] getDigest() {
    return restRequest.getDigest();
  }

  @Override
  public long getBytesReceived() {
    return restRequest.getBytesReceived();
  }

  @Override
  public long getBlobBytesReceived() {
    return restRequest.getBlobBytesReceived();
  }

  @Override
  public NettyRequest.NettyRequestContext getRestRequestContext() {
    return (NettyRequest.NettyRequestContext) restRequest.getRestRequestContext();
  }
}
