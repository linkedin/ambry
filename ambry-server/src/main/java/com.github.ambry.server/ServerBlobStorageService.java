/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server;

import com.github.ambry.commons.CopyingAsyncWritableChannel;
import com.github.ambry.network.NettyServerRequest;
import com.github.ambry.network.NettyServerRequestResponseChannel;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerBlobStorageService implements BlobStorageService {
  private final Logger logger = LoggerFactory.getLogger(ServerBlobStorageService.class);
  NettyServerRequestResponseChannel requestResponseChannel;

  public ServerBlobStorageService(NettyServerRequestResponseChannel requestResponseChannel) {
    this.requestResponseChannel = requestResponseChannel;
  }

  @Override
  public void start() throws InstantiationException {

  }

  @Override
  public void shutdown() {

  }

  @Override
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    System.out.println("get");
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    System.out.println("post");
    CopyingAsyncWritableChannel asyncWritableChannel = new CopyingAsyncWritableChannel();
    restRequest.readInto(asyncWritableChannel, (result, exception) -> {
      try {
        requestResponseChannel.sendRequest(
            new NettyServerRequest(restRequest, restResponseChannel, asyncWritableChannel.getContentAsInputStream()));
      } catch (InterruptedException e) {
        logger.error("Failed to SendRequest to requestResponseChannel.", e);
      }
    });
  }

  @Override
  public void handlePut(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    System.out.println("put");
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel) {

  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel) {

  }

  @Override
  public void handleOptions(RestRequest restRequest, RestResponseChannel restResponseChannel) {

  }
}