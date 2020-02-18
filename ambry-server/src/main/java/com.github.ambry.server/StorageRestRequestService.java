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

import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.network.NettyServerRequest;
import com.github.ambry.network.NettyServerRequestResponseChannel;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestService;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an Ambry storage server specific implementation of {@link RestRequestService}.
 * All the operations that need to be performed by the Ambry storage server are supported here.
 */
public class StorageRestRequestService implements RestRequestService {
  private static final Logger logger = LoggerFactory.getLogger(StorageRestRequestService.class);
  NettyServerRequestResponseChannel requestResponseChannel;

  public StorageRestRequestService(NettyServerRequestResponseChannel requestResponseChannel) {
    this.requestResponseChannel = requestResponseChannel;
  }

  @Override
  public void setupResponseHandler(RestResponseHandler responseHandler) {

  }

  @Override
  public void start() throws InstantiationException {

  }

  @Override
  public void shutdown() {

  }

  @Override
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel) {
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    RetainingAsyncWritableChannel asyncWritableChannel = new RetainingAsyncWritableChannel();
    restRequest.readInto(asyncWritableChannel, (result, exception) -> {
      try {
        requestResponseChannel.sendRequest(new NettyServerRequest(restRequest, restResponseChannel,
            asyncWritableChannel.consumeContentAsByteBuf()));
      } catch (InterruptedException e) {
        //TODO  close requestResponseChannel.closeConnection() on error
        logger.error("Failed to SendRequest to requestResponseChannel.", e);
      }
    });
  }

  @Override
  public void handlePut(RestRequest restRequest, RestResponseChannel restResponseChannel) {
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
