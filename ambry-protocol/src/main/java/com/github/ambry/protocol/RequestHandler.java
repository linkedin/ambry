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
package com.github.ambry.protocol;

import com.github.ambry.network.NetworkRequest;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.server.EmptyRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Request handler class
 */
public class RequestHandler implements Runnable {
  private final int id;
  private final RequestResponseChannel requestChannel;
  private final RequestAPI requests;
  private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

  public RequestHandler(int id, RequestResponseChannel requestChannel, RequestAPI requests) {
    this.id = id;
    this.requestChannel = requestChannel;
    this.requests = requests;
  }

  public void run() {
    NetworkRequest req = null;
    while (true) {
      try {
        req = requestChannel.receiveRequest();
        if (req.equals(EmptyRequest.getInstance())) {
          logger.debug("Request handler {} received shut down command", id);
          return;
        }
        requests.handleRequests(req);
        logger.trace("Request handler {} handling request {}", id, req);
      } catch (Throwable e) {
        // TODO add metric to track background threads
        logger.error("Exception when handling request", e);
        // this is bad and we need to shutdown the app
        Runtime.getRuntime().halt(1);
      } finally {
        if (req != null) {
          req.release();
          req = null;
        }
      }
    }
  }

  public void shutdown() throws InterruptedException {
    requestChannel.sendRequest(EmptyRequest.getInstance());
  }
}

