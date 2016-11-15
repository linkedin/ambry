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
package com.github.ambry.server;

import com.github.ambry.network.Request;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Request handler class
 */
public class RequestHandler implements Runnable {
  private final int id;
  private final RequestResponseChannel requestChannel;
  private final AmbryRequests requests;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public RequestHandler(int id, RequestResponseChannel requestChannel, AmbryRequests requests) {
    this.id = id;
    this.requestChannel = requestChannel;
    this.requests = requests;
  }

  public void run() {
    while (true) {
      try {
        Request req = requestChannel.receiveRequest();
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
      }
    }
  }

  public void shutdown() throws InterruptedException {
    requestChannel.sendRequest(EmptyRequest.getInstance());
  }
}

// Request handler pool. A pool of threads that handle requests
class RequestHandlerPool {

  private Thread[] threads = null;
  private RequestHandler[] handlers = null;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public RequestHandlerPool(int numThreads, RequestResponseChannel requestResponseChannel, AmbryRequests requests) {
    threads = new Thread[numThreads];
    handlers = new RequestHandler[numThreads];
    for (int i = 0; i < numThreads; i++) {
      handlers[i] = new RequestHandler(i, requestResponseChannel, requests);
      threads[i] = Utils.daemonThread("request-handler-" + i, handlers[i]);
      threads[i].start();
    }
  }

  public void shutdown() {
    try {
      logger.info("shutting down");
      for (RequestHandler handler : handlers) {
        handler.shutdown();
      }
      for (Thread thread : threads) {
        thread.join();
      }
      logger.info("shut down completely");
    } catch (Exception e) {
      logger.error("error when shutting down request handler pool {}", e);
    }
  }
}
