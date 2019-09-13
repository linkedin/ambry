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
package com.github.ambry.network;

import com.github.ambry.server.RequestAPI;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Request handler pool. A pool of threads that handle requests
 */
public class RequestHandlerPool {

  private Thread[] threads = null;
  private RequestHandler[] handlers = null;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public RequestHandlerPool(int numThreads, RequestResponseChannel requestResponseChannel, RequestAPI requests) {
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