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
package com.github.ambry.protocol;

import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Request handler pool. A pool of threads that handle requests
 */
public class RequestHandlerPool implements Closeable {

  private Thread[] threads = null;
  private RequestHandler[] handlers = null;
  private final RequestResponseChannel requestResponseChannel;
  private static final Logger logger = LoggerFactory.getLogger(RequestHandlerPool.class);

  /**
   * Create and start a pool of {@link RequestHandler}s.
   * @param numThreads the number of handler threads to create.
   * @param requestResponseChannel the {@link RequestResponseChannel} for the handlers to use.
   * @param requests the {@link RequestAPI} instance used by the handlers for dispatching requests.
   */
  public RequestHandlerPool(int numThreads, RequestResponseChannel requestResponseChannel, RequestAPI requests) {
    threads = new Thread[numThreads];
    handlers = new RequestHandler[numThreads];
    this.requestResponseChannel = requestResponseChannel;
    for (int i = 0; i < numThreads; i++) {
      handlers[i] = new RequestHandler(i, requestResponseChannel, requests);
      threads[i] = Utils.daemonThread("request-handler-" + i, handlers[i]);
      threads[i].start();
    }
  }

  /**
   * @return the {@link RequestResponseChannel} used by this pool.
   */
  public RequestResponseChannel getChannel() {
    return requestResponseChannel;
  }

  /**
   * Drain the pool: shut down the handler threads.
   */
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

  @Override
  public void close() throws IOException {
    shutdown();
  }
}
