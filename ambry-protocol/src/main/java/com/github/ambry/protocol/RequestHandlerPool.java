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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Request handler pool. A pool of threads that handle requests
 */
public class RequestHandlerPool implements Closeable {
  private final RequestResponseChannel requestResponseChannel;
  private static final Logger logger = LoggerFactory.getLogger(RequestHandlerPool.class);
  ExecutorService requestHandlerService;
  ExecutorService requestDropperService = null;

  /**
   * Create and start a pool of {@link RequestHandler}s.
   * @param numThreads the number of handler threads to create.
   * @param requestResponseChannel the {@link RequestResponseChannel} for the handlers to use.
   * @param requests the {@link RequestAPI} instance used by the handlers for dispatching requests.
   * @param handlerPrefix the prefix of the thread name.
   * @param enableDropper enable the dropper thread if it's true
   */
  public RequestHandlerPool(int numThreads, RequestResponseChannel requestResponseChannel, RequestAPI requests,
      String handlerPrefix, boolean enableDropper) {
    this.requestResponseChannel = requestResponseChannel;
    // 1. Start worker threads for handling requests
    ThreadFactory requestHandlerThreadFactory =
        new ThreadFactoryBuilder().setNameFormat(handlerPrefix + "request-handler-%d")
            .setDaemon(true)
            .setUncaughtExceptionHandler((t, e) -> logger.error("Encountered throwable in {}", t, e))
            .build();
    requestHandlerService = Executors.newFixedThreadPool(numThreads, requestHandlerThreadFactory);
    for (int i = 0; i < numThreads; i++) {
      requestHandlerService.submit(new RequestHandler(i, requestResponseChannel, requests));
    }

    // 2. Start a worker thread for rejecting stale requests.
    if (enableDropper) {
      ThreadFactory requestDropperThreadFactory =
          new ThreadFactoryBuilder().setNameFormat(handlerPrefix + "request-dropper-%d")
              .setDaemon(true)
              .setUncaughtExceptionHandler((t, e) -> logger.error("Encountered throwable in {}", t, e))
              .build();
      requestDropperService = Executors.newSingleThreadExecutor(requestDropperThreadFactory);
      requestDropperService.submit(new RequestDropper(requestResponseChannel, requests));
    }
  }

  /**
   * Create and start a pool of {@link RequestHandler}s.
   * @param numThreads the number of handler threads to create.
   * @param requestResponseChannel the {@link RequestResponseChannel} for the handlers to use.
   * @param requests the {@link RequestAPI} instance used by the handlers for dispatching requests.
   */
  public RequestHandlerPool(int numThreads, RequestResponseChannel requestResponseChannel, RequestAPI requests) {
    this(numThreads, requestResponseChannel, requests, "", true);
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
      if (requestHandlerService != null) {
        logger.info("Shutting down request handler threads");
        Utils.shutDownExecutorService(requestHandlerService, 5, TimeUnit.MINUTES);
      }
      if (requestDropperService != null) {
        logger.info("Shutting down request dropper threads");
        Utils.shutDownExecutorService(requestDropperService, 5, TimeUnit.MINUTES);
      }
      // Shutdown request response channel to release memory of any requests still present in the channel
      if (requestResponseChannel != null) {
        requestResponseChannel.shutdown();
      }
      logger.info("shut down completely");
    } catch (Exception e) {
      logger.error("error when shutting down request handler pool", e);
    }
  }

  @Override
  public void close() throws IOException {
    shutdown();
  }
}
