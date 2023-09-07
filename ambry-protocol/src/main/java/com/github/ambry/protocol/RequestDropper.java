/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used to query request channel for overflow or expired requests and drop them.
 */
public class RequestDropper implements Runnable {
  private final RequestResponseChannel requestChannel;
  private final RequestAPI requests;
  private static final Logger logger = LoggerFactory.getLogger(RequestDropper.class);
  private final AtomicBoolean running = new AtomicBoolean(true);

  public RequestDropper(RequestResponseChannel requestChannel, RequestAPI requests) {
    this.requestChannel = requestChannel;
    this.requests = requests;
  }

  public void run() {
    Collection<NetworkRequest> requestsToDrop = null;
    while (isRunning()) {
      try {
        // Get timed out requests from the channel. If there are no timed out requests, we wait until a dropped request
        // is available.
        requestsToDrop = requestChannel.getDroppedRequests();
        for (NetworkRequest requestToDrop : requestsToDrop) {
          if (requestToDrop.equals(EmptyRequest.getInstance())) {
            logger.debug("Request dropper received shut down command");
            return;
          }
          requests.dropRequest(requestToDrop);
          logger.trace("Request dropper dropping request {}", requestToDrop);
        }
      } catch (Throwable e) {
        // TODO add metric to track background threads
        logger.error("Exception when handling request", e);
        // this is bad and we need to shutdown the app
        Runtime.getRuntime().halt(1);
      } finally {
        // Release any resources associated with requests.
        if (requestsToDrop != null) {
          requestsToDrop.forEach(NetworkRequest::release);
        }
      }
    }
  }

  public void shutdown() throws InterruptedException {
    running.set(false);
    requestChannel.sendRequest(EmptyRequest.getInstance());
  }

  /**
   * Information on whether this request handler is accepting requests. This will return {@code false} as soon as
   * {@link this#shutdown()} is called.
   * @return {@code true} if in a state to receive requests. {@code false} otherwise.
   */
  private boolean isRunning() {
    return running.get();
  }
}

