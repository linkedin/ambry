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
package com.github.ambry.network;

interface NetworkRequestQueue {

  /**
   * Inserts request into queue.
   * @param request to be inserted.
   * @return {@code True} if request is added to request queue. Else {@code False}
   */
  boolean offer(NetworkRequest request) throws InterruptedException;

  /**
   * Get next request to serve (waiting if necessary).
   * @return {@link NetworkRequest} to be served.
   */
  NetworkRequest take() throws InterruptedException;

  /**
   * @return the size of the queue.
   */
  int size();

  /**
   * @param request queued request
   * @return {@code True} if the request is expired. Else, {@code False}
   */
  boolean isExpired(NetworkRequest request);

  /**
   * Releases the resources in the queue.
   */
  void close();
}
