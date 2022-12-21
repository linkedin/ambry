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

import java.util.List;


interface NetworkRequestQueue {

  /**
   * Inserts request into queue.
   * @param request to be inserted.
   */
  void offer(NetworkRequest request) throws InterruptedException;

  /**
   * Get next request to serve (waiting if necessary).
   * @return {@link NetworkRequest} to be served.
   */
  NetworkRequest take() throws InterruptedException;

  /**
   * Get list of requests which have timed out in queue or couldn't be added to queue due to capacity.
   * @return list of {@link NetworkRequest}s to be dropped.
   */
  List<NetworkRequest> getDroppedRequests() throws InterruptedException;

  /**
   * @return the size of the queue containing active requests.
   */
  int numActiveRequests();

  /**
   * @return the size of the queue containing dropped requests.
   */
  int numDroppedRequests();

  /**
   * Releases the resources in the queue.
   */
  void close();
}
