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
   * Inserts the element into queue.
   * @param request element to be inserted.
   * @return false if an element fails to be inserted.
   */
  boolean offer(NetworkRequest request);

  /**
   * @return {@link NetworkRequestBundle} that contains collections of requests to be served or dropped due to time out.
   * @throws InterruptedException
   */
  NetworkRequestBundle take() throws InterruptedException;

  /**
   * @return the size of the queue.
   */
  int size();

  /**
   * Releases the resources in the queue.
   */
  void close();
}