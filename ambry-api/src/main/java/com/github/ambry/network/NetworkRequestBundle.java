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

import java.util.Collection;
import java.util.Collections;


/**
 * A collection of requests to either serve or drop.
 */
public class NetworkRequestBundle {
  private final NetworkRequest requestToServe;
  private final Collection<NetworkRequest> requestsToDrop;

  NetworkRequestBundle(NetworkRequest requestToServe, Collection<NetworkRequest> requestsToDrop) {
    this.requestToServe = requestToServe;
    this.requestsToDrop = requestsToDrop;
  }

  /**
   * @return a request that the request handler should serve. Can be null if there are no pending requests that have
   * not timed out.
   */
  public NetworkRequest getRequestToServe() {
    return requestToServe;
  }

  /**
   * @return requests that have spent too long in the queue and should be dropped by the request handler.
   */
  public Collection<NetworkRequest> getRequestsToDrop() {
    return requestsToDrop;
  }
}