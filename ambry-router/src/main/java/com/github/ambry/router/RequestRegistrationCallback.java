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
package com.github.ambry.router;

import com.github.ambry.network.RequestInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * The callback to be used when requests are created or dropped and needs to be sent out or closed. The operation
 * manager passes this callback to the associated operation class and the operation uses this callback when requests are
 * created and need to be sent out. The callback will then update data structures common to all of the different
 * "Manager" classes. The updates to these data structures are not thread safe and this callback should only be called
 * from the main event loop thread.
 */
class RequestRegistrationCallback<T> {
  private final Map<Integer, T> correlationIdToOperation;
  private List<RequestInfo> requestsToSend = null;
  private Set<Integer> requestsToDrop = null;

  /**
   * @param correlationIdToOperation used to keep a mapping from correlation ID to the operation that the request
   *                                 corresponds to.
   */
  RequestRegistrationCallback(Map<Integer, T> correlationIdToOperation) {
    this.correlationIdToOperation = correlationIdToOperation;
  }

  /**
   * @return the list where new requests to send are added.
   */
  List<RequestInfo> getRequestsToSend() {
    return requestsToSend;
  }

  /**
   * @return the list where the correlation IDs of requests to drop are added.
   */
  Set<Integer> getRequestsToDrop() {
    return requestsToDrop;
  }

  /**
   * @param requestsToSend the list to add {@link RequestInfo} for requests to send to.
   */
  void setRequestsToSend(List<RequestInfo> requestsToSend) {
    this.requestsToSend = requestsToSend;
  }

  /**
   * @param requestsToDrop the list to add the correlation IDs of requests to drop to.
   */
  void setRequestsToDrop(Set<Integer> requestsToDrop) {
    this.requestsToDrop = requestsToDrop;
  }

  /**
   * @param routerOperation the operation that the request corresponds to.
   * @param requestInfo the request to send out.
   */
  void registerRequestToSend(T routerOperation, RequestInfo requestInfo) {
    // TODO: netty send here>?
    if (requestsToSend != null) {
      requestsToSend.add(requestInfo);
    }
    correlationIdToOperation.put(requestInfo.getRequest().getCorrelationId(), routerOperation);
  }

  /**
   * Register a request to "drop". The current default networking layer drops requests by closing connections.
   * @param correlationId the correlation ID of the request to drop.
   */
  void registerRequestToDrop(int correlationId) {
    if (requestsToDrop != null) {
      requestsToDrop.add(correlationId);
    }
    // do not remove from correlationIdToOperation here because it will be handled by manager classes when they receive
    // a ResponseInfo.
  }
}
