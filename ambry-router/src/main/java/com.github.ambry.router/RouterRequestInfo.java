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

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.Send;


/**
 * {@link RequestInfo} class extension specifically for the requests sent out by a router. This adds
 * additional information about the {@link ReplicaId} to which this request will be sent.
 */
class RouterRequestInfo extends RequestInfo {
  private final ReplicaId replicaId;

  /**
   * Construct a RouterRequestInfo.
   * @param host the host associated with the request.
   * @param port the port on the host associated with the request.
   * @param request the {@link Send} object that is the request payload.
   * @param replicaId the {@link ReplicaId} to which this request is targeted.
   */
  RouterRequestInfo(String host, Port port, Send request, ReplicaId replicaId) {
    super(host, port, request);
    this.replicaId = replicaId;
  }

  /**
   * @return the {@link ReplicaId} associated with this request.
   */
  ReplicaId getReplicaId() {
    return replicaId;
  }
}

