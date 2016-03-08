/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import java.util.List;


/**
 * DeleteManager handles Delete operations. This is just a template for now.
 */
class DeleteManager {
  public DeleteManager(NonBlockingRouter router) {
    //@todo
  }

  public FutureResult<Void> submitDeleteBlobOperation(long operationId, String blobId, FutureResult<Void> futureResult,
      Callback<Void> callback) {
    //@todo
    return null;
  }

  public void poll(List<RequestInfo> requests) {
    //@todo
  }

  void handleResponse(ResponseInfo responseInfo) {
    // @todo
  }

  void close() {
    // @todo
  }
}
