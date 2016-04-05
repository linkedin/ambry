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
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.network.RequestInfo;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * GetManager manages GetBlob and GetBlobInfo operations. This is just a template for now.
 * These methods have to be thread safe.
 */
class GetManager {
  GetManager(ClusterMap clusterMap) {
    //@todo
  }

  void submitGetBlobOperation(long operationId, String blobId, FutureResult<ReadableStreamChannel> futureResult,
      Callback<ReadableStreamChannel> callback) {
    //@todo
  }

  void submitGetBlobInfoOperation(long operationId, String blobId, FutureResult<BlobInfo> futureResult,
      Callback<BlobInfo> callback) {
    //@todo
  }

  void poll(List<RequestInfo> requests) {
    //@todo
  }

  void handleResponse(ByteBuffer response) {
    // @todo
  }
}

