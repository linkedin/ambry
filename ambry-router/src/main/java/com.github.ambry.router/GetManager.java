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

