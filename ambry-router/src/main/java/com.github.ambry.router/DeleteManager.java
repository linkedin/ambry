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
