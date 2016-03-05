package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.network.RequestInfo;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * DeleteManager handles Delete operations. This is just a template for now.
 */
class DeleteManager {
  public DeleteManager(ClusterMap clusterMap) {
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

  void handleResponse(ByteBuffer response) {
    // @todo
  }
}
