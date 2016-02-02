package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.network.NetworkSend;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * DeleteManager handles Delete operations. This is just a template for now.
 */
class DeleteManager {
  public DeleteManager(ConnectionManager connectionManager, ClusterMap clusterMap) {
    //@todo
  }

  public FutureResult<Void> submitDeleteBlobOperation(long operationId, String blobId, FutureResult<Void> futureResult,
      Callback<Void> callback) {
    //@todo
    return null;
  }

  public void poll(List<NetworkSend> requests) {
    //@todo
  }

  void handleResponse(ByteBuffer response) {
    // @todo
  }
}
