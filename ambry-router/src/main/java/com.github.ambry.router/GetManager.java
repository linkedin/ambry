package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.network.NetworkSend;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * GetManager manages GetBlob and GetBlobInfo operations. This is just a template for now.
 */
public class GetManager {
  public GetManager(ConnectionManager connectionManager, ClusterMap clusterMap) {
    //@todo
  }

  public FutureResult<ReadableStreamChannel> submitGetBlobOperation(long operationId, String blobId,
      Callback<ReadableStreamChannel> callback) {
    //@todo
    return null;
  }

  public FutureResult<BlobInfo> submitGetBlobInfoOperation(long operationId, String blobId,
      Callback<BlobInfo> callback) {
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

