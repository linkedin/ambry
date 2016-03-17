package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.utils.Time;
import java.util.List;


/**
 * GetManager manages GetBlob and GetBlobInfo operations. This is just a template for now.
 * These methods have to be thread safe.
 */
class GetManager {
  /**
   * Create a GetManager
   * @param clusterMap The {@link ClusterMap} of the cluster.
   * @param responseHandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param routerConfig  The {@link RouterConfig} containing the configs for the PutManager.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param time The {@link Time} instance to use.
   */
  GetManager(ClusterMap clusterMap, ResponseHandler responseHandler, RouterConfig routerConfig,
      NonBlockingRouterMetrics routerMetrics, Time time) {
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

  void handleResponse(ResponseInfo responseInfo) {
    // @todo
  }

  void close() {
    // @todo
  }
}

