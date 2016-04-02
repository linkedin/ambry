package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.Time;
import java.util.List;


/**
 * DeleteManager handles Delete operations. This is just a template for now.
 */
class DeleteManager {
  /**
   * Create a DeleteManager
   * @param clusterMap The {@link ClusterMap} of the cluster.
   * @param responseHandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param notificationSystem The {@link NotificationSystem} used for notifying blob creations.
   * @param routerConfig  The {@link RouterConfig} containing the configs for the PutManager.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param time The {@link Time} instance to use.
   */
  DeleteManager(ClusterMap clusterMap, ResponseHandler responseHandler, NotificationSystem notificationSystem,
      RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, Time time) {
    //@todo
  }

  public FutureResult<Void> submitDeleteBlobOperation(String blobId, FutureResult<Void> futureResult,
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
