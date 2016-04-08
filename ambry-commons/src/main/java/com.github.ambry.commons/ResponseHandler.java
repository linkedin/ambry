package com.github.ambry.commons;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import java.io.IOException;
import java.net.SocketException;


/**
 * ResponseHandler can be used by components whenever an operation encounters an error or an exception, to delegate
 * the responsibility of conveying appropriate replica related errors to the cluster map.
 * It can also be used to convey the information that a replica related operation was successful.
 * The cluster map uses this information to set soft states and dynamically handle failures.
 */

public class ResponseHandler {

  private ClusterMap clusterMap;

  public ResponseHandler(ClusterMap clusterMap) {
    this.clusterMap = clusterMap;
  }

  public void onRequestResponseError(ReplicaId replicaId, ServerErrorCode errorCode) {
    switch (errorCode) {
      case IO_Error:
      case Disk_Unavailable:
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
        break;
      case Partition_ReadOnly:
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Partition_ReadOnly);
        //fall through
      default:
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Ok);
        break;
    }
    // Regardless of what the error code is (or there is no error), it is a node response event.
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Node_Response);
  }

  public void onRequestResponseException(ReplicaId replicaId, Exception e) {
    if (e instanceof SocketException ||
        e instanceof IOException ||
        e instanceof ConnectionPoolTimeoutException) {
      clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Node_Timeout);
    }
  }
}
