package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaFailureType;
import com.github.ambry.clustermap.ReplicaId;

import java.io.IOException;
import java.net.SocketException;

/**
 * ResponseFailureHandler can be used by components to alter cluster map soft states when there is a failure.
 */

public class ResponseFailureHandler {

    private ClusterMap clusterMap;

    public ResponseFailureHandler(ClusterMap clusterMap) {
        this.clusterMap = clusterMap;
    }

    public void onRequestResponseError(ReplicaId replicaId, ServerErrorCode errorCode) {
      switch (errorCode) {
          case IO_Error:
          case Disk_Unavailable:
              clusterMap.onReplicaError(replicaId, ReplicaFailureType.Disk_Error);
            break;
          case Partition_ReadOnly:
              clusterMap.onReplicaError(replicaId, ReplicaFailureType.Partition_ReadOnly);
            break;
      }
    }

    public void onRequestResponseException(ReplicaId replicaId, Exception e) {
      if (e instanceof SocketException ||
          e instanceof IOException ||
          e instanceof ConnectionPoolTimeoutException) {
          clusterMap.onReplicaError(replicaId, ReplicaFailureType.Node_Timeout);
      }
    }
}

