package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaFailureType;
import com.github.ambry.clustermap.ReplicaId;

public class ResponseFailureHandler {

    private ClusterMap clusterMap;

    public ResponseFailureHandler(ClusterMap clusterMap) {
        this.clusterMap = clusterMap;
    }

    public void onOperationTimeout(ReplicaId replicaId) {
      clusterMap.onReplicaError(replicaId, ReplicaFailureType.Node_Timeout);
    }

    public void onOperationError(ReplicaId replicaId, ReplicaFailureType error) {
      clusterMap.onReplicaError(replicaId, error);
    }

    public void onServerError(ReplicaId replicaId, ServerErrorCode errorCode) {
      if (errorCode == ServerErrorCode.IO_Error || errorCode == ServerErrorCode.Disk_Unavailable) {
        onOperationError(replicaId, ReplicaFailureType.Disk_Error);
      }

      if (errorCode == ServerErrorCode.Partition_ReadOnly) {
        onOperationError(replicaId, ReplicaFailureType.Partition_ReadOnly);
      }
    }

    public void onRequestResponseError(ReplicaId replicaId, RequestResponseError errorCode) {
      if (errorCode == RequestResponseError.IO_ERROR) {
        onOperationError(replicaId, ReplicaFailureType.Disk_Error);
      }
      if (errorCode == RequestResponseError.TIMEOUT_ERROR) {
        onOperationTimeout(replicaId);
      }
    }
}

