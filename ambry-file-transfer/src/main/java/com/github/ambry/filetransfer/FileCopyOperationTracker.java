package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.List;


public class FileCopyOperationTracker {

  private final PartitionId partitionId;
  private final FileCopyOperationState fileCopyOperationState;


  public FileCopyOperationTracker(PartitionId partitionId, FileCopyOperationState fileCopyOperationState) {
    this.partitionId = partitionId;
    this.fileCopyOperationState = fileCopyOperationState;

  }

  void start(){
    while(fileCopyOperationState.equals(FileCopyOperationState.CHUNK_DATA_EXCHANGE_COMPLETE)) {
      switch (fileCopyOperationState){
        case Start:
          break;
        case META_DATA_REQUEST_SENT:
          break;
        case META_DATA_RESPONSE_RECEIVED:
          break;
        case CHUNK_DATA_REQUEST_IN_PROGRESS:
          break;
        case CHUNK_DATA_EXCHANGE_COMPLETE:
          break;
      }

      if (fileCopyOperationState.equals(FileCopyOperationState.Start)){


        List<ReplicaId> replicaIds = (List<ReplicaId>) partitionId.getReplicaIds();
        String hostName = replicaIds.get(0).getDataNodeId().getHostname();
        String partitionName = String.valueOf(partitionId.getId());
        //fileCopyOperationState = FileCopyOperationState.META_DATA_REQUEST_SENT;
      }
    }
  }
}
