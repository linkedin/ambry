package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.AmbryReplicaSyncUpManager;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;


public class FileCopyStatusListenerImpl implements FileCopyStatusListener {

  private final ReplicaSyncUpManager replicaSyncUpManager;
  private final ReplicaId replicaId;

  public FileCopyStatusListenerImpl(ReplicaSyncUpManager replicaSyncUpManager, ReplicaId replicaId) {
    this.replicaSyncUpManager = replicaSyncUpManager;
    this.replicaId = replicaId;
  }
  @Override
  public void onFileCopySuccess() {
    replicaSyncUpManager.onFileCopyComplete(replicaId);
  }

  @Override
  public void onFileCopyFailure(Exception e) {
    replicaSyncUpManager.onFileCopyError(replicaId);
  }
}
