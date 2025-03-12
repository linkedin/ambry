package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.List;

public interface FileCopyThreadPoolManager {
  List<ReplicaId> createThreadPool(int numberOfThreads);
  List<DiskId> provideDisksToHydrate();
  void submitReplicaForHydration(ReplicaId replicaId);
  boolean removeReplicaForHydration(ReplicaId replicaId);
}
