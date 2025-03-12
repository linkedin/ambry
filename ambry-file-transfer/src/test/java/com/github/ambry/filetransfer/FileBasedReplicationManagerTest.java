package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Before;


public class FileBasedReplicationManagerTest {
  private MockClusterMap clusterMap;
  private PrioritizationManager prioritizationManager;

  @Before
  public void initializeCluster() throws IOException {
    clusterMap = new MockClusterMap(false, true, 1, 3, 3, false, false, null);
    prioritizationManager = new PrioritizationManager() {
      boolean isRunning;
      ConcurrentHashMap<DiskId, List<ReplicaId>> diskToReplicaMap = new ConcurrentHashMap<>();
      @Override
      public void start() {
        isRunning = true;
      }

      @Override
      public void shutdown() {
        isRunning = false;
      }

      @Override
      public boolean isRunning() {
        return true;
      }

      @Override
      public List<ReplicaId> getPartitionListForDisk(DiskId diskId, int numberOfReplicasPerDisk) {
        return null;
      }

      @Override
      public boolean addReplica(ReplicaId replicaId) {
        diskToReplicaMap.putIfAbsent(replicaId.getDiskId(), new ArrayList<ReplicaId>());
        return false;
      }
    };
  }
}
