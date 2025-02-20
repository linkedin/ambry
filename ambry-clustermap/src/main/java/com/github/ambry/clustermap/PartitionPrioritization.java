package com.github.ambry.clustermap;

import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class PartitionPrioritization implements Runnable {

  interface PrioritiedPartitionChecker {
    void prepare(List<PartitionId> partitionIds);
    List<List<PartitionId>> getPrioritizedList();
  }

  StoreManager storeManager;
  Map<String, CountDownLatch> partitionToLatch = new HashMap<>();

  List<PrioritiedPartitionChecker> _prioritiedPartitionCheckerList = new ArrayList<>();
// is there thread for each partition in helix
  // send all prioritised by batches of disruption
  // if no prioritised partition remaining on disk, send rest of partitions
  Map<DiskId, List<ReplicaId>> bootStrappingReplica;

  Map<DiskId, List<List<Replica>>> pendingBatches;

  Map<DiskId, List<ReplicaId>> superPrioritisedPartition;

  public PartitionPrioritization(List<PrioritiedPartitionChecker> checkers) {
    _prioritiedPartitionCheckerList = checkers;
  }

  @Override
  public void run() {
    ReplicaId localReplica = storeManager.getReplica("partitionName");
    Store store = storeManager.getStore(localReplica.getPartitionId());
    if(!store.isBootstrapInProgress()) {
      partitionToLatch.get("partitionName").countDown();
    }

    //how to get resource tag ? as ListatefulsetName is from RT  may be check partition in datanode , how to get that
    // bootstrapping logic

  }

  public class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      //add

      CountDownLatch latch = new CountDownLatch(1);
      partitionToLatch.put(partitionName, latch);
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      //remove from queue
    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {

    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {

    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {

    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {

    }

    @Override
    public void onPartitionBecomeDroppedFromOffline(String partitionName) {

    }
  }
}
