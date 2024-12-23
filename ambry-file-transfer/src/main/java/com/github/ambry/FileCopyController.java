package com.github.ambry;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class FileCopyController {
  private Map<String, ReplicaId> diskToInFlightPartitionsMap;

  private Map<ReplicaId, ReplicaOperationTracker> replicaToReplicaOperationTracker;

  private final List<String> listOfDisksToBeHydrated;

  private boolean isRunning = false;
  PrioritisationManager _prioritisationManager;

  public FileCopyController(PrioritisationManager prioritisationManager, StoreManager storeManager) {
    this._prioritisationManager = prioritisationManager;
    diskToInFlightPartitionsMap = new HashMap<>();
    if (!_prioritisationManager.isRunning()) {
      //TODO: throw Exception.
    }
    listOfDisksToBeHydrated = new ArrayList<>();
  }

  public void start() throws InterruptedException {
    if (!isRunning) isRunning = true;

    while (true) {
      Thread.sleep(60000);
      /**
       * Check if a new disk is to be added to the list of disks to be hydrated.
       */
      if (_prioritisationManager.getListOfDisks().size() > 0) {
        for (String disk : _prioritisationManager.getListOfDisks()) {
          if (!listOfDisksToBeHydrated.contains(disk)) {
            listOfDisksToBeHydrated.add(disk);
          }
        }
      }

      //iterate over each item in diskToInFlightPartitionsMap
      for (Map.Entry<String, ReplicaId> entry : diskToInFlightPartitionsMap.entrySet()) {
        ReplicaId replicaId = entry.getValue();

        if (listOfDisksToBeHydrated.contains(replicaId)) {

        }
      }
    }
  }

  public void fileCopyAndStateBuild(String partitionName){
    //complete file copy For partitionName
    //build state for partitionName
  }

  protected List<AmbryFileCopyThread> createThreadPool(String datacenter, int numberOfThreads, boolean startThread) {

    return new ArrayList<>();
  }
  public void shutdown() {
    for(Map.Entry<ReplicaId, ReplicaOperationTracker> entry : replicaToReplicaOperationTracker.entrySet()) {
      entry.getValue().shutdown();
    }
    isRunning = false;
  }
}
