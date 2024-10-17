package com.github.ambry;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.FileCopyConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.StoreKeyFactory;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
Responsibilities of this class is to receive state transitions for a replica,
ensure that the these replicas are stored in a prioritised data structure.
Then for each file,
* */
public class FileCopyManager {
  Map<String, String> diskToPartitionQueue;

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public FileCopyManager(FileCopyConfig fileCopyConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, NetworkClientFactory networkClientFactory,
      MetricRegistry metricRegistry, ClusterParticipant clusterParticipant){
    if(clusterParticipant != null){
      clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.FileCopyManagerListener,
          new PartitionStateChangeListenerImpl());
      logger.info("File Copy Manager's state change listener registered!");
    }
  }
  public void start() throws InterruptedException, IOException {

  }
  class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    public void waitFileCopyCompleted(){
      return ;
    }
    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      //diskToPartitionQueue.put(partitionName);
    }

    @Override
    public void onPartitionBecomeHydrateFromOffline(String partitionName) {

    }

    @Override
    public void onPartitionBecomeBootstrapFromHydrate(String partitionName) {

    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {

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
