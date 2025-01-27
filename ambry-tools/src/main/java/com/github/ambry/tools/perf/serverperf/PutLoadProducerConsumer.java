package com.github.ambry.tools.perf.serverperf;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.tools.perf.serverperf.ServerPerformance.ServerPerformanceConfig;
import java.util.List;


public class PutLoadProducerConsumer implements LoadProducerConsumer {
  private final ServerPerfNetworkQueue networkQueue;
  private final ServerPerformanceConfig config;
  private final ClusterMap clusterMap;

  public PutLoadProducerConsumer(ServerPerfNetworkQueue networkQueue, ServerPerformanceConfig config,
      ClusterMap clusterMap) {
    this.networkQueue = networkQueue;
    this.config = config;
    this.clusterMap = clusterMap;
  }

  @Override
  public void produce() throws Exception {
    DataNodeId dataNodeId = clusterMap.getDataNodeId(config.serverPerformanceHostname, config.serverPerformancePort);
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);

    replicaIds.forEach(replicaId -> {
      if (!replicaId.isUnsealed()) {
        return;
      }

      //    PutRequest putRequest = new PutRequest(1, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(), )
    });
  }

  @Override
  public void consume() throws Exception {

  }
}
