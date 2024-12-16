package com.github.ambry;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.config.FileCopyConfig;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class AmbryFileCopyThread implements Runnable{
  private final ReentrantLock lock = new ReentrantLock();

  private volatile boolean running;
  private final Map<ReplicaId, ReplicaOperationTracker> replicaToReplicaOperationTrackerMap;
  private final String threadName;
  private final NetworkClient networkClient;
  private final ClusterMap clusterMap;
  private final DataNodeId dataNodeId;
  private final FileCopyConfig fileCopyConfig;
  private final FileCopyMetrics fileCopyMetrics;
  private final ReplicaSyncUpManager replicaSyncUpManager;
  private final AtomicInteger correlationIdGenerator;

  public AmbryFileCopyThread(String threadName, NetworkClient networkClient, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, FileCopyConfig fileCopyConfig,
      FileCopyMetrics fileCopyMetrics, ReplicaSyncUpManager replicaSyncUpManager) {
    this.threadName = threadName;
    this.networkClient = networkClient;
    this.clusterMap = clusterMap;
    this.dataNodeId = dataNodeId;
    this.fileCopyConfig = fileCopyConfig;
    this.fileCopyMetrics = fileCopyMetrics;
    this.replicaSyncUpManager = replicaSyncUpManager;
    this.correlationIdGenerator = correlationIdGenerator;
    this.replicaToReplicaOperationTrackerMap = new HashMap<>();
    if(!running)
      running = true;
  }
  @Override
  public void run() {
    while (running) {
      doChunkBasedReplication();
    }
    // This is a dummy implementation. You can replace this with your own implementation.
    System.out.println("Copying file from source to destination");
  }
  //write thread safe code for add replica method
  public void addReplica(ReplicaId replicaId, ReplicaOperationTracker replicaOperationTracker) {
    lock.lock();
    try {
      if(replicaToReplicaOperationTrackerMap.containsKey(replicaId)){
        throw new IllegalArgumentException("Replica already exists: " + replicaId);
      }else{
        replicaToReplicaOperationTrackerMap.put(replicaId, replicaOperationTracker);
      }
    } finally {
      System.out.println("Adding replica: " + replicaId);
      lock.unlock();
    }
  }



  private void doChunkBasedReplication(){

    List<RequestInfo> requestInfos = pollForRequests();
    if(requestInfos.isEmpty()){
      return;
    }
  }

  private void onResponses(List<ResponseInfo> responseInfos, Map<Integer, RequestInfo> correlationIdToRequest) {
    lock.lock();
    try {
      for (ResponseInfo responseInfo : responseInfos) {
//        RequestInfo requestInfo = correlationIdToRequest.remove(responseInfo.getRequestInfo().getRequest().getCorrelationId());
//        if (requestInfo == null) {
//          throw new IllegalStateException("Received a response that does not have a corresponding request");
//        }
//        ReplicaOperationTracker replicaOperationTracker = replicaToReplicaOperationTrackerMap.get(requestInfo.getReplicaId());
//        if (replicaOperationTracker == null) {
//          throw new IllegalStateException("ReplicaOperationTracker not found for replica " + requestInfo.getReplicaId());
//        }
//        replicaOperationTracker.onResponse(responseInfo);
      }
    } finally {
      lock.unlock();
    }
  }

  private List<RequestInfo> pollForRequests(){
    lock.lock();
    try {
      return replicaToReplicaOperationTrackerMap.values().stream().map(ReplicaOperationTracker::pollForRequests)
          .collect(Collectors.toList());
    } finally {
      lock.unlock();
    }
  }
}
