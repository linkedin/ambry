package com.github.ambry.replication;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.FindToken;


/**
 * Holding company for deserialized replica token data.
 */
public class ReplicaTokenInfo {
  private final RemoteReplicaInfo replicaInfo;
  private final PartitionId partitionId;
  private final String hostname;
  private final String replicaPath;
  private final int port;
  private final long totalBytesReadFromLocalStore;
  private final FindToken replicaToken;

  public ReplicaTokenInfo(RemoteReplicaInfo replicaInfo) {
    this.replicaInfo = replicaInfo;
    this.partitionId = replicaInfo.getReplicaId().getPartitionId();
    this.hostname = replicaInfo.getReplicaId().getDataNodeId().getHostname();
    this.port = replicaInfo.getReplicaId().getDataNodeId().getPort();
    this.replicaPath = replicaInfo.getReplicaId().getReplicaPath();
    this.totalBytesReadFromLocalStore = replicaInfo.getTotalBytesReadFromLocalStore();
    this.replicaToken = replicaInfo.getTokenToPersist();
  }

  public ReplicaTokenInfo(PartitionId partitionId, String hostname, String replicaPath, int port,
      long totalBytesReadFromLocalStore, FindToken replicaToken) {
    this.replicaInfo = null;
    this.partitionId = partitionId;
    this.hostname = hostname;
    this.replicaPath = replicaPath;
    this.port = port;
    this.totalBytesReadFromLocalStore = totalBytesReadFromLocalStore;
    this.replicaToken = replicaToken;
  }

  public RemoteReplicaInfo getReplicaInfo() {
    return replicaInfo;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public String getHostname() {
    return hostname;
  }

  public String getReplicaPath() {
    return replicaPath;
  }

  public int getPort() {
    return port;
  }

  public long getTotalBytesReadFromLocalStore() {
    return totalBytesReadFromLocalStore;
  }

  public FindToken getReplicaToken() {
    return replicaToken;
  }

}
