/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.replication;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.Port;
import com.github.ambry.store.Store;
import com.github.ambry.utils.Time;
import java.util.Objects;

/*
 * The token persist logic ensures that a token corresponding to an entry in the store is never persisted in the
 * replicaTokens file before the entry itself is persisted in the store. This is done as follows. Objects of this
 * class maintain 3 tokens: tokenSafeToPersist, candidateTokenToPersist and currentToken:
 *
 * tokenSafeToPersist: this is the token that we know is safe to be persisted. The associated store entry from the
 * remote replica is guaranteed to have been persisted by the store.
 *
 * candidateTokenToPersist: this is the token that we would like to persist next. We would go ahead with this
 * only if we know for sure that the associated store entry has been persisted. We ensure safety by maintaining the
 * time at which candidateTokenToPersist was set and ensuring that tokenSafeToPersist is assigned this value only if
 * sufficient time has passed since the time we set candidateTokenToPersist.
 *
 * currentToken: this is the latest token associated with the latest record obtained from the remote replica.
 *
 * tokenSafeToPersist <= candidateTokenToPersist <= currentToken
 * (Note: when a token gets reset by the remote, the above equation may not hold true immediately after, but it should
 * eventually hold true.)
 */

public class RemoteReplicaInfo {
  private final ReplicaId replicaId;
  private final ReplicaId localReplicaId;
  private final Store localStore;
  private final Port port;
  private final Time time;
  // tracks the point up to which a node is in sync with a remote replica
  private final long tokenPersistIntervalInMs;

  // The latest known token
  private FindToken currentToken = null;
  // The token that will be safe to persist eventually
  private FindToken candidateTokenToPersist;
  // The time at which the candidate token is set
  private long timeCandidateSetInMs;
  // The token that is known to be safe to persist.
  private FindToken tokenSafeToPersist;
  private long totalBytesReadFromLocalStore;
  private long localLagFromRemoteStore = -1;
  private long reEnableReplicationTime = 0;
  private ReplicaThread replicaThread;

  public RemoteReplicaInfo(ReplicaId replicaId, ReplicaId localReplicaId, Store localStore, FindToken token,
      long tokenPersistIntervalInMs, Time time, Port port) {
    this.replicaId = replicaId;
    this.localReplicaId = localReplicaId;
    this.totalBytesReadFromLocalStore = 0;
    this.localStore = localStore;
    this.time = time;
    this.port = port;
    this.tokenPersistIntervalInMs = tokenPersistIntervalInMs;
    initializeTokens(token);
  }

  public ReplicaId getReplicaId() {
    return replicaId;
  }

  ReplicaId getLocalReplicaId() {
    return localReplicaId;
  }

  Store getLocalStore() {
    return localStore;
  }

  Port getPort() {
    return this.port;
  }

  /**
   * Gets the time re-enable replication for this replica.
   * @return time to re-enable replication in ms.
   */
  long getReEnableReplicationTime() {
    return reEnableReplicationTime;
  }

  /**
   * Sets the time to re-enable replication for this replica.
   * @param reEnableReplicationTime time to re-enable replication in ms.
   */
  void setReEnableReplicationTime(long reEnableReplicationTime) {
    this.reEnableReplicationTime = reEnableReplicationTime;
  }

  long getRemoteLagFromLocalInBytes() {
    if (localStore != null) {
      return this.localStore.getSizeInBytes() - this.totalBytesReadFromLocalStore;
    } else {
      return 0;
    }
  }

  long getLocalLagFromRemoteInBytes() {
    return localLagFromRemoteStore;
  }

  synchronized FindToken getToken() {
    return currentToken;
  }

  synchronized ReplicaThread getReplicaThread() {
    return replicaThread;
  }

  synchronized void setReplicaThread(ReplicaThread replicaThread) {
    this.replicaThread = replicaThread;
  }

  public void setTotalBytesReadFromLocalStore(long totalBytesReadFromLocalStore) {
    this.totalBytesReadFromLocalStore = totalBytesReadFromLocalStore;
  }

  void setLocalLagFromRemoteInBytes(long localLagFromRemoteStore) {
    this.localLagFromRemoteStore = localLagFromRemoteStore;
  }

  long getTotalBytesReadFromLocalStore() {
    return this.totalBytesReadFromLocalStore;
  }

  synchronized void setToken(FindToken token) {
    // reference assignment is atomic in java but we want to be completely safe. performance is
    // not important here
    currentToken = token;
  }

  public void initializeTokens(FindToken token) {
    currentToken = token;
    candidateTokenToPersist = token;
    tokenSafeToPersist = token;
    timeCandidateSetInMs = time.milliseconds();
  }

  /**
   * get the token to persist. Returns either the candidate token if enough time has passed since it was
   * set, or the last token again.
   */
  synchronized FindToken getTokenToPersist() {
    if (time.milliseconds() - timeCandidateSetInMs > tokenPersistIntervalInMs) {
      // candidateTokenToPersist is now safe to be persisted.
      tokenSafeToPersist = candidateTokenToPersist;
    }
    return tokenSafeToPersist;
  }

  synchronized void onTokenPersisted() {
    /* Only update the candidate token if it qualified as the token safe to be persisted in the previous get call.
     * If not, keep it as it is.
     */
    if (tokenSafeToPersist == candidateTokenToPersist) {
      candidateTokenToPersist = currentToken;
      timeCandidateSetInMs = time.milliseconds();
    }
  }

  @Override
  public String toString() {
    return replicaId.getPartitionId() + ":" + replicaId.getDataNodeId() + ":" + replicaId.getReplicaPath();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    RemoteReplicaInfo info = (RemoteReplicaInfo) obj;
    if (Objects.equals(port, info.getPort()) && info.getReplicaId().getReplicaPath().equals(replicaId.getMountPath())
        && info.getLocalReplicaId().getReplicaPath().equals(localReplicaId.getMountPath())) {
      return true;
    }
    return false;
  }

  /**
   * Data structure to hold deserialized replica token data.
   */
  public static class ReplicaTokenInfo {
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

    @Override
    public String toString() {
      return "ReplicaTokenInfo: " + "partitionId:" + partitionId + " hostname: " + hostname + " port: " + port
          + " replicaPath: " + replicaPath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return Objects.equals(replicaToken.toBytes(), ((ReplicaTokenInfo) o).getReplicaToken().toBytes());
    }
  }
}


