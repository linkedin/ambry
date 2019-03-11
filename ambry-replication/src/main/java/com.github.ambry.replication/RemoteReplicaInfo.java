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

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.Port;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.Store;
import com.github.ambry.utils.Time;


class RemoteReplicaInfo {
  private final ReplicaId replicaId;
  private final ReplicaId localReplicaId;
  private final Object lock = new Object();
  private final Store localStore;
  private final Port port;
  private final Time time;
  // tracks the point up to which a node is in sync with a remote replica
  private final long tokenPersistIntervalInMs;

  // The latest known token
  private FindToken currentToken = null;
  // The token that will be safe to persist eventually
  private FindToken candidateTokenToPersist = null;
  // The time at which the candidate token is set
  private long timeCandidateSetInMs;
  // The token that is known to be safe to persist.
  private FindToken tokenSafeToPersist = null;
  private long totalBytesReadFromLocalStore;
  private long localLagFromRemoteStore = -1;
  private long reEnableReplicationTime = 0;

  RemoteReplicaInfo(ReplicaId replicaId, ReplicaId localReplicaId, Store localStore, FindToken token,
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

  ReplicaId getReplicaId() {
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

  FindToken getToken() {
    synchronized (lock) {
      return currentToken;
    }
  }

  void setTotalBytesReadFromLocalStore(long totalBytesReadFromLocalStore) {
    this.totalBytesReadFromLocalStore = totalBytesReadFromLocalStore;
  }

  void setLocalLagFromRemoteInBytes(long localLagFromRemoteStore) {
    this.localLagFromRemoteStore = localLagFromRemoteStore;
  }

  long getTotalBytesReadFromLocalStore() {
    return this.totalBytesReadFromLocalStore;
  }

  void setToken(FindToken token) {
    // reference assignment is atomic in java but we want to be completely safe. performance is
    // not important here
    synchronized (lock) {
      this.currentToken = token;
    }
  }

  void initializeTokens(FindToken token) {
    synchronized (lock) {
      this.currentToken = token;
      this.candidateTokenToPersist = token;
      this.tokenSafeToPersist = token;
      this.timeCandidateSetInMs = time.milliseconds();
    }
  }

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

  /**
   * get the token to persist. Returns either the candidate token if enough time has passed since it was
   * set, or the last token again.
   */
  FindToken getTokenToPersist() {
    synchronized (lock) {
      if (time.milliseconds() - timeCandidateSetInMs > tokenPersistIntervalInMs) {
        // candidateTokenToPersist is now safe to be persisted.
        tokenSafeToPersist = candidateTokenToPersist;
      }
      return tokenSafeToPersist;
    }
  }

  void onTokenPersisted() {
    synchronized (lock) {
      /* Only update the candidate token if it qualified as the token safe to be persisted in the previous get call.
       * If not, keep it as it is.
       */
      if (tokenSafeToPersist == candidateTokenToPersist) {
        candidateTokenToPersist = currentToken;
        timeCandidateSetInMs = time.milliseconds();
      }
    }
  }

  @Override
  public String toString() {
    return replicaId.toString();
  }
}


