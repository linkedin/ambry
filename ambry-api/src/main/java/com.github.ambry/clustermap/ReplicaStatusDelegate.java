/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.clustermap;

import com.github.ambry.store.Store;


/**
 * Delegate class allowing BlobStore to set the replica sealed/stopped status
 */
public class ReplicaStatusDelegate {

  private final ClusterParticipant clusterParticipant;

  public ReplicaStatusDelegate(ClusterParticipant clusterParticipant) {
    this.clusterParticipant = clusterParticipant;
  }

  /**
   * Sets replicaId to read-only status
   * @param replicaId
   */
  public boolean seal(ReplicaId replicaId) {
    return clusterParticipant.setReplicaSealedState(replicaId, true);
  }

  /**
   * Sets replicaId to read-write status
   * @param replicaId
   */
  public boolean unseal(ReplicaId replicaId) {
    return clusterParticipant.setReplicaSealedState(replicaId, false);
  }

  /**
   * Sets stopped status of given store
   * @param replicaId the {@link ReplicaId} of the {@link Store} of which the stopped state would be set.
   * @param isStopped whether to mark the store as stopped ({@code true}).
   * @return {@code true} if state is successful set. {@code false} if not.
   */
  public boolean setReplicaStoppedState(ReplicaId replicaId, boolean isStopped) {
    return clusterParticipant.setReplicaStoppedState(replicaId, isStopped);
  }
}
