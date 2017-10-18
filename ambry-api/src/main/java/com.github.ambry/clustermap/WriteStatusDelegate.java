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

/**
 * Delegate class allowing BlobStore to set the replica sealed status
 */
public class WriteStatusDelegate {

  private final ClusterParticipant clusterParticipant;

  public WriteStatusDelegate(ClusterParticipant clusterParticipant) {
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
}
