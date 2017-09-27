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
 * Delegate class giving a layer of abstraction between BlobStore and HelixParticipant
 */
public class ClusterManagerWriteStatusDelegate {

  private static ClusterManagerWriteStatusDelegate instance;

  private final ClusterParticipant clusterParticipant;

  /**
   * Creates / retrieves singleton ClusterManagerWriteStatusDelegate.  Not thread safe.
   * @param clusterParticipant
   * @return ClusterManagerWriteStatusDelegate singleton
   */
  public static ClusterManagerWriteStatusDelegate getInstance(ClusterParticipant clusterParticipant) {
    if (instance == null)
    {
      instance = new ClusterManagerWriteStatusDelegate(clusterParticipant);
    }
    return instance;
  }

  private ClusterManagerWriteStatusDelegate(ClusterParticipant clusterParticipant) {
    this.clusterParticipant = clusterParticipant;
  }

  /**
   * Tells ClusterManager that blob store is now read only
   * @param replicaId
   */
  public void setToRO(ReplicaId replicaId) {
    clusterParticipant.setReplicaSealedState(replicaId, true);
  }

  /**
   * Tells ClusterManager that blob store is now read write
   * @param replicaId
   */
  public void setToRW(ReplicaId replicaId) {
    clusterParticipant.setReplicaSealedState(replicaId, false);
  }
}
