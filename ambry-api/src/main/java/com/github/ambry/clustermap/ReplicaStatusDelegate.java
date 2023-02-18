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

import java.util.ArrayList;
import java.util.List;


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
   * @param replicaId the {@link ReplicaId} whose status would be set to read-only.
   * @return {@code true} if replica is successfully sealed. {@code false} if not.
   */
  public boolean seal(ReplicaId replicaId) {
    return clusterParticipant.setReplicaSealedState(replicaId, true);
  }

  /**
   * Sets replicaId to partially sealed status.
   * @param replicaId the {@link ReplicaId} whose status would be set to partially sealed.
   * @return {@code true} if replica is successfully partial-sealed. {@code false} if not.
   */
  public boolean partialSeal(ReplicaId replicaId) {
    // TODO Under implementation.
    return false;
  }

  /**
   * Sets replicaId to read-write status
   * @param replicaId the {@link ReplicaId} whose status would be set to read-write.
   * @return {@code true} if replica is successfully unsealed. {@code false} if not.
   */
  public boolean unseal(ReplicaId replicaId) {
    return clusterParticipant.setReplicaSealedState(replicaId, false);
  }

  /**
   * Sets replicaId to partial-unsealed status. Unsealed replicas are READ_WRITE for all blobs.
   * @param replicaId the {@link ReplicaId} whose status would be set to read write.
   * @return {@code true} if replica is successfully unsealed. {@code false} if not.
   */
  public boolean partialUnseal(ReplicaId replicaId) {
    // TODO Under implementation.
    return false;
  }

  /**
   * Sets a list of replicaIds to stopped status
   * @param replicaIds a list of replicas whose status would be set to stopped.
   * @return {@code true} if replica is successfully marked as stopped. {@code false} if not.
   */
  public boolean markStopped(List<ReplicaId> replicaIds) {
    return clusterParticipant.setReplicaStoppedState(replicaIds, true);
  }

  /**
   * Sets a list of replicaIds to started status
   * @param replicaIds a list of replicas whose status would be set to started.
   * @return {@code true} if replica is successfully unmarked and becomes started. {@code false} if not.
   */
  public boolean unmarkStopped(List<ReplicaId> replicaIds) {
    return clusterParticipant.setReplicaStoppedState(replicaIds, false);
  }

  /**
   * Disable given replica on current node (this will trigger LEADER -> STANDBY -> INACTIVE transition)
   * @param replicaId the {@link ReplicaId} to disable
   */
  public void disableReplica(ReplicaId replicaId) {
    clusterParticipant.setReplicaDisabledState(replicaId, true);
  }

  /**
   * Enable given replica on current node (this will trigger OFFLINE -> BOOTSTRAP -> STANDBY transition)
   * @param replicaId the {@link ReplicaId} to disable
   */
  public void enableReplica(ReplicaId replicaId) {
    clusterParticipant.setReplicaDisabledState(replicaId, false);
  }

  /**
   * @return a list of stopped replicas in InstanceConfig of local node.
   */
  public List<String> getStoppedReplicas() {
    return clusterParticipant.getStoppedReplicas();
  }

  /**
   * @return a list of sealed replicas in InstanceConfig of local node.
   */
  public List<String> getSealedReplicas() {
    return clusterParticipant.getSealedReplicas();
  }

  /**
   * @return a list of partially sealed replicas in InstanceConfig of local node.
   */
  public List<String> getPartiallySealedReplicas() {
    // TODO get partial sealed replicas from HelixParticipant.
    return new ArrayList<>();
  }

  /**
   * @return {@code true} if the underlying participant supports dynamic partition state changes.
   */
  public boolean supportsStateChanges() {
    return clusterParticipant.supportsStateChanges();
  }
}
