/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.AmbryPartition;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class PrioritizationManager {
  private Map<DiskId, ReplicaId> diskToReplicaQueue;

  private final List<DiskId> listOfDisks;
  private boolean running;
  public PrioritizationManager() {
    diskToReplicaQueue = new HashMap<>();
    running = false;
    this.listOfDisks = new ArrayList<>();
  }

  public void start() {
    running = true;
    // Start the PrioritisationManager
  }

  public boolean isRunning(){
    return running;
  }

  public void shutdown() {
    // Shutdown the PrioritisationManager
  }

  public void addReplica(String partitionName) {
    // Add a replica to the PrioritisationManager
  }

<<<<<<< Updated upstream
  public void removeReplica(String partitionName) {
    // Remove a task from the PrioritisationManager
  }

  public void updatePartitionState(String partitionName) {
    // Update the state of a task in the PrioritisationManager
  }

  public void updatePartitionProgress(String partitionName) {
    // Update the progress of a task in the PrioritisationManager
  }

  public void updatePartitionResult() {
    // Update the result of a task in the PrioritisationManager
  }

  public List<DiskId> getListOfDisks(){
    return  Collections.unmodifiableList(listOfDisks);
  }

  public String getPartitionForDisk(DiskId diskId){
    // Get a partition from the PrioritisationManager
    return null;
  }

  public String getReplica(String partitionName) {
    // Get a replica from the PrioritisationManager
    return null;
  }
=======
  /**
   * Add a replica to the prioritization manager.
   * @param replicaId the {@link ReplicaId} to add.
   */
  boolean addReplica(ReplicaId replicaId);

  boolean removeReplica(DiskId diskId, ReplicaId replicaId);
>>>>>>> Stashed changes
}
