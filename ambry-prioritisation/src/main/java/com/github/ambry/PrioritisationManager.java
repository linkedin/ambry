package com.github.ambry;

import com.github.ambry.clustermap.AmbryPartition;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.HashMap;
import java.util.Map;


public class PrioritisationManager {
  private Map<String, String> diskToReplicaQueue;

  private boolean running;
  public PrioritisationManager() {
    diskToReplicaQueue = new HashMap<>();
    running = false;
    // Constructor
  }

  public void start() {
    running = true;
  }

  public boolean isRunning(){
    return running;
    // Start the PrioritisationManager
  }

  public void shutdown() {
    // Shutdown the PrioritisationManager
  }

  public void addReplica(String partitionName) {
    // Add a replica to the PrioritisationManager
  }

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

  public String getPartitionForDisk(DiskId diskId){
    // Get a partition from the PrioritisationManager
    return null;
  }

  public String getReplica(String partitionName) {
    // Get a replica from the PrioritisationManager
    return null;
  }
}
