package com.github.ambry.replica.prioritization;

import com.github.ambry.config.ReplicaPrioritizationStrategy;


public class FileBasedReplicationPrioritizationManagerFactory implements PrioritizationManagerFactory{

  @Override
  public PrioritizationManager getPrioritizationManager(ReplicaPrioritizationStrategy replicaPrioritizationStrategy) {
    if(replicaPrioritizationStrategy == ReplicaPrioritizationStrategy.FirstComeFirstServe) {
      return new FCFSPrioritizationManager();
    }
    else
      return null;
  }
}
