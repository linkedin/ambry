package com.github.ambry;

import com.github.ambry.clustermap.PartitionStateChangeListener;


public class FileCopyManager {
  class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {
    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {

    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {

    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {

    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {

    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {

    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {

    }

    @Override
    public void onPartitionBecomeDroppedFromOffline(String partitionName) {

    }
  }
