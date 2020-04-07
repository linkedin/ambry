/*
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
package com.github.ambry.clustermap;

/**
 * {@link PartitionStateChangeListener} takes action when partition state changes.
 */
public interface PartitionStateChangeListener {

  /**
   * Action to take when partition becomes bootstrap from offline.
   * @param partitionName of the partition.
   */
  void onPartitionBecomeBootstrapFromOffline(String partitionName);

  /**
   * Action to take when partition becomes standby from bootstrap.
   * @param partitionName of the partition.
   */
  void onPartitionBecomeStandbyFromBootstrap(String partitionName);

  /**
   * Action to take on becoming leader of a partition.
   * @param partitionName of the partition.
   */
  void onPartitionBecomeLeaderFromStandby(String partitionName);

  /**
   * Action to take on being removed as leader of a partition.
   * @param partitionName of the partition.
   */
  void onPartitionBecomeStandbyFromLeader(String partitionName);

  /**
   * Action to take when partition becomes inactive from standby.
   * @param partitionName of the partition
   */
  void onPartitionBecomeInactiveFromStandby(String partitionName);

  /**
   * Action to take when partition becomes offline from inactive.
   * @param partitionName of the partition
   */
  void onPartitionBecomeOfflineFromInactive(String partitionName);

  /**
   * Action to take when partition becomes dropped from offline.
   * @param partitionName of the partition.
   */
  void onPartitionBecomeDroppedFromOffline(String partitionName);

  /**
   * Action to take when partition becomes dropped from error.
   * @param partitionName of the partition.
   */
  default void onPartitionBecomeDroppedFromError(String partitionName) {
  }

  /**
   * Action to take when partition becomes offline from error.
   * @param partitionName of the partition.
   */
  default void onPartitionBecomeOfflineFromError(String partitionName) {
  }

  /**
   * Action to take when reset method is called on certain partition.
   * @param partitionName of the partition.
   */
  default void onReset(String partitionName) {
  }
}
