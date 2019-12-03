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
package com.github.ambry.clustermap;

/**
 * The type of partition state model listener.
 * The state model listeners implement {@link com.github.ambry.clustermap.PartitionStateChangeListener} in different
 * components (i.e. StorageManager, ReplicationManager etc) and take actions when state transition occurs.
 */
public enum StateModelListenerType {
  /**
   * The partition state change listener owned by storage manager. It invokes some store operations when partition state
   * transition occurs. For example, when new replica transits from OFFLINE to BOOTSTRAP, storage manager instantiates
   * blob store associated with this replica and adds it into disk manager and compaction manager.
   */
  StorageManagerListener,
  /**
   * The partition state change listener owned by replication manager. It performs some replica operations in response to
   * partition state transition. For example, when new replica transits from BOOTSTRAP to STANDBY, replication manager
   * keeps checking replication lag of this replica and ensures it catches up with its peer replicas.
   */
  ReplicationManagerListener,
  /**
   * The partition state change listener owned by stats manager. It takes actions when new replica is added (OFFLINE ->
   * BOOTSTRAP) or old replica is removed (INACTIVE -> OFFLINE)
   */
  StatsManagerListener,
  /**
   * The partition state change listener owned by cloud-to-store replication manager. It takes actions when replica
   * leadership hand-off occurs. For example, if any replica becomes LEADER from STANDBY, it is supposed to replicate
   * data from VCR nodes. This is part of two-way replication between Ambry and cloud.
   */
  CloudToStoreReplicationManagerListener
}
