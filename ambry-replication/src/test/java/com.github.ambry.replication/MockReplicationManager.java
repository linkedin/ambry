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
package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import java.io.DataInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;


/**
 * An extension of {@link ReplicationManager} to help with testing.
 */
public class MockReplicationManager extends ReplicationManager {
  // General variables
  public RuntimeException exceptionToThrow = null;
  // Variables for controlling and examining the values provided to controlReplicationForPartitions()
  public Boolean controlReplicationReturnVal;
  public Boolean addReplicaReturnVal = null;
  public Collection<PartitionId> idsVal;
  public List<String> originsVal;
  public Boolean enableVal;
  // Variables for controlling getRemoteReplicaLagFromLocalInBytes()
  // the key is partitionId:hostname:replicaPath
  public Map<String, Long> lagOverrides = null;
  CountDownLatch listenerExecutionLatch = null;
  MockReplicationListener replicationListener = new MockReplicationListener();

  /**
   * Static construction helper
   * @param verifiableProperties the {@link VerifiableProperties} to use for config.
   * @param storageManager the {@link StorageManager} to use.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param dataNodeId the {@link DataNodeId} to use.
   * @param storeKeyConverterFactory the {@link StoreKeyConverterFactory} to use.
   * @return an instance of {@link MockReplicationManager}
   * @throws ReplicationException
   */
  public static MockReplicationManager getReplicationManager(VerifiableProperties verifiableProperties,
      StorageManager storageManager, ClusterMap clusterMap, DataNodeId dataNodeId,
      StoreKeyConverterFactory storeKeyConverterFactory) throws ReplicationException {
    ReplicationConfig replicationConfig = new ReplicationConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    StoreConfig storeConfig = new StoreConfig(verifiableProperties);
    return new MockReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, clusterMap,
        dataNodeId, storeKeyConverterFactory, null);
  }

  public MockReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StorageManager storageManager, ClusterMap clusterMap, DataNodeId dataNodeId,
      StoreKeyConverterFactory storeKeyConverterFactory, ClusterParticipant clusterParticipant)
      throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeConfig, storageManager, new StoreKeyFactory() {
          @Override
          public StoreKey getStoreKey(DataInputStream stream) {
            return null;
          }

          @Override
          public StoreKey getStoreKey(String input) {
            return null;
          }
        }, clusterMap, null, dataNodeId, null, clusterMap.getMetricRegistry(), null, storeKeyConverterFactory,
        BlobIdTransformer.class.getName(), clusterParticipant);
    reset();
  }

  @Override
  public void start() {
    startupLatch.countDown();
    started = true;
  }

  @Override
  public boolean controlReplicationForPartitions(Collection<PartitionId> ids, List<String> origins, boolean enable) {
    failIfRequired();
    if (controlReplicationReturnVal == null) {
      throw new IllegalStateException("Return val not set. Don't know what to return");
    }
    idsVal = ids;
    originsVal = origins;
    enableVal = enable;
    return controlReplicationReturnVal;
  }

  @Override
  public long getRemoteReplicaLagFromLocalInBytes(PartitionId partitionId, String hostName, String replicaPath) {
    failIfRequired();
    long lag;
    String key = getPartitionLagKey(partitionId, hostName, replicaPath);
    if (lagOverrides == null || !lagOverrides.containsKey(key)) {
      lag = super.getRemoteReplicaLagFromLocalInBytes(partitionId, hostName, replicaPath);
    } else {
      lag = lagOverrides.get(key);
    }
    return lag;
  }

  @Override
  public boolean addReplica(ReplicaId replicaId) {
    return addReplicaReturnVal == null ? super.addReplica(replicaId) : addReplicaReturnVal;
  }

  /**
   * Resets all state
   */
  public void reset() {
    exceptionToThrow = null;
    controlReplicationReturnVal = null;
    idsVal = null;
    originsVal = null;
    enableVal = null;
    lagOverrides = null;
  }

  /**
   * Gets the key for the lag override in {@code lagOverrides} using the given parameters.
   * @param partitionId the {@link PartitionId} whose replica {@code hostname} is.
   * @param hostname the hostname of the replica whose lag override key is required.
   * @param replicaPath the replica path of the replica whose lag override key is required.
   * @return
   */
  public static String getPartitionLagKey(PartitionId partitionId, String hostname, String replicaPath) {
    return partitionId.toString() + ":" + hostname + ":" + replicaPath;
  }

  /**
   * @return dataNodeIdToReplicaThread map
   */
  Map<DataNodeId, ReplicaThread> getDataNodeIdToReplicaThreadMap() {
    return dataNodeIdToReplicaThread;
  }

  /**
   * @return partitionToPartitionInfo map
   */
  Map<PartitionId, PartitionInfo> getPartitionToPartitionInfoMap() {
    return partitionToPartitionInfo;
  }

  /**
   * @return mountPathToPartitionInfos map
   */
  Map<String, Set<PartitionInfo>> getMountPathToPartitionInfosMap() {
    return mountPathToPartitionInfos;
  }

  /**
   * Mock that replication manager starts with an exception and boolean variable started is not updated to true.
   */
  void startWithException() {
    startupLatch.countDown();
  }

  /**
   * Throws a {@link RuntimeException} if the {@link MockReplicationManager} is required to.
   */
  private void failIfRequired() {
    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
  }

  /**
   * A class extends implementation of {@link ReplicationManager.PartitionStateChangeListenerImpl} to help manipulate
   * ordering of threads' execution.
   */
  private class MockReplicationListener extends ReplicationManager.PartitionStateChangeListenerImpl {
    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      super.onPartitionBecomeStandbyFromBootstrap(partitionName);
      if (listenerExecutionLatch != null) {
        listenerExecutionLatch.countDown();
      }
    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {
      super.onPartitionBecomeInactiveFromStandby(partitionName);
      if (listenerExecutionLatch != null) {
        listenerExecutionLatch.countDown();
      }
    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {
      super.onPartitionBecomeOfflineFromInactive(partitionName);
      if (listenerExecutionLatch != null) {
        listenerExecutionLatch.countDown();
      }
    }
  }
}
