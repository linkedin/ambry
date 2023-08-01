/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Time;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;


/**
 * Manages replication from servers to VCR
 */
public class AzureStorageReplicationManager extends VcrReplicationManager {

  /**
   * Constructor
   * @param properties
   * @param registry
   * @param clusterMap
   * @param storeManager
   * @param storeKeyFactory
   * @param vcrClusterParticipant
   * @param scheduler
   * @param networkClientFactory
   * @param requestNotification
   * @param storeKeyConverterFactory
   * @throws ReplicationException
   * @throws IllegalStateException
   */
  public AzureStorageReplicationManager(VerifiableProperties properties, MetricRegistry registry, ClusterMap clusterMap,
      StoreManager storeManager, StoreKeyFactory storeKeyFactory, VcrClusterParticipant vcrClusterParticipant,
      ScheduledExecutorService scheduler, NetworkClientFactory networkClientFactory, NotificationSystem requestNotification, StoreKeyConverterFactory storeKeyConverterFactory)
      throws ReplicationException, IllegalStateException {
    super(new CloudConfig(properties), new ReplicationConfig(properties), new ClusterMapConfig(properties), new StoreConfig(properties), storeManager, storeKeyFactory, clusterMap, vcrClusterParticipant, scheduler,
        networkClientFactory, new VcrMetrics(registry), requestNotification, storeKeyConverterFactory, new ServerConfig(properties).serverMessageTransformer);
    this.persistor = new AzureStorageTokenWriter(mountPathToPartitionInfos, properties, replicationMetrics, clusterMap, tokenHelper);
  }

  @Override
  protected void scheduleTasks() {
    // TODO: Implement and schedule partition compactor
  }

  /**
   * Returns {@link AzureStorageReplicationThread}
   */
  protected ReplicaThread getReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient, ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry, boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate, ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    return new AzureStorageReplicationThread(threadName, tokenHelper, clusterMap, correlationIdGenerator, dataNodeId,
        networkClient, replicationConfig, replicationMetrics, notification, storeKeyConverter, transformer,
        metricRegistry, replicatingOverSsl, datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate,
        leaderBasedReplicationAdmin, this.persistor);
  }
}
