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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.CloudDataNode;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.ClusterSpectator;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * {@link CloudToStoreReplicationManager} replicates data from Vcr nodes to ambry data nodes.
 */
public class CloudToStoreReplicationManager extends ReplicationEngine {
  private final ClusterMapConfig clusterMapConfig;
  private final StoreConfig storeConfig;
  private final StoreManager storeManager;
  private final ClusterSpectator vcrClusterSpectator;
  private final ClusterParticipant clusterParticipant;
  private static final String cloudReplicaTokenFileName = "cloudReplicaTokens";
  private AtomicReference<ConcurrentHashMap<String, CloudDataNode>> instanceNameToCloudDataNode;
  private AtomicReference<ConcurrentSkipListSet<CloudDataNode>> vcrNodes;
  private final ConcurrentHashMap<String, PartitionId> localPartitionNameToPartition;
  private final Object notificationLock = new Object();

  /**
   * Constructor for {@link CloudToStoreReplicationManager}
   * @param replicationConfig {@link ReplicationConfig} object.
   * @param clusterMapConfig {@link ClusterMapConfig} object.
   * @param storeConfig {@link StoreConfig} object.
   * @param storeManager {@link StoreManager} object to get stores for replicas.
   * @param storeKeyFactory {@link StoreKeyFactory} object.
   * @param clusterMap {@link ClusterMap} object to get the ambry datanode cluster map.
   * @param scheduler {@link ScheduledExecutorService} object for scheduling token persistence.
   * @param currentNode {@link DataNodeId} representing the current node.
   * @param connectionPool {@link ConnectionPool} object representing the connection pool to talk to replicas.
   * @param metricRegistry {@link MetricRegistry} object.
   * @param requestNotification {@link NotificationSystem} object to notify on events.
   * @param storeKeyConverterFactory {@link StoreKeyConverterFactory} object.
   * @param transformerClassName name of the class to transform and validate replication messages.
   * @param vcrClusterSpectator {@link ClusterSpectator} object to get changes in vcr cluster map.
   * @param clusterParticipant {@link ClusterParticipant} object to get changes in partition state of partitions on datanodes.
   * @throws ReplicationException
   */
  public CloudToStoreReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId currentNode, ConnectionPool connectionPool,
      MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName,
      ClusterSpectator vcrClusterSpectator, ClusterParticipant clusterParticipant) throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler, currentNode,
        Collections.emptyList(), connectionPool, metricRegistry, requestNotification, storeKeyConverterFactory,
        transformerClassName);
    this.clusterMapConfig = clusterMapConfig;
    this.storeConfig = storeConfig;
    this.storeManager = storeManager;
    this.vcrClusterSpectator = vcrClusterSpectator;
    this.clusterParticipant = clusterParticipant;
    this.instanceNameToCloudDataNode = new AtomicReference<>(new ConcurrentHashMap<>());
    this.vcrNodes = new AtomicReference<>(new ConcurrentSkipListSet<>());
    this.persistor =
        new DiskTokenPersistor(cloudReplicaTokenFileName, mountPathToPartitionInfos, replicationMetrics, clusterMap,
            tokenHelper, storeManager);
    this.localPartitionNameToPartition = mapPartitionNameToPartition(clusterMap, currentNode);
  }

  /**
   * Create a {@link Map} of partition name to {@link PartitionId} for local replicas on specified node from
   * specified cluster map.
   * @param clusterMap specified {@link ClusterMap} object.
   * @param localNode specified {@link DataNodeId} object.
   * @return
   */
  private ConcurrentHashMap<String, PartitionId> mapPartitionNameToPartition(ClusterMap clusterMap,
      DataNodeId localNode) {
    return clusterMap.getReplicaIds(localNode)
        .stream()
        .collect(Collectors.toMap(replicaId -> replicaId.getPartitionId().toPathString(), ReplicaId::getPartitionId,
            (e1, e2) -> e2, ConcurrentHashMap::new));
  }

  @Override
  public void start() {
    // Add listener for vcr instance config changes
    vcrClusterSpectator.registerInstanceConfigChangeListener(new InstanceConfigChangeListenerImpl());
    vcrClusterSpectator.registerLiveInstanceChangeListener(new LiveInstanceChangeListenerImpl());

    // Add listener for new coming assigned partition
    clusterParticipant.registerPartitionStateChangeListener(new PartitionStateChangeListenerImpl());

    // start background persistent thread
    // start scheduler thread to persist index in the background
    scheduler.scheduleAtFixedRate(persistor, replicationConfig.replicationTokenFlushDelaySeconds,
        replicationConfig.replicationTokenFlushIntervalSeconds, TimeUnit.SECONDS);
  }

  /**
   * Return the list of {@link PartitionId}s that have a replica on the specified list of nodes.
   * @param nodes list of specified nodes.
   * @return {@link List} of {@link PartitionId}s.
   */
  private List<PartitionId> getPartitionsOnNodes(Set<CloudDataNode> nodes) {
    List<PartitionId> partitionsOnNodes = new LinkedList<>();
    Set<String> removedHostNames = nodes.stream().map(DataNodeId::getHostname).collect(Collectors.toSet());
    for (Map.Entry<PartitionId, PartitionInfo> entry : partitionToPartitionInfo.entrySet()) {
      List<RemoteReplicaInfo> remotes = entry.getValue()
          .getRemoteReplicaInfos()
          .stream()
          .filter(remoteReplicaInfo -> removedHostNames.contains(
              remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname()))
          .collect(Collectors.toList());
      if (!remotes.isEmpty()) {
        partitionsOnNodes.add(entry.getKey());
      }
    }
    return partitionsOnNodes;
  }

  /**
   * Add a replica of given partition and its {@link RemoteReplicaInfo}s to backup list.
   * @param partitionName name of the partition of the replica to add.
   * @throws ReplicationException if replicas initialization failed.
   */
  private void addCloudReplica(String partitionName) throws ReplicationException {
    if (!localPartitionNameToPartition.containsKey(partitionName)) {
      logger.warn("Got partition leader notification for partition {} that is not present on the node", partitionName);
      return;
    }
    PartitionId partitionId = localPartitionNameToPartition.get(partitionName);
    Store store = storeManager.getStore(partitionId);
    if (store == null) {
      logger.warn("Unable to add cloud replica for partition {} as store for the partition doesn't exist.",
          partitionName);
      return;
    }
    ReplicaId localReplicaId = (ReplicaId) partitionId.getReplicaIds()
        .stream()
        .filter(r -> (r.getDataNodeId().getHostname().equals(dataNodeId.getHostname())))
        .toArray()[0];
    CloudReplica peerReplica = new CloudReplica(partitionId, getCloudDataNode());
    FindTokenFactory findTokenFactory = tokenHelper.getFindTokenFactoryFromReplicaType(peerReplica.getReplicaType());
    RemoteReplicaInfo remoteReplicaInfo =
        new RemoteReplicaInfo(peerReplica, localReplicaId, store, findTokenFactory.getNewFindToken(),
            storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec * Replication_Delay_Multiplier,
            SystemTime.getInstance(), peerReplica.getDataNodeId().getPortToConnectTo());
    replicationMetrics.addMetricsForRemoteReplicaInfo(remoteReplicaInfo);

    // Note that for each replica on a Ambry server node, there is only one cloud replica that it will be replicating from.
    List<RemoteReplicaInfo> remoteReplicaInfos = Collections.singletonList(remoteReplicaInfo);
    PartitionInfo partitionInfo = new PartitionInfo(remoteReplicaInfos, partitionId, store, localReplicaId);
    partitionToPartitionInfo.put(partitionId, partitionInfo);
    mountPathToPartitionInfos.computeIfAbsent(localReplicaId.getMountPath(), key -> ConcurrentHashMap.newKeySet())
        .add(partitionInfo);
    logger.info("Cloud Partition {} added to {}", partitionName, dataNodeId);

    // Reload replication token if exist.
    reloadReplicationTokenIfExists(localReplicaId, remoteReplicaInfos);

    // Add remoteReplicaInfos to {@link ReplicaThread}.
    addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, true);
    if (replicationConfig.replicationTrackPerPartitionLagFromRemote) {
      replicationMetrics.addLagMetricForPartition(partitionId);
    }
  }

  /**
   * Remove a replica of given partition and its {@link RemoteReplicaInfo}s from the backup list.
   * @param partitionName the partition of the replica to removed.
   */
  private void removeCloudReplica(String partitionName) {
    if (!localPartitionNameToPartition.containsKey(partitionName)) {
      logger.warn("Got partition standby notification for partition {} that is not present on the node", partitionName);
      return;
    }
    PartitionId partitionId = localPartitionNameToPartition.get(partitionName);
    stopPartitionReplication(partitionId);
    logger.info("Cloud Partition {} removed from {}", partitionId, dataNodeId);
  }

  /**
   * Randomly select a {@link DataNodeId} from list of {@code vcrNodes}.
   * @return randomly selected {@link DataNodeId} object.
   */
  private DataNodeId getCloudDataNode() {
    return vcrNodes.get().toArray(new CloudDataNode[0])[Utils.getRandomShort(new Random()) % vcrNodes.get().size()];
  }

  /**
   * When there is a change in vcr nodes state, update the new list of live vcr nodes.
   * Also if there are nodes that are removed as part of change, then replication from
   * those nodes should stop and the partitions should find new nodes to replicate from.
   * Note that this method is not thread safe in the wake of arriving helix notifications.
   * @param newVcrNodes Set of new vcr nodes.
   */
  private void handleChangeInVcrNodes(ConcurrentSkipListSet<CloudDataNode> newVcrNodes) {
    Set<CloudDataNode> removedNodes = new HashSet<>(vcrNodes.get());
    removedNodes.removeAll(newVcrNodes);
    vcrNodes.set(newVcrNodes);
    List<PartitionId> partitionsOnRemovedNodes = getPartitionsOnNodes(removedNodes);
    for (PartitionId partitionId : partitionsOnRemovedNodes) {
      boolean removed = false;
      try {
        // We first remove replica to stop replication from removed node, and then add replica so that it can pick a
        // new cloud node to start replicating from.
        removeCloudReplica(partitionId.toPathString());
        addCloudReplica(partitionId.toPathString());
      } catch (ReplicationException rex) {
        replicationMetrics.addCloudPartitionErrorCount.inc();
        logger.error("Exception {} during remove/add replica for partitionId {}", rex, partitionId);
      }
    }
  }

  /**
   * {@link InstanceConfigChangeListener} for vcr cluster.
   */
  private class InstanceConfigChangeListenerImpl implements InstanceConfigChangeListener {
    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
      logger.info("Instance config change notification received with instanceConfigs: {}", instanceConfigs);
      ConcurrentSkipListSet<CloudDataNode> newVcrNodes = new ConcurrentSkipListSet<>();
      ConcurrentHashMap<String, CloudDataNode> newInstanceNameToCloudDataNode = new ConcurrentHashMap<>();

      // create a new list of available vcr nodes.
      for (InstanceConfig instanceConfig : instanceConfigs) {
        String instanceName = instanceConfig.getInstanceName();
        Port sslPort =
            getSslPortStr(instanceConfig) == null ? null : new Port(getSslPortStr(instanceConfig), PortType.SSL);
        CloudDataNode cloudDataNode = new CloudDataNode(instanceConfig.getHostName(),
            new Port(Integer.valueOf(instanceConfig.getPort()), PortType.PLAINTEXT), sslPort,
            clusterMapConfig.clustermapVcrDatacenterName, clusterMapConfig);
        newInstanceNameToCloudDataNode.put(instanceName, cloudDataNode);
        newVcrNodes.add(cloudDataNode);
      }

      synchronized (notificationLock) {
        instanceNameToCloudDataNode.set(newInstanceNameToCloudDataNode);
        handleChangeInVcrNodes(newVcrNodes);
      }
    }
  }

  /**
   * {@link LiveInstanceChangeListener} for vcr cluster.
   */
  private class LiveInstanceChangeListenerImpl implements LiveInstanceChangeListener {
    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
      logger.info("Live instance change notification received. liveInstances: {}", liveInstances);
      ConcurrentSkipListSet<CloudDataNode> newVcrNodes = new ConcurrentSkipListSet<>();
      // react to change in liveness of vcr nodes if the instance was earlier reported by helix as part of
      // {@code onInstanceConfigChange} notification.
      synchronized (notificationLock) {
        for (LiveInstance liveInstance : liveInstances) {
          if (instanceNameToCloudDataNode.get().containsKey(liveInstance.getInstanceName())) {
            newVcrNodes.add(instanceNameToCloudDataNode.get().get(liveInstance.getInstanceName()));
          }
        }
        handleChangeInVcrNodes(newVcrNodes);
      }
    }
  }

  /**
   * {@link PartitionStateChangeListener} to capture changes in partition state.
   */
  private class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {
    @Override
    public void onPartitionStateChangeToLeaderFromStandby(String partitionName) {
      logger.info("Partition state change notification from Standby to Leader received for partition {}",
          partitionName);
      synchronized (notificationLock) {
        try {
          addCloudReplica(partitionName);
        } catch (ReplicationException rex) {
          logger.error("Exception {} while adding replication for partition {}", rex, partitionName);
          replicationMetrics.addCloudPartitionErrorCount.inc();
        }
      }
    }

    @Override
    public void onPartitionStateChangeToStandbyFromLeader(String partitionName) {
      logger.info("Partition state change notification from Leader to Standby received for partition {}",
          partitionName);
      synchronized (notificationLock) {
        removeCloudReplica(partitionName);
      }
    }
  }
}
