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
import java.io.IOException;
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
  private ConcurrentHashMap<String, CloudDataNode> instanceNameToCloudDataNode;
  private ConcurrentSkipListSet<CloudDataNode> vcrNodes;
  private final ConcurrentHashMap<String, PartitionId> localPartitionNameToPartition;
  private final Object notificationLock = new Object();

  /**
   * Constructor for {@link CloudToStoreReplicationManager}
   * @param replicationConfig {@link ReplicationConfig} object.
   * @param clusterMapConfig {@link ClusterMapConfig} objeect.
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
    this.instanceNameToCloudDataNode = new ConcurrentHashMap<>();
    this.vcrNodes = new ConcurrentSkipListSet<>();
    this.persistor =
        new DiskTokenPersistor(cloudReplicaTokenFileName, mountPathToPartitionInfos, replicationMetrics, clusterMap,
            tokenHelper);
    this.localPartitionNameToPartition = mapPartitionNameToPartition(clusterMap, currentNode);
  }

  private ConcurrentHashMap<String, PartitionId> mapPartitionNameToPartition(ClusterMap clusterMap,
      DataNodeId localNode) {
    List<? extends ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    return localReplicas.stream()
        .collect(Collectors.toMap(replicaId -> replicaId.getPartitionId().toPathString(), ReplicaId::getPartitionId,
            (e1, e2) -> e2, ConcurrentHashMap::new));
  }

  @Override
  public void start() throws ReplicationException {
    // Add listener for vcr instance config changes
    vcrClusterSpectator.registerInstanceConfigChangeListener(new InstanceConfigChangeListener() {
      @Override
      public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
        ConcurrentSkipListSet<CloudDataNode> newVcrNodes = new ConcurrentSkipListSet<>();
        ConcurrentHashMap<String, CloudDataNode> newInstanceNameToCloudDataNode = new ConcurrentHashMap<>();
        Set<CloudDataNode> removedNodes = new HashSet<>(vcrNodes);
        synchronized (notificationLock) {
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
          removedNodes.removeAll(newVcrNodes);
          vcrNodes = newVcrNodes;
          instanceNameToCloudDataNode = newInstanceNameToCloudDataNode;
          List<PartitionId> partitionsOnRemovedNodes = getPartitionsOnRemovedNodes(removedNodes);
          for (PartitionId partitionId : partitionsOnRemovedNodes) {
            try {
              removeCloudReplica(partitionId.toPathString());
              addCloudReplica(partitionId.toPathString());
            } catch (ReplicationException rex) {
              logger.error("Could not remove/add replica for partitionId {}", partitionId);
            }
          }
        }
      }
    });

    // Add listener for vcr instance liveness changes
    vcrClusterSpectator.registerLiveInstanceChangeListener(new LiveInstanceChangeListener() {
      @Override
      public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
        ConcurrentSkipListSet<CloudDataNode> newVcrNodes = new ConcurrentSkipListSet<>();
        synchronized (notificationLock) {
          for (LiveInstance liveInstance : liveInstances) {
            if (instanceNameToCloudDataNode.containsKey(liveInstance.getInstanceName())) {
              newVcrNodes.add(instanceNameToCloudDataNode.get(liveInstance.getInstanceName()));
            }
          }
          Set<CloudDataNode> removedNodes = new HashSet<>(vcrNodes);
          removedNodes.removeAll(newVcrNodes);
          vcrNodes = newVcrNodes;
          List<PartitionId> partitionsOnRemovedNodes = getPartitionsOnRemovedNodes(removedNodes);
          for (PartitionId partitionId : partitionsOnRemovedNodes) {
            try {
              removeCloudReplica(partitionId.toPathString());
              addCloudReplica(partitionId.toPathString());
            } catch (ReplicationException rex) {
              logger.error("Could not remove/add replica for partitionId {}", partitionId);
            }
          }
        }
      }
    });

    // Add listener for new coming assigned partition
    clusterParticipant.registerPartitionStateChangeListener(new PartitionStateChangeListener() {
      @Override
      public void onPartitionLeadFromStandby(String partitionName) {
        synchronized (notificationLock) {
          try {
            addCloudReplica(partitionName);
          } catch (ReplicationException rex) {
            logger.error("Could not add replication for paritition {}", partitionName);
          }
        }
      }

      @Override
      public void onPartitionStandbyFromLead(String partitionName) {
        synchronized (notificationLock) {
          try {
            removeCloudReplica(partitionName);
          } catch (Exception e) {
            // Helix will run into error state if exception throws in Helix context.
            logger.error("Exception on removing Partition {} from {}: ", partitionName, dataNodeId, e);
          }
        }
      }
    });

    // start background persistent thread
    // start scheduler thread to persist index in the background
    scheduler.scheduleAtFixedRate(persistor, replicationConfig.replicationTokenFlushDelaySeconds,
        replicationConfig.replicationTokenFlushIntervalSeconds, TimeUnit.SECONDS);
  }

  private List<PartitionId> getPartitionsOnRemovedNodes(Set<CloudDataNode> removedNodes) {
    List<PartitionId> partitionsOnRemovedNodes = new LinkedList<>();
    Set<String> removedHostNames = removedNodes.stream().map(DataNodeId::getHostname).collect(Collectors.toSet());
    for (Map.Entry<PartitionId, PartitionInfo> entry : partitionToPartitionInfo.entrySet()) {
      List<RemoteReplicaInfo> remotes = entry.getValue()
          .getRemoteReplicaInfos()
          .stream()
          .filter(remoteReplicaInfo -> removedHostNames.contains(
              remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname()))
          .collect(Collectors.toList());
      if (!remotes.isEmpty()) {
        partitionsOnRemovedNodes.add(entry.getKey());
      }
    }
    return partitionsOnRemovedNodes;
  }

  /**
   * Add a replica of given partition and its {@link RemoteReplicaInfo}s to backup list.
   * @param partitionName name of the partition of the replica to add.
   * @throws ReplicationException if replicas initialization failed.
   */
  private void addCloudReplica(String partitionName) throws ReplicationException {
    if (!localPartitionNameToPartition.containsKey(partitionName)) {
      logger.error("Got partition leader notification for partition {} that is not present on the node", partitionName);
      return;
    }
    PartitionId partitionId = localPartitionNameToPartition.get(partitionName);
    Store store = storeManager.getStore(partitionId);
    if (store == null) {
      logger.error("Unable to add cloud replica for partition {} as store for the partition doesn't exist.",
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
    List<RemoteReplicaInfo> remoteReplicaInfos = Collections.singletonList(remoteReplicaInfo);
    PartitionInfo partitionInfo = new PartitionInfo(remoteReplicaInfos, partitionId, store, localReplicaId);
    partitionToPartitionInfo.put(partitionId, partitionInfo);
    mountPathToPartitionInfos.computeIfAbsent(localReplicaId.getMountPath(), key -> ConcurrentHashMap.newKeySet())
        .add(partitionInfo);
    logger.info("Cloud Partition {} added to {}", partitionName, dataNodeId);

    // Reload replication token if exist.
    List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfos = persistor.retrieve(localReplicaId.getMountPath());
    if (tokenInfos.size() != 0) {
      for (RemoteReplicaInfo remoteInfo : remoteReplicaInfos) {
        boolean tokenReloaded = false;
        for (RemoteReplicaInfo.ReplicaTokenInfo tokenInfo : tokenInfos) {
          if (isTokenForRemoteReplicaInfo(remoteInfo, tokenInfo)) {
            logger.info("Read token for partition {} remote host {} port {} token {}", partitionId,
                tokenInfo.getHostname(), tokenInfo.getPort(), tokenInfo.getReplicaToken());
            tokenReloaded = true;
            remoteInfo.initializeTokens(tokenInfo.getReplicaToken());
            remoteInfo.setTotalBytesReadFromLocalStore(tokenInfo.getTotalBytesReadFromLocalStore());
            break;
          }
        }
        if (!tokenReloaded) {
          // This may happen on clusterMap update: replica removed or added.
          // Or error on token persist/retrieve.
          logger.warn("Token not found or reload failed. remoteReplicaInfo: {} tokenInfos: {}", remoteInfo, tokenInfos);
        }
      }
    }
    // Add remoteReplicaInfos to {@link ReplicaThread}.
    addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, true);
    if (replicationConfig.replicationTrackPerPartitionLagFromRemote) {
      replicationMetrics.addLagMetricForPartition(partitionId);
    }
  }

  /**
   * Remove a replica of given partition and its {@link RemoteReplicaInfo}s from the backup list.
   * @param partitionName the partition of the replica to removed.
   * @throws ReplicationException if replicas initialization failed.
   */
  private void removeCloudReplica(String partitionName) throws ReplicationException {
    if (!localPartitionNameToPartition.containsKey(partitionName)) {
      logger.error("Got partition standby notification for partition {} that is not present on the node",
          partitionName);
      return;
    }
    PartitionId partitionId = localPartitionNameToPartition.get(partitionName);
    PartitionInfo partitionInfo = partitionToPartitionInfo.remove(partitionId);
    if (partitionInfo == null) {
      logger.error("Partition {} not exist when remove from {}. ", partitionId, dataNodeId);
      throw new ReplicationException("Partition not found");
    }
    removeRemoteReplicaInfoFromReplicaThread(partitionInfo.getRemoteReplicaInfos());
    if (replicationConfig.replicationPersistTokenOnShutdownOrReplicaRemove) {
      try {
        persistor.write(partitionInfo.getLocalReplicaId().getMountPath(), false);
      } catch (IOException | ReplicationException e) {
        logger.error("Exception on token write when remove Partition {} from {}: ", partitionId, dataNodeId, e);
        throw new ReplicationException("Exception on token write.");
      }
    }
    logger.info("Cloud Partition {} removed from {}", partitionId, dataNodeId);
  }

  private DataNodeId getCloudDataNode() {
    return vcrNodes.toArray(new CloudDataNode[0])[Utils.getRandomShort(new Random()) % vcrNodes.size()];
  }
}
