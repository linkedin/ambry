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
package com.github.ambry.cloud;

import com.azure.data.tables.models.TableEntity;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.CloudDataNode;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.CloudServiceDataNode;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.VcrClusterSpectator;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.config.CloudConfig.*;


/**
 * {@link RecoveryManager} replicates data from Vcr nodes to ambry data nodes.
 * Cloud -> Store, run on storage node.
 */
public class RecoveryManager extends ReplicationEngine {
  private final ClusterMapConfig clusterMapConfig;
  private final VcrClusterSpectator vcrClusterSpectator;
  private final ClusterParticipant clusterParticipant;
  private static final String cloudReplicaTokenFileName = "cloudReplicaTokens";
  private AtomicReference<ConcurrentHashMap<String, CloudDataNode>> instanceNameToCloudDataNode;
  private AtomicReference<List<CloudDataNode>> vcrNodes;
  private final Object notificationLock = new Object();
  private final boolean trackPerDatacenterLagInMetric;
  private static final Random random = new Random();
  private final RecoveryMetrics recoveryMetrics;
  public static final String RECOVER_TOKEN_FILE_PREFIX = "recovery_token";

  /**
   * Constructor for {@link RecoveryManager}
   * @param replicationConfig {@link ReplicationConfig} object.
   * @param clusterMapConfig {@link ClusterMapConfig} object.
   * @param storeConfig {@link StoreConfig} object.
   * @param storeManager {@link StoreManager} object to get stores for replicas.
   * @param storeKeyFactory {@link StoreKeyFactory} object.
   * @param clusterMap {@link ClusterMap} object to get the ambry datanode cluster map.
   * @param scheduler {@link ScheduledExecutorService} object for scheduling token persistence.
   * @param currentNode {@link DataNodeId} representing the current node.
   * @param metricRegistry {@link MetricRegistry} object.
   * @param requestNotification {@link NotificationSystem} object to notify on events.
   * @param storeKeyConverterFactory {@link StoreKeyConverterFactory} object.
   * @param transformerClassName name of the class to transform and validate replication messages.
   * @param vcrClusterSpectator {@link VcrClusterSpectator} object to get changes in vcr cluster map.
   * @param clusterParticipant {@link ClusterParticipant} object to get changes in partition state of partitions on datanodes.
   * @throws ReplicationException
   */
  public RecoveryManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId currentNode, NetworkClientFactory networkClientFactory,
      MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName,
      VcrClusterSpectator vcrClusterSpectator, ClusterParticipant clusterParticipant)
      throws ReplicationException, IOException {
    super(replicationConfig, clusterMapConfig, storeConfig, storeKeyFactory, clusterMap, scheduler, currentNode,
        Collections.emptyList(), networkClientFactory, metricRegistry, requestNotification, storeKeyConverterFactory,
        transformerClassName, clusterParticipant, storeManager, null, false);
    this.clusterMapConfig = clusterMapConfig;
    this.vcrClusterSpectator = vcrClusterSpectator;
    this.clusterParticipant = clusterParticipant;
    this.instanceNameToCloudDataNode = new AtomicReference<>(new ConcurrentHashMap<>());
    this.vcrNodes = new AtomicReference<>(new ArrayList<>());
    this.persistor = null; // No need of a persistor
    trackPerDatacenterLagInMetric = replicationConfig.replicationTrackPerDatacenterLagFromLocal;
    this.recoveryMetrics = new RecoveryMetrics(metricRegistry);
    this.networkClientFactory.getNetworkClient(); // Test connection to Azure Storage and other services
  }

  @Override
  public void start() {
    // Use a static clustermap file on the host to know partitions assigned to the host.
    // Assume that there is no helix/zookeeper available post disaster.
    // In this design, server does not talk to vcr but directly to Azure.
    this.clusterMap.getReplicaIds(this.dataNodeId).forEach(replica -> {
      try {
        addCloudReplica(String.valueOf(replica.getPartitionId().getId()));
      } catch (Throwable e) {
        logger.error("Failed to add cloud replica for partition-{}", replica.getPartitionId().getId());
      }
    });
    started = true;
    startupLatch.countDown();
    logger.info("RecoveryManager started.");
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
   * Returns Recovery thread
   */
  protected RecoveryThread getReplicationThread(String threadName) {
    try {
      String dc = dataNodeId.getDatacenterName();
      StoreKeyConverter storeKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
      Transformer transformer = Utils.getObj(transformerClassName, storeKeyFactory, storeKeyConverter);
      return new RecoveryThread(threadName, tokenHelper, clusterMap, correlationIdGenerator, dataNodeId,
          networkClientFactory.getNetworkClient(), replicationConfig, replicationMetrics, notification,
          storeKeyConverter, transformer, metricRegistry, sslEnabledDatacenters.contains(dc), dc,
          new ResponseHandler(clusterMap), time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin,
          this);
    } catch (IOException | ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns Recovery thread
   */
  @Override
  protected ReplicaThread getReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    return new RecoveryThread(threadName, tokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient,
        replicationConfig, replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry,
        replicatingOverSsl, datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate,
        leaderBasedReplicationAdmin, this);
  }

  public String getRecoveryTokenFilename(RemoteReplicaInfo replica) {
    return String.format("%s%s%s_%s", replica.getLocalReplicaId().getMountPath(),
        File.separatorChar, RECOVER_TOKEN_FILE_PREFIX, replica.getReplicaId().getPartitionId().getId());
  }

  @Override
  public int reloadReplicationTokenIfExists(ReplicaId localReplica, List<RemoteReplicaInfo> peerReplicas)
      throws ReplicationException {
    // For each replica of a partition
    for (RemoteReplicaInfo peerReplica : peerReplicas) {
      try {
        // Read json token and set it in peer replica info
        String tokenFile = getRecoveryTokenFilename(peerReplica);
        String jsonString = Utils.readStringFromFile(tokenFile);
        RecoveryToken recoveryToken = new RecoveryToken(new JSONObject(jsonString));
        peerReplica.setToken(recoveryToken);
        logger.info("Loaded recovery token {} into memory", tokenFile);
      } catch (IOException e) {
        // If fail to read token from file, just set a new token. This will restart recovery from the top.
        peerReplica.setToken(new RecoveryToken());
        logger.info("Created new recovery token for {}", peerReplica.getReplicaId().getPartitionId().getId());
      }
    }
    return 0;
  }

  /**
   * Add a replica of given partition and its {@link RemoteReplicaInfo}s to backup list.
   * @param partitionName name of the partition of the replica to add.
   * @throws ReplicationException if replicas initialization failed.
   */
  private void addCloudReplica(String partitionName) throws Exception {
    // Adding cloud replica occurs when replica becomes leader from standby. Hence, if this a new added replica, it
    // should be present in storage manager already.
    ReplicaId localReplica = storeManager.getReplica(partitionName);
    if (localReplica == null) {
      logger.warn("Got partition leader notification for partition {} that is not present on the node", partitionName);
      return;
    }
    PartitionId partitionId = localReplica.getPartitionId();
    Store store = storeManager.getStore(partitionId);
    if (store == null) {
      logger.warn("Unable to add cloud replica for partition {} as store for the partition is not present or started.",
          partitionName);
      return;
    }
    DataNodeId cloudDataNode = new CloudServiceDataNode(clusterMapConfig);
    CloudReplica peerCloudReplica = new CloudReplica(partitionId, cloudDataNode);
    FindTokenFactory findTokenFactory =
        tokenHelper.getFindTokenFactoryFromReplicaType(peerCloudReplica.getReplicaType());
    RemoteReplicaInfo remoteReplicaInfo =
        new RemoteReplicaInfo(peerCloudReplica, localReplica, store, findTokenFactory.getNewFindToken(),
            storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec * Replication_Delay_Multiplier,
            SystemTime.getInstance(), peerCloudReplica.getDataNodeId().getPortToConnectTo());
    replicationMetrics.addMetricsForRemoteReplicaInfo(remoteReplicaInfo, trackPerDatacenterLagInMetric);

    // Note that for each replica on a Ambry server node, there is only one cloud replica that it will be replicating from.
    List<RemoteReplicaInfo> remoteReplicaInfos = Collections.singletonList(remoteReplicaInfo);
    PartitionInfo partitionInfo = new PartitionInfo(remoteReplicaInfos, partitionId, store, localReplica);
    partitionToPartitionInfo.put(partitionId, partitionInfo);
    mountPathToPartitionInfos.computeIfAbsent(localReplica.getMountPath(), key -> ConcurrentHashMap.newKeySet())
        .add(partitionInfo);
    logger.info("Cloud Partition {} added to {}. CloudNode {} port {}", partitionName, dataNodeId, cloudDataNode,
        cloudDataNode.getPortToConnectTo());

    // Reload replication token if exist.
    reloadReplicationTokenIfExists(localReplica, remoteReplicaInfos);

    // Add remoteReplicaInfos to {@link ReplicaThread}.
    addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, true);
    if (replicationConfig.replicationTrackPerPartitionLagFromRemote) {
      replicationMetrics.addLagMetricForPartition(partitionId, true);
    }
    replicationMetrics.addCatchUpPointMetricForPartition(partitionId);
    logger.info("Added cloud replica for partition-{} for recovery", partitionName);
  }

  /**
   * Check if a token is for the given {@link RemoteReplicaInfo} based on partition id.
   * @param remoteReplicaInfo The remoteReplicaInfo to check.
   * @param tokenInfo The tokenInfo to check.
   * @return true if partition id matches. false otherwise.
   */
  @Override
  protected boolean isTokenForRemoteReplicaInfo(RemoteReplicaInfo remoteReplicaInfo,
      RemoteReplicaInfo.ReplicaTokenInfo tokenInfo) {
    // Note that in case of cloudReplicaTokens, the actual remote vcr node might not match as the vcr node is chosen at
    // random during initialization. So it's enough to just match the partitionId in the token so that replication
    // can start from cloud from where it left off.
    return tokenInfo.getPartitionId().equals(remoteReplicaInfo.getReplicaId().getPartitionId());
  }

  @Override
  protected boolean shouldReplicateFromDc(String datacenterName) {
    return true;
  }

  /**
   * Remove a replica of given partition and its {@link RemoteReplicaInfo}s from the backup list.
   * @param partitionName the partition of the replica to be removed.
   */
  private void removeCloudReplica(String partitionName) {
    // Removing cloud replica occurs when replica from LEADER to STANDBY (this may be triggered by "Move Replica" or
    // regular leadership hand-off due to server deployment). No matter what triggers this transition, the local replica
    // should be present in storage manager at this point of time.
    ReplicaId localReplica = storeManager.getReplica(partitionName);
    if (localReplica == null) {
      logger.warn("Attempting to remove cloud partition {} that is not present on the node", partitionName);
      return;
    }
    PartitionId partitionId = localReplica.getPartitionId();
    stopPartitionReplication(partitionId);
    replicationMetrics.removeLagMetricForPartition(partitionId);
    replicationMetrics.removeCatchupPointMetricForPartition(partitionId);
    logger.info("Cloud Partition {} removed from {}", partitionId, dataNodeId);
  }

  /**
   * Randomly select a {@link DataNodeId} from list of {@code vcrNodes}.
   * @return randomly selected {@link DataNodeId} object.
   * @throws ReplicationException if there are no vcr nodes.
   */
  DataNodeId getCloudDataNode() throws ReplicationException {
    List<CloudDataNode> nodes = vcrNodes.get();
    if (nodes.isEmpty()) {
      throw new ReplicationException("No VCR node found to replicate partition from cloud.");
    }
    return nodes.get(Utils.getRandomShort(random) % nodes.size());
  }

  /**
   * When there is a change in vcr nodes state, update the new list of live vcr nodes.
   * Also if there are nodes that are removed as part of change, then replication from
   * those nodes should stop and the partitions should find new nodes to replicate from.
   * Note that this method is not thread safe in the wake of arriving helix notifications.
   * @param newVcrNodes Set of new vcr nodes.
   */
  private void handleChangeInVcrNodes(Set<CloudDataNode> newVcrNodes) {
    Set<CloudDataNode> removedNodes = new HashSet<>(vcrNodes.get());
    removedNodes.removeAll(newVcrNodes);
    vcrNodes.set(new ArrayList<>(newVcrNodes));
    logger.info("Handling VCR nodes change. The removed vcr nodes: {}", removedNodes);
    List<PartitionId> partitionsOnRemovedNodes = getPartitionsOnNodes(removedNodes);
    for (PartitionId partitionId : partitionsOnRemovedNodes) {
      try {
        // We first remove replica to stop replication from removed node, and then add replica so that it can pick a
        // new cloud node to start replicating from.
        removeCloudReplica(partitionId.toPathString());
        addCloudReplica(partitionId.toPathString());
      } catch (Exception rex) {
        recoveryMetrics.addCloudPartitionErrorCount.inc();
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
      Set<CloudDataNode> newVcrNodes = new HashSet<>();
      ConcurrentHashMap<String, CloudDataNode> newInstanceNameToCloudDataNode = new ConcurrentHashMap<>();

      // create a new list of available vcr nodes.
      for (InstanceConfig instanceConfig : instanceConfigs) {
        if (instanceConfig.getRecord().getBooleanField(VCR_HELIX_CONFIG_READY, false)) {
          // only when VCR_HELIX_CONFIG_READY, we take action on it.
          String instanceName = instanceConfig.getInstanceName();
          Port sslPort =
              getSslPortStr(instanceConfig) == null ? null : new Port(getSslPortStr(instanceConfig), PortType.SSL);
          Port http2Port = getHttp2PortStr(instanceConfig) == null ? null
              : new Port(getHttp2PortStr(instanceConfig), PortType.HTTP2);
          CloudDataNode cloudDataNode = new CloudDataNode(instanceConfig.getHostName(),
              new Port(Integer.parseInt(instanceConfig.getPort()), PortType.PLAINTEXT), sslPort, http2Port,
              clusterMapConfig.clustermapVcrDatacenterName, clusterMapConfig);
          newInstanceNameToCloudDataNode.put(instanceName, cloudDataNode);
          newVcrNodes.add(cloudDataNode);
          logger.info("Instance config change. VCR Node {} added. SslPort: {}, Http2Port: {}", cloudDataNode, sslPort,
              http2Port);
        } else {
          logger.info("Instance config change received, but VCR_HELIX_CONFIG_READY is false. Instance: {}:{}",
              instanceConfig.getHostName(), instanceConfig.getPort());
        }
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
      Set<CloudDataNode> newVcrNodes = new HashSet<>();
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
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      logger.info("Partition state change notification from Offline to Bootstrap received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      logger.info("Partition state change notification from Bootstrap to Standby received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {
      logger.info("Partition state change notification from Standby to Leader received for partition {}",
          partitionName);
      if (shouldReplicatePartition(partitionName)) {
        synchronized (notificationLock) {
          try {
            addCloudReplica(partitionName);
          } catch (Exception rex) {
            logger.error("Exception {} while adding replication for partition {}", rex, partitionName);
            recoveryMetrics.addCloudPartitionErrorCount.inc();
          }
        }
      }
    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {
      logger.info("Partition state change notification from Leader to Standby received for partition {}",
          partitionName);
      if (shouldReplicatePartition(partitionName)) {
        synchronized (notificationLock) {
          removeCloudReplica(partitionName);
        }
      }
    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {
      logger.info("Partition state change notification from Standby to Inactive received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {
      logger.info("Partition state change notification from Inactive to Offline received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeDroppedFromOffline(String partitionName) {
      logger.info("Partition state change notification from Offline to Dropped received for partition {}",
          partitionName);
    }

    /**
     * If only config specified list of partitions are being replicated from cloud, then check that the partition
     * belongs to the specified list.
     * @param partitionName Name of the partition to be checked.
     * @return true if all the partitions are being replicated or if the partition in the list of partitions to be
     *         replicated. false otherwise.
     */
    private boolean shouldReplicatePartition(String partitionName) {
      if (!replicationConfig.replicationVcrRecoveryPartitions.isEmpty()
          && !replicationConfig.replicationVcrRecoveryPartitions.contains(partitionName)) {
        logger.info("Ignoring state change of partition {} as it is not in recovery partition config", partitionName);
        return false;
      }
      return true;
    }
  }
}
