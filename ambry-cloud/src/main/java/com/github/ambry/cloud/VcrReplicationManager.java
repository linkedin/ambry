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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapChangeListener;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.HelixVcrUtil;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.clustermap.VcrClusterParticipantListener;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.helix.lock.DistributedLock;
import org.apache.helix.lock.LockScope;
import org.apache.helix.lock.helix.HelixLockScope;
import org.apache.helix.lock.helix.ZKDistributedNonblockingLock;


/**
 * {@link VcrReplicationManager} is used to backup partitions to Cloud. Partitions assignment is handled by Helix.
 */
public class VcrReplicationManager extends ReplicationEngine {
  private final CloudConfig cloudConfig;
  private final VcrMetrics vcrMetrics;
  private final VcrClusterParticipant vcrClusterParticipant;
  private final CloudStorageCompactor cloudStorageCompactor;
  private final CloudContainerCompactor cloudContainerCompactor;
  private final Map<String, Store> partitionStoreMap = new HashMap<>();
  private final boolean trackPerDatacenterLagInMetric;
  private final Lock vcrHelixUpdateLock = new ReentrantLock();
  private volatile boolean isVcrHelixUpdater = false;
  private volatile boolean isVcrHelixUpdateInProgress = false;
  private volatile boolean isAmbryListenerToUpdateVcrHelixRegistered = false;
  private volatile ScheduledFuture<?> vcrHelixUpdateFuture = null;
  private volatile ScheduledFuture<?> ambryVcrHelixSyncCheckTaskFuture = null;
  private final HelixVcrUtil.VcrHelixConfig vcrHelixConfig;
  private DistributedLock vcrUpdateDistributedLock = null;

  public VcrReplicationManager(CloudConfig cloudConfig, ReplicationConfig replicationConfig,
      ClusterMapConfig clusterMapConfig, StoreConfig storeConfig, StoreManager storeManager,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, VcrClusterParticipant vcrClusterParticipant,
      CloudDestination cloudDestination, ScheduledExecutorService scheduler, ConnectionPool connectionPool,
      VcrMetrics vcrMetrics, NotificationSystem requestNotification, StoreKeyConverterFactory storeKeyConverterFactory,
      String transformerClassName) throws ReplicationException, IllegalStateException {
    super(replicationConfig, clusterMapConfig, storeConfig, storeKeyFactory, clusterMap, scheduler,
        vcrClusterParticipant.getCurrentDataNodeId(), Collections.emptyList(), connectionPool,
        vcrMetrics.getMetricRegistry(), requestNotification, storeKeyConverterFactory, transformerClassName, null,
        storeManager, null, true);
    this.cloudConfig = cloudConfig;
    this.vcrClusterParticipant = vcrClusterParticipant;
    this.vcrMetrics = vcrMetrics;
    this.persistor =
        new CloudTokenPersistor(replicaTokenFileName, mountPathToPartitionInfos, replicationMetrics, clusterMap,
            tokenHelper, cloudDestination);
    this.cloudStorageCompactor =
        cloudConfig.cloudBlobCompactionEnabled ? new CloudStorageCompactor(cloudDestination, cloudConfig,
            partitionToPartitionInfo.keySet(), vcrMetrics) : null;
    this.cloudContainerCompactor = cloudDestination.getContainerCompactor();
    trackPerDatacenterLagInMetric = replicationConfig.replicationTrackPerDatacenterLagFromLocal;
    // We need a datacenter to replicate from, which should be specified in the cloud config.
    if (cloudConfig.vcrSourceDatacenters.isEmpty()) {
      throw new IllegalStateException("One or more VCR cross colo replication peer datacenter should be specified");
    }
    try {
      vcrHelixConfig =
          new ObjectMapper().readValue(cloudConfig.vcrHelixUpdateConfig, HelixVcrUtil.VcrHelixConfig.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("VcrHelixConfig is not correct");
    }
    vcrMetrics.registerVcrHelixUpdateGauge(this::getVcrHelixUpdaterAsCount, this::getVcrHelixUpdateInProgressAsCount);
  }

  @Override
  public void start() throws ReplicationException {
    // Add listener for new coming assigned partition
    vcrClusterParticipant.addListener(new VcrClusterParticipantListener() {
      @Override
      public void onPartitionAdded(PartitionId partitionId) {
        if (partitionId.isEqual(cloudConfig.vcrHelixUpdaterPartitionId)) {
          vcrHelixUpdateLock.lock();
          try {
            if (!isAmbryListenerToUpdateVcrHelixRegistered) {
              // Prepare the vcrUpdateDistributedLock. Only one instance can update vcr helix cluster at one time.
              // It's possible isVcrHelixUpdater to be true on two nodes.
              // For example, at time "t" node A is the owner of partition 1. Due to some partition reassignment
              // (lets say new node addition), partition 1 get assigned to node B at time "t+1". In this case it's possible
              // for Node B to get notification of addPartition of partition 1 at "t+2" before Node A gets removePartition
              // notification (at t+4). If a main cluster update happens between "t+2" and "t+4", then two nodes might try
              // to update vcr cluster at the same time. Therefore, we need this distributed lock.
              LockScope distributedLockScope = new HelixLockScope(HelixLockScope.LockScopeProperty.CLUSTER,
                  Arrays.asList(cloudConfig.vcrClusterName, cloudConfig.vcrClusterName));
              vcrUpdateDistributedLock =
                  new ZKDistributedNonblockingLock(distributedLockScope, cloudConfig.vcrClusterZkConnectString,
                      cloudConfig.vcrHelixLockTimeoutInMs, "Updating VCR Cluster", clusterMapConfig.clusterMapHostName);
              // Only register the listener once. Unfortunately, we can't unregister a listener, so we use
              // isAmbryListenerToUpdateVcrHelixRegistered as the flag.
              clusterMap.registerClusterMapListener(new AmbryListenerToUpdateVcrHelix());
              isAmbryListenerToUpdateVcrHelixRegistered = true;
              // Schedule a fixed rate task to check if ambry helix and vcr helix on sync.
              ambryVcrHelixSyncCheckTaskFuture = scheduler.scheduleAtFixedRate(() -> checkAmbryHelixAndVcrHelixOnSync(),
                  cloudConfig.vcrHelixSyncCheckIntervalInSeconds, cloudConfig.vcrHelixSyncCheckIntervalInSeconds,
                  TimeUnit.SECONDS);
              logger.info("VCR updater registered.");
            }
            isVcrHelixUpdater = true;
            scheduleVcrHelix("VCR starts");
          } finally {
            vcrHelixUpdateLock.unlock();
          }
        }
        try {
          addReplica(partitionId);
          logger.info("Partition {} added to {}", partitionId, dataNodeId);
        } catch (ReplicationException e) {
          vcrMetrics.addPartitionErrorCount.inc();
          logger.error("Exception on adding Partition {} to {}: ", partitionId, dataNodeId, e);
        } catch (Exception e) {
          // Helix will run into error state if exception throws in Helix context.
          vcrMetrics.addPartitionErrorCount.inc();
          logger.error("Unknown Exception on adding Partition {} to {}: ", partitionId, dataNodeId, e);
        }
      }

      @Override
      public void onPartitionRemoved(PartitionId partitionId) {
        if (partitionId.isEqual(cloudConfig.vcrHelixUpdaterPartitionId)) {
          vcrHelixUpdateLock.lock();
          try {
            isVcrHelixUpdater = false;
            if (vcrHelixUpdateFuture != null) {
              vcrHelixUpdateFuture.cancel(false);
            }
            if (ambryVcrHelixSyncCheckTaskFuture != null) {
              ambryVcrHelixSyncCheckTaskFuture.cancel(false);
            }
          } finally {
            vcrHelixUpdateLock.unlock();
          }
        }
        try {
          removeReplica(partitionId);
        } catch (Exception e) {
          // Helix will run into error state if exception throws in Helix context.
          vcrMetrics.removePartitionErrorCount.inc();
          logger.error("Exception on removing Partition {} from {}: ", partitionId, dataNodeId, e);
        }
      }
    });

    try {
      vcrClusterParticipant.participate();
    } catch (Exception e) {
      throw new ReplicationException("Cluster participate failed.", e);
    }

    // start background persistent thread
    // start scheduler thread to persist index in the background
    scheduleTask(persistor, true, replicationConfig.replicationTokenFlushDelaySeconds,
        replicationConfig.replicationTokenFlushIntervalSeconds, "replica token persistor");

    // Schedule thread to purge dead blobs for this VCR's partitions
    // after delay to allow startup to finish.
    scheduleTask(cloudStorageCompactor, cloudConfig.cloudBlobCompactionEnabled,
        cloudConfig.cloudBlobCompactionStartupDelaySecs,
        TimeUnit.HOURS.toSeconds(cloudConfig.cloudBlobCompactionIntervalHours), "cloud blob compaction");

    // Schedule thread to purge blobs belonging to deprecated containers for this VCR's partitions
    // after delay to allow startup to finish.
    scheduleTask(() -> cloudContainerCompactor.compactAssignedDeprecatedContainers(
        vcrClusterParticipant.getAssignedPartitionIds()), cloudConfig.cloudContainerCompactionEnabled,
        cloudConfig.cloudContainerCompactionStartupDelaySecs,
        TimeUnit.HOURS.toSeconds(cloudConfig.cloudContainerCompactionIntervalHours), "cloud container compaction");
    started = true;
    startupLatch.countDown();
  }

  /**
   * Schedule the specified task if enabled with the specified delay and interval.
   * @param task {@link Runnable} task to be scheduled.
   * @param isEnabled flag indicating if the task is enabled. If false the task is not scheduled.
   * @param delaySec initial delay to allow startup to finish before starting task.
   * @param intervalSec period between successive executions.
   * @param taskName name of the task being scheduled.
   */
  private void scheduleTask(Runnable task, boolean isEnabled, long delaySec, long intervalSec, String taskName) {
    if (isEnabled) {
      scheduler.scheduleAtFixedRate(task, delaySec, intervalSec, TimeUnit.SECONDS);
      logger.info("Scheduled {} task to run every {} seconds starting in {} seconds.", taskName, intervalSec, delaySec);
    } else {
      logger.warn("Running with {} turned off!", taskName);
    }
  }

  /**
   * Add a replica of given {@link PartitionId} and its {@link RemoteReplicaInfo}s to backup list.
   * @param partitionId the {@link PartitionId} of the replica to add.
   * @throws ReplicationException if replicas initialization failed.
   */
  void addReplica(PartitionId partitionId) throws ReplicationException {
    if (partitionToPartitionInfo.containsKey(partitionId)) {
      throw new ReplicationException("Partition " + partitionId + " already exists on " + dataNodeId);
    }
    ReplicaId cloudReplica = new CloudReplica(partitionId, vcrClusterParticipant.getCurrentDataNodeId());
    if (!storeManager.addBlobStore(cloudReplica)) {
      logger.error("Can't start cloudstore for replica {}", cloudReplica);
      throw new ReplicationException("Can't start cloudstore for replica " + cloudReplica);
    }
    List<? extends ReplicaId> peerReplicas = cloudReplica.getPeerReplicaIds();
    List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
    Store store = storeManager.getStore(partitionId);
    if (peerReplicas != null) {
      for (ReplicaId peerReplica : peerReplicas) {
        if (!shouldReplicateFromDc(peerReplica.getDataNodeId().getDatacenterName())) {
          continue;
        }
        // We need to ensure that a replica token gets persisted only after the corresponding data in the
        // store gets flushed to cloud. We use the store flush interval multiplied by a constant factor
        // to determine the token flush interval
        FindTokenFactory findTokenFactory =
            tokenHelper.getFindTokenFactoryFromReplicaType(peerReplica.getReplicaType());
        RemoteReplicaInfo remoteReplicaInfo =
            new RemoteReplicaInfo(peerReplica, cloudReplica, store, findTokenFactory.getNewFindToken(),
                storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec * Replication_Delay_Multiplier,
                SystemTime.getInstance(), peerReplica.getDataNodeId().getPortToConnectTo());
        replicationMetrics.addMetricsForRemoteReplicaInfo(remoteReplicaInfo, trackPerDatacenterLagInMetric);
        remoteReplicaInfos.add(remoteReplicaInfo);
      }
      rwLock.writeLock().lock();
      try {
        updatePartitionInfoMaps(remoteReplicaInfos, cloudReplica);
        partitionStoreMap.put(partitionId.toPathString(), store);
        // Reload replication token if exist.
        int tokenReloadFailCount = reloadReplicationTokenIfExists(cloudReplica, remoteReplicaInfos);
        vcrMetrics.tokenReloadWarnCount.inc(tokenReloadFailCount);

        // Add remoteReplicaInfos to {@link ReplicaThread}.
        addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, true);
        if (replicationConfig.replicationTrackPerPartitionLagFromRemote) {
          replicationMetrics.addLagMetricForPartition(partitionId, true);
        }
      } finally {
        rwLock.writeLock().unlock();
      }
    } else {
      try {
        storeManager.shutdownBlobStore(partitionId);
        storeManager.removeBlobStore(partitionId);
      } finally {
        throw new ReplicationException(
            "Failed to add Partition " + partitionId + " on " + dataNodeId + " , because no peer replicas found.");
      }
    }
  }

  /**
   * Remove a replica of given {@link PartitionId} and its {@link RemoteReplicaInfo}s from the backup list.
   * @param partitionId the {@link PartitionId} of the replica to removed.
   */
  void removeReplica(PartitionId partitionId) throws IOException, StoreException {
    rwLock.writeLock().lock();
    try {
      stopPartitionReplication(partitionId);
    } finally {
      rwLock.writeLock().unlock();
    }
    Store cloudStore = partitionStoreMap.get(partitionId.toPathString());
    if (cloudStore != null) {
      storeManager.shutdownBlobStore(partitionId);
      storeManager.removeBlobStore(partitionId);
    } else {
      logger.warn("Store not found for partition {}", partitionId);
    }
    logger.info("Partition {} removed from {}", partitionId, dataNodeId);
    // We don't close cloudBlobStore because because replicate in ReplicaThread is using a copy of
    // remoteReplicaInfo which needs CloudBlobStore.
  }

  @Override
  public void shutdown() throws ReplicationException {
    // TODO: can do these in parallel
    if (cloudStorageCompactor != null) {
      cloudStorageCompactor.shutdown();
    }
    if (cloudContainerCompactor != null) {
      cloudContainerCompactor.shutdown();
    }
    super.shutdown();
  }

  public VcrMetrics getVcrMetrics() {
    return vcrMetrics;
  }

  /** For testing only */
  CloudStorageCompactor getCloudStorageCompactor() {
    return cloudStorageCompactor;
  }

  @Override
  public void updateTotalBytesReadByRemoteReplica(PartitionId partitionId, String hostName, String replicaPath,
      long totalBytesRead) {
    // Since replica metadata request for a single partition can goto multiple vcr nodes, totalBytesReadByRemoteReplica
    // cannot be  populated locally on any vcr node.
  }

  @Override
  public long getRemoteReplicaLagFromLocalInBytes(PartitionId partitionId, String hostName, String replicaPath) {
    // TODO get replica lag from cosmos?
    return -1;
  }

  @Override
  protected String getReplicaThreadName(String datacenterToReplicateFrom, int threadIndexWithinPool) {
    return "Vcr" + super.getReplicaThreadName(datacenterToReplicateFrom, threadIndexWithinPool);
  }

  /**
   * Check if replication is allowed from given datacenter.
   * @param datacenterName datacenter name to check.
   * @return true if replication is allowed. false otherwise.
   */
  @Override
  protected boolean shouldReplicateFromDc(String datacenterName) {
    return cloudConfig.vcrSourceDatacenters.contains(datacenterName);
  }

  /**
   * Check and schedule a VCR helix update task. We schedule updateVcrHelix in 2 cases:
   * 1. On a node become online role of partition, which usually happens on restart or deployment.
   * 2. On Ambry cluster change.
   */
  private void scheduleVcrHelix(String reason) {
    if (vcrHelixUpdateFuture != null && vcrHelixUpdateFuture.cancel(false)) {
      // If a vcrHelixUpdate task is scheduled, try to cancel it first.
      logger.info("There was a scheduled vcrHelixUpdate task. Canceled.");
      vcrHelixUpdateFuture = null;
    }
    // either success cancel or not, we should schedule a new job to updateVcrHelix
    vcrHelixUpdateFuture =
        scheduler.schedule(() -> updateVcrHelix(reason), cloudConfig.vcrHelixUpdateDelayTimeInSeconds,
            TimeUnit.SECONDS);
    logger.info("VcrHelixUpdate task scheduled. Will run in {} seconds.", cloudConfig.vcrHelixUpdateDelayTimeInSeconds);
  }

  /**
   * The actual performer to update VCR Helix:
   */
  synchronized private void updateVcrHelix(String reason) {
    logger.info("Going to update VCR Helix Cluster. Reason: {}, Dryrun: {}", reason, cloudConfig.vcrHelixUpdateDryRun);
    int retryCount = 0;
    while (retryCount <= cloudConfig.vcrHelixLockMaxRetryCount && !vcrUpdateDistributedLock.tryLock()) {
      logger.warn("Could not obtain vcr update distributed lock. Sleep and retry {}/{}.", retryCount,
          cloudConfig.vcrHelixLockMaxRetryCount);
      try {
        Thread.sleep(cloudConfig.vcrWaitTimeIfHelixLockNotObtainedInMs);
      } catch (InterruptedException e) {
        logger.warn("Vcr sleep on helix lock interrupted", e);
      }
      retryCount++;
      if (retryCount == cloudConfig.vcrHelixLockMaxRetryCount) {
        logger.warn("Still can't obtain lock after {} retries with backoff time {}ms", retryCount,
            cloudConfig.vcrWaitTimeIfHelixLockNotObtainedInMs);
        return;
      }
    }
    logger.info("vcrUpdateDistributedLock obtained");
    logger.debug("Current partitions in clustermap data structure: {}",
        clusterMap.getAllPartitionIds(null).stream().map(Object::toString).collect(Collectors.joining(",")));
    try {
      String localDcZkStr = ((HelixClusterManager) clusterMap).getLocalDcZkConnectString();
      isVcrHelixUpdateInProgress = true;
      HelixVcrUtil.updateResourceAndPartition(localDcZkStr, clusterMapConfig.clusterMapClusterName,
          cloudConfig.vcrClusterZkConnectString, cloudConfig.vcrClusterName, vcrHelixConfig,
          cloudConfig.vcrHelixUpdateDryRun);
      vcrMetrics.vcrHelixUpdateSuccessCount.inc();
    } catch (Exception e) {
      // SRE and DEVs should be alerted on this metric.
      vcrMetrics.vcrHelixUpdateFailCount.inc();
      logger.warn("VCR Helix cluster update failed: ", e);
    } finally {
      isVcrHelixUpdateInProgress = false;
      vcrUpdateDistributedLock.unlock();
    }
    logger.info("VCR Helix cluster update done.");
  }

  /**
   * A method to check if Ambry Helix and VCR Helix are on sync or not.
   * If not, it will log warnning message and emit metric.
   */
  private void checkAmbryHelixAndVcrHelixOnSync() {
    boolean isSrcAndDstSync = false;
    try {
      String localDcZkStr = ((HelixClusterManager) clusterMap).getLocalDcZkConnectString();
      isSrcAndDstSync = HelixVcrUtil.isSrcDestSync(localDcZkStr, clusterMapConfig.clusterMapClusterName,
          cloudConfig.vcrClusterZkConnectString, cloudConfig.vcrClusterName);
    } catch (Exception e) {
      logger.warn("Ambry Helix and Vcr Helix sync check runs into exception: ", e);
    }
    if (vcrHelixUpdateFuture == null && !isSrcAndDstSync) {
      logger.warn("Ambry Helix cluster and VCR helix cluster are not on sync");
      // Raise alert on this metric
      vcrMetrics.vcrHelixNotOnSync.inc();
    }
  }

  int getVcrHelixUpdaterAsCount() {
    return isVcrHelixUpdater ? 1 : 0;
  }

  int getVcrHelixUpdateInProgressAsCount() {
    return isVcrHelixUpdateInProgress ? 1 : 0;
  }

  class AmbryListenerToUpdateVcrHelix implements ClusterMapChangeListener {
    @Override
    public void onReplicaAddedOrRemoved(List<ReplicaId> addedReplicas, List<ReplicaId> removedReplicas) {
      logger.info("onReplicaAddedOrRemoved event triggered by clustermap change.");
      if (isVcrHelixUpdater) { // For most VCR node, the value is false, they don't need to enter the lock.
        vcrHelixUpdateLock.lock();
        try {
          if (isVcrHelixUpdater) {
            scheduleVcrHelix("Ambry clustermap changed");
          }
        } finally {
          vcrHelixUpdateLock.unlock();
        }
      }
    }
  }
}
