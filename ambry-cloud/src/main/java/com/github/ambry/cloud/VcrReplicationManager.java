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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureMetrics;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapChangeListener;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.HelixVcrUtil;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.clustermap.VcrClusterParticipantListener;
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
import com.github.ambry.replication.FindToken;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
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
  private final String localDatacenterName;
  protected String azureTableNameReplicaTokens;
  protected AzureCloudConfig azureCloudConfig;
  protected AzureMetrics azureMetrics;
  protected VerifiableProperties properties;
  protected CloudDestination cloudDestination;
  private CloudStorageCompactor cloudStorageCompactor;
  protected ScheduledExecutorService cloudCompactionScheduler;
  private final Lock vcrHelixUpdateLock = new ReentrantLock();
  private volatile boolean isVcrHelixUpdater = false;
  private volatile boolean isVcrHelixUpdateInProgress = false;
  private volatile boolean isAmbryListenerToUpdateVcrHelixRegistered = false;
  private volatile ScheduledFuture<?> vcrHelixUpdateFuture = null;
  private volatile ScheduledFuture<?> ambryVcrHelixSyncCheckTaskFuture = null;
  private final HelixVcrUtil.VcrHelixConfig vcrHelixConfig;
  private DistributedLock vcrUpdateDistributedLock = null;
  private final List<ReplicaThread> threadPool;
  private final AtomicInteger threadIndex;

  public static final String BACKUP_NODE = "backupNode";
  public static final String TOKEN_TYPE = "tokenType";
  public static final String LOG_SEGMENT = "logSegment";
  public static final String OFFSET = "offset";
  public static final String STORE_KEY = "storeKey";
  public static final String BINARY_TOKEN = "binaryToken";

  public VcrReplicationManager(VerifiableProperties properties, StoreManager storeManager,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, VcrClusterParticipant vcrClusterParticipant,
      CloudDestination cloudDestination, ScheduledExecutorService scheduler, NetworkClientFactory networkClientFactory,
      NotificationSystem requestNotification, StoreKeyConverterFactory storeKeyConverterFactory)
      throws ReplicationException, IllegalStateException {
    super(new ReplicationConfig(properties), new ClusterMapConfig(properties), new StoreConfig(properties),
        storeKeyFactory, clusterMap, scheduler, vcrClusterParticipant.getCurrentDataNodeId(), Collections.emptyList(),
        networkClientFactory, clusterMap.getMetricRegistry(), requestNotification, storeKeyConverterFactory,
        new ServerConfig(properties).serverMessageTransformer, null, storeManager, null,
        true);
    this.properties = properties;
    this.cloudConfig = new CloudConfig(properties);
    this.azureCloudConfig = new AzureCloudConfig(properties);
    this.vcrMetrics = new VcrMetrics(metricRegistry);
    this.azureMetrics = new AzureMetrics(metricRegistry);
    this.vcrClusterParticipant = vcrClusterParticipant;
    try {
      vcrHelixConfig =
          new ObjectMapper().readValue(cloudConfig.vcrHelixUpdateConfig, HelixVcrUtil.VcrHelixConfig.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("VcrHelixConfig is not correct");
    }
    vcrMetrics.registerVcrHelixUpdateGauge(this::getVcrHelixUpdaterAsCount, this::getVcrHelixUpdateInProgressAsCount);
    if (cloudConfig.cloudBlobCompactionEnabled) {
      this.cloudStorageCompactor =  new CloudStorageCompactor(properties, metricRegistry, cloudDestination,
          partitionToPartitionInfo.keySet());
      /*
        Create a new scheduler and schedule 1 daemon for compaction. No need to config this.
        The existing scheduling framework schedules tasks at a fixed rate, not a fixed delay and does not use daemons.
        This is does not suit compaction. We cannot determine the time it takes for a single run of cloud-compaction to finish.
        With fixed-rate scheduling, a second compaction can start immediately after the previous run without any delay.
        A better approach is to let compaction take its time and introduce a delay between consecutive cycles to not waste resources.
        Daemon threads ensure the server exits on a shutdown, because JVM _will_ exit when only daemons remain at the end.
       */
      this.cloudCompactionScheduler = Utils.newScheduler(1, "cloud-compaction-controller-",true);
      logger.info("[COMPACT] Created CloudStorageCompactor and compactionScheduler");
    }
    this.cloudDestination = cloudDestination;
    // Create the table at the start so that we can catch issues in creation, and the table is ready for threads to log
    this.cloudDestination.getTableClient(this.azureCloudConfig.azureTableNameCorruptBlobs);
    azureTableNameReplicaTokens = this.azureCloudConfig.azureTableNameReplicaTokens;
    this.cloudDestination.getTableClient(azureTableNameReplicaTokens);
    // Cross-colo replication is unsupported in VCR
    localDatacenterName = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());
    logger.info("Local datacenter id = {}, Local datacenter name = {}", clusterMap.getLocalDatacenterId(),
        localDatacenterName);
    threadPool = createThreadPool(localDatacenterName, getNumReplThreads(cloudConfig.backupNodeCpuScale), false);
    threadIndex = new AtomicInteger(0);
  }

  /**
   * Returns {@link VcrReplicaThread}
   */
  @Override
  protected ReplicaThread getReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    return new VcrReplicaThread(threadName, tokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient,
        notification, storeKeyConverter, transformer, replicatingOverSsl, datacenterName, responseHandler, time,
        replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin, cloudDestination, properties);
  }

  /**
   * Returns the number of replication threads to create
   * @return num replication threads
   */
  protected int getNumReplThreads(double cpuScale) {
    if (cpuScale <= 0) {
      // Fall here for 0 and avoid a call to runtime
      return (int) Math.abs(cpuScale);
    }
    return (int) (Double.valueOf(Runtime.getRuntime().availableProcessors()) * cpuScale);
  }

  @Override
  protected void createThread(ReplicaThread replicaThread, boolean unused) {
    replicaThread.setThread(Utils.newThread(replicaThread.getName(), replicaThread, false));
  }

  /**
   * Assign all replicas of a partition to the same thread to avoid concurrent updates to Azure
   *
   * @param partitionId Partition
   * @param remoteReplicaInfos List of {@link RemoteReplicaInfo} List of replicas, must belong to same partition
   */
  protected void addPartitionToReplicaThread(PartitionId partitionId, List<RemoteReplicaInfo> remoteReplicaInfos) {
    /**
     * This fn must be called with rwlock.writeLock() held.
     * It is called by helix when a new partition (not a new replica) is assigned to VCR node.
     * addPartition -> addPartitionToReplicaThread
     */
    // All replicas belong to the same partition, so just assign all to the same thread
    ReplicaThread rthread = threadPool.get(threadIndex.incrementAndGet() % threadPool.size());
    for (RemoteReplicaInfo rinfo : remoteReplicaInfos) {
      String datacenter = rinfo.getReplicaId().getDataNodeId().getDatacenterName();
      if (!localDatacenterName.equalsIgnoreCase(datacenter)) {
        throw new UnsupportedOperationException(
            String.format("Cross-colo replication from %s to %s is unsupported in VCR for replica {}",
                localDatacenterName, datacenter, rinfo));
      }
      rthread.addRemoteReplicaInfo(rinfo);
      rinfo.setReplicaThread(rthread);
      logger.info("[PARTITION] Added replica {} to thread {}", rinfo, rthread.getName());
    }
    partitionToPartitionInfo.get(partitionId).setReplicaThread(rthread);
    rthread.startThread();
  }

  /**
   * Assign all replicas of a partition to the same thread to avoid concurrent updates to Azure
   * @param remoteReplicaInfos List of {@link RemoteReplicaInfo} These replicas belong to different partitions
   * @param unused unused variable
   */
  @Override
  protected void addRemoteReplicaInfoToReplicaThread(List<RemoteReplicaInfo> remoteReplicaInfos, boolean unused) {
    /**
     * This fn must be called with rwlock.writeLock() held.
     * It is called by helix when a new replica (not a new partition) is assigned to VCR node.
     * onReplicaAddedOrRemoved -> addPartitionToReplicaThread
     */
    try {
      for (RemoteReplicaInfo rinfo : remoteReplicaInfos) {
        try {
          // Replicas here may belong to different partitions, but they must already be assigned to this node
          PartitionId partitionId = rinfo.getReplicaId().getPartitionId();
          ReplicaThread rthread = partitionToPartitionInfo.get(partitionId).getReplicaThread();
          String datacenter = rinfo.getReplicaId().getDataNodeId().getDatacenterName();
          if (!localDatacenterName.equalsIgnoreCase(datacenter)) {
            throw new UnsupportedOperationException(
                String.format("Cross-colo replication from %s to %s is unsupported in VCR for replica {}",
                    localDatacenterName, datacenter, rinfo));
          }
          rthread.addRemoteReplicaInfo(rinfo);
          rinfo.setReplicaThread(rthread);
          logger.info("[REPLICA] Added replica {} to thread {}", rinfo, rthread.getName());
        } catch (Throwable e) {
          vcrMetrics.addPartitionErrorCount.inc();
          logger.error("[REPLICA] Failed to add replica {} to thread due to {}", rinfo, e);
        }
      }
    } catch (Throwable e) {
      vcrMetrics.addPartitionErrorCount.inc();
      logger.error("Failed to add replica(s) due to {}", e);
    }
  }

  @Override
  public void retrieveReplicaTokensAndPersistIfNecessary(String mountPath) {
    // nothing to do as tokens are loaded in reloadReplicationTokenIfExists() when helix adds replica
  }

  @Override
  public int reloadReplicationTokenIfExists(ReplicaId localReplica, List<RemoteReplicaInfo> peerReplicas) {
    throw new UnsupportedOperationException("Reload replication-tokens just before replication");
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
          } catch (Throwable e) {
            logger.error("Failed to start VCR updater due to {}", e);
          } finally {
            vcrHelixUpdateLock.unlock();
          }
        }
        addPartition(partitionId);
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
          } catch (Throwable e) {
            logger.error("Failed to stop VCR updater due to {}", e);
          } finally {
            vcrHelixUpdateLock.unlock();
          }
        }
        removePartition(partitionId);
      }
    });

    try {
      vcrClusterParticipant.participate();
    } catch (Exception e) {
      throw new ReplicationException("Cluster participate failed.", e);
    }

    scheduleTasks();

    started = true;
    startupLatch.countDown();
  }

  protected void scheduleTasks() {
    // Do not schedule any background token writer

    if (cloudConfig.cloudBlobCompactionEnabled && cloudStorageCompactor != null) {
      logger.info("[COMPACT] Waiting {} seconds to populate partitions for compaction", cloudConfig.cloudBlobCompactionStartupDelaySecs);
      cloudCompactionScheduler.scheduleWithFixedDelay(cloudStorageCompactor, cloudConfig.cloudBlobCompactionStartupDelaySecs,
          TimeUnit.HOURS.toSeconds(cloudConfig.cloudBlobCompactionIntervalHours), TimeUnit.SECONDS);
    }
  }

  @Override
  protected RemoteReplicaInfo createRemoteReplica(ReplicaId remote, ReplicaId local, Store store, FindToken token) {
    return
        new RemoteReplicaInfo(this.replicationConfig, remote, local, store,
            token, -1, SystemTime.getInstance(),
            remote.getDataNodeId().getPortToConnectTo());
  }

  /**
   * Add a replica of given {@link PartitionId} and its {@link RemoteReplicaInfo}s to backup list.
   * @param partitionId the {@link PartitionId} of the replica to add.
   * @throws ReplicationException if replicas initialization failed.
   */
  void addPartition(PartitionId partitionId) {
    try {
      rwLock.writeLock().lock();
      if (partitionToPartitionInfo.containsKey(partitionId)) {
        vcrMetrics.addPartitionErrorCount.inc();
        logger.warn("Partition {} already exists", partitionId.getId());
        return;
      }
      ReplicaId cloudReplica = new CloudReplica(partitionId, vcrClusterParticipant.getCurrentDataNodeId());
      List<? extends ReplicaId> peerReplicas = cloudReplica.getPeerReplicaIds();
      if (peerReplicas == null || peerReplicas.size() == 0) {
        vcrMetrics.addPartitionErrorCount.inc();
        logger.error("No peer replicas for {}", partitionId.getId());
        return;
      }
      List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
      if (!storeManager.addBlobStore(cloudReplica)) {
        vcrMetrics.addPartitionErrorCount.inc();
        logger.error("Failed to add Store for partition {}", partitionId.getId());
        return;
      }
      Store store = storeManager.getStore(partitionId);
      for (ReplicaId peerReplica : peerReplicas) {
        if (peerReplica.getDataNodeId().getDatacenterName().equalsIgnoreCase(localDatacenterName)) {
          remoteReplicaInfos.add(createRemoteReplica(peerReplica, cloudReplica, store, null));
        }
      }
      if (!remoteReplicaInfos.isEmpty()) {
        updatePartitionInfoMaps(remoteReplicaInfos, cloudReplica);
        addPartitionToReplicaThread(partitionId, remoteReplicaInfos);
      }
    } catch (Throwable e) {
      // Helix will run into error state if exception throws in Helix context.
      vcrMetrics.addPartitionErrorCount.inc();
      logger.error("Failed to add partition {} due to {}", partitionId.getId(), e);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Remove a replica of given {@link PartitionId} and its {@link RemoteReplicaInfo}s from the backup list.
   * @param partitionId the {@link PartitionId} of the replica to removed.
   */
  void removePartition(PartitionId partitionId) {
    rwLock.writeLock().lock();
    try {
      PartitionInfo partitionInfo = partitionToPartitionInfo.get(partitionId);
      stopPartitionReplication(partitionId);
      storeManager.shutdownBlobStore(partitionId);
      storeManager.removeBlobStore(partitionId);
      partitionInfo.setReplicaThread(null);
      logger.info("Partition {} removed from {}", partitionId, dataNodeId);
    } catch (Throwable e) {
      // Helix will run into error state if exception throws in Helix context.
      vcrMetrics.removePartitionErrorCount.inc();
      logger.error("Failed to remove partition-{} due to {}", partitionId, dataNodeId, e);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public void shutdown() throws ReplicationException {
    if (cloudStorageCompactor != null) {
      cloudStorageCompactor.shutdown();
      Utils.shutDownExecutorService(cloudCompactionScheduler, cloudConfig.cloudBlobCompactionShutdownTimeoutSecs,
          TimeUnit.SECONDS);
    }
    super.shutdown();
  }

  public VcrMetrics getVcrMetrics() {
    return vcrMetrics;
  }

  /** For testing only */
  public CloudStorageCompactor getCloudStorageCompactor() {
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
