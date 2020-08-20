/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A replica thread is responsible for handling replication for a set of partitions assigned to it
 */
public class ReplicaThread implements Runnable {

  private final Map<DataNodeId, Set<RemoteReplicaInfo>> replicasToReplicateGroupedByNode = new HashMap<>();
  private final Set<PartitionId> replicationDisabledPartitions = new HashSet<>();
  private final Set<PartitionId> unmodifiableReplicationDisabledPartitions =
      Collections.unmodifiableSet(replicationDisabledPartitions);
  private final Set<PartitionId> allReplicatedPartitions = new HashSet<>();
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private volatile boolean running;
  private final FindTokenHelper findTokenHelper;
  private final ClusterMap clusterMap;
  private final AtomicInteger correlationIdGenerator;
  private final DataNodeId dataNodeId;
  private final ConnectionPool connectionPool;
  private final ReplicationConfig replicationConfig;
  private final ReplicationMetrics replicationMetrics;
  private final String threadName;
  private final NotificationSystem notification;
  private static final Logger logger = LoggerFactory.getLogger(ReplicaThread.class);
  private final StoreKeyConverter storeKeyConverter;
  private final Transformer transformer;
  private final MetricRegistry metricRegistry;
  private final ResponseHandler responseHandler;
  private final boolean replicatingFromRemoteColo;
  private final boolean replicatingOverSsl;
  private final String datacenterName;
  private final long threadThrottleDurationMs;
  private final Time time;
  private final Counter throttleCount;
  private final Counter syncedBackOffCount;
  private final Counter idleCount;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition pauseCondition = lock.newCondition();
  private final ReplicaSyncUpManager replicaSyncUpManager;
  private final int maxReplicaCountPerRequest;
  private final Predicate<MessageInfo> skipPredicate;
  private volatile boolean allDisabled = false;
  private final ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin;

  public ReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, ConnectionPool connectionPool,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate) {
    this(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, connectionPool, replicationConfig,
        replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry, replicatingOverSsl,
        datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate, null);
  }

  public ReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, ConnectionPool connectionPool,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    this.threadName = threadName;
    this.running = true;
    this.findTokenHelper = findTokenHelper;
    this.clusterMap = clusterMap;
    this.correlationIdGenerator = correlationIdGenerator;
    this.dataNodeId = dataNodeId;
    this.connectionPool = connectionPool;
    this.replicationConfig = replicationConfig;
    this.replicationMetrics = replicationMetrics;
    this.notification = notification;
    this.storeKeyConverter = storeKeyConverter;
    this.transformer = transformer;
    this.metricRegistry = metricRegistry;
    this.responseHandler = responseHandler;
    this.replicatingFromRemoteColo = !(dataNodeId.getDatacenterName().equals(datacenterName));
    this.replicatingOverSsl = replicatingOverSsl;
    this.datacenterName = datacenterName;
    this.time = time;
    this.replicaSyncUpManager = replicaSyncUpManager;
    this.skipPredicate = skipPredicate;
    if (replicatingFromRemoteColo) {
      threadThrottleDurationMs = replicationConfig.replicationInterReplicaThreadThrottleSleepDurationMs;
      syncedBackOffCount = replicationMetrics.interColoReplicaSyncedBackoffCount;
      idleCount = replicationMetrics.interColoReplicaThreadIdleCount;
      throttleCount = replicationMetrics.interColoReplicaThreadThrottleCount;
    } else {
      threadThrottleDurationMs = replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs;
      syncedBackOffCount = replicationMetrics.intraColoReplicaSyncedBackoffCount;
      idleCount = replicationMetrics.intraColoReplicaThreadIdleCount;
      throttleCount = replicationMetrics.intraColoReplicaThreadThrottleCount;
    }
    this.maxReplicaCountPerRequest = replicationConfig.replicationMaxPartitionCountPerRequest;
    this.leaderBasedReplicationAdmin = leaderBasedReplicationAdmin;
  }

  /**
   * Enables/disables replication on the given {@code ids}.
   * @param ids the {@link PartitionId}s to enable/disable it on.
   * @param enable whether to enable ({@code true}) or disable.
   */
  void controlReplicationForPartitions(Collection<PartitionId> ids, boolean enable) {
    lock.lock();
    try {
      for (PartitionId id : ids) {
        if (allReplicatedPartitions.contains(id)) {
          if (enable) {
            if (replicationDisabledPartitions.remove(id)) {
              allDisabled = false;
              pauseCondition.signal();
            }
          } else {
            replicationDisabledPartitions.add(id);
            allDisabled = allReplicatedPartitions.size() == replicationDisabledPartitions.size();
          }
          logger.info("Disable status of replication of {} from {} is {}. allDisabled for {} is {}", id, datacenterName,
              replicationDisabledPartitions.contains(id), getName(), allDisabled);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return {@link Set} of {@link PartitionId}s for which replication is disabled.
   */
  Set<PartitionId> getReplicationDisabledPartitions() {
    return unmodifiableReplicationDisabledPartitions;
  }

  String getName() {
    return threadName;
  }

  @Override
  public void run() {
    try {
      logger.trace("Starting replica thread on Local node: {} Thread name: {}", dataNodeId, threadName);
      for (Map.Entry<DataNodeId, Set<RemoteReplicaInfo>> replicasToReplicateEntry : replicasToReplicateGroupedByNode.entrySet()) {
        logger.trace("Remote node: {} Thread name: {} ReplicasToReplicate: {}", replicasToReplicateEntry.getKey(),
            threadName, replicasToReplicateEntry.getValue());
      }
      logger.info("Begin iteration for thread {}", threadName);
      while (running) {
        replicate();
        lock.lock();
        try {
          if (running && allDisabled) {
            pauseCondition.await();
          }
        } catch (Exception e) {
          logger.error("Received interrupted exception during pause", e);
        } finally {
          lock.unlock();
        }
      }
    } finally {
      running = false;
      shutdownLatch.countDown();
    }
  }

  /**
   * Remove {@link RemoteReplicaInfo} from current {@link ReplicaThread}.
   * @param remoteReplicaInfo {@link RemoteReplicaInfo} to remove.
   */
  void removeRemoteReplicaInfo(RemoteReplicaInfo remoteReplicaInfo) {
    lock.lock();
    try {
      DataNodeId dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
      Set<RemoteReplicaInfo> remoteReplicaInfos = replicasToReplicateGroupedByNode.get(dataNodeId);
      if (remoteReplicaInfos != null) {
        if (!remoteReplicaInfos.remove(remoteReplicaInfo)) {
          replicationMetrics.remoteReplicaInfoRemoveError.inc();
          logger.error("ReplicaThread: {}, RemoteReplicaInfo {} not found.", threadName, remoteReplicaInfo);
        }
      } else {
        replicationMetrics.remoteReplicaInfoRemoveError.inc();
        logger.error("ReplicaThread: {}, RemoteReplicaInfos Set is not created for DataNode {}, RemoteReplicaInfo: {}.",
            threadName, dataNodeId, remoteReplicaInfo);
      }
    } finally {
      lock.unlock();
    }
    logger.trace("RemoteReplicaInfo {} is removed from ReplicaThread {}.", remoteReplicaInfo, threadName);
  }

  /**
   * Add a {@link RemoteReplicaInfo} to current {@link ReplicaThread}.
   * @param remoteReplicaInfo {@link RemoteReplicaInfo} to add.
   */
  public void addRemoteReplicaInfo(RemoteReplicaInfo remoteReplicaInfo) {
    lock.lock();
    try {
      allReplicatedPartitions.add(remoteReplicaInfo.getReplicaId().getPartitionId());
      DataNodeId dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
      if (!replicasToReplicateGroupedByNode.computeIfAbsent(dataNodeId, key -> new HashSet<>())
          .add(remoteReplicaInfo)) {
        replicationMetrics.remoteReplicaInfoAddError.inc();
        logger.error("ReplicaThread: {}, RemoteReplicaInfo {} already exists.", threadName, remoteReplicaInfo);
      }
    } finally {
      lock.unlock();
    }
    logger.trace("RemoteReplicaInfo {} is added to ReplicaThread {}. Now working on {} dataNodeIds.", remoteReplicaInfo,
        threadName, replicasToReplicateGroupedByNode.keySet().size());
  }

  /**
   * @return a deep copy of replicasToReplicateGroupedByNode but return type is Map<DataNodeId, List<RemoteReplicaInfo>>.
   */
  Map<DataNodeId, List<RemoteReplicaInfo>> getRemoteReplicaInfos() {
    lock.lock();
    try {
      return replicasToReplicateGroupedByNode.entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));
    } finally {
      lock.unlock();
    }
  }

  /**
   * Do replication for replicas grouped by {@link DataNodeId}
   * A replication cycle between two replicas involves the following steps:
   *    1. Exchange metadata : fetch the metadata of blobs added to remote replica since the last synchronization point
   *    and filter the ones missing in local store.
   *    2. Fetch missing blobs: fetch the missing blobs by issuing GET request to remote replica and write them to
   *       the local store
   *
   *  During cross-colo replication, depending on the {@link ReplicationModelType}, the missing blobs are either fetched
   *  from all remote replicas (if modelType == ALL_TO_ALL) or only fetched for local leader replicas from their remote
   *  leader replicas (if modelType == LEADER_BASED). In the latter case, non-leader replica pairs (leader <-> standby,
   *  standby <-> leader, standby <-> standby) will get their missing blobs from their corresponding leader<->leader
   *  exchanges and intra-dc replication.
   *
   *  Here is a table listing on what is exchanged between local and remote replicas based on their roles
   *  (leader/standby) when {@link ReplicationModelType is LEADER_BASED}.
   *
   *              |   Local Leader    |     Local Standby   |   Remote Leader   |  Remote Standby
   *            -------------------------------------------------------------------------------------
   *     Leader:  |        ---        |  metadata and data  | metadata and data |   metadata only
   *     Standby: | metadata and data |  metadata and data  | metadata only     |   metadata only
   *
   */
  public void replicate() {
    boolean allCaughtUp = true;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToRemoteReplicaInfo = getRemoteReplicaInfos();

    logger.trace("Replicating from {} DataNodes.", replicasToReplicateGroupedByNode.size());
    for (Map.Entry<DataNodeId, List<RemoteReplicaInfo>> entry : dataNodeToRemoteReplicaInfo.entrySet()) {
      DataNodeId remoteNode = entry.getKey();
      if (!running) {
        break;
      }
      List<RemoteReplicaInfo> replicasToReplicatePerNode = entry.getValue();
      Timer.Context context = null;
      Timer.Context portTypeBasedContext = null;
      if (replicatingFromRemoteColo) {
        context = replicationMetrics.interColoReplicationLatency.get(remoteNode.getDatacenterName()).time();
        if (replicatingOverSsl) {
          portTypeBasedContext =
              replicationMetrics.sslInterColoReplicationLatency.get(remoteNode.getDatacenterName()).time();
        } else {
          portTypeBasedContext =
              replicationMetrics.plainTextInterColoReplicationLatency.get(remoteNode.getDatacenterName()).time();
        }
      } else {
        context = replicationMetrics.intraColoReplicationLatency.time();
        if (replicatingOverSsl) {
          portTypeBasedContext = replicationMetrics.sslIntraColoReplicationLatency.time();
        } else {
          portTypeBasedContext = replicationMetrics.plainTextIntraColoReplicationLatency.time();
        }
      }
      ConnectedChannel connectedChannel = null;
      long checkoutConnectionTimeInMs = -1;
      long exchangeMetadataTimeInMs = -1;
      long fixMissingStoreKeysTimeInMs = -1;
      long replicationStartTimeInMs = time.milliseconds();
      long startTimeInMs = replicationStartTimeInMs;

      // Get a list of active replicas that needs be included for this replication cycle
      List<RemoteReplicaInfo> activeReplicasPerNode = new ArrayList<>();
      for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
        ReplicaId replicaId = remoteReplicaInfo.getReplicaId();
        boolean inBackoff = time.milliseconds() < remoteReplicaInfo.getReEnableReplicationTime();
        if (replicaId.isDown() || inBackoff
            || remoteReplicaInfo.getLocalStore().getCurrentState() == ReplicaState.OFFLINE
            || replicationDisabledPartitions.contains(replicaId.getPartitionId())) {
          logger.debug(
              "Skipping replication on replica {} because one of following conditions is true: remote replica is down "
                  + "= {}; in backoff = {}; local store is offline = {}; replication is disabled = {}.",
              replicaId.getPartitionId().toPathString(), replicaId.isDown(), inBackoff,
              remoteReplicaInfo.getLocalStore().getCurrentState() == ReplicaState.OFFLINE,
              replicationDisabledPartitions.contains(replicaId.getPartitionId()));
          continue;
        }

        if (replicatingFromRemoteColo && leaderBasedReplicationAdmin != null) {
          // In leader-based cross colo replication, missing blobs are only exchanged between leader replica pairs (i.e.
          // local replica and remote replica are leaders of their partition). For all non-leader replica pairs (i.e.
          // leader <-> standby, standby <-> leader, standby <-> standby), we expect their missing keys to come from
          // leader pair exchanges via intra-dc replication.

          // Check and process missing keys (if any) for non-leader replica pairs from their metadata exchanges in
          // previous replication cycle. These keys should most likely be now obtained now their corresponding
          // leader<->leader replication.
          processMissingKeysFromPreviousMetadataResponse(remoteReplicaInfo);

          // If we still have some missing keys from previous metadata exchange, don't include the remote replica for
          // current replication cycle to avoid sending duplicate metadata request.
          if (containsMissingKeysFromPreviousMetadataExchange(remoteReplicaInfo)) {
            continue;
          }
        }
        activeReplicasPerNode.add(remoteReplicaInfo);
      }
      logger.trace("Replicating from {} RemoteReplicaInfos.", activeReplicasPerNode.size());

      // use a variable to track current replica list to replicate (for logging purpose)
      List<RemoteReplicaInfo> currentReplicaList = activeReplicasPerNode;
      try {
        if (activeReplicasPerNode.size() > 0) {
          allCaughtUp = false;
          // if maxReplicaCountPerRequest > 0, split remote replicas on same node into multiple lists; otherwise there is
          // no limit.
          List<List<RemoteReplicaInfo>> activeReplicaSubLists =
              maxReplicaCountPerRequest > 0 ? Utils.partitionList(activeReplicasPerNode, maxReplicaCountPerRequest)
                  : Collections.singletonList(activeReplicasPerNode);

          connectedChannel =
              connectionPool.checkOutConnection(remoteNode.getHostname(), activeReplicasPerNode.get(0).getPort(),
                  replicationConfig.replicationConnectionPoolCheckoutTimeoutMs);
          checkoutConnectionTimeInMs = time.milliseconds() - startTimeInMs;
          // we checkout ConnectedChannel once and replicate remote replicas in batch via same ConnectedChannel
          for (List<RemoteReplicaInfo> replicaSubList : activeReplicaSubLists) {
            exchangeMetadataTimeInMs = -1;
            fixMissingStoreKeysTimeInMs = -1;
            currentReplicaList = replicaSubList;
            logger.debug("Exchanging metadata with {} remote replicas on {}", currentReplicaList.size(), remoteNode);
            startTimeInMs = time.milliseconds();
            List<ExchangeMetadataResponse> exchangeMetadataResponseList =
                exchangeMetadata(connectedChannel, replicaSubList);
            exchangeMetadataTimeInMs = time.milliseconds() - startTimeInMs;

            if (replicatingFromRemoteColo && leaderBasedReplicationAdmin != null) {

              // If leader based replication is enabled and we are replicating from remote colo, fetch the missing blobs
              // only for local leader replicas from their corresponding peer leader replicas (Leader <-> Leader).
              // Non-leader replica pairs (standby <-> leaders, leader <-> standby, standby <-> standby) will get their
              // missing blobs from their leader pair exchanges and intra-dc replication.

              List<RemoteReplicaInfo> leaderReplicaList = new ArrayList<>();
              List<ExchangeMetadataResponse> exchangeMetadataResponseListForLeaderReplicas = new ArrayList<>();
              getLeaderReplicaList(replicaSubList, exchangeMetadataResponseList, leaderReplicaList,
                  exchangeMetadataResponseListForLeaderReplicas);
              replicaSubList = leaderReplicaList;
              exchangeMetadataResponseList = exchangeMetadataResponseListForLeaderReplicas;
            }

            startTimeInMs = time.milliseconds();
            if (replicaSubList.size() > 0) {
              fixMissingStoreKeys(connectedChannel, replicaSubList, exchangeMetadataResponseList, false);
            }
            fixMissingStoreKeysTimeInMs = time.milliseconds() - startTimeInMs;
          }
        }

        if (replicatingFromRemoteColo && leaderBasedReplicationAdmin != null) {
          // Get a list of blocked standby replicas whose missing keys haven't arrived for long time.
          // Use case: In leader-based cross colo replication, standby replicas don't send GET requests for missing keys
          // found in metadata exchange and expect them to come via leader <-> leader replication.
          // This is a safety condition to ensure that standby replicas are not stuck waiting for the keys to come from leader
          // by fetching the missing keys themselves.
          // TODO: As an improvement to this, we can first fetch missing blobs from local leader/other replicas in intra-dc first.
          // TODO: If the result to fetch a blob from local dc is Blob_Not_Found, then we can fetch it from replicas in remote datacenter.
          // This will involve co-ordination between replica threads containing replicas of same partition.
          List<RemoteReplicaInfo> standbyReplicasTimedOutOnNoProgress =
              getRemoteStandbyReplicasTimedOutOnNoProgress(replicasToReplicatePerNode);
          if (standbyReplicasTimedOutOnNoProgress.size() > 0) {
            allCaughtUp = false;
            currentReplicaList = standbyReplicasTimedOutOnNoProgress;
            if (connectedChannel == null) {
              connectedChannel = connectionPool.checkOutConnection(remoteNode.getHostname(),
                  standbyReplicasTimedOutOnNoProgress.get(0).getPort(),
                  replicationConfig.replicationConnectionPoolCheckoutTimeoutMs);
              checkoutConnectionTimeInMs = time.milliseconds() - startTimeInMs;
            }

            List<ExchangeMetadataResponse> exchangeMetadataResponseListForBlockedReplicas =
                standbyReplicasTimedOutOnNoProgress.stream()
                    .map(remoteReplicaInfo -> new ExchangeMetadataResponse(
                        remoteReplicaInfo.getExchangeMetadataResponse()))
                    .collect(Collectors.toList());

            // Convert (and cache) the remote keys that are being fetched as the StoreKeyConverter would have cleared
            // these keys from its cache while it is replicating with other replicas before time out happened for these standby replicas.
            List<StoreKey> storeKeysToConvert = exchangeMetadataResponseListForBlockedReplicas.stream()
                .map(ExchangeMetadataResponse::getMissingStoreKeys)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
            convertStoreKeys(storeKeysToConvert);

            exchangeMetadataTimeInMs = 0;
            fixMissingStoreKeysTimeInMs = -1;
            logger.debug(
                "Sending GET request to fetch missing keys for standby remote replicas {} timed out on no progress",
                currentReplicaList);
            fixMissingStoreKeys(connectedChannel, standbyReplicasTimedOutOnNoProgress,
                exchangeMetadataResponseListForBlockedReplicas, true);
            fixMissingStoreKeysTimeInMs = time.milliseconds() - startTimeInMs;
          }
        }
      } catch (Throwable e) {
        if (checkoutConnectionTimeInMs == -1) {
          // throwable happened in checkout connection phase
          checkoutConnectionTimeInMs = time.milliseconds() - startTimeInMs;
          responseHandler.onEvent(currentReplicaList.get(0).getReplicaId(), e);
        } else if (exchangeMetadataTimeInMs == -1) {
          // throwable happened in exchange metadata phase
          exchangeMetadataTimeInMs = time.milliseconds() - startTimeInMs;
        } else if (fixMissingStoreKeysTimeInMs == -1) {
          // throwable happened in fix missing store phase
          fixMissingStoreKeysTimeInMs = time.milliseconds() - startTimeInMs;
        }
        logger.error(
            "Error while talking to peer: Remote node: {}, Thread name: {}, Remote replicas: {}, Current active "
                + "remote replica list: {}, Checkout connection time: {}, Exchange metadata time: {}, Fix missing "
                + "store key time {}", remoteNode, threadName, replicasToReplicatePerNode, currentReplicaList,
            checkoutConnectionTimeInMs, exchangeMetadataTimeInMs, fixMissingStoreKeysTimeInMs, e);
        replicationMetrics.incrementReplicationErrors(replicatingOverSsl);
        if (connectedChannel != null) {
          connectionPool.destroyConnection(connectedChannel);
          connectedChannel = null;
        }
      } finally {
        long totalReplicationTime = time.milliseconds() - replicationStartTimeInMs;
        replicationMetrics.updateTotalReplicationTime(totalReplicationTime, replicatingFromRemoteColo,
            replicatingOverSsl, datacenterName);
        if (connectedChannel != null) {
          connectionPool.checkInConnection(connectedChannel);
        }
        context.stop();
        portTypeBasedContext.stop();
      }
    }

    long sleepDurationMs = 0;
    if (allCaughtUp && replicationConfig.replicationReplicaThreadIdleSleepDurationMs > 0) {
      sleepDurationMs = replicationConfig.replicationReplicaThreadIdleSleepDurationMs;
      idleCount.inc();
    } else if (threadThrottleDurationMs > 0) {
      sleepDurationMs = threadThrottleDurationMs;
      throttleCount.inc();
    }

    if (sleepDurationMs > 0) {
      try {
        long currentTime = time.milliseconds();
        time.sleep(sleepDurationMs);
        logger.trace("Replica thread: {} slept for {} ms", threadName, time.milliseconds() - currentTime);
      } catch (InterruptedException e) {
        logger.error("Received interrupted exception during throttling", e);
      }
    }
  }

  /**
   * Gets all the metadata about messages from the remote replicas since last token. Checks the messages with the local
   * store and finds all the messages that are missing. For the messages that are not missing, updates the delete
   * and ttl state.
   * @param connectedChannel The connected channel that represents a connection to the remote replica
   * @param replicasToReplicatePerNode The information about the replicas that is being replicated
   * @return - List of ExchangeMetadataResponse that contains the set of store keys that are missing from the local
   *           store and are present in the remote replicas and also the new token from the remote replicas
   * @throws IOException
   * @throws ReplicationException
   */
  List<ExchangeMetadataResponse> exchangeMetadata(ConnectedChannel connectedChannel,
      List<RemoteReplicaInfo> replicasToReplicatePerNode) throws IOException, ReplicationException {
    long exchangeMetadataStartTimeInMs = time.milliseconds();
    List<ExchangeMetadataResponse> exchangeMetadataResponseList = new ArrayList<>();
    if (replicasToReplicatePerNode.size() > 0) {
      try {
        DataNodeId remoteNode = replicasToReplicatePerNode.get(0).getReplicaId().getDataNodeId();
        ReplicaMetadataResponse response =
            getReplicaMetadataResponse(replicasToReplicatePerNode, connectedChannel, remoteNode);
        long startTimeInMs = time.milliseconds();

        Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap = batchConvertReplicaMetadataResponseKeys(response);

        for (int i = 0; i < response.getReplicaMetadataResponseInfoList().size(); i++) {
          RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
          ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
              response.getReplicaMetadataResponseInfoList().get(i);
          responseHandler.onEvent(remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
          if (replicaMetadataResponseInfo.getError() == ServerErrorCode.No_Error) {
            // Skip stores that were stopped during call to getReplicaMetadataResponse
            if (!remoteReplicaInfo.getLocalStore().isStarted()) {
              exchangeMetadataResponseList.add(new ExchangeMetadataResponse(ServerErrorCode.Temporarily_Disabled));
            } else {
              try {
                logger.trace(
                    "Remote node: {} Thread name: {} Remote replica: {} Token from remote: {} Replica lag: {} ",
                    remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                    replicaMetadataResponseInfo.getFindToken(),
                    replicaMetadataResponseInfo.getRemoteReplicaLagInBytes());
                Set<MessageInfo> remoteMissingStoreMessages =
                    getMissingStoreMessages(replicaMetadataResponseInfo, remoteNode, remoteReplicaInfo);
                processReplicaMetadataResponse(remoteMissingStoreMessages, replicaMetadataResponseInfo,
                    remoteReplicaInfo, remoteNode, remoteKeyToLocalKeyMap);

                // Get the converted keys for the missing keys of this replica (to store them along with missing keys in
                // the exchange metadata response). For leader based replication, these are used during processing
                // of missing keys for non-leader replica pairs which will come later via leader<->leader replication.
                Map<StoreKey, StoreKey> remoteKeyToLocalKeySubMap = new HashMap<>();
                remoteMissingStoreMessages.forEach(remoteMissingStoreMessage -> {
                  StoreKey remoteKey = remoteMissingStoreMessage.getStoreKey();
                  remoteKeyToLocalKeySubMap.put(remoteKey, remoteKeyToLocalKeyMap.get(remoteKey));
                });

                ExchangeMetadataResponse exchangeMetadataResponse =
                    new ExchangeMetadataResponse(remoteMissingStoreMessages, replicaMetadataResponseInfo.getFindToken(),
                        replicaMetadataResponseInfo.getRemoteReplicaLagInBytes(), remoteKeyToLocalKeySubMap, time);

                // update replication lag in ReplicaSyncUpManager
                if (replicaSyncUpManager != null
                    && remoteReplicaInfo.getLocalStore().getCurrentState() == ReplicaState.BOOTSTRAP) {
                  ReplicaId localReplica = remoteReplicaInfo.getLocalReplicaId();
                  ReplicaId remoteReplica = remoteReplicaInfo.getReplicaId();
                  boolean updated = replicaSyncUpManager.updateLagBetweenReplicas(localReplica, remoteReplica,
                      exchangeMetadataResponse.localLagFromRemoteInBytes);
                  // if updated is false, it means local replica is not found in replicaSyncUpManager and is therefore not
                  // in bootstrap state.
                  if (updated && replicaSyncUpManager.isSyncUpComplete(localReplica)) {
                    // complete BOOTSTRAP -> STANDBY transition
                    remoteReplicaInfo.getLocalStore().setCurrentState(ReplicaState.STANDBY);
                    replicaSyncUpManager.onBootstrapComplete(localReplica);
                    remoteReplicaInfo.getLocalStore().completeBootstrap();
                  }
                }

                // add exchangeMetadataResponse to list after replicaSyncUpManager(if not null) has completed update. The
                // reason is replicaSyncUpManager may also throw exception and add one more exchangeMetadataResponse
                // associated with same RemoteReplicaInfo.
                exchangeMetadataResponseList.add(exchangeMetadataResponse);

                // If remote token has not moved forward, wait for back off time before resending next metadata request
                if (remoteReplicaInfo.getToken().equals(exchangeMetadataResponse.remoteToken)) {
                  remoteReplicaInfo.setReEnableReplicationTime(
                      time.milliseconds() + replicationConfig.replicationSyncedReplicaBackoffDurationMs);
                  syncedBackOffCount.inc();
                }

                // There are no missing keys. We just advance the token
                if (exchangeMetadataResponse.missingStoreMessages.size() == 0) {
                  remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
                  remoteReplicaInfo.setLocalLagFromRemoteInBytes(exchangeMetadataResponse.localLagFromRemoteInBytes);
                  logger.trace(
                      "Remote node: {} Thread name: {} Remote replica: {} Token after speaking to remote node: {}",
                      remoteNode, threadName, remoteReplicaInfo.getReplicaId(), exchangeMetadataResponse.remoteToken);
                }

                replicationMetrics.updateLagMetricForRemoteReplica(remoteReplicaInfo,
                    exchangeMetadataResponse.localLagFromRemoteInBytes);
                if (replicaMetadataResponseInfo.getMessageInfoList().size() > 0) {
                  replicationMetrics.updateCatchupPointMetricForCloudReplica(remoteReplicaInfo,
                      replicaMetadataResponseInfo.getMessageInfoList()
                          .get(replicaMetadataResponseInfo.getMessageInfoList().size() - 1)
                          .getOperationTimeMs());
                }
              } catch (Exception e) {
                if (e instanceof StoreException
                    && ((StoreException) e).getErrorCode() == StoreErrorCodes.Store_Not_Started) {
                  // Must have just been stopped, just skip it and move on.
                  logger.info("Local store not started for remote replica: {}", remoteReplicaInfo.getReplicaId());
                  exchangeMetadataResponseList.add(new ExchangeMetadataResponse(ServerErrorCode.Temporarily_Disabled));
                } else {
                  logger.error("Remote node: {} Thread name: {} Remote replica: {}", remoteNode, threadName,
                      remoteReplicaInfo.getReplicaId(), e);
                  replicationMetrics.updateLocalStoreError(remoteReplicaInfo.getReplicaId());
                  responseHandler.onEvent(remoteReplicaInfo.getReplicaId(), e);
                  exchangeMetadataResponseList.add(new ExchangeMetadataResponse(ServerErrorCode.Unknown_Error));
                }
              }
            }
          } else {
            replicationMetrics.updateMetadataRequestError(remoteReplicaInfo.getReplicaId());
            logger.error("Remote node: {} Thread name: {} Remote replica: {} Server error: {}", remoteNode, threadName,
                remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
            exchangeMetadataResponseList.add(new ExchangeMetadataResponse(replicaMetadataResponseInfo.getError()));
          }

          if (replicatingFromRemoteColo && leaderBasedReplicationAdmin != null) {
            ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
            if (exchangeMetadataResponse.serverErrorCode.equals(ServerErrorCode.No_Error)) {

              // If leader-based replication is enabled, store the meta data exchange received for the remote replica as
              // standby replicas will not send GET request for the missing store keys and track them from leader <->
              // leader exchanges and intra-dc replication.
              remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(exchangeMetadataResponse));

              // It is possible that some of the missing keys found in exchange metadata response are written in parallel
              // by other replica threads since the time we calculated it. Go through the local store once more and
              // update missing keys set stored in the exchangeMetadataResponse for the remote replica.
              refreshMissingStoreMessagesForStandbyReplica(remoteReplicaInfo);
            }
          }
        }
        long processMetadataResponseTimeInMs = time.milliseconds() - startTimeInMs;
        logger.trace("Remote node: {} Thread name: {} processMetadataResponseTime: {}", remoteNode, threadName,
            processMetadataResponseTimeInMs);
      } finally {
        long exchangeMetadataTime = time.milliseconds() - exchangeMetadataStartTimeInMs;
        replicationMetrics.updateExchangeMetadataTime(exchangeMetadataTime, replicatingFromRemoteColo,
            replicatingOverSsl, datacenterName);
      }
    }
    return exchangeMetadataResponseList;
  }

  /**
   * Gets all the messages from the remote node for the missing keys and writes them to the local store
   * @param connectedChannel The connected channel that represents a connection to the remote replica
   * @param replicasToReplicatePerNode The information about the replicas that is being replicated
   * @param exchangeMetadataResponseList The missing keys in the local stores whose message needs to be retrieved
   *                                     from the remote stores
   * @param remoteColoGetRequestForStandby boolean which indicates if we are getting missing keys for standby or
   *                                       non-leader replica pairs during leader-based replication.
   * @throws IOException
   * @throws ReplicationException
   */
  void fixMissingStoreKeys(ConnectedChannel connectedChannel, List<RemoteReplicaInfo> replicasToReplicatePerNode,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList, boolean remoteColoGetRequestForStandby)
      throws IOException, ReplicationException {
    long fixMissingStoreKeysStartTimeInMs = time.milliseconds();
    try {
      if (exchangeMetadataResponseList.size() != replicasToReplicatePerNode.size()
          || replicasToReplicatePerNode.size() == 0) {
        throw new IllegalArgumentException("ExchangeMetadataResponseList size " + exchangeMetadataResponseList.size()
            + " and replicasToReplicatePerNode size " + replicasToReplicatePerNode.size()
            + " should be the same and greater than zero");
      }
      DataNodeId remoteNode = replicasToReplicatePerNode.get(0).getReplicaId().getDataNodeId();
      GetResponse getResponse =
          getMessagesForMissingKeys(connectedChannel, exchangeMetadataResponseList, replicasToReplicatePerNode,
              remoteNode, remoteColoGetRequestForStandby);
      writeMessagesToLocalStoreAndAdvanceTokens(exchangeMetadataResponseList, getResponse, replicasToReplicatePerNode,
          remoteNode, remoteColoGetRequestForStandby);
    } finally {
      long fixMissingStoreKeysTime = time.milliseconds() - fixMissingStoreKeysStartTimeInMs;
      replicationMetrics.updateFixMissingStoreKeysTime(fixMissingStoreKeysTime, replicatingFromRemoteColo,
          replicatingOverSsl, datacenterName);
    }
  }

  /**
   * Gets the replica metadata response for a list of remote replicas on a given remote data node
   * @param replicasToReplicatePerNode The list of remote replicas for a node
   * @param connectedChannel The connection channel to the node
   * @param remoteNode The remote node from which replication needs to happen
   * @return ReplicaMetadataResponse, the response from replica metadata request to remote node
   * @throws ReplicationException
   * @throws IOException
   */
  ReplicaMetadataResponse getReplicaMetadataResponse(List<RemoteReplicaInfo> replicasToReplicatePerNode,
      ConnectedChannel connectedChannel, DataNodeId remoteNode) throws ReplicationException, IOException {
    long replicaMetadataRequestStartTime = time.milliseconds();
    List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList = new ArrayList<ReplicaMetadataRequestInfo>();
    for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
      ReplicaMetadataRequestInfo replicaMetadataRequestInfo =
          new ReplicaMetadataRequestInfo(remoteReplicaInfo.getReplicaId().getPartitionId(),
              remoteReplicaInfo.getToken(), dataNodeId.getHostname(),
              remoteReplicaInfo.getLocalReplicaId().getReplicaPath(), remoteReplicaInfo.getReplicaId().getReplicaType(),
              replicationConfig.replicaMetadataRequestVersion);
      replicaMetadataRequestInfoList.add(replicaMetadataRequestInfo);
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token going to be sent to remote: {} ",
          remoteNode, threadName, remoteReplicaInfo.getReplicaId(), remoteReplicaInfo.getToken());
    }

    try {
      ReplicaMetadataRequest request = new ReplicaMetadataRequest(correlationIdGenerator.incrementAndGet(),
          "replication-metadata-" + dataNodeId.getHostname() + "[" + dataNodeId.getDatacenterName() + "]",
          replicaMetadataRequestInfoList, replicationConfig.replicationFetchSizeInBytes,
          replicationConfig.replicaMetadataRequestVersion);
      connectedChannel.send(request);
      ChannelOutput channelOutput = connectedChannel.receive();
      ByteBufferInputStream byteBufferInputStream =
          new ByteBufferInputStream(channelOutput.getInputStream(), (int) channelOutput.getStreamSize());
      logger.trace("Remote node: {} Thread name: {} Remote replicas: {} ByteBuffer size after deserialization: {} ",
          remoteNode, threadName, replicasToReplicatePerNode, byteBufferInputStream.available());
      ReplicaMetadataResponse response =
          ReplicaMetadataResponse.readFrom(new DataInputStream(byteBufferInputStream), findTokenHelper, clusterMap);

      long metadataRequestTime = time.milliseconds() - replicaMetadataRequestStartTime;
      replicationMetrics.updateMetadataRequestTime(metadataRequestTime, replicatingFromRemoteColo, replicatingOverSsl,
          datacenterName);

      if (response.getError() != ServerErrorCode.No_Error
          || response.getReplicaMetadataResponseInfoList().size() != replicasToReplicatePerNode.size()) {
        int replicaMetadataResponseInfoListSize = response.getReplicaMetadataResponseInfoList() == null ? 0
            : response.getReplicaMetadataResponseInfoList().size();
        logger.error(
            "Remote node: {} Thread name: {} Remote replicas: {} Replica metadata response error: {} ReplicaMetadataResponseInfoListSize: {} ReplicasToReplicatePerNodeSize: {}",
            remoteNode, threadName, replicasToReplicatePerNode, response.getError(),
            replicaMetadataResponseInfoListSize, replicasToReplicatePerNode.size());
        throw new ReplicationException("Replica Metadata Response Error " + response.getError());
      }
      return response;
    } catch (IOException e) {
      responseHandler.onEvent(replicasToReplicatePerNode.get(0).getReplicaId(), e);
      throw e;
    }
  }

  /**
   * Gets the missing store messages by comparing the messages from the remote node
   * @param replicaMetadataResponseInfo The response that contains the messages from the remote node
   * @param remoteNode The remote node from which replication needs to happen
   * @param remoteReplicaInfo The remote replica that contains information about the remote replica id
   * @return List of store messages that are missing from the local store
   * @throws StoreException if store error (usually IOError) occurs when getting missing keys.
   */
  Set<MessageInfo> getMissingStoreMessages(ReplicaMetadataResponseInfo replicaMetadataResponseInfo,
      DataNodeId remoteNode, RemoteReplicaInfo remoteReplicaInfo) throws StoreException {
    long startTime = time.milliseconds();
    List<MessageInfo> messageInfoList = replicaMetadataResponseInfo.getMessageInfoList();
    Map<MessageInfo, StoreKey> remoteMessageToConvertedKeyNonNull = new HashMap<>();

    for (MessageInfo messageInfo : messageInfoList) {
      StoreKey storeKey = messageInfo.getStoreKey();
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key from remote: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), storeKey);
      StoreKey convertedKey = storeKeyConverter.getConverted(storeKey);
      if (skipPredicate == null) {
        logger.debug("SkipPredicate is null");
      }
      if (convertedKey != null && (!replicationConfig.replicationContainerDeletionEnabled || skipPredicate == null
          || !skipPredicate.test(messageInfo))) {
        remoteMessageToConvertedKeyNonNull.put(messageInfo, convertedKey);
      }
    }
    Set<StoreKey> convertedMissingStoreKeys =
        remoteReplicaInfo.getLocalStore().findMissingKeys(new ArrayList<>(remoteMessageToConvertedKeyNonNull.values()));
    Set<MessageInfo> missingRemoteMessages = new HashSet<>();
    remoteMessageToConvertedKeyNonNull.forEach((messageInfo, convertedKey) -> {
      if (convertedMissingStoreKeys.contains(convertedKey)) {
        logger.trace(
            "Remote node: {} Thread name: {} Remote replica: {} Key missing id (converted): {} Key missing id (remote): {}",
            remoteNode, threadName, remoteReplicaInfo.getReplicaId(), convertedKey, messageInfo.getStoreKey());
        missingRemoteMessages.add(messageInfo);
      }
    });
    if (messageInfoList.size() != 0 && missingRemoteMessages.size() == 0) {
      // Catching up
      replicationMetrics.allResponsedKeysExist.inc();
    }
    replicationMetrics.updateCheckMissingKeysTime(time.milliseconds() - startTime, replicatingFromRemoteColo,
        datacenterName);
    return missingRemoteMessages;
  }

  /**
   * Takes the missing store messages and the message list from the remote store and identifies ones that are deleted
   * on the remote store and updates them locally. Also, if the message that is missing is deleted in the remote
   * store, we remove the message from the list of missing messages
   * @param missingRemoteStoreMessages The list of messages missing from the local store
   * @param replicaMetadataResponseInfo The replica metadata response from the remote store
   * @param remoteReplicaInfo The remote replica that is being replicated from
   * @param remoteNode The remote node from which replication needs to happen
   * @param remoteKeyToLocalKeyMap map mapping remote keys to local key equivalents
   * @throws StoreException
   */
  void processReplicaMetadataResponse(Set<MessageInfo> missingRemoteStoreMessages,
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo, RemoteReplicaInfo remoteReplicaInfo,
      DataNodeId remoteNode, Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap) throws StoreException {
    long startTime = time.milliseconds();
    List<MessageInfo> messageInfoList = replicaMetadataResponseInfo.getMessageInfoList();
    for (MessageInfo messageInfo : messageInfoList) {
      BlobId blobId = (BlobId) messageInfo.getStoreKey();
      if (remoteReplicaInfo.getLocalReplicaId().getPartitionId().compareTo(blobId.getPartition()) != 0) {
        throw new IllegalStateException(
            "Blob id is not in the expected partition Actual partition " + blobId.getPartition()
                + " Expected partition " + remoteReplicaInfo.getLocalReplicaId().getPartitionId());
      }
      BlobId localKey = (BlobId) remoteKeyToLocalKeyMap.get(messageInfo.getStoreKey());
      if (localKey == null) {
        missingRemoteStoreMessages.remove(messageInfo);
        logger.trace("Remote node: {} Thread name: {} Remote replica: {} Remote key deprecated locally: {}", remoteNode,
            threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
      } else if (missingRemoteStoreMessages.contains(messageInfo)) {
        // The key is missing in the local store, we either send get request to fetch the content of this key, or does
        // nothing if the key is deleted or expired remotely.
        if (messageInfo.isDeleted()) {
          // if the key is not present locally and if the remote replica has the message in deleted state,
          // it is not considered missing locally.
          missingRemoteStoreMessages.remove(messageInfo);
          logger.trace(
              "Remote node: {} Thread name: {} Remote replica: {} Key in deleted state remotely: {} Local key: {}",
              remoteNode, threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey(), localKey);
          // A Repair event for Delete signifies that a Delete message was received from the remote and it is fired
          // as long as the Delete is guaranteed to have taken effect locally.
          if (notification != null) {
            notification.onBlobReplicaDeleted(dataNodeId.getHostname(), dataNodeId.getPort(), localKey.getID(),
                BlobReplicaSourceType.REPAIRED);
          }
        } else if (messageInfo.isExpired()) {
          // if the key is not present locally and if the remote replica has the key as expired,
          // it is not considered missing locally.
          missingRemoteStoreMessages.remove(messageInfo);
          logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key in expired state remotely {}",
              remoteNode, threadName, remoteReplicaInfo.getReplicaId(), localKey);
        }
      } else {
        // The key is present in the local store. Compare blob properties (ttl_update, delete, undelete fields) in
        // received message info with blob in local store and apply updates as needed. For ex, mark local blob for deletion if
        // it is deleted in the remote store and not deleted yet locally.

        // if the blob is from deprecated container, then nothing needs to be done.
        if (replicationConfig.replicationContainerDeletionEnabled && skipPredicate != null && skipPredicate.test(
            messageInfo)) {
          continue;
        }
        applyUpdatesToBlobInLocalStore(messageInfo, remoteReplicaInfo, localKey);
      }
    }
    if (replicatingFromRemoteColo) {
      replicationMetrics.interColoProcessMetadataResponseTime.get(datacenterName)
          .update(time.milliseconds() - startTime);
    } else {
      replicationMetrics.intraColoProcessMetadataResponseTime.update(time.milliseconds() - startTime);
    }
  }

  /**
   * Compares blob metadata in received message from remote replica with the blob in local store and updates its
   * ttl_update/delete/undelete properties. This is called when a replicated blob from remote replica is found on local store.
   * @param messageInfo message information of the blob from remote replica
   * @param remoteReplicaInfo remote replica information
   * @param localKey local blob information
   * @throws StoreException
   */
  public void applyUpdatesToBlobInLocalStore(MessageInfo messageInfo, RemoteReplicaInfo remoteReplicaInfo,
      BlobId localKey) throws StoreException {
    MessageInfo localMessageInfo = remoteReplicaInfo.getLocalStore().findKey(localKey);
    boolean deletedLocally = localMessageInfo.isDeleted();
    boolean ttlUpdatedLocally = localMessageInfo.isTtlUpdated();
    short localLifeVersion = localMessageInfo.getLifeVersion();
    short remoteLifeVersion = messageInfo.getLifeVersion();
    if (localLifeVersion > remoteLifeVersion) {
      // if the lifeVersion in local store is greater than the remote lifeVersion, then nothing needs to be done.
    } else if (localLifeVersion == remoteLifeVersion) {
      // we are operating in the same version, in this case, delete would be the final state.
      if (!deletedLocally) {
        // Only adds record when it's not deleted yet. Since delete is the final state for this lifeVersion, if there
        // is a delete record for the current lifeVersion, then nothing needs to be done.
        MessageInfo info = new MessageInfo(localKey, 0, localKey.getAccountId(), localKey.getContainerId(),
            messageInfo.getOperationTimeMs(), remoteLifeVersion);
        if (messageInfo.isTtlUpdated() && !ttlUpdatedLocally) {
          applyTtlUpdate(info, remoteReplicaInfo);
        }
        if (messageInfo.isDeleted()) {
          applyDelete(info, remoteReplicaInfo);
        }
      }
    } else {
      // if we are here, then the remote lifeVersion is greater than the local lifeVersion.
      // we need to reconcile the local state with the remote state.
      //
      // There are three states we have to reconcile: lifeVersion, ttl_update, is_deleted.
      // To reconcile lifeVersion and is_deleted, we have to add a Delete or Undelete record, based on what the final state is.
      // to reconcile ttl_update, if the final state is delete, then, we have to add ttl_update before delete, other, we can add ttl_update after undelete.
      MessageInfo info = new MessageInfo(localKey, 0, localKey.getAccountId(), localKey.getContainerId(),
          messageInfo.getOperationTimeMs(), remoteLifeVersion);
      boolean shouldInsertTtlUpdate = false;
      if (messageInfo.isTtlUpdated() && !ttlUpdatedLocally) {
        // make a patch for ttl update
        // if the remote state is delete, then we can't insert TTL_UPDATE after delete, we have to insert a ttl_update here
        if (messageInfo.isDeleted()) {
          // since ttl update can only follow Put or Undelete, make sure it's not locally deleted.
          // we can reuse the lifeVersion for undelete and ttl update, since the delete would be the final state of
          // this lifeVersion.
          if (deletedLocally) {
            applyUndelete(info, remoteReplicaInfo);
          }
          applyTtlUpdate(info, remoteReplicaInfo);
        } else {
          // if final state is not delete, then to bump lifeVersion in local store to remote lifeVersion, we have to
          // add a undelete, and then add a ttl update.
          shouldInsertTtlUpdate = true;
        }
      }

      // if we are here, then the ttl update is matched
      if (messageInfo.isDeleted()) {
        applyDelete(info, remoteReplicaInfo);
      } else {
        applyUndelete(info, remoteReplicaInfo);
        if (shouldInsertTtlUpdate) {
          applyTtlUpdate(info, remoteReplicaInfo);
        }
      }
    }
  }

  /**
   * Batch converts all keys in the {@link ReplicaMetadataResponse} response.
   * Intention is that conversion is done all at once so that followup calls to
   * {@link StoreKeyConverter#getConverted(StoreKey)} will work
   * @param response the {@link ReplicaMetadataResponse} whose keys will be converted
   * @return the map from the {@link StoreKeyConverter#convert(Collection)} call
   * @throws IOException thrown if {@link StoreKeyConverter#convert(Collection)} fails
   */
  Map<StoreKey, StoreKey> batchConvertReplicaMetadataResponseKeys(ReplicaMetadataResponse response) throws IOException {
    List<StoreKey> storeKeysToConvert = new ArrayList<>();
    for (ReplicaMetadataResponseInfo replicaMetadataResponseInfo : response.getReplicaMetadataResponseInfoList()) {
      if ((replicaMetadataResponseInfo.getError() == ServerErrorCode.No_Error) && (
          replicaMetadataResponseInfo.getMessageInfoList() != null)) {
        for (MessageInfo messageInfo : replicaMetadataResponseInfo.getMessageInfoList()) {
          storeKeysToConvert.add(messageInfo.getStoreKey());
        }
      }
    }
    return convertStoreKeys(storeKeysToConvert);
  }

  /**
   * Converts all input remote store keys to local keys using {@link StoreKeyConverter}
   * @param storeKeysToConvert the {@link List<StoreKey>} list of keys to be converted
   * @return the map from the {@link StoreKeyConverter#convert(Collection)} call
   * @throws IOException thrown if {@link StoreKeyConverter#convert(Collection)} fails
   */
  Map<StoreKey, StoreKey> convertStoreKeys(List<StoreKey> storeKeysToConvert) throws IOException {
    try {
      storeKeyConverter.dropCache();
      return storeKeyConverter.convert(storeKeysToConvert);
    } catch (Exception e) {
      throw new IOException("Problem with store key conversion", e);
    }
  }

  /**
   * Gets the messages for the keys that are missing from the local store by issuing a {@link GetRequest} to the remote
   * node, if there are any missing keys. If there are no missing keys to be fetched, then no request is issued and a
   * null response is returned.
   * @param connectedChannel The connection channel to the remote node
   * @param exchangeMetadataResponseList The list of metadata response from the remote node
   * @param replicasToReplicatePerNode The list of remote replicas for the remote node
   * @param remoteNode The remote node from which replication needs to happen
   * @param remoteColoGetRequestForStandby boolean which indicates if we are getting missing keys for standby or
   *                                       non-leader replica pairs during leader-based replication.
   * @return The response that contains the missing messages; or null if no request was issued because there were no
   * keys missing.
   * @throws ReplicationException
   * @throws IOException
   */
  private GetResponse getMessagesForMissingKeys(ConnectedChannel connectedChannel,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList, List<RemoteReplicaInfo> replicasToReplicatePerNode,
      DataNodeId remoteNode, boolean remoteColoGetRequestForStandby) throws ReplicationException, IOException {
    List<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.No_Error) {
        Set<StoreKey> missingStoreKeys = exchangeMetadataResponse.getMissingStoreKeys();
        if (missingStoreKeys.size() > 0) {
          ArrayList<BlobId> keysToFetch = new ArrayList<BlobId>();
          for (StoreKey storeKey : missingStoreKeys) {
            keysToFetch.add((BlobId) storeKey);
          }
          PartitionRequestInfo partitionRequestInfo =
              new PartitionRequestInfo(remoteReplicaInfo.getReplicaId().getPartitionId(), keysToFetch);
          partitionRequestInfoList.add(partitionRequestInfo);
        }
      }
    }
    GetResponse getResponse = null;
    if (!partitionRequestInfoList.isEmpty()) {
      GetRequest getRequest = new GetRequest(correlationIdGenerator.incrementAndGet(),
          GetRequest.Replication_Client_Id_Prefix + dataNodeId.getHostname() + "[" + dataNodeId.getDatacenterName()
              + "]", MessageFormatFlags.All, partitionRequestInfoList,
          replicationConfig.replicationIncludeAll ? GetOption.Include_All : GetOption.None);
      long startTime = time.milliseconds();
      try {
        connectedChannel.send(getRequest);
        ChannelOutput channelOutput = connectedChannel.receive();
        getResponse = GetResponse.readFrom(new DataInputStream(channelOutput.getInputStream()), clusterMap);
        long getRequestTime = time.milliseconds() - startTime;
        replicationMetrics.updateGetRequestTime(getRequestTime, replicatingFromRemoteColo, replicatingOverSsl,
            datacenterName, remoteColoGetRequestForStandby);
        if (getResponse.getError() != ServerErrorCode.No_Error) {
          logger.error("Remote node: {} Thread name: {} Remote replicas: {} GetResponse from replication: {}",
              remoteNode, threadName, replicasToReplicatePerNode, getResponse.getError());
          throw new ReplicationException(
              " Get Request returned error when trying to get missing keys " + getResponse.getError());
        }
      } catch (IOException e) {
        responseHandler.onEvent(replicasToReplicatePerNode.get(0).getReplicaId(), e);
        throw e;
      }
    }
    return getResponse;
  }

  /**
   * Writes the messages (if any) to the local stores from the remote stores for the missing keys, and advances tokens.
   * @param exchangeMetadataResponseList The list of metadata response from the remote node
   * @param getResponse The {@link GetResponse} that contains the missing messages. This may be null if there are no
   *                    missing messages to write as per the exchange metadata response. In that case this method will
   *                    simply advance the tokens for every store.
   * @param replicasToReplicatePerNode The list of remote replicas for the remote node
   * @param remoteNode The remote node from which replication needs to happen
   * @param remoteColoGetRequestForStandby boolean which indicates if we are getting missing keys for standby or
   *                                       non-leader replica pairs during leader-based replication.
   * @throws IOException
   */
  private void writeMessagesToLocalStoreAndAdvanceTokens(List<ExchangeMetadataResponse> exchangeMetadataResponseList,
      GetResponse getResponse, List<RemoteReplicaInfo> replicasToReplicatePerNode, DataNodeId remoteNode,
      boolean remoteColoGetRequestForStandby) throws IOException {
    int partitionResponseInfoIndex = 0;
    long totalBytesFixed = 0;
    long totalBlobsFixed = 0;
    long startTime = time.milliseconds();
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.No_Error) {
        if (exchangeMetadataResponse.missingStoreMessages.size() > 0) {
          PartitionResponseInfo partitionResponseInfo =
              getResponse.getPartitionResponseInfoList().get(partitionResponseInfoIndex);
          responseHandler.onEvent(remoteReplicaInfo.getReplicaId(), partitionResponseInfo.getErrorCode());
          partitionResponseInfoIndex++;
          if (!partitionResponseInfo.getPartition()
              .toPathString()
              .equals(remoteReplicaInfo.getReplicaId().getPartitionId().toPathString())) {
            throw new IllegalStateException(
                "The partition id from partitionResponseInfo " + partitionResponseInfo.getPartition()
                    + " and from remoteReplicaInfo " + remoteReplicaInfo.getReplicaId().getPartitionId()
                    + " are not the same");
          }
          if (partitionResponseInfo.getErrorCode() == ServerErrorCode.No_Error) {
            List<MessageInfo> messageInfoList = partitionResponseInfo.getMessageInfoList();
            try {
              logger.trace("Remote node: {} Thread name: {} Remote replica: {} Messages to fix: {} "
                      + "Partition: {} Local mount path: {}", remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                  exchangeMetadataResponse.getMissingStoreKeys(), remoteReplicaInfo.getReplicaId().getPartitionId(),
                  remoteReplicaInfo.getLocalReplicaId().getMountPath());

              MessageFormatWriteSet writeset;
              MessageSievingInputStream validMessageDetectionInputStream =
                  new MessageSievingInputStream(getResponse.getInputStream(), messageInfoList,
                      Collections.singletonList(transformer), metricRegistry);
              if (validMessageDetectionInputStream.hasInvalidMessages()) {
                replicationMetrics.incrementInvalidMessageError(partitionResponseInfo.getPartition());
                logger.error("Out of {} messages, {} invalid messages were found in message stream from {}",
                    messageInfoList.size(),
                    messageInfoList.size() - validMessageDetectionInputStream.getValidMessageInfoList().size(),
                    remoteReplicaInfo.getReplicaId());
              }
              messageInfoList = validMessageDetectionInputStream.getValidMessageInfoList();
              if (messageInfoList.size() == 0) {
                logger.debug(
                    "MessageInfoList is of size 0 as all messages are invalidated, deprecated, deleted or expired.");
              } else {
                writeset = new MessageFormatWriteSet(validMessageDetectionInputStream, messageInfoList, false);
                remoteReplicaInfo.getLocalStore().put(writeset);
              }

              for (MessageInfo messageInfo : messageInfoList) {
                totalBytesFixed += messageInfo.getSize();
                logger.trace("Remote node: {} Thread name: {} Remote replica: {} Message replicated: {} Partition: {} "
                        + "Local mount path: {} Message size: {}", remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                    messageInfo.getStoreKey(), remoteReplicaInfo.getReplicaId().getPartitionId(),
                    remoteReplicaInfo.getLocalReplicaId().getMountPath(), messageInfo.getSize());
                if (notification != null) {
                  notification.onBlobReplicaCreated(dataNodeId.getHostname(), dataNodeId.getPort(),
                      messageInfo.getStoreKey().getID(), BlobReplicaSourceType.REPAIRED);
                }
                if (messageInfo.isTtlUpdated()) {
                  applyTtlUpdate(messageInfo, remoteReplicaInfo);
                }
              }
              totalBlobsFixed += messageInfoList.size();

              if (leaderBasedReplicationAdmin != null) {
                // If leader based replication is enabled, we will only fetch missing blobs for local leaders from their
                // remote leaders. For non-leader replicas pairs (leader <-> standby, standby <-> leader, standby <->
                // standby), we will store the missing keys and track them via leader<->leader exchanges and intra-dc
                // replication.
                // Notify all the replicas of the partition on newly written messages so that non-leader replica pairs
                // can update their missing keys and advance token if needed.
                leaderBasedReplicationAdmin.onMessageWriteForPartition(partitionResponseInfo.getPartition(),
                    messageInfoList);
              }

              remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
              remoteReplicaInfo.setLocalLagFromRemoteInBytes(exchangeMetadataResponse.localLagFromRemoteInBytes);
              // reset stored metadata response for this replica
              remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(ServerErrorCode.No_Error));

              logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token after speaking to remote node: {}",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), exchangeMetadataResponse.remoteToken);
            } catch (StoreException e) {
              if (e.getErrorCode() != StoreErrorCodes.Already_Exist) {
                replicationMetrics.updateLocalStoreError(remoteReplicaInfo.getReplicaId());
                logger.error("Remote node: {} Thread name: {} Remote replica: {}", remoteNode, threadName,
                    remoteReplicaInfo.getReplicaId(), e);
              }
            }
          } else if (partitionResponseInfo.getErrorCode() == ServerErrorCode.Blob_Deleted) {
            replicationMetrics.blobDeletedOnGetCount.inc();
            logger.trace("One of the blobs to GET is deleted: Remote node: {} Thread name: {} Remote replica: {}",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId());
          } else if (partitionResponseInfo.getErrorCode() == ServerErrorCode.Blob_Authorization_Failure) {
            replicationMetrics.blobAuthorizationFailureCount.inc();
            logger.error(
                "One of the blobs authorization failed: Remote node: {} Thread name: {} Remote replica: {} Keys are: {}",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                exchangeMetadataResponse.getMissingStoreKeys());
          } else {
            replicationMetrics.updateGetRequestError(remoteReplicaInfo.getReplicaId());
            logger.error("Remote node: {} Thread name: {} Remote replica: {} Server error: {}", remoteNode, threadName,
                remoteReplicaInfo.getReplicaId(), partitionResponseInfo.getErrorCode());
          }
        }
      }
    }
    long batchStoreWriteTime = time.milliseconds() - startTime;
    replicationMetrics.updateBatchStoreWriteTime(batchStoreWriteTime, totalBytesFixed, totalBlobsFixed,
        replicatingFromRemoteColo, replicatingOverSsl, datacenterName, remoteColoGetRequestForStandby);
  }

  /**
   * Applies a TTL update to the blob described by {@code messageInfo}.
   * @param messageInfo the {@link MessageInfo} that will be transformed into a TTL update
   * @param remoteReplicaInfo The remote replica that is being replicated from
   * @throws StoreException
   */
  private void applyTtlUpdate(MessageInfo messageInfo, RemoteReplicaInfo remoteReplicaInfo) throws StoreException {
    DataNodeId remoteNode = remoteReplicaInfo.getReplicaId().getDataNodeId();
    try {
      // NOTE: It is possible that the key in question may have expired and this TTL update is being applied after it
      // is deemed expired. The store will accept the op (BlobStore looks at whether the op was valid to do at the time
      // of the op, not current time) but if compaction is running at the same time and has decided to clean up the
      // record before this ttl update was applied (and this didn't find the key missing because compaction has not yet
      // committed), then we have a bad situation where only a TTL update exists in the store. This problem has to be
      // addressed. This can only happen if replication is far behind (for e.g due to a server being down for a long
      // time). Won't happen if a server is being recreated.
      messageInfo = new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(), true,
          messageInfo.isUndeleted(), messageInfo.getExpirationTimeInMs(), messageInfo.getCrc(),
          messageInfo.getAccountId(), messageInfo.getContainerId(), messageInfo.getOperationTimeMs(),
          messageInfo.getLifeVersion());
      remoteReplicaInfo.getLocalStore().updateTtl(Collections.singletonList(messageInfo));
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key ttl updated id: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
    } catch (StoreException e) {
      // The blob may be deleted or updated which is alright
      if (e.getErrorCode() == StoreErrorCodes.ID_Deleted || e.getErrorCode() == StoreErrorCodes.Already_Updated) {
        logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key already updated: {}", remoteNode,
            threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
      } else {
        throw e;
      }
    }
    // A Repair event for an update signifies that an update message was received from the remote and it is fired
    // as long as the update is guaranteed to have taken effect locally.
    if (notification != null) {
      notification.onBlobReplicaUpdated(dataNodeId.getHostname(), dataNodeId.getPort(),
          messageInfo.getStoreKey().getID(), BlobReplicaSourceType.REPAIRED, UpdateType.TTL_UPDATE, messageInfo);
    }
  }

  /**
   * Applies an undelete to the blob described by {@code messageInfo}.
   * @param messageInfo the {@link MessageInfo} that will be transformed into an undelete
   * @param remoteReplicaInfo The remote replica that is being replicated from
   * @throws StoreException
   */
  private void applyUndelete(MessageInfo messageInfo, RemoteReplicaInfo remoteReplicaInfo) throws StoreException {
    DataNodeId remoteNode = remoteReplicaInfo.getReplicaId().getDataNodeId();
    try {
      messageInfo = new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(),
          messageInfo.isTtlUpdated(), true, messageInfo.getExpirationTimeInMs(), messageInfo.getCrc(),
          messageInfo.getAccountId(), messageInfo.getContainerId(), messageInfo.getOperationTimeMs(),
          messageInfo.getLifeVersion());
      remoteReplicaInfo.getLocalStore().undelete(messageInfo);
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key undelete id: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
    } catch (StoreException e) {
      // The blob may be undeleted, which is alright
      if (e.getErrorCode() == StoreErrorCodes.Life_Version_Conflict
          || e.getErrorCode() == StoreErrorCodes.ID_Undeleted) {
        logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key {}: {}", remoteNode, threadName,
            remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey(), e.getErrorCode().name());
      } else {
        throw e;
      }
    }
    // A Repair event for an undelete signifies that an undelete message was received from the remote and it is fired
    // as long as the undelete is guaranteed to have taken effect locally.
    if (notification != null) {
      notification.onBlobReplicaUndeleted(dataNodeId.getHostname(), dataNodeId.getPort(),
          messageInfo.getStoreKey().getID(), BlobReplicaSourceType.REPAIRED);
    }
  }

  /**
   * Applies a delete to the blob described by {@code messageInfo}.
   * @param messageInfo the {@link MessageInfo} that will be transformed into a delete
   * @param remoteReplicaInfo The remote replica that is being replicated from
   * @throws StoreException
   */
  private void applyDelete(MessageInfo messageInfo, RemoteReplicaInfo remoteReplicaInfo) throws StoreException {
    DataNodeId remoteNode = remoteReplicaInfo.getReplicaId().getDataNodeId();
    try {
      messageInfo =
          new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), true, messageInfo.isTtlUpdated(), false,
              messageInfo.getExpirationTimeInMs(), messageInfo.getCrc(), messageInfo.getAccountId(),
              messageInfo.getContainerId(), messageInfo.getOperationTimeMs(), messageInfo.getLifeVersion());
      remoteReplicaInfo.getLocalStore().delete(Collections.singletonList(messageInfo));
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key delete: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
    } catch (StoreException e) {
      // The blob may be deleted or updated which is alright
      if (e.getErrorCode() == StoreErrorCodes.ID_Deleted || e.getErrorCode() == StoreErrorCodes.Life_Version_Conflict) {
        logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key {}: {}", remoteNode, threadName,
            remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey(), e.getErrorCode().name());
      } else {
        throw e;
      }
    }
    // A Repair event for Delete signifies that a Delete message was received from the remote and it is fired
    // as long as the Delete is guaranteed to have taken effect locally.
    if (notification != null) {
      notification.onBlobReplicaDeleted(dataNodeId.getHostname(), dataNodeId.getPort(),
          messageInfo.getStoreKey().getID(), BlobReplicaSourceType.REPAIRED);
    }
  }

  /**
   * Get list of remote replica infos whose local replica is a leader of the partition of this data center and
   * remote replica is a leader of the partition of remote data center. This list is used for leader-based cross colo
   * replication to exchange missing blobs between only leader replicas. For non-leader replica pairs (leader <->
   * standby, standby <-> leader, standby <-> standby), we will wait the missing blobs to come from their leader interactions.
   * @param remoteReplicaInfos list of all remote replicas
   * @param exchangeMetadataResponseList list of metadata responses received from the remote replicas
   * @param leaderReplicaInfosOutput output list of leader replicas. It will populated in this method.
   * @param exchangeMetadataResponseListForLeaderReplicaInfosOutput output list of metadata responses received for the leader
   *                                                       replicas. It will be populated in this method.
   * @throws IllegalArgumentException
   */
  void getLeaderReplicaList(List<RemoteReplicaInfo> remoteReplicaInfos,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList, List<RemoteReplicaInfo> leaderReplicaInfosOutput,
      List<ExchangeMetadataResponse> exchangeMetadataResponseListForLeaderReplicaInfosOutput)
      throws IllegalArgumentException {

    if (exchangeMetadataResponseList.size() != remoteReplicaInfos.size()) {
      throw new IllegalArgumentException("ExchangeMetadataResponseList size " + exchangeMetadataResponseList.size()
          + " and replicasToReplicatePerNode size " + remoteReplicaInfos.size() + " should be the same");
    }

    for (int i = 0; i < remoteReplicaInfos.size(); i++) {
      RemoteReplicaInfo remoteReplicaInfo = remoteReplicaInfos.get(i);
      ReplicaId localReplica = remoteReplicaInfo.getLocalReplicaId();
      ReplicaId remoteReplica = remoteReplicaInfo.getReplicaId();
      // Check if local replica and remote replica are leaders for their partition.
      if (leaderBasedReplicationAdmin.isLeaderPair(localReplica, remoteReplica)) {
        leaderReplicaInfosOutput.add(remoteReplicaInfo);
        exchangeMetadataResponseListForLeaderReplicaInfosOutput.add(exchangeMetadataResponseList.get(i));
      }
    }
  }

  /**
   * Returns list of remote replica infos whose missing blobs in their metadata response haven't arrived within
   * time = replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds.
   * @param remoteReplicaInfos list of remote replica infos
   * @return list of remote replica infos which have timed out due to no progress
   */
  List<RemoteReplicaInfo> getRemoteStandbyReplicasTimedOutOnNoProgress(List<RemoteReplicaInfo> remoteReplicaInfos) {

    // Use case: In leader-based cross colo replication, non-leader replica pairs don't fetch blobs for missing keys
    // found in metadata exchange and expect them to come from leader<->leader replication and intra-dc replication.
    // However, if for any reason, some of their missing blobs never arrive via local leader, this is a safety feature
    // for standbys to fetch the blobs themselves in order to avoid being stuck.

    // Example scenario: For DELETE after PUT use case in remote data center, it is possible that standby replicas get
    // only PUT record in its replication cycle (DELETE record will come in next cycle) while leader gets both
    // PUT and DELETE together in its replication cycle. Due to that, deleted blob is not fetched by leader and is not
    // replicated from leader to standby. As a result, the corresponding PUT record in standby's missing blobs set is
    // never received.

    // Time out period is configurable via replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds. If
    // replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds == -1, this safety feature is disabled.

    List<RemoteReplicaInfo> remoteReplicasTimedOut = new ArrayList<>();
    if (replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds != -1) {
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfos) {
        ReplicaId localReplica = remoteReplicaInfo.getLocalReplicaId();
        ReplicaId remoteReplica = remoteReplicaInfo.getReplicaId();
        ExchangeMetadataResponse exchangeMetadataResponse = remoteReplicaInfo.getExchangeMetadataResponse();
        if (!leaderBasedReplicationAdmin.isLeaderPair(localReplica, remoteReplica)
            && exchangeMetadataResponse.hasMissingStoreMessages()
            && (time.seconds() - exchangeMetadataResponse.lastMissingMessageReceivedTimeSec)
            > replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds) {
          remoteReplicasTimedOut.add(remoteReplicaInfo);
        }
      }
    }
    return remoteReplicasTimedOut;
  }

  /**
   * Compare message infos of remote standby replica (whose blobs are now received from leader replicas) with message info
   * of blobs in local store and reconcile blob properties like ttl_update, delete, undelete. If blobs for all the missing messages
   * of the standby replica are received and updated, move the remote token of the standby forward.
   * @param remoteReplicaInfo remote replica information
   */
  void processMissingKeysFromPreviousMetadataResponse(RemoteReplicaInfo remoteReplicaInfo) {
    try {
      ExchangeMetadataResponse exchangeMetadataResponse = remoteReplicaInfo.getExchangeMetadataResponse();
      if (!exchangeMetadataResponse.isEmpty()) {
        Set<MessageInfo> receivedStoreMessagesWithUpdatesPending =
            exchangeMetadataResponse.getReceivedStoreMessagesWithUpdatesPending();
        Set<MessageInfo> receivedMessagesWithUpdatesCompleted = new HashSet<>();

        // 1. Go through messages for this replica whose keys were previously missing in local store and are now received
        // (via other replica threads) and compare the message infos with message infos of keys in local store to apply
        // updates to them (if needed) to reconcile delete, ttl_update and undelete states.
        for (MessageInfo messageInfo : receivedStoreMessagesWithUpdatesPending) {
          BlobId localStoreKey =
              (BlobId) exchangeMetadataResponse.remoteKeyToLocalKeyMap.get(messageInfo.getStoreKey());
          if (localStoreKey != null) {
            applyUpdatesToBlobInLocalStore(messageInfo, remoteReplicaInfo, localStoreKey);
          }
          receivedMessagesWithUpdatesCompleted.add(messageInfo);
        }

        // 2. Remove the messages whose updates have been completed
        exchangeMetadataResponse.removeReceivedStoreMessagesWithUpdatesPending(receivedMessagesWithUpdatesCompleted);

        // 3. If metadata response for this replica is now empty, i.e. all the missing keys are received and blob
        // updates for them have been completed, move the remote token forward and update local lag from remote for this replica.
        if (exchangeMetadataResponse.isEmpty()) {
          remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
          remoteReplicaInfo.setLocalLagFromRemoteInBytes(exchangeMetadataResponse.localLagFromRemoteInBytes);
          logger.trace("Thread name: {}, updating token {} and lag {} for partition {} for Remote replica {}",
              threadName, exchangeMetadataResponse.remoteToken, exchangeMetadataResponse.localLagFromRemoteInBytes,
              remoteReplicaInfo.getReplicaId().getPartitionId().toString(), remoteReplicaInfo.getReplicaId());
          remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(ServerErrorCode.No_Error));
        }
      }
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.Store_Not_Started) {
        logger.info("Local store not started for remote replica: {}", remoteReplicaInfo.getReplicaId());
      } else {
        logger.error("Exception occurred while updating blob from Remote replica {} for partition: {}",
            remoteReplicaInfo.getReplicaId(), remoteReplicaInfo.getReplicaId().getPartitionId().toString(), e);
        replicationMetrics.updateLocalStoreError(remoteReplicaInfo.getReplicaId());
      }
      // reset stored metadata response so that metadata request is sent again for this replica
      remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(ServerErrorCode.No_Error));
    }
  }

  /**
   * Checks if the input remote replica and its corresponding local replica are not leaders of their partitions and
   * contains any messages from its previous metadata exchange still missing in local store, i.e. they haven't arrived
   * from leader via intra-dc replication.
   * @param remoteReplicaInfo remote replica information
   * @return true if missing messages in previous metadata exchange are not yet received
   */
  boolean containsMissingKeysFromPreviousMetadataExchange(RemoteReplicaInfo remoteReplicaInfo) {
    ReplicaId localReplica = remoteReplicaInfo.getLocalReplicaId();
    ReplicaId remoteReplica = remoteReplicaInfo.getReplicaId();
    ExchangeMetadataResponse exchangeMetadataResponse = remoteReplicaInfo.getExchangeMetadataResponse();
    return !leaderBasedReplicationAdmin.isLeaderPair(localReplica, remoteReplica)
        && !exchangeMetadataResponse.isEmpty();
  }

  /**
   * Refreshes missing messages found in the exchange metadata response for the input replica by checking in the local store again.
   * @param remoteReplicaInfo remote replica information
   */
  private void refreshMissingStoreMessagesForStandbyReplica(RemoteReplicaInfo remoteReplicaInfo) {

    ExchangeMetadataResponse exchangeMetadataResponse = remoteReplicaInfo.getExchangeMetadataResponse();
    Set<MessageInfo> missingStoreMessages = exchangeMetadataResponse.getMissingStoreMessages();

    if (missingStoreMessages != null && !missingStoreMessages.isEmpty()) {
      Set<MessageInfo> missingStoreMessagesFoundInStore = new HashSet<>();
      try {
        // construct map of message info -> converted non-null local key
        Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap = exchangeMetadataResponse.remoteKeyToLocalKeyMap;
        Map<MessageInfo, StoreKey> remoteMessageToConvertedKeyNonNull = new HashMap<>();
        for (MessageInfo messageInfo : missingStoreMessages) {
          StoreKey convertedKey = remoteKeyToLocalKeyMap.get(messageInfo.getStoreKey());
          if (convertedKey != null) {
            remoteMessageToConvertedKeyNonNull.put(messageInfo, convertedKey);
          }
        }

        // Find the set of store keys that are still missing in the store
        Set<StoreKey> convertedMissingStoreKeys = remoteReplicaInfo.getLocalStore()
            .findMissingKeys(new ArrayList<>(remoteMessageToConvertedKeyNonNull.values()));

        // Filter the remote messages whose keys are now found in store, i.e. not present in convertedMissingStoreKeys set.
        remoteMessageToConvertedKeyNonNull.forEach((messageInfo, convertedKey) -> {
          if (!convertedMissingStoreKeys.contains(convertedKey)) {
            missingStoreMessagesFoundInStore.add(messageInfo);
          }
        });

        // update the missing store messages being tracked for this replica
        exchangeMetadataResponse.removeMissingStoreMessages(missingStoreMessagesFoundInStore);
      } catch (StoreException e) {
        logger.error(
            "Exception occurred while checking for missing keys in local store for partition {} and Remote replica: {}",
            remoteReplicaInfo.getReplicaId().getPartitionId().toPathString(), remoteReplicaInfo.getReplicaId(), e);
        // reset stored metadata response so that metadata request is sent again for this replica
        remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(ServerErrorCode.No_Error));
      }
    }
  }

  /**
   * Return associated {@link ReplicationMetrics}. Used in test.
   */
  ReplicationMetrics getReplicationMetrics() {
    return replicationMetrics;
  }

  static class ExchangeMetadataResponse {
    // Set of messages from remote replica missing in the local store.
    final Set<MessageInfo> missingStoreMessages;
    // Set of messages whose blobs are now present in local store  but their properties (ttl_update, delete, undelete)
    // needs to be updated.
    final Set<MessageInfo> receivedStoreMessagesWithUpdatesPending;
    // map of remote keys to their converted local store keys
    final Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap;
    final FindToken remoteToken;
    final long localLagFromRemoteInBytes;
    final ServerErrorCode serverErrorCode;
    final Time time;
    // Time (in secs) at which last missing message was received. This is used in leader-based cross colo replication
    // to do cross colo fetches for non-leader replica pairs if there are still few missing local store messages and
    // there is no progress on them for ReplicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds
    // since the time the last missing message was received(lastMissingMessageReceivedTimeSec).
    long lastMissingMessageReceivedTimeSec;

    ExchangeMetadataResponse(Set<MessageInfo> missingStoreMessages, FindToken remoteToken,
        long localLagFromRemoteInBytes, Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap, Time time) {
      this.missingStoreMessages = missingStoreMessages;
      this.remoteKeyToLocalKeyMap = remoteKeyToLocalKeyMap;
      this.remoteToken = remoteToken;
      this.localLagFromRemoteInBytes = localLagFromRemoteInBytes;
      this.serverErrorCode = ServerErrorCode.No_Error;
      this.time = time;
      this.lastMissingMessageReceivedTimeSec = time.seconds();
      this.receivedStoreMessagesWithUpdatesPending = new HashSet<>();
    }

    ExchangeMetadataResponse(ServerErrorCode errorCode) {
      this.missingStoreMessages = null;
      this.remoteKeyToLocalKeyMap = null;
      this.remoteToken = null;
      this.localLagFromRemoteInBytes = -1;
      this.serverErrorCode = errorCode;
      this.lastMissingMessageReceivedTimeSec = -1;
      this.time = null;
      this.receivedStoreMessagesWithUpdatesPending = null;
    }

    /**
     * Shallow copy Constructor for {@link ExchangeMetadataResponse}, which copies all field references with exception
     * of 'missingStoreMessages' for which it creates new Set object.
     * @param other other {@link ExchangeMetadataResponse} object.
     */
    ExchangeMetadataResponse(ExchangeMetadataResponse other) {
      // Create a copy of 'missingStoreMessages' since it is mutable and sharing between multiple ExchangeMetadataResponses
      // can make operations on it such as size(), isEmpty(), etc. hard to track.
      // For example, an inter-colo thread could be fetching missing store messages from its copy of exchangeMetadataResponse
      // in fixMissingKeys() method while an intra-colo thread could be emptying the missing store messages in
      // exchangeMetadataResponse stored in RemoteReplicaInfo. Referencing same object of missingStoreMessages could
      // lead to race conditions and is avoided for simplicity.
      this.missingStoreMessages = other.missingStoreMessages == null ? null : new HashSet<>(other.missingStoreMessages);
      this.remoteKeyToLocalKeyMap = other.remoteKeyToLocalKeyMap;
      this.remoteToken = other.remoteToken;
      this.localLagFromRemoteInBytes = other.localLagFromRemoteInBytes;
      this.serverErrorCode = other.serverErrorCode;
      this.time = other.time;
      this.lastMissingMessageReceivedTimeSec = other.lastMissingMessageReceivedTimeSec;
      this.receivedStoreMessagesWithUpdatesPending = other.receivedStoreMessagesWithUpdatesPending;
    }

    /**
     * Checks if there are any missing store messages in this meta data exchange.
     * @return set of missing store messages
     */
    synchronized boolean hasMissingStoreMessages() {
      return missingStoreMessages != null && !missingStoreMessages.isEmpty();
    }

    /**
     * Get missing store messages in this metadata exchange.
     * @return set of missing store messages
     */
    synchronized Set<MessageInfo> getMissingStoreMessages() {
      return missingStoreMessages;
    }

    /**
     * Removes missing store messages (who blobs are now written to local store) from 'missingStoreMessages' set and
     * adds them to 'receivedStoreMessagesWithUpdatesPending' set. Replica threads will go through
     * receivedStoreMessagesWithUpdatesPending set and compare message info of these remote messages with message info
     * of matching blobs in local store to reconcile blob properties ttl_update/delete/undelete.
     * @param messagesWrittenToStore messages that needs to be removed from missingStoreMessages set and added to
     *                     receivedStoreMessagesWithUpdatesPending.
     */
    synchronized void removeMissingStoreMessages(Set<MessageInfo> messagesWrittenToStore) {
      if (missingStoreMessages != null && receivedStoreMessagesWithUpdatesPending != null) {
        boolean missingMessagesReceived = false;
        for (MessageInfo messageToRemove : messagesWrittenToStore) {
          if (missingStoreMessages.remove(messageToRemove)) {
            receivedStoreMessagesWithUpdatesPending.add(messageToRemove);
            missingMessagesReceived = true;
          }
        }
        if (missingMessagesReceived) {
          lastMissingMessageReceivedTimeSec = time.seconds();
        }
      }
    }

    /**
     * Get remote messages that were previously missing and are now received but whose blob properties (
     * ttl_update, delete, undelete) still needs to be compared to properties of blob in local store and reconciled.
     * @return set of messages that are now found in store
     */
    synchronized Set<MessageInfo> getReceivedStoreMessagesWithUpdatesPending() {
      return receivedStoreMessagesWithUpdatesPending;
    }

    /**
     * Remove messages from receivedStoreMessagesWithUpdatesPending set. Blob properties of all these messages have been
     * compared to blobs in local store and their ttl_update, delete, un-delete states have been reconciled.
     * @param receivedStoreMessagesWithUpdatesCompleted messages that needs to be removed from
     *                                                  receivedStoreMessagesWithUpdatesPending set
     */
    synchronized void removeReceivedStoreMessagesWithUpdatesPending(
        Set<MessageInfo> receivedStoreMessagesWithUpdatesCompleted) {
      if (receivedStoreMessagesWithUpdatesPending != null) {
        receivedStoreMessagesWithUpdatesPending.removeAll(receivedStoreMessagesWithUpdatesCompleted);
      }
    }

    /**
     * Checks if this metadata response is empty. I.e, checks if there are no keys that are either missing in local store
     * or have updates pending.
     * @return true if both missingStoreMessages and receivedStoreMessagesWithUpdatesPending are empty.
     */
    synchronized boolean isEmpty() {
      return missingStoreMessages == null || receivedStoreMessagesWithUpdatesPending == null || (
          missingStoreMessages.isEmpty() && receivedStoreMessagesWithUpdatesPending.isEmpty());
    }

    /**
     *  get the keys corresponding to the missing messages in local store
     */
    Set<StoreKey> getMissingStoreKeys() {
      return missingStoreMessages == null ? Collections.emptySet()
          : missingStoreMessages.stream().map(MessageInfo::getStoreKey).collect(Collectors.toSet());
    }
  }

  boolean isThreadUp() {
    return running;
  }

  void shutdown() throws InterruptedException {
    running = false;
    lock.lock();
    try {
      pauseCondition.signal();
    } finally {
      lock.unlock();
    }
    shutdownLatch.await();
  }
}
