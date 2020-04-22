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
import com.github.ambry.messageformat.MessageFormatException;
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
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
  private final Logger logger = LoggerFactory.getLogger(getClass());
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

  private volatile boolean allDisabled = false;

  public ReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, ConnectionPool connectionPool,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager) {
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
      logger.trace("Starting replica thread on Local node: " + dataNodeId + " Thread name: " + threadName);
      for (Map.Entry<DataNodeId, Set<RemoteReplicaInfo>> replicasToReplicateEntry : replicasToReplicateGroupedByNode.entrySet()) {
        logger.trace("Remote node: " + replicasToReplicateEntry.getKey() + " Thread name: " + threadName
            + " ReplicasToReplicate: " + replicasToReplicateEntry.getValue());
      }
      logger.info("Begin iteration for thread " + threadName);
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
      long replicationStartTimeInMs = SystemTime.getInstance().milliseconds();
      long startTimeInMs = replicationStartTimeInMs;

      List<RemoteReplicaInfo> activeReplicasPerNode = new ArrayList<>();
      for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
        ReplicaId replicaId = remoteReplicaInfo.getReplicaId();
        boolean inBackoff = time.milliseconds() < remoteReplicaInfo.getReEnableReplicationTime();
        if (replicationDisabledPartitions.contains(replicaId.getPartitionId()) || replicaId.isDown() || inBackoff
            || !remoteReplicaInfo.getLocalStore().isStarted()) {
          continue;
        }
        activeReplicasPerNode.add(remoteReplicaInfo);
      }
      logger.trace("Replicating from {} RemoteReplicaInfos.", activeReplicasPerNode.size());
      if (activeReplicasPerNode.size() > 0) {
        allCaughtUp = false;
        // if maxReplicaCountPerRequest > 0, split remote replicas on same node into multiple lists; otherwise there is
        // no limit.
        List<List<RemoteReplicaInfo>> activeReplicaSubLists =
            maxReplicaCountPerRequest > 0 ? Utils.partitionList(activeReplicasPerNode, maxReplicaCountPerRequest)
                : Collections.singletonList(activeReplicasPerNode);

        // use a variable to track current replica list to replicate (for logging purpose)
        List<RemoteReplicaInfo> currentReplicaList = activeReplicasPerNode;
        try {
          connectedChannel =
              connectionPool.checkOutConnection(remoteNode.getHostname(), activeReplicasPerNode.get(0).getPort(),
                  replicationConfig.replicationConnectionPoolCheckoutTimeoutMs);
          checkoutConnectionTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;
          // we checkout ConnectedChannel once and replicate remote replicas in batch via same ConnectedChannel
          for (List<RemoteReplicaInfo> replicaSubList : activeReplicaSubLists) {
            exchangeMetadataTimeInMs = -1;
            fixMissingStoreKeysTimeInMs = -1;
            currentReplicaList = replicaSubList;
            logger.debug("Exchanging metadata with {} remote replicas on {}", currentReplicaList.size(), remoteNode);
            startTimeInMs = SystemTime.getInstance().milliseconds();
            List<ExchangeMetadataResponse> exchangeMetadataResponseList =
                exchangeMetadata(connectedChannel, replicaSubList);
            exchangeMetadataTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;
            startTimeInMs = SystemTime.getInstance().milliseconds();
            fixMissingStoreKeys(connectedChannel, replicaSubList, exchangeMetadataResponseList);
            fixMissingStoreKeysTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;
          }
        } catch (Throwable e) {
          if (checkoutConnectionTimeInMs == -1) {
            // throwable happened in checkout connection phase
            checkoutConnectionTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;
            responseHandler.onEvent(activeReplicasPerNode.get(0).getReplicaId(), e);
          } else if (exchangeMetadataTimeInMs == -1) {
            // throwable happened in exchange metadata phase
            exchangeMetadataTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;
          } else if (fixMissingStoreKeysTimeInMs == -1) {
            // throwable happened in fix missing store phase
            fixMissingStoreKeysTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;
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
          long totalReplicationTime = SystemTime.getInstance().milliseconds() - replicationStartTimeInMs;
          replicationMetrics.updateTotalReplicationTime(totalReplicationTime, replicatingFromRemoteColo,
              replicatingOverSsl, datacenterName);
          if (connectedChannel != null) {
            connectionPool.checkInConnection(connectedChannel);
          }
          context.stop();
          portTypeBasedContext.stop();
        }
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
    long exchangeMetadataStartTimeInMs = SystemTime.getInstance().milliseconds();
    List<ExchangeMetadataResponse> exchangeMetadataResponseList = new ArrayList<>();
    if (replicasToReplicatePerNode.size() > 0) {
      try {
        DataNodeId remoteNode = replicasToReplicatePerNode.get(0).getReplicaId().getDataNodeId();
        ReplicaMetadataResponse response =
            getReplicaMetadataResponse(replicasToReplicatePerNode, connectedChannel, remoteNode);
        long startTimeInMs = SystemTime.getInstance().milliseconds();

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
              continue;
            }
            try {
              logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token from remote: {} Replica lag: {} ",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getFindToken(),
                  replicaMetadataResponseInfo.getRemoteReplicaLagInBytes());
              Set<StoreKey> remoteMissingStoreKeys =
                  getMissingStoreKeys(replicaMetadataResponseInfo, remoteNode, remoteReplicaInfo);
              processReplicaMetadataResponse(remoteMissingStoreKeys, replicaMetadataResponseInfo, remoteReplicaInfo,
                  remoteNode, remoteKeyToLocalKeyMap);
              ExchangeMetadataResponse exchangeMetadataResponse =
                  new ExchangeMetadataResponse(remoteMissingStoreKeys, replicaMetadataResponseInfo.getFindToken(),
                      replicaMetadataResponseInfo.getRemoteReplicaLagInBytes());
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
              replicationMetrics.updateLagMetricForRemoteReplica(remoteReplicaInfo,
                  exchangeMetadataResponse.localLagFromRemoteInBytes);
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
          } else {
            replicationMetrics.updateMetadataRequestError(remoteReplicaInfo.getReplicaId());
            logger.error("Remote node: {} Thread name: {} Remote replica: {} Server error: {}", remoteNode, threadName,
                remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
            exchangeMetadataResponseList.add(new ExchangeMetadataResponse(replicaMetadataResponseInfo.getError()));
          }
        }
        long processMetadataResponseTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;
        logger.trace("Remote node: {} Thread name: {} processMetadataResponseTime: {}", remoteNode, threadName,
            processMetadataResponseTimeInMs);
      } finally {
        long exchangeMetadataTime = SystemTime.getInstance().milliseconds() - exchangeMetadataStartTimeInMs;
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
   * @throws IOException
   * @throws MessageFormatException
   * @throws ReplicationException
   */
  void fixMissingStoreKeys(ConnectedChannel connectedChannel, List<RemoteReplicaInfo> replicasToReplicatePerNode,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList)
      throws IOException, MessageFormatException, ReplicationException {
    long fixMissingStoreKeysStartTimeInMs = SystemTime.getInstance().milliseconds();
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
              remoteNode);
      writeMessagesToLocalStoreAndAdvanceTokens(exchangeMetadataResponseList, getResponse, replicasToReplicatePerNode,
          remoteNode);
    } finally {
      long fixMissingStoreKeysTime = SystemTime.getInstance().milliseconds() - fixMissingStoreKeysStartTimeInMs;
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
  private ReplicaMetadataResponse getReplicaMetadataResponse(List<RemoteReplicaInfo> replicasToReplicatePerNode,
      ConnectedChannel connectedChannel, DataNodeId remoteNode) throws ReplicationException, IOException {
    long replicaMetadataRequestStartTime = SystemTime.getInstance().milliseconds();
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
          "replication-metadata-" + dataNodeId.getHostname(), replicaMetadataRequestInfoList,
          replicationConfig.replicationFetchSizeInBytes, replicationConfig.replicaMetadataRequestVersion);
      connectedChannel.send(request);
      ChannelOutput channelOutput = connectedChannel.receive();
      ByteBufferInputStream byteBufferInputStream =
          new ByteBufferInputStream(channelOutput.getInputStream(), (int) channelOutput.getStreamSize());
      logger.trace("Remote node: {} Thread name: {} Remote replicas: {} ByteBuffer size after deserialization: {} ",
          remoteNode, threadName, replicasToReplicatePerNode, byteBufferInputStream.available());
      ReplicaMetadataResponse response =
          ReplicaMetadataResponse.readFrom(new DataInputStream(byteBufferInputStream), findTokenHelper, clusterMap);

      long metadataRequestTime = SystemTime.getInstance().milliseconds() - replicaMetadataRequestStartTime;
      replicationMetrics.updateMetadataRequestTime(metadataRequestTime, replicatingFromRemoteColo, replicatingOverSsl,
          datacenterName);

      if (response.getError() != ServerErrorCode.No_Error
          || response.getReplicaMetadataResponseInfoList().size() != replicasToReplicatePerNode.size()) {
        int replicaMetadataResponseInfoListSize = response.getReplicaMetadataResponseInfoList() == null ? 0
            : response.getReplicaMetadataResponseInfoList().size();
        logger.error("Remote node: " + remoteNode + " Thread name: " + threadName + " Remote replicas: "
            + replicasToReplicatePerNode + " Replica metadata response error: " + response.getError()
            + " ReplicaMetadataResponseInfoListSize: " + replicaMetadataResponseInfoListSize
            + " ReplicasToReplicatePerNodeSize: " + replicasToReplicatePerNode.size());
        throw new ReplicationException("Replica Metadata Response Error " + response.getError());
      }
      return response;
    } catch (IOException e) {
      responseHandler.onEvent(replicasToReplicatePerNode.get(0).getReplicaId(), e);
      throw e;
    }
  }

  /**
   * Gets the missing store keys by comparing the messages from the remote node
   * @param replicaMetadataResponseInfo The response that contains the messages from the remote node
   * @param remoteNode The remote node from which replication needs to happen
   * @param remoteReplicaInfo The remote replica that contains information about the remote replica id
   * @return List of store keys that are missing from the local store
   * @throws StoreException if store error (usually IOError) occurs when getting missing keys.
   */
  private Set<StoreKey> getMissingStoreKeys(ReplicaMetadataResponseInfo replicaMetadataResponseInfo,
      DataNodeId remoteNode, RemoteReplicaInfo remoteReplicaInfo) throws StoreException {
    long startTime = SystemTime.getInstance().milliseconds();
    List<MessageInfo> messageInfoList = replicaMetadataResponseInfo.getMessageInfoList();
    Map<StoreKey, StoreKey> remoteToConvertedNonNull = new HashMap<>();

    for (MessageInfo messageInfo : messageInfoList) {
      StoreKey storeKey = messageInfo.getStoreKey();
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key from remote: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), storeKey);
      StoreKey convertedKey = storeKeyConverter.getConverted(storeKey);
      if (convertedKey != null) {
        remoteToConvertedNonNull.put(storeKey, convertedKey);
      }
    }
    Set<StoreKey> convertedMissingStoreKeys =
        remoteReplicaInfo.getLocalStore().findMissingKeys(new ArrayList<>(remoteToConvertedNonNull.values()));
    Set<StoreKey> missingRemoteStoreKeys = new HashSet<>();
    remoteToConvertedNonNull.forEach((remoteKey, convertedKey) -> {
      if (convertedMissingStoreKeys.contains(convertedKey)) {
        logger.trace(
            "Remote node: {} Thread name: {} Remote replica: {} Key missing id (converted): {} Key missing id (remote): {}",
            remoteNode, threadName, remoteReplicaInfo.getReplicaId(), convertedKey, remoteKey);
        missingRemoteStoreKeys.add(remoteKey);
      }
    });
    if (messageInfoList.size() != 0 && missingRemoteStoreKeys.size() == 0) {
      // Catching up
      replicationMetrics.allResponsedKeysExist.inc();
    }
    replicationMetrics.updateCheckMissingKeysTime(SystemTime.getInstance().milliseconds() - startTime,
        replicatingFromRemoteColo, datacenterName);
    return missingRemoteStoreKeys;
  }

  /**
   * Takes the missing keys and the message list from the remote store and identifies messages that are deleted
   * on the remote store and updates them locally. Also, if the message that is missing is deleted in the remote
   * store, we remove the message from the list of missing keys
   * @param missingRemoteStoreKeys The list of keys missing from the local store
   * @param replicaMetadataResponseInfo The replica metadata response from the remote store
   * @param remoteReplicaInfo The remote replica that is being replicated from
   * @param remoteNode The remote node from which replication needs to happen
   * @param remoteKeyToLocalKeyMap map mapping remote keys to local key equivalents
   * @throws StoreException
   */
  private void processReplicaMetadataResponse(Set<StoreKey> missingRemoteStoreKeys,
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo, RemoteReplicaInfo remoteReplicaInfo,
      DataNodeId remoteNode, Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap) throws StoreException {
    long startTime = SystemTime.getInstance().milliseconds();
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
        missingRemoteStoreKeys.remove(messageInfo.getStoreKey());
        logger.trace("Remote node: {} Thread name: {} Remote replica: {} Remote key deprecated locally: {}", remoteNode,
            threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
      } else if (missingRemoteStoreKeys.contains(messageInfo.getStoreKey())) {
        // The key is missing in the local store, we either send get request to fetch the content of this key, or does
        // nothing if the key is deleted or expired remotely.
        if (messageInfo.isDeleted()) {
          // if the key is not present locally and if the remote replica has the message in deleted state,
          // it is not considered missing locally.
          missingRemoteStoreKeys.remove(messageInfo.getStoreKey());
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
          missingRemoteStoreKeys.remove(messageInfo.getStoreKey());
          logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key in expired state remotely {}",
              remoteNode, threadName, remoteReplicaInfo.getReplicaId(), localKey);
        }
      } else {
        // the key is present in the local store. Mark it for deletion if it is deleted in the remote store and not
        // deleted yet locally
        MessageInfo localMessageInfo = remoteReplicaInfo.getLocalStore().findKey(localKey);
        boolean deletedLocally = localMessageInfo.isDeleted();
        boolean ttlUpdatedLocally = localMessageInfo.isTtlUpdated();
        short localLifeVersion = localMessageInfo.getLifeVersion();
        short remoteLifeVersion = messageInfo.getLifeVersion();
        if (localLifeVersion > remoteLifeVersion) {
          // if the lifeVersion in local store is greater than the remote lifeVersion, then nothing needs to be done.
          continue;
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
              // if final state is not delete, then to bump lifeVerion in local store to remote lifeVersion, we have to
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
    }
    if (replicatingFromRemoteColo) {
      replicationMetrics.interColoProcessMetadataResponseTime.get(datacenterName)
          .update(SystemTime.getInstance().milliseconds() - startTime);
    } else {
      replicationMetrics.intraColoProcessMetadataResponseTime.update(
          SystemTime.getInstance().milliseconds() - startTime);
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
  private Map<StoreKey, StoreKey> batchConvertReplicaMetadataResponseKeys(ReplicaMetadataResponse response)
      throws IOException {
    try {
      List<StoreKey> storeKeysToConvert = new ArrayList<>();
      for (ReplicaMetadataResponseInfo replicaMetadataResponseInfo : response.getReplicaMetadataResponseInfoList()) {
        if ((replicaMetadataResponseInfo.getError() == ServerErrorCode.No_Error) && (
            replicaMetadataResponseInfo.getMessageInfoList() != null)) {
          for (MessageInfo messageInfo : replicaMetadataResponseInfo.getMessageInfoList()) {
            storeKeysToConvert.add(messageInfo.getStoreKey());
          }
        }
      }
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
   * @return The response that contains the missing messages; or null if no request was issued because there were no
   * keys missing.
   * @throws ReplicationException
   * @throws IOException
   */
  private GetResponse getMessagesForMissingKeys(ConnectedChannel connectedChannel,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList, List<RemoteReplicaInfo> replicasToReplicatePerNode,
      DataNodeId remoteNode) throws ReplicationException, IOException {
    List<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.No_Error) {
        Set<StoreKey> missingStoreKeys = exchangeMetadataResponse.missingStoreKeys;
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
          GetRequest.Replication_Client_Id_Prefix + dataNodeId.getHostname(), MessageFormatFlags.All,
          partitionRequestInfoList, replicationConfig.replicationIncludeAll ? GetOption.Include_All : GetOption.None);
      long startTime = SystemTime.getInstance().milliseconds();
      try {
        connectedChannel.send(getRequest);
        ChannelOutput channelOutput = connectedChannel.receive();
        getResponse = GetResponse.readFrom(new DataInputStream(channelOutput.getInputStream()), clusterMap);
        long getRequestTime = SystemTime.getInstance().milliseconds() - startTime;
        replicationMetrics.updateGetRequestTime(getRequestTime, replicatingFromRemoteColo, replicatingOverSsl,
            datacenterName);
        if (getResponse.getError() != ServerErrorCode.No_Error) {
          logger.error("Remote node: " + remoteNode + " Thread name: " + threadName + " Remote replicas: "
              + replicasToReplicatePerNode + " GetResponse from replication: " + getResponse.getError());
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
   * @throws IOException
   */
  private void writeMessagesToLocalStoreAndAdvanceTokens(List<ExchangeMetadataResponse> exchangeMetadataResponseList,
      GetResponse getResponse, List<RemoteReplicaInfo> replicasToReplicatePerNode, DataNodeId remoteNode)
      throws IOException {
    int partitionResponseInfoIndex = 0;
    long totalBytesFixed = 0;
    long totalBlobsFixed = 0;
    long startTime = SystemTime.getInstance().milliseconds();
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.No_Error) {
        if (remoteReplicaInfo.getToken().equals(exchangeMetadataResponse.remoteToken)) {
          remoteReplicaInfo.setReEnableReplicationTime(
              time.milliseconds() + replicationConfig.replicationSyncedReplicaBackoffDurationMs);
          syncedBackOffCount.inc();
        }
        if (exchangeMetadataResponse.missingStoreKeys.size() > 0) {
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
                  exchangeMetadataResponse.missingStoreKeys, remoteReplicaInfo.getReplicaId().getPartitionId(),
                  remoteReplicaInfo.getLocalReplicaId().getMountPath());

              MessageFormatWriteSet writeset;
              MessageSievingInputStream validMessageDetectionInputStream =
                  new MessageSievingInputStream(getResponse.getInputStream(), messageInfoList,
                      Collections.singletonList(transformer), metricRegistry);
              if (validMessageDetectionInputStream.hasInvalidMessages()) {
                replicationMetrics.incrementInvalidMessageError(partitionResponseInfo.getPartition());
                logger.error("Out of " + (messageInfoList.size()) + " messages, " + (messageInfoList.size()
                    - validMessageDetectionInputStream.getValidMessageInfoList().size())
                    + " invalid messages were found in message stream from " + remoteReplicaInfo.getReplicaId());
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
              remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
              remoteReplicaInfo.setLocalLagFromRemoteInBytes(exchangeMetadataResponse.localLagFromRemoteInBytes);
              logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token after speaking to remote node: {}",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), exchangeMetadataResponse.remoteToken);
            } catch (StoreException e) {
              if (e.getErrorCode() != StoreErrorCodes.Already_Exist) {
                replicationMetrics.updateLocalStoreError(remoteReplicaInfo.getReplicaId());
                logger.error("Remote node: " + remoteNode + " Thread name: " + threadName + " Remote replica: "
                    + remoteReplicaInfo.getReplicaId(), e);
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
                remoteNode, threadName, remoteReplicaInfo.getReplicaId(), exchangeMetadataResponse.missingStoreKeys);
          } else {
            replicationMetrics.updateGetRequestError(remoteReplicaInfo.getReplicaId());
            logger.error("Remote node: {} Thread name: {} Remote replica: {} Server error: {}", remoteNode, threadName,
                remoteReplicaInfo.getReplicaId(), partitionResponseInfo.getErrorCode());
          }
        } else {
          // There are no missing keys. We just advance the token
          remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
          remoteReplicaInfo.setLocalLagFromRemoteInBytes(exchangeMetadataResponse.localLagFromRemoteInBytes);
          logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token after speaking to remote node: {}",
              remoteNode, threadName, remoteReplicaInfo.getReplicaId(), exchangeMetadataResponse.remoteToken);
        }
      }
    }
    long batchStoreWriteTime = SystemTime.getInstance().milliseconds() - startTime;
    replicationMetrics.updateBatchStoreWriteTime(batchStoreWriteTime, totalBytesFixed, totalBlobsFixed,
        replicatingFromRemoteColo, replicatingOverSsl, datacenterName);
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
        logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key already undeleted: {}", remoteNode,
            threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
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
      if (e.getErrorCode() == StoreErrorCodes.ID_Deleted) {
        logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key already deleted: {}", remoteNode,
            threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
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
   * Return associated {@link ReplicationMetrics}. Used in test.
   */
  ReplicationMetrics getReplicationMetrics() {
    return replicationMetrics;
  }

  static class ExchangeMetadataResponse {
    final Set<StoreKey> missingStoreKeys;
    final FindToken remoteToken;
    final long localLagFromRemoteInBytes;
    final ServerErrorCode serverErrorCode;

    ExchangeMetadataResponse(Set<StoreKey> missingStoreKeys, FindToken remoteToken, long localLagFromRemoteInBytes) {
      this.missingStoreKeys = missingStoreKeys;
      this.remoteToken = remoteToken;
      this.localLagFromRemoteInBytes = localLagFromRemoteInBytes;
      this.serverErrorCode = ServerErrorCode.No_Error;
    }

    ExchangeMetadataResponse(ServerErrorCode errorCode) {
      missingStoreKeys = null;
      remoteToken = null;
      localLagFromRemoteInBytes = -1;
      this.serverErrorCode = errorCode;
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
