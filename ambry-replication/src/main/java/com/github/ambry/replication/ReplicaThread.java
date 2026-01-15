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
import com.github.ambry.clustermap.CloudDataNode;
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
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
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
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.replication.continuous.ActiveGroupTracker;
import com.github.ambry.replication.continuous.DataNodeTracker;
import com.github.ambry.replication.continuous.GroupTracker;
import com.github.ambry.replication.continuous.ReplicaStatus;
import com.github.ambry.replication.continuous.ReplicaTracker;
import com.github.ambry.replication.continuous.StandByGroupTracker;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
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
  protected final FindTokenHelper findTokenHelper;
  private final ClusterMap clusterMap;
  private final AtomicInteger correlationIdGenerator;
  protected final DataNodeId dataNodeId;
  private final NetworkClient networkClient;
  private final ReplicationConfig replicationConfig;
  private final ReplicationMetrics replicationMetrics;
  private final String threadName;
  private final NotificationSystem notification;
  private static final Logger logger = LoggerFactory.getLogger(ReplicaThread.class);
  protected final StoreKeyConverter storeKeyConverter;
  private final Transformer transformer;
  private final MetricRegistry metricRegistry;
  private final ResponseHandler responseHandler;
  private final boolean replicatingFromRemoteColo;
  private final boolean replicatingOverSsl;
  private final String datacenterName;
  private final long threadThrottleDurationMs;
  protected final Time time;
  private final Counter throttleCount;
  private final Counter continuousReplicationReplicaThrottleCount;
  private final Counter syncedBackOffCount;
  private final Counter idleCount;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition pauseCondition = lock.newCondition();
  private final ReplicaSyncUpManager replicaSyncUpManager;
  private final int maxReplicaCountPerRequest;
  private final boolean enableContinuousReplication;
  private final Predicate<MessageInfo> skipPredicate;
  private final int continuousReplicationGroupIterationLimit;
  private volatile boolean terminateCurrentContinuousReplicationCycle = false;
  private volatile boolean allDisabled = false;
  private final ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin;
  protected Thread thread;
  protected AtomicBoolean threadStarted;

  // This is used in the test cases
  private Map<DataNodeId, List<ExchangeMetadataResponse>> exchangeMetadataResponsesInEachCycle = null;

  private volatile ExchangeMetadataListener exchangeMetadataListener = null;

  public ReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, ReplicationConfig replicationConfig,
      ReplicationMetrics replicationMetrics, NotificationSystem notification, StoreKeyConverter storeKeyConverter,
      Transformer transformer, MetricRegistry metricRegistry, boolean replicatingOverSsl, String datacenterName,
      ResponseHandler responseHandler, Time time, ReplicaSyncUpManager replicaSyncUpManager,
      Predicate<MessageInfo> skipPredicate) {
    this(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, null, replicationConfig,
        replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry, replicatingOverSsl,
        datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate, null);
  }

  public ReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    this(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient, replicationConfig,
        replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry, replicatingOverSsl, datacenterName,
        responseHandler, time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin, null);
  }

  public ReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin, Set<PartitionId> replicationDisabledPartitions) {
    this.threadName = threadName;
    this.running = true;
    this.findTokenHelper = findTokenHelper;
    this.clusterMap = clusterMap;
    this.correlationIdGenerator = correlationIdGenerator;
    this.dataNodeId = dataNodeId;
    this.networkClient = networkClient;
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
      continuousReplicationReplicaThrottleCount = replicationMetrics.interColoContinuousReplicationReplicaThrottleCount;
    } else {
      threadThrottleDurationMs = replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs;
      syncedBackOffCount = replicationMetrics.intraColoReplicaSyncedBackoffCount;
      idleCount = replicationMetrics.intraColoReplicaThreadIdleCount;
      throttleCount = replicationMetrics.intraColoReplicaThreadThrottleCount;
      continuousReplicationReplicaThrottleCount = replicationMetrics.intraColoContinuousReplicationReplicaThrottleCount;
    }
    this.enableContinuousReplication = isContinuousReplicationEnabled(replicationConfig);
    this.continuousReplicationGroupIterationLimit = replicationConfig.replicationGroupIterationLimit;
    this.maxReplicaCountPerRequest = replicationConfig.replicationMaxPartitionCountPerRequest;
    this.leaderBasedReplicationAdmin = leaderBasedReplicationAdmin;
    threadStarted = new AtomicBoolean(false);

    if (replicationDisabledPartitions != null && !replicationDisabledPartitions.isEmpty()) {
      this.replicationDisabledPartitions.addAll(replicationDisabledPartitions);
    }
    replicationMetrics.populateReplicaThreadMetrics(threadName);
  }

  protected boolean isContinuousReplicationEnabled(ReplicationConfig replicationConfig) {
    return replicationConfig.replicationEnableContinuousReplication;
  }

  public boolean startThread() {
   if (threadStarted.compareAndSet(false, true)) {
     thread.start();
     return true;
   }
   return false;
  }

  public void setThread(Thread thread) {
    this.thread = thread;
  }

  public Thread getThread() {
    return thread;
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
      terminateCurrentContinuousReplicationCycle = true;
      lock.unlock();
    }
  }

  /**
   * @return {@link Set} of {@link PartitionId}s for which replication is disabled.
   */
  Set<PartitionId> getReplicationDisabledPartitions() {
    return unmodifiableReplicationDisabledPartitions;
  }

  public String getName() {
    return threadName;
  }

  /**
   * Only used in test.
   * @return The network client.
   */
  NetworkClient getNetworkClient() {
    return networkClient;
  }

  /**
   * Only used in test.
   * @param value
   */
  protected void setTerminateCurrentContinuousReplicationCycle(boolean value) {
    terminateCurrentContinuousReplicationCycle = value;
  }

  /**
   * Logs replication progress of local node against some remote node
   * @param remoteReplicaInfo remote replica information
   * @param exchangeMetadataResponse metadata information from remote node
   */
  protected void logReplicationStatus(RemoteReplicaInfo remoteReplicaInfo,
      ExchangeMetadataResponse exchangeMetadataResponse) {
    if (logger.isTraceEnabled()) {
      logger.trace("ReplicationStatus | {} | {} | isSealed = {} | Token = {} | localLagFromRemoteInBytes = {}",
          remoteReplicaInfo, remoteReplicaInfo.getReplicaId().getReplicaType(),
          remoteReplicaInfo.getReplicaId().isSealed(), remoteReplicaInfo.getToken().toString(),
          exchangeMetadataResponse.localLagFromRemoteInBytes);
    }
  }

  @Override
  public void run() {
    try {
      if (logger.isTraceEnabled()) {
        logger.trace("Starting replica thread on Local node: {} Thread name: {}", dataNodeId, threadName);
        for (Map.Entry<DataNodeId, Set<RemoteReplicaInfo>> replicasToReplicateEntry : replicasToReplicateGroupedByNode.entrySet()) {
          logger.trace("Remote node: {} Thread name: {} ReplicasToReplicate: {}", replicasToReplicateEntry.getKey(),
              threadName, replicasToReplicateEntry.getValue());
        }
      }
      logger.info("Begin iteration for thread {}", threadName);
      while (running) {
        // TODO: Replicate method performs one cycle of replication for every replica and faster replicas would be
        //  blocked by the slower replicas. There is an optimization we can do, which is let each replica runs replication
        //  independently without waiting for each other. This would make sure that slower replicas won't block faster
        //  replicas.
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
  public void removeRemoteReplicaInfo(RemoteReplicaInfo remoteReplicaInfo) {
    lock.lock();
    try {
      DataNodeId dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
      Set<RemoteReplicaInfo> remoteReplicaInfos = replicasToReplicateGroupedByNode.get(dataNodeId);
      if (remoteReplicaInfos != null) {
        if (!remoteReplicaInfos.remove(remoteReplicaInfo)) {
          replicationMetrics.remoteReplicaInfoRemoveError.inc();
          logger.error("ReplicaThread: {}, RemoteReplicaInfo {} not found.", threadName, remoteReplicaInfo);
        } else {
          logger.info("RemoteReplicaInfo {} is removed from ReplicaThread {}.", remoteReplicaInfo, threadName);
          decreaseAssignedRemoteReplicaInfosMetric();
        }
      } else {
        replicationMetrics.remoteReplicaInfoRemoveError.inc();
        logger.error("ReplicaThread: {}, RemoteReplicaInfos Set is not created for DataNode {}, RemoteReplicaInfo: {}.",
            threadName, dataNodeId, remoteReplicaInfo);
      }
    } finally {
      terminateCurrentContinuousReplicationCycle = true;
      lock.unlock();
    }
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
      // Use a linked hash set to make sure that we maintain a predictable order based on the insertion
      boolean doesRemoteReplicaInfoAlreadyExists = replicasToReplicateGroupedByNode.computeIfAbsent(dataNodeId, key -> new LinkedHashSet<>())
          .add(remoteReplicaInfo);
      if (!doesRemoteReplicaInfoAlreadyExists) {
        replicationMetrics.remoteReplicaInfoAddError.inc();
        // Since VCR is also listening Ambry Clustermap change, this may happen if events happens in following order:
        // 1. VCR in memory ambry-clustermap updated
        // 2. VcrClusterParticipantListener adds remote replicas for the newly added partition
        // 3. ClusterMapChangeListener adds remote replicas
        logger.warn("ReplicaThread: {}, RemoteReplicaInfo {} already exists.", threadName, remoteReplicaInfo);
      } else {
        increaseAssignedRemoteReplicaInfosMetric();
      }
    } finally {
      terminateCurrentContinuousReplicationCycle = true;
      lock.unlock();
    }
    if (logger.isTraceEnabled()) {
      logger.trace("RemoteReplicaInfo {} is added to ReplicaThread {}. Now working on {} dataNodeIds.", remoteReplicaInfo,
          threadName, replicasToReplicateGroupedByNode.keySet().size());
    }
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
   * A custom filter that inheritors can override depending upon use case to pick which replicas to replicate from
   * @param replicas A map of replicas {host -> {replicas}}
   */
  protected Map<DataNodeId, List<RemoteReplicaInfo>> selectReplicas(Map<DataNodeId, List<RemoteReplicaInfo>> replicas) {
    // custom filter that inheritors can override for various use cases
    return replicas;
  }

  /**
   * Runs continuous replication if enabled otherwise runs cyclic replication
   */
  public void replicate() {
    if (enableContinuousReplication) {
      replicateContinuous();
    } else {
      replicateCyclic();
    }
  }

  /**
   * Polls the give remote replica groups for requests, submits these requests to networkClient,
   * gets responses from networkClient and submits these to corresponding remote replica groups
   *
   * @param remoteReplicaGroups remoteReplica groups
   * @param correlationIdToReplicaGroup map of correlation id to remote replica group
   * @param correlationIdToRequestInfo map of correlation id to request info
   */
  private void pollRequestsAndSubmitResponses(List<RemoteReplicaGroup> remoteReplicaGroups,
      Map<Integer, RemoteReplicaGroup> correlationIdToReplicaGroup,
      Map<Integer, RequestInfo> correlationIdToRequestInfo) {

    List<RequestInfo> requestInfos =
        pollRemoteReplicaGroups(remoteReplicaGroups, correlationIdToRequestInfo, correlationIdToReplicaGroup);
    logger.trace("Thread name: {} Requests generated", threadName);

    // generate responses for requests that are timed out
    List<ResponseInfo> responseInfosForTimedOutRequests =
        filterTimedOutRequests(correlationIdToRequestInfo, correlationIdToReplicaGroup);
    logger.trace("Thread name: {} Filtered timed out Requests", threadName);

    // mark timed out requests to be dropped by network client
    Set<Integer> requestsToDrop = responseInfosForTimedOutRequests.stream()
        .map(r -> r.getRequestInfo().getRequest().getCorrelationId())
        .collect(Collectors.toSet());

    if (!requestsToDrop.isEmpty()) {
      replicationMetrics.incrementTimeoutRequestErrorCount(requestsToDrop.size(), replicatingFromRemoteColo,
          datacenterName);
    }

    final int pollTimeoutMs = (int) replicationConfig.replicationRequestNetworkPollTimeoutMs;

    // submit requests and get response for earlier submitted requests
    List<ResponseInfo> responseInfos = networkClient.sendAndPoll(requestInfos, requestsToDrop, pollTimeoutMs);
    logger.trace("Thread name: {} received {} responses from network client", threadName, responseInfos.size());
    responseInfos.addAll(responseInfosForTimedOutRequests);
    // process responses
    onResponses(responseInfos, correlationIdToRequestInfo, correlationIdToReplicaGroup);
  }

  /**
   * Computes the exchangeMetaDataResponse for each cycle
   * Prints logs for remote replica groups which have errors
   *
   * @param remoteReplicaGroups remoteReplica groups
   * @param groupsAddedToExchangeMetadatResponse set of group ids for which exchange metadata is added to {@link #exchangeMetadataResponsesInEachCycle}
   */
  private void processFinishedGroups(List<RemoteReplicaGroup> remoteReplicaGroups,
      Set<Integer> groupsAddedToExchangeMetadatResponse) {

    /*for each data node we are caching the first metadata responses received for remote replica groups.
     this will be used in tests only.
     */
    remoteReplicaGroups.stream()
        .filter(g -> !groupsAddedToExchangeMetadatResponse.contains(g.id) && g.isDone()
            && g.getExchangeMetadataResponseList() != null)
        .forEach(g -> {
          exchangeMetadataResponsesInEachCycle.computeIfAbsent(g.getRemoteDataNode(), k -> new ArrayList<>())
              .addAll(g.getExchangeMetadataResponseList());
          groupsAddedToExchangeMetadatResponse.add(g.id);
        });

    // print logs for groups which have error
    remoteReplicaGroups.stream()
        .filter(g -> g.isDone() && g.getException() != null)
        .forEach(g -> logger.error("Remote node: {} Thread name: {} RemoteReplicaGroup {} has exception {}",
            g.getRemoteDataNode(), threadName, g.getRemoteReplicaInfos(), g.getException()));
  }

  /**
   * Do replication for replicas grouped by {@link DataNodeId}
   * We will keep trying to generate groups until we do not get any groups by calling generateRemoteReplicaGroups.
   * We will generate requests and then process responses when received for these groups.
   *
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
  public void replicateContinuous() {
    long oneRoundStartTimeMs = time.milliseconds();
    terminateCurrentContinuousReplicationCycle = false;
    logger.trace("Thread name: {} Start RemoteReplicaGroup replication", threadName);

    exchangeMetadataResponsesInEachCycle = new HashMap<>();
    Set<Integer> groupsAddedToExchangeMetadatResponse = new HashSet<>();
    boolean allReplicasCaughtEarly = false;

    try {
      storeKeyConverter.dropCache();

      Map<Integer, RemoteReplicaGroup> correlationIdToReplicaGroup = new HashMap<>();
      Map<Integer, RequestInfo> correlationIdToRequestInfo = new LinkedHashMap<>();

      RemoteReplicaGroupPoller poller = new RemoteReplicaGroupPoller();
      // we keep polling groups until we can, i.e. there are no more inflight groups

      do {
        if (!running) {
          break;
        }
        List<RemoteReplicaGroup> inflightRemoteReplicaGroups = poller.enqueue();

        pollRequestsAndSubmitResponses(inflightRemoteReplicaGroups, correlationIdToReplicaGroup,
            correlationIdToRequestInfo);

        processFinishedGroups(inflightRemoteReplicaGroups, groupsAddedToExchangeMetadatResponse);
        logger.trace("Thread name: {} Finish one iteration for RemoteReplicaGroup replication", threadName);
      } while (poller.shouldContinue());
      logger.trace("Thread name: {} Finish all RemoteReplicaGroup replication", threadName);
      allReplicasCaughtEarly = poller.allReplicasCaughtUpEarly();
    } catch (Exception e) {
      logger.error("Thread name: {} found some error while replicating from remote hosts", threadName, e);
    } finally {
      long cycleEndTime = time.milliseconds();
      replicationMetrics.updateOneCycleReplicationTime(cycleEndTime - oneRoundStartTimeMs,
          replicatingFromRemoteColo, datacenterName);
      updatePerReplicationCycleTimeMetric(cycleEndTime - oneRoundStartTimeMs);
      updateReplicationCycleIterationsMetric();
    }

    // check and make thread sleep and publish metrics for throttling
    maybeSleepAfterReplication(allReplicasCaughtEarly);
    logger.trace("Thread name: {} Exiting replication, all iterations Done!", threadName);
  }

  /**
   * Updates the cycle replication time for replicaThread
   * @param value cycle time
   */
  private void updatePerReplicationCycleTimeMetric(long value) {
    replicationMetrics.updateReplicaThreadOneCycleReplicationTime(threadName, value);
  }

  /**
   * Updates the number of iterations for replication by this replicaThread
   */
  private void updateReplicationCycleIterationsMetric() {
    replicationMetrics.updateReplicaThreadCycleIteration(threadName);
  }

  public void increaseAssignedRemoteReplicaInfosMetric() {
    replicationMetrics.increaseReplicaThreadAssignedRemoteReplicaInfo(threadName);
  }

  public void decreaseAssignedRemoteReplicaInfosMetric() {
    replicationMetrics.decreaseReplicaThreadAssignedRemoteReplicaInfo(threadName);
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
  public void replicateCyclic() {
    exchangeMetadataResponsesInEachCycle = new HashMap<>();
    long oneRoundStartTimeMs = time.milliseconds();
    logger.trace("Thread name: {} Start RemoteReplicaGroup replication", threadName);
    List<RemoteReplicaGroup> remoteReplicaGroups = new ArrayList<>();
    int remoteReplicaGroupId = 0;
    try {
      // Before each cycle of replication, we clean up the cache in key converter.
      storeKeyConverter.dropCache();
      Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToRemoteReplicaInfo = selectReplicas(getRemoteReplicaInfos());
      for (Map.Entry<DataNodeId, List<RemoteReplicaInfo>> entry : dataNodeToRemoteReplicaInfo.entrySet()) {
        DataNodeId remoteNode = entry.getKey();
        List<RemoteReplicaInfo> replicasToReplicatePerNode = entry.getValue();
        List<RemoteReplicaInfo> activeReplicasPerNode = new ArrayList<>();
        List<RemoteReplicaInfo> standbyReplicasTimedOutOnNoProgress = new ArrayList<>();
        filterRemoteReplicasToReplicate(replicasToReplicatePerNode, activeReplicasPerNode,
            standbyReplicasTimedOutOnNoProgress);

        if (activeReplicasPerNode.size() > 0) {
          List<List<RemoteReplicaInfo>> activeReplicaSubLists =
              maxReplicaCountPerRequest > 0 ? Utils.partitionList(activeReplicasPerNode, maxReplicaCountPerRequest)
                  : Collections.singletonList(activeReplicasPerNode);
          for (List<RemoteReplicaInfo> replicaSubList : activeReplicaSubLists) {
            RemoteReplicaGroup group =
                new RemoteReplicaGroup(replicaSubList, remoteNode, false, remoteReplicaGroupId++);
            remoteReplicaGroups.add(group);
          }
        }

        if (standbyReplicasTimedOutOnNoProgress.size() > 0) {
          RemoteReplicaGroup group =
              new RemoteReplicaGroup(standbyReplicasTimedOutOnNoProgress, remoteNode, true, remoteReplicaGroupId++);
          remoteReplicaGroups.add(group);
        }

      }
      // A map from correlation id to RemoteReplicaGroup. This is used to find the group when response comes back.
      Map<Integer, RemoteReplicaGroup> correlationIdToReplicaGroup = new HashMap<>();
      // A map from correlation id to RequestInfo. This is used to find timed out RequestInfos.
      Map<Integer, RequestInfo> correlationIdToRequestInfo = new LinkedHashMap<>();

      while (remoteReplicaGroups.size() > 0 && !remoteReplicaGroups.stream().allMatch(RemoteReplicaGroup::isDone)) {
        if (!running) {
          break;
        }

        List<RequestInfo> requestInfos =
            pollRemoteReplicaGroups(remoteReplicaGroups, correlationIdToRequestInfo, correlationIdToReplicaGroup);
        List<ResponseInfo> responseInfosForTimedOutRequests =
            filterTimedOutRequests(correlationIdToRequestInfo, correlationIdToReplicaGroup);
        Set<Integer> requestsToDrop = responseInfosForTimedOutRequests.stream()
            .map(r -> r.getRequestInfo().getRequest().getCorrelationId())
            .collect(Collectors.toSet());
        if (!requestsToDrop.isEmpty()) {
          replicationMetrics.incrementTimeoutRequestErrorCount(requestsToDrop.size(), replicatingFromRemoteColo,
              datacenterName);
        }
        final int pollTimeoutMs = (int) replicationConfig.replicationRequestNetworkPollTimeoutMs;
        List<ResponseInfo> responseInfos = networkClient.sendAndPoll(requestInfos, requestsToDrop, pollTimeoutMs);
        // Add response for dropped request because there is no guarantee that sendAndPoll would return a response for
        // dropped requests. Even if the network client returns response infos for dropped requests, onResponse should
        // still be able to handle duplicate response infos for the same request.
        responseInfos.addAll(responseInfosForTimedOutRequests);
        onResponses(responseInfos, correlationIdToRequestInfo, correlationIdToReplicaGroup);
        remoteReplicaGroups.stream()
            .filter(RemoteReplicaGroup::shouldSetFinishTime)
            .forEach(group -> group.setFinishTime(time.milliseconds()));
      }
      logger.trace("Thread name: {} Finish all RemoteReplicaGroup replication", threadName);
      remoteReplicaGroups.stream()
          .filter(g -> g.getExchangeMetadataResponseList() != null)
          .forEach(
              g -> exchangeMetadataResponsesInEachCycle.computeIfAbsent(g.getRemoteDataNode(), k -> new ArrayList<>())
                  .addAll(g.getExchangeMetadataResponseList()));

      // Print out the exceptions.
      remoteReplicaGroups.stream()
          .filter(g -> g.isDone() && g.getException() != null)
          .forEach(g -> logger.error("Remote node: {} Thread name: {} RemoteReplicaGroup {} has exception {}",
              g.getRemoteDataNode(), threadName, g.getRemoteReplicaInfos()
                  .stream()
                  .map(remoteReplicaInfo -> remoteReplicaInfo.toString() + ": localstore "
                      + remoteReplicaInfo.getLocalStore().toString())
                  .collect(Collectors.toList()), g.getException()));
    } catch (Throwable e) {
      logger.error("Thread name: {} found some error while replicating from remote hosts", threadName, e);
    } finally {
      long cycleEndTime = time.milliseconds();
      replicationMetrics.updateOneCycleReplicationTime(cycleEndTime - oneRoundStartTimeMs, replicatingFromRemoteColo,
          datacenterName);
      updatePerReplicationCycleTimeMetric(cycleEndTime - oneRoundStartTimeMs);
      updateReplicationCycleIterationsMetric();
      emitCyclicReplicationIdleMetrics(remoteReplicaGroups, oneRoundStartTimeMs, cycleEndTime);
    }
    maybeSleepAfterReplication(remoteReplicaGroups.isEmpty());
  }


  /**
   * We should call this method to convert store keys, This will convert the keys and add the keys which were
   * converted to the list.
   * @param storeKeysToConvert store keys to convert
   * @param storeKeysConversionLog list to store which keys were converted
   * @return Map
   * @throws Exception
   */
  Map<StoreKey, StoreKey> convertStoreKeys(Collection<StoreKey> storeKeysToConvert,
      List<StoreKey> storeKeysConversionLog) throws Exception {
    Map<StoreKey, StoreKey> conversionMap = this.storeKeyConverter.convert(storeKeysToConvert);
    storeKeysConversionLog.addAll(new ArrayList<>(conversionMap.keySet()));
    return conversionMap;
  }

  /**
   * Emits metrics for
   * 1. Cumulative time groups are idle in a cycle i.e groups are in DONE state but cycle continues
   * 2. Cumulative percentage of time per cycle groups are idle
   * @param groups list of RemoteReplicaGroups
   * @param cycleStartTime start time of replication cycle
   * @param cycleEndTime end time of replication cycle
   */
  private void emitCyclicReplicationIdleMetrics(List<RemoteReplicaGroup> groups, long cycleStartTime, long cycleEndTime) {
    long totalIdleTime = 0;
    for (RemoteReplicaGroup group : groups) {
      totalIdleTime = totalIdleTime + cycleEndTime - group.getFinishTime();
    }
    replicationMetrics.updateReplicaBatchTotalIdleTimeMs(totalIdleTime, replicatingFromRemoteColo, datacenterName);

    long cumulativeTime = groups.size() * (cycleEndTime - cycleStartTime);
    if (cumulativeTime > 0) {
      long idleTimePercentage = (totalIdleTime * 100) / cumulativeTime;
      replicationMetrics.updateReplicaBatchTotalIdleTimePercentage(idleTimePercentage, replicatingFromRemoteColo,
          datacenterName);
    }
  }

  void setExchangeMetadataListener(ExchangeMetadataListener exchangeMetadataListener) {
    this.exchangeMetadataListener = exchangeMetadataListener;
  }

  /**
   * A callback method to invoke after calling handleReplicaMetadataResponse method for given RemoteReplicaGroup.
   * Subclass can override this method to put more control to each group.
   * @param group
   */
  protected void afterHandleReplicaMetadataResponse(RemoteReplicaGroup group) {
    // noop
  }

  /**
   * A callback method to invoke after calling handleGetResponse method for given RemoteReplicaGroup.
   * Subclass can override this method to put more control to each group.
   * @param group
   */
  protected void afterHandleGetResponse(RemoteReplicaGroup group) {
    // noop
  }

  /**
   * Polling the request from each {@link RemoteReplicaGroup}. Adding new requests to the {@code correlationIdToRequestInfo}
   * map and the {@code correlationIdToRemoteReplicaGroup} map.
   * @param remoteReplicaGroups The list of the {@link RemoteReplicaGroup} to poll requests from.
   * @param correlationIdToRequestInfo The map from correlation id to request info, new request would be added to this map.
   * @param correlationIdToRemoteReplicaGroup The map from correlation id to remote replica group, new request would be
   *                                          added to this map.
   * @return A list of {@link RequestInfo}s.
   */
  private List<RequestInfo> pollRemoteReplicaGroups(List<RemoteReplicaGroup> remoteReplicaGroups,
      Map<Integer, RequestInfo> correlationIdToRequestInfo,
      Map<Integer, RemoteReplicaGroup> correlationIdToRemoteReplicaGroup) {
    logger.trace("Thread Name: {} Polling {} RemoteReplicaGroups for requests", threadName, remoteReplicaGroups.size());
    List<RequestInfo> requestInfos = new ArrayList<>();
    for (RemoteReplicaGroup group : remoteReplicaGroups) {
      if (group.isDone()) {
        logger.trace("Remote Node: {} Thread Name: {} RemoteReplicaGroup {} is Done", group.getRemoteDataNode(),
            threadName, group.getId());
        continue;
      }
      RequestInfo requestInfo = group.poll();
      if (requestInfo != null) {
        requestInfos.add(requestInfo);
        correlationIdToRemoteReplicaGroup.put(requestInfo.getRequest().getCorrelationId(), group);
        correlationIdToRequestInfo.put(requestInfo.getRequest().getCorrelationId(), requestInfo);
      }
    }
    logger.trace("Thread Name: {} There are {} requests polled from RemoteReplicaGroups", threadName,
        requestInfos.size());
    return requestInfos;
  }

  /**
   * Filter requests that are timed out and return corresponding response info for them. This method would also remove
   * the timed out request form the map.
   * @param correlationIdToRequest The map from correlation id to request.
   * @param correlationIdToRemoteReplicaGroup The map from correlation id to RemoteReplicaGroup
   * @return A list of {@link ResponseInfo} for timed out requests.
   */
  private List<ResponseInfo> filterTimedOutRequests(Map<Integer, RequestInfo> correlationIdToRequest,
      Map<Integer, RemoteReplicaGroup> correlationIdToRemoteReplicaGroup) {
    logger.trace("Thread Name: {} Trying to filter out timed out requests", threadName);
    List<ResponseInfo> responseInfosForTimedOutRequests = new ArrayList<>();
    Iterator<Map.Entry<Integer, RequestInfo>> inFlightRequestIterator = correlationIdToRequest.entrySet().iterator();
    while (inFlightRequestIterator.hasNext()) {
      Map.Entry<Integer, RequestInfo> entry = inFlightRequestIterator.next();
      RequestInfo requestInfo = entry.getValue();
      long currentTimeInMs = time.milliseconds();
      if (currentTimeInMs > requestInfo.getRequestCreateTime() + replicationConfig.replicationRequestNetworkTimeoutMs) {
        inFlightRequestIterator.remove();
        responseInfosForTimedOutRequests.add(new ResponseInfo(requestInfo, NetworkClientErrorCode.TimeoutError, null));
        RemoteReplicaGroup group = correlationIdToRemoteReplicaGroup.get(entry.getKey());
        if (group != null) {
          logger.error(
              "Remote node: {} Thread name: {} RemoteReplicaGroup {} Request {} timed out, it was issued at {}",
              group.getRemoteDataNode(), threadName, group.getId(), entry.getKey(), requestInfo.getRequestCreateTime());
        } else {
          // This shouldn't happen
          logger.error("Thread name: {} Request {} timed out", threadName, entry.getKey());
        }
      } else {
        // The correlationIdToRequest should be a LinkedHashMap that has a predictable iteration order based on insertion.
        // The correlation id increases as we insert it to the map. So if current request is not timed out, then all the
        // requests after are not timed out.
        break;
      }
    }
    if (!responseInfosForTimedOutRequests.isEmpty()) {
      logger.error("Thread Name: {} There are {} requests timed out", threadName,
          responseInfosForTimedOutRequests.size());
    }
    return responseInfosForTimedOutRequests;
  }

  /**
   * Handle the response from the nonblocking network client.
   * @param responseInfos The list of {@link ResponseInfo}s.
   * @param correlationIdToRequest The map from correlation id to request.
   * @param correlationIdToReplicaGroup The map from correlation id to remote replica group.
   */
  private void onResponses(List<ResponseInfo> responseInfos, Map<Integer, RequestInfo> correlationIdToRequest,
      Map<Integer, RemoteReplicaGroup> correlationIdToReplicaGroup) {
    logger.trace("Thread Name: {} There are {} Responses to handle", threadName, responseInfos.size());
    for (ResponseInfo responseInfo : responseInfos) {
      RequestInfo requestInfo = responseInfo.getRequestInfo();
      if (requestInfo == null) {
        // Only possible when warming up the network client
        DataNodeId dataNodeId = responseInfo.getDataNode();
        responseHandler.onConnectionTimeout(dataNodeId);
      } else {
        int correlationId = requestInfo.getRequest().getCorrelationId();
        // Request comes back, remove this request from the map
        correlationIdToRequest.remove(correlationId);
        RemoteReplicaGroup remoteReplicaGroup = correlationIdToReplicaGroup.remove(correlationId);
        // This correlation id might be removed because the corresponding request timed out. But later we get the response
        // back from the network client.
        if (remoteReplicaGroup != null) {
          RequestOrResponseType type = ((RequestOrResponse) requestInfo.getRequest()).getRequestType();
          logger.debug("Remote node: {} Thread name: {} RemoteReplicaGroup {} Handling response of type {} for {}",
              remoteReplicaGroup.getRemoteDataNode(), threadName, remoteReplicaGroup.getId(), type, correlationId);
          switch (type) {
            case ReplicaMetadataRequest:
              remoteReplicaGroup.handleReplicaMetadataResponse(responseInfo);
              afterHandleReplicaMetadataResponse(remoteReplicaGroup);
              break;
            case GetRequest:
              remoteReplicaGroup.handleGetResponse(responseInfo);
              afterHandleGetResponse(remoteReplicaGroup);
              break;
            default:
              logger.error(
                  "Remote node: {} Thread name: {} RemoteReplicaGroup {} Unexpected type: {} received, discarding",
                  remoteReplicaGroup.getRemoteDataNode(), threadName, remoteReplicaGroup.getId(), type);
          }
        } else {
          logger.trace("Thread Name: {} No RemoteReplicaGroup found for correlation Id {}, it might already time out",
              threadName, correlationId);
        }
      }
    }
    // Release all the ResponseInfo
    responseInfos.forEach(ResponseInfo::release);
  }

  /**
   * Maybe sleep for a while after one round of replication. If all the replicas are caught up and the configuration
   * shows we should sleep, then sleep for a while so we can save some CPU.
   * @param allCaughtUp True when all replicas are caught up.
   */
  private void maybeSleepAfterReplication(boolean allCaughtUp) {
    long sleepDurationMs = 0;
    if (allCaughtUp && replicationConfig.replicationReplicaThreadIdleSleepDurationMs > 0) {
      sleepDurationMs = replicationConfig.replicationReplicaThreadIdleSleepDurationMs;
      idleCount.inc();
    } else if (threadThrottleDurationMs > 0) {
      sleepDurationMs = threadThrottleDurationMs;
      throttleCount.inc();
    }

    if (running && sleepDurationMs > 0) {
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
   * performs below checks and gives status of replica
   *
   * 1. check if replica is OFFLINE then return OFFLINE status
   *
   * 2. check these conditions
   *    1. check if we are doing cross colo replication
   *    2. check if leader based replication is enabled
   *    3. our local replica is not leader
   *    4. we have missing blobs from last ReplicaMetadataRequest
   *    return STANDBY_TIMED_OUT_ON_NO_PROGRESS if last missing blob arrived  before replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds
   *    return STANDBY otherwise as we need to wait for data to come through intra colo replication
   *
   * 3. return ACTIVE status if above criteria are not met
   *
   *  We can pull data only from ACTIVE replicas and STANDBY_TIMED_OUT_ON_NO_PROGRESS replicas
   */
  ReplicaStatus getReplicaStatus(RemoteReplicaInfo remoteReplicaInfo) {
    ReplicaId replicaId = remoteReplicaInfo.getReplicaId();
    boolean inBackoff = time.milliseconds() < remoteReplicaInfo.getReEnableReplicationTime();
    if (replicaId.isDown() || inBackoff || remoteReplicaInfo.getLocalStore().getCurrentState() == ReplicaState.OFFLINE
        || replicationDisabledPartitions.contains(replicaId.getPartitionId())) {

      if (replicationConfig.replicationEnableInfoLogsForSkippedPartitions) {
        logger.info(
            "Skipping replication on replica {} because one of following conditions is true: remote replica is down "
                + "= {}; in backoff = {}; local store is offline = {}; replication is disabled = {}.",
            replicaId.getPartitionId().toPathString(), replicaId.isDown(), inBackoff,
            remoteReplicaInfo.getLocalStore().getCurrentState() == ReplicaState.OFFLINE,
            replicationDisabledPartitions.contains(replicaId.getPartitionId()));
      } else {
        logger.trace(
            "Skipping replication on replica {} because one of following conditions is true: remote replica is down "
                + "= {}; in backoff = {}; local store is offline = {}; replication is disabled = {}.",
            replicaId.getPartitionId().toPathString(), replicaId.isDown(), inBackoff,
            remoteReplicaInfo.getLocalStore().getCurrentState() == ReplicaState.OFFLINE,
            replicationDisabledPartitions.contains(replicaId.getPartitionId()));
      }

      return ReplicaStatus.OFFLINE;
    }

    if (replicatingFromRemoteColo && leaderBasedReplicationAdmin != null) {
      // check if all missing keys for standby replicas from previous replication cycle are now obtained
      // via leader replica. If we still have missing keys, don't include them in current replication cycle
      // to avoid sending duplicate metadata requests since their token wouldn't have advanced.
      processMissingKeysFromPreviousMetadataResponse(remoteReplicaInfo);
      if (containsMissingKeysFromPreviousMetadataExchange(remoteReplicaInfo)) {

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

        if (replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds != -1) {
          ReplicaId localReplica = remoteReplicaInfo.getLocalReplicaId();
          ReplicaId remoteReplica = remoteReplicaInfo.getReplicaId();
          ExchangeMetadataResponse exchangeMetadataResponse = remoteReplicaInfo.getExchangeMetadataResponse();
          if (!leaderBasedReplicationAdmin.isLeaderPair(localReplica, remoteReplica)
              && exchangeMetadataResponse.hasMissingStoreMessages()
              && (time.seconds() - exchangeMetadataResponse.lastMissingMessageReceivedTimeSec)
              > replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds) {
            return ReplicaStatus.STANDBY_TIMED_OUT_ON_NO_PROGRESS;
          }
        }
        return ReplicaStatus.STANDBY;
      }
    }
    return ReplicaStatus.ACTIVE;
  }

  /**
   * Filter the remote replicas in the list of {@link RemoteReplicaInfo}s. It will filter out the replicas that are down
   * or in backoff state or is disabled. It will also filter out replicas that tries to replicate from remote datacenter
   * but still have missing blobs from last ReplicaMetadataRequest when the leader based replication is enabled.
   * @param replicasToReplicatePerNode All the replicas.
   * @param activeReplicasPerNode A list to store all the active replicas after filtering.
   * @param standbyReplicasTimedOutOnNoProgress A list to store all the standby replicas that still have missing blobs from
   *                                      previous ReplicaMetadataRequest and are timed out
   */
  void filterRemoteReplicasToReplicate(List<RemoteReplicaInfo> replicasToReplicatePerNode,
      List<RemoteReplicaInfo> activeReplicasPerNode, List<RemoteReplicaInfo> standbyReplicasTimedOutOnNoProgress) {
    // Get a list of active and standBy replicas that have timed out that needs be included for this replication cycle
    for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
      ReplicaStatus replicaStatus = getReplicaStatus(remoteReplicaInfo);
      switch (replicaStatus){
        case OFFLINE:
        case STANDBY:
          break;
        case ACTIVE:
          activeReplicasPerNode.add(remoteReplicaInfo);
          break;
        case STANDBY_TIMED_OUT_ON_NO_PROGRESS:
          standbyReplicasTimedOutOnNoProgress.add(remoteReplicaInfo);
          break;
      }
    }
  }

  /**
   * Handle {@link ReplicaMetadataResponse} returned from the given remote {@link DataNodeId}. It will find all the missing
   * keys in the local blobstore that exist in the remote data node. And for those keys that exist in both blob stores, it
   * will apply update to local blob stores if needed.
   * @param response The {@link ReplicaMetadataResponse}.
   * @param replicasToReplicatePerNode
   * @param remoteNode The remote {@link DataNodeId}.
   * @param storeKeysConversionLog list to store storeKeys that were converted
   * @return A list of {@link ExchangeMetadataResponse}.
   * @throws IOException
   */
  List<ExchangeMetadataResponse> handleReplicaMetadataResponse(ReplicaMetadataResponse response,
      List<RemoteReplicaInfo> replicasToReplicatePerNode, DataNodeId remoteNode, List<StoreKey> storeKeysConversionLog) throws Exception {
    long startTimeInMs = time.milliseconds();
    List<ExchangeMetadataResponse> exchangeMetadataResponseList = new ArrayList<>();
    Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap = batchConvertReplicaMetadataResponseKeys(response, storeKeysConversionLog);

    for (int i = 0; i < response.getReplicaMetadataResponseInfoList().size(); i++) {
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo = response.getReplicaMetadataResponseInfoList().get(i);
      responseHandler.onEvent(remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
      if (replicaMetadataResponseInfo.getError() == ServerErrorCode.NoError) {
        // Skip stores that were stopped during call to getReplicaMetadataResponse
        if (!remoteReplicaInfo.getLocalStore().isStarted()) {
          exchangeMetadataResponseList.add(new ExchangeMetadataResponse(ServerErrorCode.TemporarilyDisabled));
        } else {
          try {
            logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token from remote: {} Replica lag: {} ",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getFindToken(),
                replicaMetadataResponseInfo.getRemoteReplicaLagInBytes());
            Set<MessageInfo> remoteMissingStoreMessages =
                getMissingStoreMessages(replicaMetadataResponseInfo, remoteNode, remoteReplicaInfo);
            processReplicaMetadataResponse(remoteMissingStoreMessages, replicaMetadataResponseInfo, remoteReplicaInfo,
                remoteNode, remoteKeyToLocalKeyMap);

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
              boolean isSyncCompleted =
                  replicaSyncUpManager.updateReplicaLagAndCheckSyncStatus(localReplica, remoteReplica,
                      exchangeMetadataResponse.localLagFromRemoteInBytes, ReplicaState.STANDBY);
              // if catchup is completed by this update call, we can complete bootstrap in local store
              if (isSyncCompleted) {
                // complete BOOTSTRAP -> STANDBY transition
                remoteReplicaInfo.getLocalStore().setCurrentState(ReplicaState.STANDBY);
                remoteReplicaInfo.getLocalStore().completeBootstrap();
              }
            }

            // If remote token has not moved forward, wait for back off time before resending next metadata request
            if (remoteReplicaInfo.getToken().equals(exchangeMetadataResponse.remoteToken)) {
              remoteReplicaInfo.setReEnableReplicationTime(
                  time.milliseconds() + replicationConfig.replicationSyncedReplicaBackoffDurationMs);
              syncedBackOffCount.inc();
            }

            // trace replication status to track progress of recovery from cloud
            logReplicationStatus(remoteReplicaInfo, exchangeMetadataResponse);

            // There are no missing keys. We just advance the token
            if (exchangeMetadataResponse.missingStoreMessages.size() == 0) {
              advanceToken(remoteReplicaInfo, exchangeMetadataResponse);
            }

            replicationMetrics.updateLagMetricForRemoteReplica(remoteReplicaInfo,
                exchangeMetadataResponse.localLagFromRemoteInBytes);

            // Add exchangeMetadataResponse to list at the end after operations such as replicaSyncUpManager(if not null)
            // has completed update, etc. The reason is we may get exceptions in between (for ex: replicaSyncUpManager may
            // throw exception) and end up adding one more exchangeMetadataResponse associated with same RemoteReplicaInfo.
            exchangeMetadataResponseList.add(exchangeMetadataResponse);
          } catch (Exception e) {
            if (e instanceof StoreException && ((StoreException) e).getErrorCode() == StoreErrorCodes.StoreNotStarted) {
              // Must have just been stopped, just skip it and move on.
              logger.info("Local store not started for remote replica: {}", remoteReplicaInfo.getReplicaId());
              exchangeMetadataResponseList.add(new ExchangeMetadataResponse(ServerErrorCode.TemporarilyDisabled));
            } else {
              logger.error("Remote node: {} Thread name: {} Remote replica: {}", remoteNode, threadName,
                  remoteReplicaInfo.getReplicaId(), e);
              replicationMetrics.updateLocalStoreError(remoteReplicaInfo.getReplicaId());
              responseHandler.onEvent(remoteReplicaInfo.getReplicaId(), e);
              exchangeMetadataResponseList.add(new ExchangeMetadataResponse(ServerErrorCode.UnknownError));
            }
          }
        }
      } else {
        replicationMetrics.incrementReplicationErrorCount(replicatingFromRemoteColo, datacenterName);
        replicationMetrics.updateMetadataRequestError(remoteReplicaInfo.getReplicaId());
        logger.error("Remote node: {} Thread name: {} Remote replica: {} Server metadata-response error: {}",
            remoteNode, threadName, remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
        exchangeMetadataResponseList.add(new ExchangeMetadataResponse(replicaMetadataResponseInfo.getError()));
      }

      if (replicatingFromRemoteColo && leaderBasedReplicationAdmin != null) {
        ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
        if (exchangeMetadataResponse.serverErrorCode.equals(ServerErrorCode.NoError)) {

          // If leader-based replication is enabled, store the metadata exchange received for the remote replica as
          // standby replicas will not send GET request for the missing store keys and track them from leader <->
          // leader exchanges and intra-dc replication.
          remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(exchangeMetadataResponse));

          // It is possible that some missing keys found in exchange metadata response are written in parallel
          // by other replica threads since the time we calculated it. Go through the local store once more and
          // update missing keys set stored in the exchangeMetadataResponse for the remote replica.
          refreshMissingStoreMessagesForStandbyReplica(remoteReplicaInfo);
        }
      }
    }
    long processMetadataResponseTimeInMs = time.milliseconds() - startTimeInMs;
    logger.trace("Remote node: {} Thread name: {} processMetadataResponseTime: {}", remoteNode, threadName,
        processMetadataResponseTimeInMs);
    return exchangeMetadataResponseList;
  }

  /**
   * Returns local replica mount path of the partition
   * @param remoteReplicaInfo Info about remote replica
   * @return Local replica mount path of the partition
   */
  protected String getLocalReplicaPath(RemoteReplicaInfo remoteReplicaInfo) {
    return remoteReplicaInfo.getLocalReplicaId().getReplicaPath();
  }

  /**
   * Create a {@link ReplicaMetadataRequest} for given list of replicas in the {@code replicasToReplicaPerNode}.
   * @param replicasToReplicatePerNode The list of {@link RemoteReplicaInfo} that contains the remote replica ids and their tokens.
   * @param remoteNode The remote {@link DataNodeId}.
   * @return A {@link ReplicaMetadataRequest} to send out later.
   */
  ReplicaMetadataRequest createReplicaMetadataRequest(List<RemoteReplicaInfo> replicasToReplicatePerNode,
      DataNodeId remoteNode) {
    boolean allLocalStoreInBootstrap = true;
    List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList = new ArrayList<>();
    for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
      ReplicaMetadataRequestInfo replicaMetadataRequestInfo =
          new ReplicaMetadataRequestInfo(remoteReplicaInfo.getReplicaId().getPartitionId(),
              remoteReplicaInfo.getToken(), dataNodeId.getHostname(), getLocalReplicaPath(remoteReplicaInfo),
              remoteReplicaInfo.getReplicaId().getReplicaType(), replicationConfig.replicaMetadataRequestVersion);
      replicaMetadataRequestInfoList.add(replicaMetadataRequestInfo);
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token going to be sent to remote: {} ",
          remoteNode, threadName, remoteReplicaInfo.getReplicaId(), remoteReplicaInfo.getToken());
      if (remoteReplicaInfo.getLocalStore().getCurrentState() != ReplicaState.BOOTSTRAP) {
        allLocalStoreInBootstrap = false;
      }
    }
    long fetchSize = replicationConfig.replicationFetchSizeInBytes;
    if (allLocalStoreInBootstrap && !replicatingFromRemoteColo) {
      fetchSize = replicationConfig.replicationFetchSizeInBytesForBootstrapIntraColo;
      logger.trace(
          "All local stores are at bootstrap mode, and this is intro colo replication, set the fetch size to {}",
          fetchSize);
    }
    return new ReplicaMetadataRequest(correlationIdGenerator.incrementAndGet(),
        "replication-metadata-" + dataNodeId.getHostname() + "[" + dataNodeId.getDatacenterName() + "]",
        replicaMetadataRequestInfoList, fetchSize, replicationConfig.replicaMetadataRequestVersion);
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
    ChannelOutput channelOutput = null;
    try {
      ReplicaMetadataRequest request = createReplicaMetadataRequest(replicasToReplicatePerNode, remoteNode);
      channelOutput = connectedChannel.sendAndReceive(request);
      if (logger.isTraceEnabled()) {
        logger.trace("Remote node: {} Thread name: {} Remote replicas: {} Stream size after deserialization: {} ",
            remoteNode, threadName, replicasToReplicatePerNode, channelOutput.getInputStream().available());
      }
      ReplicaMetadataResponse response =
          ReplicaMetadataResponse.readFrom(channelOutput.getInputStream(), findTokenHelper, clusterMap);

      long metadataRequestTime = time.milliseconds() - replicaMetadataRequestStartTime;
      replicationMetrics.updateMetadataRequestTime(metadataRequestTime, replicatingFromRemoteColo, replicatingOverSsl,
          datacenterName);

      if (response.getError() != ServerErrorCode.NoError
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
    } catch (Exception e) {
      responseHandler.onEvent(replicasToReplicatePerNode.get(0).getReplicaId(), e);
      throw e;
    } finally {
      if (channelOutput != null && (channelOutput.getInputStream() instanceof NettyByteBufDataInputStream)) {
        // Release buffer if and only if the inputStream is NettyByteBuf based.
        ((NettyByteBufDataInputStream) channelOutput.getInputStream()).getBuffer().release();
      }
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
        String errorMessage = String.format(
            "Blob id %s is not in the expected partition Actual partition %s " + " Expected partition %s"
                + "Replica %s", blobId.getID(), blobId.getPartition(),
            remoteReplicaInfo.getLocalReplicaId().getPartitionId(), remoteReplicaInfo.getReplicaId());
        replicationMetrics.unexpectedBlobIdError.inc();
        if (replicationConfig.replicationIgnoreUnexpectedBlobIdError) {
          logger.error(errorMessage);
          missingRemoteStoreMessages.remove(messageInfo);
          continue;
        } else {
          throw new IllegalStateException(errorMessage);
        }
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
    if (localLifeVersion == remoteLifeVersion) {
      // we are operating in the same version, in this case, delete would be the final state.
      if (!deletedLocally) {
        // Only adds record when it's not deleted yet. Since delete is the final state for this lifeVersion, if there
        // is a delete record for the current lifeVersion, then nothing needs to be done.
        MessageInfo info = new MessageInfo.Builder(messageInfo).storeKey(localKey)
            .size(0)
            .accountId(localKey.getAccountId())
            .containerId(localKey.getContainerId())
            .build();
        if (messageInfo.isTtlUpdated() && !ttlUpdatedLocally) {
          applyTtlUpdate(info, remoteReplicaInfo);
        }
        if (messageInfo.isDeleted()) {
          applyDelete(info, remoteReplicaInfo);
        }
      }
    } else if (localLifeVersion < remoteLifeVersion) {
      // If we are here, then the remote lifeVersion is greater than the local lifeVersion.
      // If the local lifeVersion is greater than the remote lifeVersion, then there is nothing to be done.
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
   * @param storeKeysConversionLog list to store storeKeys that were converted
   * @return the map from the {@link StoreKeyConverter#convert(Collection)} call
   * @throws IOException thrown if {@link StoreKeyConverter#convert(Collection)} fails
   */
  Map<StoreKey, StoreKey> batchConvertReplicaMetadataResponseKeys(ReplicaMetadataResponse response, List<StoreKey> storeKeysConversionLog) throws Exception {
    List<StoreKey> storeKeysToConvert = new ArrayList<>();
    for (ReplicaMetadataResponseInfo replicaMetadataResponseInfo : response.getReplicaMetadataResponseInfoList()) {
      if ((replicaMetadataResponseInfo.getError() == ServerErrorCode.NoError) && (
          replicaMetadataResponseInfo.getMessageInfoList() != null)) {
        for (MessageInfo messageInfo : replicaMetadataResponseInfo.getMessageInfoList()) {
          storeKeysToConvert.add(messageInfo.getStoreKey());
        }
      }
    }
    return convertStoreKeys(storeKeysToConvert, storeKeysConversionLog);
  }

  /**
   * Create a {@link GetRequest} for the given replica ids in the list of {@link RemoteReplicaInfo}.
   * @param replicasToReplicatePerNode The list of {@link RemoteReplicaInfo} that contains the remote replica ids and their tokens.
   * @param exchangeMetadataResponseList The list of {@link ExchangeMetadataResponse} that contains missing keys for each partitions.
   * @param remoteNode The remote {@link DataNodeId}.
   * @return A {@link GetRequest} that contains all the missing keys.
   */
  GetRequest createGetRequest(List<RemoteReplicaInfo> replicasToReplicatePerNode,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList, DataNodeId remoteNode) {
    List<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.NoError) {
        Set<StoreKey> missingStoreKeys = exchangeMetadataResponse.getMissingStoreKeys();
        if (missingStoreKeys.size() > 0) {
          if (remoteNode instanceof CloudDataNode && logger.isTraceEnabled()) {
            logger.trace("Replicating blobs from CloudDataNode: {}", missingStoreKeys);
          }
          ArrayList<BlobId> keysToFetch = new ArrayList<>();
          for (StoreKey storeKey : missingStoreKeys) {
            keysToFetch.add((BlobId) storeKey);
          }
          PartitionRequestInfo partitionRequestInfo =
              new PartitionRequestInfo(remoteReplicaInfo.getReplicaId().getPartitionId(), keysToFetch);
          partitionRequestInfoList.add(partitionRequestInfo);
        }
      }
    }
    if (!partitionRequestInfoList.isEmpty()) {
      return new GetRequest(correlationIdGenerator.incrementAndGet(),
          GetRequest.Replication_Client_Id_Prefix + dataNodeId.getHostname() + "[" + dataNodeId.getDatacenterName()
              + "]", MessageFormatFlags.All, partitionRequestInfoList,
          replicationConfig.replicationIncludeAll ? GetOption.Include_All : GetOption.None);
    }
    return null;
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
    GetRequest getRequest = createGetRequest(replicasToReplicatePerNode, exchangeMetadataResponseList, remoteNode);
    if (getRequest == null) {
      return null;
    }
    GetResponse getResponse;
    long startTime = time.milliseconds();
    try {
      ChannelOutput channelOutput = connectedChannel.sendAndReceive(getRequest);
      getResponse = GetResponse.readFrom(channelOutput.getInputStream(), clusterMap);
      long getRequestTime = time.milliseconds() - startTime;
      replicationMetrics.updateGetRequestTime(getRequestTime, replicatingFromRemoteColo, replicatingOverSsl,
          datacenterName, remoteColoGetRequestForStandby);
      if (getResponse.getError() != ServerErrorCode.NoError) {
        logger.error("Remote node: {} Thread name: {} Remote replicas: {} GetResponse from replication: {}", remoteNode,
            threadName, replicasToReplicatePerNode, getResponse.getError());
        throw new ReplicationException(
            " Get Request returned error when trying to get missing keys " + getResponse.getError());
      }
    } catch (IOException e) {
      responseHandler.onEvent(replicasToReplicatePerNode.get(0).getReplicaId(), e);
      throw e;
    }
    return getResponse;
  }

  /**
   * Handle the {@link GetResponse} from the remote data node, it will write the messages (if any) to the local stores
   * from the remote stores for the missing keys, and advances tokens.
   * @param getResponse The {@link GetResponse} that contains the missing messages. This may be null if there are no
   *                    missing messages to write as per the exchange metadata response. In that case this method will
   *                    simply advance the tokens for every store.
   * @param replicasToReplicatePerNode The list of remote replicas for the remote node
   * @param exchangeMetadataResponseList The list of metadata response from the remote node
   * @param remoteNode The remote node from which replication needs to happen
   * @param remoteColoGetRequestForStandby boolean which indicates if we are getting missing keys for standby or
   *                                       non-leader replica pairs during leader-based replication.
   * @throws IOException
   */
  private void handleGetResponse(GetResponse getResponse, List<RemoteReplicaInfo> replicasToReplicatePerNode,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList, DataNodeId remoteNode,
      boolean remoteColoGetRequestForStandby) throws IOException {
    int partitionResponseInfoIndex = 0;
    long totalBytesFixed = 0;
    long totalBlobsFixed = 0;
    long startTime = time.milliseconds();
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      // TODO: if remoteReplicaInfo.getLocalStore() is closed, write will fail
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.NoError) {
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
          if (partitionResponseInfo.getErrorCode() == ServerErrorCode.NoError) {
            List<MessageInfo> messageInfoList = partitionResponseInfo.getMessageInfoList();
            try {
              if (logger.isTraceEnabled()) {
                logger.trace("Remote node: {} Thread name: {} Remote replica: {} Messages to fix: {} "
                        + "Partition: {} Local mount path: {}", remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                    exchangeMetadataResponse.getMissingStoreKeys(), remoteReplicaInfo.getReplicaId().getPartitionId(),
                    remoteReplicaInfo.getLocalReplicaId().getMountPath());
              }

              List<Transformer> transformers = transformer == null ? Collections.emptyList()
                  : Collections.singletonList(transformer);
              MessageSievingInputStream validMessageDetectionInputStream =
                  new MessageSievingInputStream(getResponse.getInputStream(), messageInfoList,
                      transformers, metricRegistry);
              String replicaName = String.format("%s:%s", remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname(),
                  remoteReplicaInfo.getReplicaId().getReplicaPath());
              validMessageDetectionInputStream.setReplicaLocation(replicaName);
              if (validMessageDetectionInputStream.hasInvalidMessages()) {
                replicationMetrics.incrementInvalidMessageError(partitionResponseInfo.getPartition());
                logger.error("Out of {} messages, {} invalid messages were found in message stream from {}",
                    messageInfoList.size(),
                    messageInfoList.size() - validMessageDetectionInputStream.getValidMessageInfoList().size(),
                    remoteReplicaInfo.getReplicaId());
              }
              messageInfoList = validMessageDetectionInputStream.getValidMessageInfoList();
              applyPut(validMessageDetectionInputStream, remoteReplicaInfo);

              for (MessageInfo messageInfo : messageInfoList) {
                totalBytesFixed += messageInfo.getSize();

                replicationMetrics.updateReplicationFetchBytes(remoteReplicaInfo, messageInfo.getSize());
                if (logger.isTraceEnabled()) {
                  logger.trace("Remote node: {} Thread name: {} Remote replica: {} Message replicated: {} Partition: {} "
                          + "Local mount path: {} Message size: {}", remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                      messageInfo.getStoreKey(), remoteReplicaInfo.getReplicaId().getPartitionId(),
                      remoteReplicaInfo.getLocalReplicaId().getMountPath(), messageInfo.getSize());
                }
                if (notification != null) {
                  notification.onBlobReplicaCreated(dataNodeId.getHostname(), dataNodeId.getPort(),
                      messageInfo.getStoreKey().getID(), BlobReplicaSourceType.REPAIRED);
                }
                // messageInfo is the Blob final state and the operation time is the blob creation time.
                // So the applyTtlUpdate will use the creation time as the Ttl operation time.
                if (isTtlUpdateNeededAfterPut(messageInfo)) {
                  applyTtlUpdate(messageInfo, remoteReplicaInfo);
                }
                StoreKey key = messageInfo.getStoreKey();
                if (key instanceof BlobId) {
                  BlobId blobId = (BlobId) key;
                  if (logger.isTraceEnabled()) {
                    long timeDifferenceInSeconds =
                        (time.milliseconds() - messageInfo.getOperationTimeMs()) / Time.MsPerSec;
                    logger.trace("Remote node: {} Thread name: {} Remote replica: {} Blob replicated: {} Partition: {} "
                            + "Local mount path: {} Blob creation time: {} now: {} time difference in seconds {}",
                        remoteNode, threadName, remoteReplicaInfo.getReplicaId(), blobId,
                        remoteReplicaInfo.getReplicaId().getPartitionId(),
                        remoteReplicaInfo.getLocalReplicaId().getMountPath(), messageInfo.getOperationTimeMs(),
                        time.milliseconds(), timeDifferenceInSeconds);
                  }
                  // Replication Lag should be recorded against the originating datacenter
                  replicationMetrics.updateReplicationLagInSecondsForBlob(
                      clusterMap.getDatacenterName(blobId.getDatacenterId()),
                      (time.milliseconds() - messageInfo.getOperationTimeMs()) / Time.MsPerSec);
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

              if (validMessageDetectionInputStream.hasInvalidMessages()
                  && !remoteReplicaInfo.isReplicationRetryCountMaxed()) {
                // If we have a few retries left, then increment the retry count and retry replication
                long retryCount = remoteReplicaInfo.incReplicationRetryCount();
                logger.error(
                    "Retrying replication due to {} corrupted messages. thread={}, replicationRetryCount={}, token={}, lagInBytes={}, partition={}, replicaId={}",
                    validMessageDetectionInputStream.getNumInvalidMessages(), threadName, retryCount,
                    remoteReplicaInfo.getToken(), remoteReplicaInfo.getLocalLagFromRemoteInBytes(),
                    remoteReplicaInfo.getReplicaId().getPartitionId().toString(), remoteReplicaInfo.getReplicaId());
              } else {
                if (validMessageDetectionInputStream.hasInvalidMessages()) {
                  replicationMetrics.replicationNumBlobsSkippedAfterRetry.inc(
                      validMessageDetectionInputStream.getNumInvalidMessages());
                  logger.error(
                      "Unable to replicate {} corrupted messages. thread={}, replicationRetryCount={}, token={}, lagInBytes={}, partition={}, replicaId={}",
                      validMessageDetectionInputStream.getNumInvalidMessages(), threadName,
                      remoteReplicaInfo.getReplicationRetryCount(), remoteReplicaInfo.getToken(),
                      remoteReplicaInfo.getLocalLagFromRemoteInBytes(),
                      remoteReplicaInfo.getReplicaId().getPartitionId().toString(), remoteReplicaInfo.getReplicaId());
                  logToExternalTable(validMessageDetectionInputStream.getInvalidMessages(), remoteReplicaInfo);
                }
                // If there are no invalid messages, or we have maxed out the retries, then advance token
                advanceToken(remoteReplicaInfo, exchangeMetadataResponse);
              }

              logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token after speaking to remote node: {}",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), exchangeMetadataResponse.remoteToken);
            } catch (StoreException e) {
              if (e.getErrorCode() != StoreErrorCodes.AlreadyExist) {
                replicationMetrics.updateLocalStoreError(remoteReplicaInfo.getReplicaId());
                logger.error("Remote node: {} Thread name: {} Remote replica: {}", remoteNode, threadName,
                    remoteReplicaInfo.getReplicaId(), e);
              }
            }
          } else if (partitionResponseInfo.getErrorCode() == ServerErrorCode.BlobDeleted) {
            replicationMetrics.blobDeletedOnGetCount.inc();
            logger.trace("One of the blobs to GET is deleted: Remote node: {} Thread name: {} Remote replica: {}",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId());
          } else if (partitionResponseInfo.getErrorCode() == ServerErrorCode.BlobAuthorizationFailure) {
            replicationMetrics.blobAuthorizationFailureCount.inc();
            logger.error(
                "One of the blobs authorization failed: Remote node: {} Thread name: {} Remote replica: {} Keys are: {}",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                exchangeMetadataResponse.getMissingStoreKeys());
          } else {
            replicationMetrics.incrementReplicationErrorCount(replicatingFromRemoteColo, datacenterName);
            replicationMetrics.updateGetRequestError(remoteReplicaInfo.getReplicaId());
            logger.error("Remote node: {} Thread name: {} Remote replica: {} Server get-response error: {}",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId(), partitionResponseInfo.getErrorCode());
          }
        }
      }
    }
    long batchStoreWriteTime = time.milliseconds() - startTime;
    replicationMetrics.updateBatchStoreWriteTime(batchStoreWriteTime, totalBytesFixed, totalBlobsFixed,
        replicatingFromRemoteColo, replicatingOverSsl, datacenterName, remoteColoGetRequestForStandby);
  }

  /**
   * Advances local token to make progress on replication
   * @param remoteReplicaInfo Remote replica info object
   * @param exchangeMetadataResponse Metadata object exchanged between replicas
   */
  protected void advanceToken(RemoteReplicaInfo remoteReplicaInfo, ExchangeMetadataResponse exchangeMetadataResponse) {
    remoteReplicaInfo.resetReplicationRetryCount();
    remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
    remoteReplicaInfo.setLocalLagFromRemoteInBytes(exchangeMetadataResponse.localLagFromRemoteInBytes);
    // reset stored metadata response for this replica
    remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(ServerErrorCode.NoError));
    if (logger.isTraceEnabled()) {
      logger.trace("Thread name: {}, updating token {} and lag {} for partition {} for Remote replica {}", threadName,
          remoteReplicaInfo.getToken(), remoteReplicaInfo.getLocalLagFromRemoteInBytes(),
          remoteReplicaInfo.getReplicaId().getPartitionId().toString(), remoteReplicaInfo.getReplicaId());
    }
  }

  /**
   * Logs a record to any external store.
   * @param messageInfoList List of replicated messages
   * @param remoteReplicaInfo Remote host info
   */
  protected void logToExternalTable(List<MessageInfo> messageInfoList, RemoteReplicaInfo remoteReplicaInfo) {
    // no-op. Inheritor can override.
  }

  /**
   * Returns true if TTL must be updated, else false.
   * For server-server replication, it doesn't really matter as disk updates are cheap.
   * For server-azure replication, it matters as an update to Azure is at least 2 network calls.
   * @param messageInfo
   * @return
   */
  protected boolean isTtlUpdateNeededAfterPut(MessageInfo messageInfo) {
    return messageInfo.isTtlUpdated();
  }

  /**
   * Applies PUT to local store and creates the blob locally
   * @param validMessageDetectionInputStream Stream of valid blob IDs
   * @param remoteReplicaInfo Info about remote replica from which we are replicating
   * @throws StoreException
   * @throws IOException
   */
  protected void applyPut(MessageSievingInputStream validMessageDetectionInputStream,
      RemoteReplicaInfo remoteReplicaInfo) throws StoreException, IOException {
    List<MessageInfo> messageInfoList = validMessageDetectionInputStream.getValidMessageInfoList();
    if (messageInfoList.size() == 0) {
      logger.debug("MessageInfoList is of size 0 as all messages are invalidated, deprecated, deleted or expired.");
    } else {
      MessageFormatWriteSet writeSet =
          new MessageFormatWriteSet(validMessageDetectionInputStream, messageInfoList, false);
      remoteReplicaInfo.getLocalStore().put(writeSet);
    }
  }

  /**
   * Applies a TTL update to the blob described by {@code messageInfo}.
   * @param messageInfo the {@link MessageInfo} that will be transformed into a TTL update
   * @param remoteReplicaInfo The remote replica that is bein1g replicated from
   * @throws StoreException
   */
  protected void applyTtlUpdate(MessageInfo messageInfo, RemoteReplicaInfo remoteReplicaInfo) throws StoreException {
    DataNodeId remoteNode = remoteReplicaInfo.getReplicaId().getDataNodeId();
    try {
      // NOTE: It is possible that the key in question may have expired and this TTL update is being applied after it
      // is deemed expired. The store will accept the op (BlobStore looks at whether the op was valid to do at the time
      // of the op, not current time) but if compaction is running at the same time and has decided to clean up the
      // record before this ttl update was applied (and this didn't find the key missing because compaction has not yet
      // committed), then we have a bad situation where only a TTL update exists in the store. This problem has to be
      // addressed. This can only happen if replication is far behind (for e.g due to a server being down for a long
      // time). Won't happen if a server is being recreated.
      messageInfo = new MessageInfo.Builder(messageInfo).isTtlUpdated(true).build();
      remoteReplicaInfo.getLocalStore().updateTtl(Collections.singletonList(messageInfo));
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key ttl updated id: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
    } catch (StoreException e) {
      // The blob may be deleted or updated which is alright
      if (e.getErrorCode() == StoreErrorCodes.IDDeleted || e.getErrorCode() == StoreErrorCodes.AlreadyUpdated) {
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
  protected void applyUndelete(MessageInfo messageInfo, RemoteReplicaInfo remoteReplicaInfo) throws StoreException {
    DataNodeId remoteNode = remoteReplicaInfo.getReplicaId().getDataNodeId();
    try {
      messageInfo = new MessageInfo.Builder(messageInfo).isUndeleted(true).isDeleted(false).build();
      remoteReplicaInfo.getLocalStore().undelete(messageInfo);
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key undelete id: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
    } catch (StoreException e) {
      // The blob may be undeleted, which is alright
      if (e.getErrorCode() == StoreErrorCodes.LifeVersionConflict || e.getErrorCode() == StoreErrorCodes.IDUndeleted) {
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
  protected void applyDelete(MessageInfo messageInfo, RemoteReplicaInfo remoteReplicaInfo) throws StoreException {
    DataNodeId remoteNode = remoteReplicaInfo.getReplicaId().getDataNodeId();
    try {
      messageInfo = new MessageInfo.Builder(messageInfo).isDeleted(true).isUndeleted(false).build();
      remoteReplicaInfo.getLocalStore().delete(Collections.singletonList(messageInfo));
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key delete: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
    } catch (StoreException e) {
      // The blob may be deleted or updated which is alright
      if (e.getErrorCode() == StoreErrorCodes.IDDeleted || e.getErrorCode() == StoreErrorCodes.LifeVersionConflict) {
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
   *
   * In addition, also include replicas which doesn't have any leaders in local data center.
   *
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
      // Check if 'local replica and remote replica are leaders' or if 'local replica doesn't have any leader'.
      if (leaderBasedReplicationAdmin.isLeaderPair(localReplica, remoteReplica)) {
        logger.trace("Sending GET request for leader replica pair {}, {}", localReplica, remoteReplica);
        leaderReplicaInfosOutput.add(remoteReplicaInfo);
        exchangeMetadataResponseListForLeaderReplicaInfosOutput.add(exchangeMetadataResponseList.get(i));
      } else if (leaderBasedReplicationAdmin.isLeaderLessPartition(localReplica.getPartitionId())) {
        logger.trace("Sending GET request for leaderless replica {}", localReplica);
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
    List<RemoteReplicaInfo> remoteReplicasTimedOut = new ArrayList<>();
    filterRemoteReplicasToReplicate(remoteReplicaInfos, new ArrayList<>(), remoteReplicasTimedOut);
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
          advanceToken(remoteReplicaInfo, exchangeMetadataResponse);
        }
      }
    } catch (StoreException e) {
      if (e.getErrorCode() == StoreErrorCodes.StoreNotStarted) {
        logger.info("Local store not started for remote replica: {}", remoteReplicaInfo.getReplicaId());
      } else {
        logger.error("Exception occurred while updating blob from Remote replica {} for partition: {}",
            remoteReplicaInfo.getReplicaId(), remoteReplicaInfo.getReplicaId().getPartitionId().toString(), e);
        replicationMetrics.updateLocalStoreError(remoteReplicaInfo.getReplicaId());
      }
      // reset stored metadata response so that metadata request is sent again for this replica
      remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(ServerErrorCode.NoError));
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

    if (!missingStoreMessages.isEmpty()) {
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
        remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(ServerErrorCode.NoError));
      }
    }
  }

  /**
   * Return associated {@link ReplicationMetrics}. Used in test.
   */
  ReplicationMetrics getReplicationMetrics() {
    return replicationMetrics;
  }

  /**
   * This is only used in test cases.
   * @return A map from DataNodeId to the list of ExchangeMetadataResponse in one replication cycle.
   */
  Map<DataNodeId, List<ExchangeMetadataResponse>> getExchangeMetadataResponsesInEachCycle() {
    return exchangeMetadataResponsesInEachCycle;
  }

  public static class ExchangeMetadataResponse {
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

    public ExchangeMetadataResponse(Set<MessageInfo> missingStoreMessages, FindToken remoteToken,
        long localLagFromRemoteInBytes, Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap, Time time) {
      this.missingStoreMessages = missingStoreMessages;
      this.remoteKeyToLocalKeyMap = remoteKeyToLocalKeyMap;
      this.remoteToken = remoteToken;
      this.localLagFromRemoteInBytes = localLagFromRemoteInBytes;
      this.serverErrorCode = ServerErrorCode.NoError;
      this.time = time;
      this.lastMissingMessageReceivedTimeSec = time.seconds();
      this.receivedStoreMessagesWithUpdatesPending = new HashSet<>();
    }

    public ExchangeMetadataResponse(Set<MessageInfo> missingStoreMessages, FindToken remoteToken,
        long localLagFromRemoteInBytes, Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap, Time time,
        Set<MessageInfo> receivedStoreMessagesWithUpdatesPending) {
      this.missingStoreMessages = missingStoreMessages;
      this.remoteKeyToLocalKeyMap = remoteKeyToLocalKeyMap;
      this.remoteToken = remoteToken;
      this.localLagFromRemoteInBytes = localLagFromRemoteInBytes;
      this.serverErrorCode = ServerErrorCode.NoError;
      this.time = time;
      this.lastMissingMessageReceivedTimeSec = time.seconds();
      this.receivedStoreMessagesWithUpdatesPending = receivedStoreMessagesWithUpdatesPending;
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

    public long getLocalLagFromRemoteInBytes() {
      return localLagFromRemoteInBytes;
    }

    /**
     * Checks if there are any missing store messages in this metadata exchange.
     * @return set of missing store messages
     */
    synchronized boolean hasMissingStoreMessages() {
      return missingStoreMessages != null && !missingStoreMessages.isEmpty();
    }

    /**
     * Get missing store messages in this metadata exchange.
     * @return set of missing store messages as a new collection.
     */
    public synchronized Set<MessageInfo> getMissingStoreMessages() {
      return missingStoreMessages == null ? Collections.emptySet() : new HashSet<>(missingStoreMessages);
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
     * @return set of messages that are now found in store as a new collection
     */
    public synchronized Set<MessageInfo> getReceivedStoreMessagesWithUpdatesPending() {
      return receivedStoreMessagesWithUpdatesPending == null ? Collections.emptySet()
          : new HashSet<>(receivedStoreMessagesWithUpdatesPending);
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
     * Get the keys corresponding to the missing messages in the local store
     * @return store keys missing in the local store
     */
    synchronized Set<StoreKey> getMissingStoreKeys() {
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
    if (networkClient != null) {
      networkClient.close();
    }
    shutdownLatch.await();
  }

  /**
   * This class is used to generate remote replica groups continuously for replication
   * Lifecycle of groups in this poller is as below
   *  1. For each data node, we create multiple sets of replicas and pre-assign group id to each set and store it in {@link ActiveGroupTracker#getPreAssignedReplicas()}
   *     These sets are stored as {@link DataNodeTracker#getActiveGroupTrackers()}
   *  2. We pre-assign group id for standBy group for each datanode and store it {@link DataNodeTracker#getStandByGroupTracker()}
   *  3. {@link #enqueue()} would try to create (and add back) those groups for these sets and group ids and
   *     store it in {@link DataNodeTracker#getActiveGroupTrackers()} and {@link DataNodeTracker#getStandByGroupTracker()}.
   *  4. Once a group finishes one iteration of work, include metadata exchange and data download,
   *     we remove group  from the {@link GroupTracker} and potentially backoff replicas in the group.
   *  5. If  any of the replicas of this group need to go through another iteration, we create group with group id as same as {@link ActiveGroupTracker#getGroupId()}
   *     the same group would be added back to {@link ActiveGroupTracker} with the available replicas only.
   *  6. While trying to create any group, if we find stand by replicas that have timed out,
   *     we will move replica to {@link ReplicaStatus#STANDBY_TIMED_OUT_ON_NO_PROGRESS} status.
   *  7. All standBy groups get created using the replicas with status {@link ReplicaStatus#STANDBY_TIMED_OUT_ON_NO_PROGRESS}
   *     of the data node and will be stored in {@link DataNodeTracker#getStandByGroupTracker()}
   *  8. When there is a new replica added to the thread, or a replica removed from this thread,
   *     or there is one group reaches max iteration, we don't create new groups, just return the existing ones,
   *     and after they all finish current iteration, {@link GroupTracker} will not be tracking any group and do-while loop would break.
   */
  class RemoteReplicaGroupPoller {
    private final List<DataNodeTracker> dataNodeTrackers;
    private boolean allReplicasCaughtUpEarly;

    RemoteReplicaGroupPoller() {
      dataNodeTrackers = new ArrayList<>();
      fillDataNodeTrackers();
      allReplicasCaughtUpEarly = false;
    }

    public List<DataNodeTracker> getDataNodeTrackers() {
      return dataNodeTrackers;
    }

    boolean allReplicasCaughtUpEarly() {
      return allReplicasCaughtUpEarly;
    }

    /**
     * Creates Data node trackers for every data node
     * Every datanode tracker has active groups and standby group
     * For each data node tracker, only one stand by group is present to reduce cross colo requests
     * Active groups have maximum {@link #maxReplicaCountPerRequest}  preassigned replicas
     * Across all datanode tracker group ids are unique
     */
    private void fillDataNodeTrackers() {
      Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToRemoteReplicaInfo = selectReplicas(getRemoteReplicaInfos());

      int currentStartGroupId = 0;

      for (Map.Entry<DataNodeId, List<RemoteReplicaInfo>> entry : dataNodeToRemoteReplicaInfo.entrySet()) {
        DataNodeId remoteHost = entry.getKey();
        List<RemoteReplicaInfo> remoteReplicasPerNode = entry.getValue();

        DataNodeTracker dataNodeTracker =
            new DataNodeTracker(remoteHost, remoteReplicasPerNode, maxReplicaCountPerRequest, currentStartGroupId, time,
                threadThrottleDurationMs);
        logger.trace("Thread name: {} for datanode {} create datanode tracker {}", threadName, remoteHost,
            dataNodeTracker);

        dataNodeTrackers.add(dataNodeTracker);
        currentStartGroupId = dataNodeTracker.getMaxGroupId() + 1;
      }
    }

    /**
     * @return Iterates over all data nodes and returns list of all remote replica groups
     * that are getting tracked
     */
    private List<RemoteReplicaGroup> getInflightGroups() {
      List<RemoteReplicaGroup> allRemoteReplicaGroups = new ArrayList<>();
      dataNodeTrackers.forEach(dataNodeTracker -> {
        allRemoteReplicaGroups.addAll(dataNodeTracker.getInflightRemoteReplicaGroups());
      });
      return allRemoteReplicaGroups;
    }

    /**
     * @return returns true if there are any remote replica groups getting tracked, false otherwise
     */
    boolean shouldContinue() {
      return !getInflightGroups().isEmpty();
    }

    /**
     * Conditions when we don't want to create new groups.
     *
     * 1. terminateCurrentContinuousReplicationCycle is true, which means there are replicas added to this thread
     *    or removed from this thread, don't create new group and just let existing groups finish,
     *    so we can later create new groups with the new set of replicas in next cycle.
     * 2. any group reaches max iteration, which means existing groups should just finish its current work and stop,
     *    thus no new groups should be created.
     * @return {@link boolean} returns true if new groups can be created, false otherwise
     */
    private boolean shouldEnqueue() {
      if (terminateCurrentContinuousReplicationCycle) {
        logger.trace("Thread name: {} not creating new groups as terminate cycle received {}", threadName,
            terminateCurrentContinuousReplicationCycle);
        return false;
      }
      int maxIterationsDoneByAnyGroup = dataNodeTrackers.stream()
          .map(DataNodeTracker::getMaxIterationAcrossGroups)
          .max(Comparator.naturalOrder())
          .orElse(0);
      if (maxIterationsDoneByAnyGroup >= continuousReplicationGroupIterationLimit) {
        logger.trace("Thread name: {} not creating new groups as iteration limit {} reached", threadName,
            maxIterationsDoneByAnyGroup);
        return false;
      }
      return true;
    }

    /**
     * For each data node tracker, if any remote replica group is done,
     * removes remote replica group from tracking, throttles replicas added in this group,
     * throttles corresponding replica and moves its state to UNKNOWN
     * For these finished groups, drops keys from store key converter
     */
    private void processFinishedGroups() {
      List<RemoteReplicaGroup> finishedGroups = dataNodeTrackers.stream()
          .map(DataNodeTracker::processFinishedGroups)
          .flatMap(List::stream)
          .collect(Collectors.toList());

      finishedGroups.forEach(remoteReplicaGroup -> {
        storeKeyConverter.tryDropCache(remoteReplicaGroup.getStoreKeysConversionLog());
        logger.trace("Thread name: {} group id {} is done and removed from group tracker", threadName, remoteReplicaGroup.getId());
      });

      logger.trace("Thread name: {} Finished processing finished groups {}", threadName, dataNodeTrackers);
    }

    /**
     * For any active group tracker that is not tracking any group, does below things
     *
     * 1. Filter pre-assigned replicas with UNKNOWN, OFFLINE and STANDBY status
     * 2. If any of these replica is throttled we increment throttling metric
     * 3. For each of these replicas we check the status of replica and update in corresponding tracker
     *
     * whenever active group is free, no replica will be in ACTIVE status and whenever stand by group is free,
     * it will pick up STANDBY_TIMED_OUT_ON_NO_PROGRESS replicas, so we do not need to recheck status of these.
     *
     * @param finishedActiveGroupTracker finished group tracker
     * @return list of replica trackers updated
     */
    private List<ReplicaTracker> fillReplicaStatusForFinishedGroup(ActiveGroupTracker finishedActiveGroupTracker) {

      List<ReplicaTracker> replicaTrackers = finishedActiveGroupTracker.getPreAssignedReplicas(
              Arrays.asList(ReplicaStatus.UNKNOWN, ReplicaStatus.OFFLINE, ReplicaStatus.STANDBY))
          .stream()
          .filter(replicaTracker -> {
            if (replicaTracker.isThrottled()) {
              logger.trace("Thread name: {} replica for tracker {} is throttled", threadName, replicaTracker);
              continuousReplicationReplicaThrottleCount.inc();
              return false;
            }
            return true;
          })
          .collect(Collectors.toList());

      replicaTrackers.forEach(replicaTracker -> {
        ReplicaStatus replicaStatus = getReplicaStatus(replicaTracker.getRemoteReplicaInfo());

        logger.trace("Thread name: {} Updated replica {} status from {} to {}", threadName,
            replicaTracker.getRemoteReplicaInfo(), replicaTracker.getReplicaStatus(), replicaStatus);

        replicaTracker.setReplicaStatus(replicaStatus);
      });
      return replicaTrackers;
    }

    /**
     * For all active group trackers which are not tracking any remote replica group, does below things
     *
     * 1. updates status of preassigned available replicas with UNKNOWN, OFFLINE and STANDBY status.
     * 2. Creates active remote replica group from updated replicas with status as ACTIVE.
     * 3. Updates group tracker for newly created active groups.
     */
    private void fillActiveGroups() {
      dataNodeTrackers.forEach(dataNodeTracker -> {
        logger.trace("Thread name: {} Trying to create active group for data node {} ", threadName,
            dataNodeTracker.getDataNodeId());
        List<ActiveGroupTracker> finishedActiveGroupTrackers = dataNodeTracker.getFinishedActiveGroupTrackers();

        finishedActiveGroupTrackers.forEach(activeGroupTracker -> {
          logger.trace("Thread name: {} Trying to create active group for active group tracker {} ", threadName,
              activeGroupTracker);

          List<ReplicaTracker> activeReplicaTrackers = fillReplicaStatusForFinishedGroup(activeGroupTracker).stream()
              .filter(replicaTracker -> replicaTracker.getReplicaStatus().equals(ReplicaStatus.ACTIVE))
              .collect(Collectors.toList());

          logger.trace("Thread name: {} trying to create group from active replicas {} for group id {} ",
              threadName, activeReplicaTrackers, activeGroupTracker.getGroupId());

          if (activeReplicaTrackers.isEmpty()) {
            return;
          }

          RemoteReplicaGroup remoteReplicaGroup = new RemoteReplicaGroup(
              activeReplicaTrackers.stream().map(ReplicaTracker::getRemoteReplicaInfo).collect(Collectors.toList()),
              dataNodeTracker.getDataNodeId(), false, activeGroupTracker.getGroupId());

          activeGroupTracker.startIteration(remoteReplicaGroup, activeReplicaTrackers);
          logger.trace("Thread name: {} Created active remote replica group for group tracker {} ", threadName,
              activeGroupTracker);
        });
      });
    }

    /**
     * For all stand by tracker, which are not tracking any remote replica group, it does below things
     *
     * 1. Gets all preassigned replicas with STANDBY_TIMED_OUT_ON_NO_PROGRESS status from preassigned replicas of active group of
     *    corresponding datanode tracker.
     * 2. Create single standby group from these replicas and store it in standby group tracker
     */
    private void fillStandByGroups() {
      dataNodeTrackers.forEach(dataNodeTracker -> {
        logger.trace("Thread name: {} Trying to create standby group for data node {} ", threadName,
            dataNodeTracker.getDataNodeId());

        StandByGroupTracker standByGroupTracker = dataNodeTracker.getStandByGroupTracker();
        if (standByGroupTracker.isInFlight()) {
          return;
        }
        logger.trace("Thread name: {} Trying to create standby group for stand by group tracker {} ", threadName,
            standByGroupTracker);

        List<ReplicaTracker> standByTimedOutNoProgressReplicaTrackers = new ArrayList<>();
        List<ActiveGroupTracker> activeGroupTrackers = dataNodeTracker.getActiveGroupTrackers();

        activeGroupTrackers.forEach(activeGroupTracker -> {
          List<ReplicaTracker> replicaTrackers = activeGroupTracker.getPreAssignedReplicas(
                  Collections.singletonList(ReplicaStatus.STANDBY_TIMED_OUT_ON_NO_PROGRESS));

          standByTimedOutNoProgressReplicaTrackers.addAll(replicaTrackers);
        });

        if (standByTimedOutNoProgressReplicaTrackers.isEmpty()) {
          return;
        }

        logger.trace("Thread name: {} Trying to create standby group with replica trackers {} ", threadName,
            standByTimedOutNoProgressReplicaTrackers);

        RemoteReplicaGroup remoteReplicaGroup = new RemoteReplicaGroup(standByTimedOutNoProgressReplicaTrackers.stream()
            .map(ReplicaTracker::getRemoteReplicaInfo)
            .collect(Collectors.toList()), dataNodeTracker.getDataNodeId(), true, standByGroupTracker.getGroupId());
        standByGroupTracker.startIteration(remoteReplicaGroup, standByTimedOutNoProgressReplicaTrackers);

        logger.trace("Thread name: {} Created stand by remote replica group for stand by group tracker {} ", threadName,
            standByGroupTracker);
      });
    }

    /**
     * Tries to update remote replica groups currently in progress and
     * creates new remote replica groups
     */
    List<RemoteReplicaGroup> enqueue() {
      processFinishedGroups();

      if (!shouldEnqueue()) {
        logger.trace("Thread name: {} Not creating any new groups", threadName);
        return getInflightGroups();
      }

      fillActiveGroups();
      fillStandByGroups();
      List<RemoteReplicaGroup> inflightGroups = getInflightGroups();

      // if we are not able to create any new group and no groups are remaining, all replicas caught up early
      if (inflightGroups.isEmpty()) {
        allReplicasCaughtUpEarly = true;
        logger.trace("Thread name: {} All replicas marked to caught up early", threadName);
      }
      return inflightGroups;
    }
  }

  /**
   * ReplicaGroupReplicationState indicates different state of RemoteReplicaGroup in one replication cycle.
   * Each replication cycle, we will break all the replicas in order different groups and each group
   * has to go through these states to finish replication with nonblocking network client.
   */
  enum ReplicaGroupReplicationState {
    /**
     * The group is ready to start replication.
     */
    STARTED,
    /**
     * The group has sent the ReplicaMetadataRequest.
     */
    REPLICA_METADATA_REQUEST_SENT,
    /**
     * The group has handled the ReplicaMetadataResponse.
     */
    REPLICA_METADATA_RESPONSE_HANDLED,
    /**
     * The group has sent the GetRequest.
     */
    GET_REQUEST_SENT,
    /**
     * The group has handled the GetResponse and the process is marked as Done.
     */
    DONE
  }

  /**
   * A group of remote replicas to replicate. This group of replicas belongs to the same data node, so we can create one
   * ReplicaMetadatRequest to exchange replication metadata and one GetRequest to copy all the missing blobs if there is
   * any.
   *
   * For replication, this group has to go through several state in sequence.
   * <ol>
   *   <li>Send ReplicaMetadataRequest to remote data node</li>
   *   <li>Wait for ReplicaMetadataResponse to come back and handle the ReplicaMetadataResponse</li>
   *   <li>Send GetRequest to remote data node</li>
   *   <li>Wait for GetResponse to come back and handle the GetResponse</li>
   * </ol>
   * Since network requests will be done through a nonblocking network client, we have to keep track of which state
   * this group is currently in. A group is in STARTED state when created. In STARTED state, it can send out one
   * {@link ReplicaMetadataRequest} and enter REPLICA_METADATA_REQUEST_SENT state. In this state, it will wait for
   * {@link ReplicaMetadataResponse} from network client. After getting the response from the network client, it
   * handles the response and moves to REPLICA_METADATA_RESPONSE_HANDLED state. in this state, it can send out one
   * {@link GetRequest} and enter GET_REQUEST_SENT state. In this state, it will wait for {@link GetResponse} from
   * network client. After getting the response from tne network client, it handles the response and moves to DONE
   * state. Any error happens in the middle of processing would force group to DONE state.
   */
  public class RemoteReplicaGroup {
    private final List<RemoteReplicaInfo> remoteReplicaInfos;
    private List<ExchangeMetadataResponse> exchangeMetadataResponseList;

    private List<RemoteReplicaInfo> remoteReplicaInfosToSend;
    private List<ExchangeMetadataResponse> exchangeMetadataResponseListToProcess;
    private final DataNodeId remoteDataNode;
    private final boolean isNonProgressStandbyReplicaGroup;
    private final int id;
    private ReplicaGroupReplicationState state;
    private Exception exception = null;
    private long replicaMetadataRequestStartTimeMs;
    private long getRequestStartTimeMs;
    private long exchangeMetadataStartTimeInMs;
    private long fixMissingStoreKeysStartTimeInMs;
    private long finishTime;
    private final List<StoreKey> storeKeysConversionLog;

    /**
     * Constructor of {@link RemoteReplicaGroup}.
     * @param remoteReplicaInfos The list {@link RemoteReplicaInfo} that point to the list of replicas.
     * @param dataNodeId The remote data node
     * @param isNonProgressStandbyReplicaGroup True if this is standby replicas trying to catch up with the missing blob content. If true,
     *                          this group should start with REPLICA_METADATA_RESPONSE_HANDLED, since it already exchanged
     *                          ReplicaMetadata before and now wants to download missing blobs.
     * @param id The id of this RemoteReplicaGroup. This id should be unique at each cycle.
     */
    public RemoteReplicaGroup(List<RemoteReplicaInfo> remoteReplicaInfos, DataNodeId dataNodeId,
        boolean isNonProgressStandbyReplicaGroup, int id) {
      this.remoteReplicaInfos = remoteReplicaInfos;
      this.remoteDataNode = dataNodeId;
      this.isNonProgressStandbyReplicaGroup = isNonProgressStandbyReplicaGroup;
      this.id = id;
      storeKeysConversionLog = new ArrayList<>();
      finishTime = 0;
      if (isNonProgressStandbyReplicaGroup) {
        exchangeMetadataResponseList = remoteReplicaInfos.stream()
            .map(remoteReplicaInfo -> new ExchangeMetadataResponse(remoteReplicaInfo.getExchangeMetadataResponse()))
            .collect(Collectors.toList());
        List<StoreKey> storeKeysToConvert = exchangeMetadataResponseList.stream()
            .map(ExchangeMetadataResponse::getMissingStoreKeys)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        remoteReplicaInfosToSend = this.remoteReplicaInfos;
        exchangeMetadataResponseListToProcess = exchangeMetadataResponseList;
        try {
          convertStoreKeys(storeKeysToConvert, storeKeysConversionLog);
          state = ReplicaGroupReplicationState.REPLICA_METADATA_RESPONSE_HANDLED;
        } catch (Exception e) {
          setException(e, "Failed to convert storeKeys");
        }
      } else {
        state = ReplicaGroupReplicationState.STARTED;
      }
      logger.trace(
          "Remote node: {} Thread name: {} Adding a new RemoteReplicaGroup {}, ReplicaInfos {}, State {} isNonProgressStandByReplicaGroup {}",
          remoteDataNode, threadName, id, remoteReplicaInfos, state, isNonProgressStandbyReplicaGroup);
    }

    public long getFinishTime() {
      return finishTime;
    }

    public void setFinishTime(long finishTime) {
      this.finishTime = finishTime;
    }

    public boolean shouldSetFinishTime() {
      return isDone() && finishTime == 0;
    }

    public List<RemoteReplicaInfo> getRemoteReplicaInfos() {
      return remoteReplicaInfos;
    }

    public DataNodeId getRemoteDataNode() {
      return remoteDataNode;
    }

    public boolean isDone() {
      return state == ReplicaGroupReplicationState.DONE;
    }

    public void setState(ReplicaGroupReplicationState state) {
      this.state = state;
    }

    public int getId() {
      return id;
    }

    public boolean isNonProgressStandbyReplicaGroup() {
      return isNonProgressStandbyReplicaGroup;
    }

    public List<StoreKey> getStoreKeysConversionLog() {
      return storeKeysConversionLog;
    }

    public List<ExchangeMetadataResponse> getExchangeMetadataResponseList() {
      return exchangeMetadataResponseList;
    }

    /**
     * Polling the {@link RequestInfo} from this group to send out. Each group has two requests to send out, one is
     * ReplicaMetadataRequest the other is GetRequest. Group only sends ReplicaMetadataRequest when it's in STARTED
     * state. And it only sends GetRequest when it's in REPLICA_METADATA_RESPONSE_HANDLED state. When group is not in
     * those states, this method would return null.
     *
     * @return {@link RequestInfo} to send out. A null might be returned when there is nothing to send.
     */
    public RequestInfo poll() {
      Port port = remoteReplicaInfos.get(0).getPort();
      long timeout = replicationConfig.replicationRequestNetworkTimeoutMs;
      if (state == ReplicaGroupReplicationState.STARTED) {
        // When the state is STARTED, we will send the ReplicaMetadataRequest out.
        replicaMetadataRequestStartTimeMs = time.milliseconds();
        exchangeMetadataStartTimeInMs = replicaMetadataRequestStartTimeMs;
        ReplicaMetadataRequest request = createReplicaMetadataRequest(remoteReplicaInfos, remoteDataNode);
        RequestInfo requestInfo =
            new RequestInfo(remoteDataNode.getHostname(), port, request, remoteReplicaInfos.get(0).getReplicaId(), null,
                time.milliseconds(), timeout, timeout);
        logger.trace(
            "Remote node: {} Thread name: {} RemoteReplicaGroup {} Create ReplicaMetadataRequest, correlation id {}, state {}",
            remoteDataNode, threadName, id, requestInfo.getRequest().getCorrelationId(), state);
        state = ReplicaGroupReplicationState.REPLICA_METADATA_REQUEST_SENT;
        return requestInfo;
      } else if (state == ReplicaGroupReplicationState.REPLICA_METADATA_RESPONSE_HANDLED) {
        // When the state is REPLICA_METADATA_RESPONSE_HANDLED, we will send the GetRequest out if needed.
        remoteReplicaInfosToSend = remoteReplicaInfos;
        exchangeMetadataResponseListToProcess = exchangeMetadataResponseList;
        if (!isNonProgressStandbyReplicaGroup && replicatingFromRemoteColo && leaderBasedReplicationAdmin != null) {
          // If this is not standby replicas trying to catch up, and this is cross-colo replication, and we enable
          // leader based replication, then filter out remote replicas that is not leader to leader.
          List<RemoteReplicaInfo> leaderReplicaList = new ArrayList<>();
          List<ExchangeMetadataResponse> exchangeMetadataResponseListForLeaderReplicas = new ArrayList<>();
          getLeaderReplicaList(remoteReplicaInfos, exchangeMetadataResponseList, leaderReplicaList,
              exchangeMetadataResponseListForLeaderReplicas);
          remoteReplicaInfosToSend = leaderReplicaList;
          exchangeMetadataResponseListToProcess = exchangeMetadataResponseListForLeaderReplicas;
        }
        if (!remoteReplicaInfosToSend.isEmpty()) {
          if (exchangeMetadataResponseListToProcess.size() != remoteReplicaInfosToSend.size()) {
            throw new IllegalArgumentException("RemoteReplicaGroup " + id + " ExchangeMetadataResponseList size "
                + exchangeMetadataResponseListToProcess.size() + " and RemoteReplicaInfo size "
                + remoteReplicaInfosToSend.size() + " should be the same and greater than zero");
          }
          GetRequest request =
              createGetRequest(remoteReplicaInfosToSend, exchangeMetadataResponseListToProcess, remoteDataNode);
          if (request != null) {
            // Only when we still have RemoteReplicaInfos and when the GetRequest is not null
            // we send the GetRequest out
            getRequestStartTimeMs = time.milliseconds();
            fixMissingStoreKeysStartTimeInMs = getRequestStartTimeMs;
            RequestInfo requestInfo = new RequestInfo(remoteDataNode.getHostname(), port, request,
                remoteReplicaInfosToSend.get(0).getReplicaId(), null, time.milliseconds(), timeout, timeout);
            logger.trace(
                "Remote node: {} Thread name: {} RemoteReplicaGroup {} Create GetRequest, correlation id {}, state {}",
                remoteDataNode, threadName, id, requestInfo.getRequest().getCorrelationId(), state);
            state = ReplicaGroupReplicationState.GET_REQUEST_SENT;
            return requestInfo;
          }
        }
        logger.trace("Remote node: {} Thread name: {} RemoteReplicaGroup {} No GetRequest needed, set state to Done",
            remoteDataNode, threadName, id);
        state = ReplicaGroupReplicationState.DONE;
      }
      return null;
    }

    /**
     * Handle the {@link ReplicaMetadataResponse} returned in {@link ResponseInfo} by nonblocking network client.
     * @param responseInfo The {@link ResponseInfo} that carries a {@link ReplicaMetadataResponse}.
     */
    public void handleReplicaMetadataResponse(ResponseInfo responseInfo) {
      if (state != ReplicaGroupReplicationState.REPLICA_METADATA_REQUEST_SENT) {
        logger.error("Remote node: {} Thread name: {} ReplicaMetadataResponse comes back after wrong state {}",
            remoteDataNode, threadName, state);
        return;
      }

      NetworkClientErrorCode networkClientErrorCode = responseInfo.getError();
      logger.trace(
          "Remote node: {} Thread name: {} RemoteReplicaGroup {} ReplicaMetadataResponse come back for correlation id {}, NetworkClientError {}",
          remoteDataNode, threadName, id, responseInfo.getRequestInfo().getRequest().getCorrelationId(),
          networkClientErrorCode);
      if (networkClientErrorCode == null) {
        replicationMetrics.updateMetadataRequestTime(time.milliseconds() - replicaMetadataRequestStartTimeMs,
            replicatingFromRemoteColo, replicatingOverSsl, datacenterName);
        try {
          // Deserialize the request from the given ResponseInfo
          DataInputStream dis = new NettyByteBufDataInputStream(responseInfo.content());
          ReplicaMetadataResponse response = ReplicaMetadataResponse.readFrom(dis, findTokenHelper, clusterMap);
          if (response.getError() != ServerErrorCode.NoError
              || response.getReplicaMetadataResponseInfoList().size() != remoteReplicaInfos.size()) {
            int replicaMetadataResponseInfoListSize = response.getReplicaMetadataResponseInfoList() == null ? 0
                : response.getReplicaMetadataResponseInfoList().size();
            replicationMetrics.incrementResponseErrorCount(replicatingFromRemoteColo, datacenterName);
            exchangeMetadataResponseList = Collections.emptyList();
            // Getting error for entire response, something unexpected happened.
            logger.error(
                "Remote node: {} Thread name: {} RemoteReplicaGroup {} ServerError for ReplicaMetadataResponse {} "
                    + "ReplicaMetadataResponseInfoListSize: {} RemoteReplicaInfoSize: {}", remoteDataNode, threadName,
                id, response.getError(), replicaMetadataResponseInfoListSize, remoteReplicaInfos.size());
            throw new ReplicationException("ReplicaMetadataResponse unexpected error " + response.getError(),
                response.getError());
          }
          exchangeMetadataResponseList =
              ReplicaThread.this.handleReplicaMetadataResponse(response, remoteReplicaInfos, remoteDataNode, storeKeysConversionLog);
          state = ReplicaGroupReplicationState.REPLICA_METADATA_RESPONSE_HANDLED;
          logger.trace(
              "Remote node: {} Thread name: {} RemoteReplicaGroup {} Handled ReplicaMetadataResponse for correlation id {}, set state to {}",
              remoteDataNode, threadName, id, responseInfo.getRequestInfo().getRequest().getCorrelationId(), state);
          long exchangeMetadataTimeInMs = time.milliseconds() - exchangeMetadataStartTimeInMs;
          replicationMetrics.updateExchangeMetadataTime(exchangeMetadataTimeInMs, replicatingFromRemoteColo,
              replicatingOverSsl, datacenterName);
        } catch (Exception e) {
          setException(e, "ReplicaMetadataResponse unexpected error");
        }
      } else {
        // We have a network client error here, mark all the replicas int request with this error.
        if (networkClientErrorCode != NetworkClientErrorCode.TimeoutError) {
          replicationMetrics.incrementRequestNetworkErrorCount(replicatingFromRemoteColo, datacenterName);
        }
        exchangeMetadataResponseList = Collections.emptyList();
        remoteReplicaInfos.forEach(r -> responseHandler.onEvent(r.getReplicaId(), networkClientErrorCode));
        setException(new IOException("NetworkClientErrorCode: " + networkClientErrorCode),
            "Failed to send ReplicaMetadataRequest");
      }
      if (exchangeMetadataListener != null) {
        remoteReplicaInfos.forEach(
            r -> exchangeMetadataListener.onComplete(r.getReplicaId().getPartitionId(), exception));
      }
    }

    /**
     * Handle the {@link GetResponse} returned in {@link ResponseInfo} by nonblocking network client.
     * @param responseInfo The {@link ResponseInfo} that carries a {@link GetResponse}.
     */
    public void handleGetResponse(ResponseInfo responseInfo) {
      if (state != ReplicaGroupReplicationState.GET_REQUEST_SENT) {
        logger.error("Remote node: {} Thread name: {} GetResponse comes back after wrong state {}", remoteDataNode,
            threadName, state);
        return;
      }
      NetworkClientErrorCode networkClientErrorCode = responseInfo.getError();
      logger.trace(
          "Remote node: {} Thread name: {} RemoteReplicaGroup {} GetResponse come back for correlation id {}, NetworkClientError {}",
          remoteDataNode, threadName, id, responseInfo.getRequestInfo().getRequest().getCorrelationId(),
          networkClientErrorCode);
      if (networkClientErrorCode == null) {
        replicationMetrics.updateGetRequestTime(time.milliseconds() - getRequestStartTimeMs, replicatingFromRemoteColo,
            replicatingOverSsl, datacenterName, isNonProgressStandbyReplicaGroup);
        try {
          // Deserialize the request from the given ResponseInfo
          DataInputStream dis = new NettyByteBufDataInputStream(responseInfo.content());
          GetResponse response = GetResponse.readFrom(dis, clusterMap);
          if (response.getError() != ServerErrorCode.NoError) {
            // Getting error for entire response, something unexpected happened.
            replicationMetrics.incrementResponseErrorCount(replicatingFromRemoteColo, datacenterName);
            logger.error("Remote node: {} Thread name: {} RemoteReplicaGroup {} ServerError for GetResponse {}",
                remoteDataNode, threadName, id, response.getError());
            throw new ReplicationException("GetResponse unexpected error " + response.getError(), response.getError());
          }
          ReplicaThread.this.handleGetResponse(response, remoteReplicaInfosToSend,
              exchangeMetadataResponseListToProcess, remoteDataNode, isNonProgressStandbyReplicaGroup);
          logger.trace(
              "Remote node: {} Thread name: {} RemoteReplicaGroup {} Handled GetResponse for correlation id {}",
              remoteDataNode, threadName, id, responseInfo.getRequestInfo().getRequest().getCorrelationId());
          long fixMissingStoreKeysTime = time.milliseconds() - fixMissingStoreKeysStartTimeInMs;
          replicationMetrics.updateFixMissingStoreKeysTime(fixMissingStoreKeysTime, replicatingFromRemoteColo,
              replicatingOverSsl, datacenterName);
        } catch (Exception e) {
          setException(e, "GetResponse unexpected error");
        }
      } else {
        if (networkClientErrorCode != NetworkClientErrorCode.TimeoutError) {
          replicationMetrics.incrementRequestNetworkErrorCount(replicatingFromRemoteColo, datacenterName);
        }
        remoteReplicaInfos.forEach(r -> responseHandler.onEvent(r.getReplicaId(), networkClientErrorCode));
        setException(new IOException("NetworkClientErrorCode: " + networkClientErrorCode), "Failed to send GetRequest");
      }
      state = ReplicaGroupReplicationState.DONE;
    }

    /**
     * Set exception for the group and change the state to DONE and log the message out.
     * @param e The exception to set
     * @param message The message to log out
     */
    private void setException(Exception e, String message) {
      if (!(e instanceof ReplicationException)) {
        replicationMetrics.incrementReplicationErrorCount(replicatingFromRemoteColo, datacenterName);
      }
      if (e instanceof ReplicationException
          && ((ReplicationException) e).getServerErrorCode() == ServerErrorCode.RetryAfterBackoff) {
        replicationMetrics.incrementRetryAfterBackoffErrorCount(replicatingFromRemoteColo, datacenterName);
        logger.trace("Remote node: {} Thread name: {} RemoteReplicaGroup {} {}", remoteDataNode, threadName, id,
            message);
      } else {
        logger.error("Remote node: {} Thread name: {} RemoteReplicaGroup {} {}", remoteDataNode, threadName, id,
            message, e);
      }
      exception = e;
      state = ReplicaGroupReplicationState.DONE;
    }

    /**
     * @return The exception.
     */
    public Exception getException() {
      return exception;
    }
  }

  @FunctionalInterface
  interface ExchangeMetadataListener {
    void onComplete(PartitionId partitionId, Exception exception);
  }
}
