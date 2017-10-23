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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A replica thread is responsible for handling replication for a set of partitions assigned to it
 */
class ReplicaThread implements Runnable {

  private final Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateGroupedByNode;
  private final Set<PartitionId> replicationDisabledPartitions = new HashSet<>();
  private final Set<PartitionId> unmodifiableReplicationDisabledPartitions =
      Collections.unmodifiableSet(replicationDisabledPartitions);
  private final Set<PartitionId> allReplicatedPartitions;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private volatile boolean running;
  private boolean waitEnabled;
  private final FindTokenFactory findTokenFactory;
  private final ClusterMap clusterMap;
  private final AtomicInteger correlationIdGenerator;
  private final DataNodeId dataNodeId;
  private final ConnectionPool connectionPool;
  private final ReplicationConfig replicationConfig;
  private final ReplicationMetrics replicationMetrics;
  private final String threadName;
  private final NotificationSystem notification;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final StoreKeyFactory storeKeyFactory;
  private final boolean validateMessageStream;
  private final MetricRegistry metricRegistry;
  private final ResponseHandler responseHandler;
  private final boolean replicatingFromRemoteColo;
  private final boolean replicatingOverSsl;
  private final String datacenterName;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition pauseCondition = lock.newCondition();

  private volatile boolean allDisabled = false;

  ReplicaThread(String threadName, Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateGroupedByNode,
      FindTokenFactory findTokenFactory, ClusterMap clusterMap, AtomicInteger correlationIdGenerator,
      DataNodeId dataNodeId, ConnectionPool connectionPool, ReplicationConfig replicationConfig,
      ReplicationMetrics replicationMetrics, NotificationSystem notification, StoreKeyFactory storeKeyFactory,
      boolean validateMessageStream, MetricRegistry metricRegistry, boolean replicatingOverSsl, String datacenterName,
      ResponseHandler responseHandler) {
    this.threadName = threadName;
    this.replicasToReplicateGroupedByNode = replicasToReplicateGroupedByNode;
    this.running = true;
    this.findTokenFactory = findTokenFactory;
    this.clusterMap = clusterMap;
    this.correlationIdGenerator = correlationIdGenerator;
    this.dataNodeId = dataNodeId;
    this.connectionPool = connectionPool;
    this.replicationConfig = replicationConfig;
    this.replicationMetrics = replicationMetrics;
    this.notification = notification;
    this.storeKeyFactory = storeKeyFactory;
    this.validateMessageStream = validateMessageStream;
    this.metricRegistry = metricRegistry;
    this.responseHandler = responseHandler;
    this.replicatingFromRemoteColo = !(dataNodeId.getDatacenterName().equals(datacenterName));
    this.waitEnabled = !replicatingFromRemoteColo;
    this.replicatingOverSsl = replicatingOverSsl;
    this.datacenterName = datacenterName;
    Set<PartitionId> partitions = new HashSet<>();
    for (Map.Entry<DataNodeId, List<RemoteReplicaInfo>> entry : replicasToReplicateGroupedByNode.entrySet()) {
      for (RemoteReplicaInfo info : entry.getValue()) {
        partitions.add(info.getReplicaId().getPartitionId());
      }
    }
    allReplicatedPartitions = Collections.unmodifiableSet(partitions);
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
      List<List<RemoteReplicaInfo>> replicasToReplicate = new ArrayList<>(replicasToReplicateGroupedByNode.size());
      for (Map.Entry<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateEntry : replicasToReplicateGroupedByNode.entrySet()) {
        logger.info("Remote node: " + replicasToReplicateEntry.getKey() + " Thread name: " + threadName
            + " ReplicasToReplicate: " + replicasToReplicateEntry.getValue());
        replicasToReplicate.add(replicasToReplicateEntry.getValue());
      }
      logger.info("Begin iteration for thread " + threadName);
      while (running) {
        replicate(replicasToReplicate);
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
   * Replicas from the given replicas
   * @param replicasToReplicate list of {@link RemoteReplicaInfo} by data node
   */
  void replicate(List<List<RemoteReplicaInfo>> replicasToReplicate) {
    // shuffle the nodes
    Collections.shuffle(replicasToReplicate);
    for (List<RemoteReplicaInfo> replicasToReplicatePerNode : replicasToReplicate) {
      if (!running) {
        break;
      }
      DataNodeId remoteNode = replicasToReplicatePerNode.get(0).getReplicaId().getDataNodeId();
      logger.trace("Remote node: {} Thread name: {} Remote replicas: {}", remoteNode, threadName,
          replicasToReplicatePerNode);
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

      List<RemoteReplicaInfo> activeReplicasPerNode = new ArrayList<RemoteReplicaInfo>();
      for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
        ReplicaId replicaId = remoteReplicaInfo.getReplicaId();
        if (!replicationDisabledPartitions.contains(replicaId.getPartitionId()) && !replicaId.isDown()) {
          activeReplicasPerNode.add(remoteReplicaInfo);
        }
      }
      if (activeReplicasPerNode.size() > 0) {
        try {
          connectedChannel =
              connectionPool.checkOutConnection(remoteNode.getHostname(), activeReplicasPerNode.get(0).getPort(),
                  replicationConfig.replicationConnectionPoolCheckoutTimeoutMs);
          checkoutConnectionTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;
          startTimeInMs = SystemTime.getInstance().milliseconds();
          List<ExchangeMetadataResponse> exchangeMetadataResponseList =
              exchangeMetadata(connectedChannel, activeReplicasPerNode);
          exchangeMetadataTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;

          startTimeInMs = SystemTime.getInstance().milliseconds();
          fixMissingStoreKeys(connectedChannel, activeReplicasPerNode, exchangeMetadataResponseList);
          fixMissingStoreKeysTimeInMs = SystemTime.getInstance().milliseconds() - startTimeInMs;
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
          logger.error("Error while talking to peer: Remote node: {}, Thread name: {}, Remote replicas: {}, Active "
                  + "remote replicas: {}, Checkout connection time: {}, Exchange metadata time: {}, Fix missing store key "
                  + "time {}", remoteNode, threadName, replicasToReplicatePerNode, activeReplicasPerNode,
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
   * @throws StoreException
   * @throws MessageFormatException
   * @throws ReplicationException
   * @throws InterruptedException
   */
  List<ExchangeMetadataResponse> exchangeMetadata(ConnectedChannel connectedChannel,
      List<RemoteReplicaInfo> replicasToReplicatePerNode)
      throws IOException, ReplicationException, InterruptedException {

    long exchangeMetadataStartTimeInMs = SystemTime.getInstance().milliseconds();
    List<ExchangeMetadataResponse> exchangeMetadataResponseList = new ArrayList<ExchangeMetadataResponse>();
    if (replicasToReplicatePerNode.size() > 0) {
      try {
        DataNodeId remoteNode = replicasToReplicatePerNode.get(0).getReplicaId().getDataNodeId();
        ReplicaMetadataResponse response =
            getReplicaMetadataResponse(replicasToReplicatePerNode, connectedChannel, remoteNode);
        long startTimeInMs = SystemTime.getInstance().milliseconds();
        waitEnabled = !replicatingFromRemoteColo;
        for (int i = 0; i < response.getReplicaMetadataResponseInfoList().size(); i++) {
          RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
          ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
              response.getReplicaMetadataResponseInfoList().get(i);
          responseHandler.onEvent(remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
          if (replicaMetadataResponseInfo.getError() == ServerErrorCode.No_Error) {
            try {
              logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token from remote: {} Replica lag: {} ",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getFindToken(),
                  replicaMetadataResponseInfo.getRemoteReplicaLagInBytes());
              if (waitEnabled) {
                waitIfRequired(replicaMetadataResponseInfo, remoteNode, remoteReplicaInfo);
              }
              Set<StoreKey> missingStoreKeys =
                  getMissingStoreKeys(replicaMetadataResponseInfo, remoteNode, remoteReplicaInfo);
              processReplicaMetadataResponse(missingStoreKeys, replicaMetadataResponseInfo, remoteReplicaInfo,
                  remoteNode);
              ExchangeMetadataResponse exchangeMetadataResponse =
                  new ExchangeMetadataResponse(missingStoreKeys, replicaMetadataResponseInfo.getFindToken(),
                      replicaMetadataResponseInfo.getRemoteReplicaLagInBytes());
              exchangeMetadataResponseList.add(exchangeMetadataResponse);
            } catch (Exception e) {
              replicationMetrics.updateLocalStoreError(remoteReplicaInfo.getReplicaId());
              logger.error(
                  "Remote node: " + remoteNode + " Thread name: " + threadName + " Remote replica: " + remoteReplicaInfo
                      .getReplicaId(), e);
              responseHandler.onEvent(remoteReplicaInfo.getReplicaId(), e);
              ExchangeMetadataResponse exchangeMetadataResponse =
                  new ExchangeMetadataResponse(ServerErrorCode.Unknown_Error);
              exchangeMetadataResponseList.add(exchangeMetadataResponse);
            }
          } else {
            replicationMetrics.updateMetadataRequestError(remoteReplicaInfo.getReplicaId());
            logger.error("Remote node: {} Thread name: {} Remote replica: {} Server error: {}", remoteNode, threadName,
                remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
            ExchangeMetadataResponse exchangeMetadataResponse =
                new ExchangeMetadataResponse(replicaMetadataResponseInfo.getError());
            exchangeMetadataResponseList.add(exchangeMetadataResponse);
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
   * @throws StoreException
   * @throws MessageFormatException
   * @throws ReplicationException
   */
  void fixMissingStoreKeys(ConnectedChannel connectedChannel, List<RemoteReplicaInfo> replicasToReplicatePerNode,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList)
      throws IOException, StoreException, MessageFormatException, ReplicationException {
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
              remoteReplicaInfo.getLocalReplicaId().getReplicaPath());
      replicaMetadataRequestInfoList.add(replicaMetadataRequestInfo);
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token going to be sent to remote: {} ",
          remoteNode, threadName, remoteReplicaInfo.getReplicaId(), remoteReplicaInfo.getToken());
    }

    try {
      ReplicaMetadataRequest request = new ReplicaMetadataRequest(correlationIdGenerator.incrementAndGet(),
          "replication-metadata-" + dataNodeId.getHostname(), replicaMetadataRequestInfoList,
          replicationConfig.replicationFetchSizeInBytes);
      connectedChannel.send(request);
      ChannelOutput channelOutput = connectedChannel.receive();
      ByteBufferInputStream byteBufferInputStream =
          new ByteBufferInputStream(channelOutput.getInputStream(), (int) channelOutput.getStreamSize());
      logger.trace("Remote node: {} Thread name: {} Remote replicas: {} ByteBuffer size after deserialization: {} ",
          remoteNode, threadName, replicasToReplicatePerNode, byteBufferInputStream.available());
      ReplicaMetadataResponse response =
          ReplicaMetadataResponse.readFrom(new DataInputStream(byteBufferInputStream), findTokenFactory, clusterMap);

      long metadataRequestTime = SystemTime.getInstance().milliseconds() - replicaMetadataRequestStartTime;
      replicationMetrics.updateMetadataRequestTime(metadataRequestTime, replicatingFromRemoteColo, replicatingOverSsl,
          datacenterName);

      if (response.getError() != ServerErrorCode.No_Error
          || response.getReplicaMetadataResponseInfoList().size() != replicasToReplicatePerNode.size()) {
        logger.error("Remote node: " + remoteNode + " Thread name: " + threadName + " Remote replicas: "
            + replicasToReplicatePerNode + " Replica metadata response error: " + response.getError()
            + " ReplicaMetadataResponseInfoListSize: " + response.getReplicaMetadataResponseInfoList().size()
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
   * @throws StoreException
   */
  private Set<StoreKey> getMissingStoreKeys(ReplicaMetadataResponseInfo replicaMetadataResponseInfo,
      DataNodeId remoteNode, RemoteReplicaInfo remoteReplicaInfo) throws StoreException {
    long startTime = SystemTime.getInstance().milliseconds();
    List<MessageInfo> messageInfoList = replicaMetadataResponseInfo.getMessageInfoList();
    List<StoreKey> storeKeysToCheck = new ArrayList<StoreKey>(messageInfoList.size());
    for (MessageInfo messageInfo : messageInfoList) {
      storeKeysToCheck.add(messageInfo.getStoreKey());
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key from remote: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
    }

    Set<StoreKey> missingStoreKeys = remoteReplicaInfo.getLocalStore().findMissingKeys(storeKeysToCheck);
    for (StoreKey storeKey : missingStoreKeys) {
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key missing id: {}", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), storeKey);
    }
    replicationMetrics.updateCheckMissingKeysTime(SystemTime.getInstance().milliseconds() - startTime,
        replicatingFromRemoteColo, datacenterName);
    return missingStoreKeys;
  }

  /**
   * Takes the missing keys and the message list from the remote store and identifies messages that are deleted
   * on the remote store and updates them locally. Also, if the message that is missing is deleted in the remote
   * store, we remove the message from the list of missing keys
   * @param missingStoreKeys The list of keys missing from the local store
   * @param replicaMetadataResponseInfo The replica metadata response from the remote store
   * @param remoteReplicaInfo The remote replica that is being replicated from
   * @param remoteNode The remote node from which replication needs to happen
   * @throws IOException
   * @throws StoreException
   * @throws MessageFormatException
   */
  private void processReplicaMetadataResponse(Set<StoreKey> missingStoreKeys,
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo, RemoteReplicaInfo remoteReplicaInfo,
      DataNodeId remoteNode) throws IOException, StoreException, MessageFormatException {
    long startTime = SystemTime.getInstance().milliseconds();
    List<MessageInfo> messageInfoList = replicaMetadataResponseInfo.getMessageInfoList();
    for (MessageInfo messageInfo : messageInfoList) {
      BlobId blobId = (BlobId) messageInfo.getStoreKey();
      if (remoteReplicaInfo.getLocalReplicaId().getPartitionId().compareTo(blobId.getPartition()) != 0) {
        throw new IllegalStateException(
            "Blob id is not in the expected partition Actual partition " + blobId.getPartition()
                + " Expected partition " + remoteReplicaInfo.getLocalReplicaId().getPartitionId());
      }
      if (!missingStoreKeys.contains(messageInfo.getStoreKey())) {
        // the key is present in the local store. Mark it for deletion if it is deleted in the remote store and not
        // deleted yet locally
        if (messageInfo.isDeleted() && !remoteReplicaInfo.getLocalStore().isKeyDeleted(messageInfo.getStoreKey())) {
          MessageFormatInputStream deleteStream =
              new DeleteMessageFormatInputStream(messageInfo.getStoreKey(), messageInfo.getAccountId(),
                  messageInfo.getContainerId(), messageInfo.getOperationTimeMs());
          MessageInfo info =
              new MessageInfo(messageInfo.getStoreKey(), deleteStream.getSize(), true, messageInfo.getAccountId(),
                  messageInfo.getContainerId(), messageInfo.getOperationTimeMs());
          ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
          infoList.add(info);
          MessageFormatWriteSet writeset = new MessageFormatWriteSet(deleteStream, infoList, false);
          try {
            remoteReplicaInfo.getLocalStore().delete(writeset);
            logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key deleted. mark for deletion id: {}",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
          } catch (StoreException e) {
            // The blob may get deleted between the time the above check is done and the delete is
            // attempted. For example, this can happen if the key gets deleted in the context of another replica
            // thread. This is more likely when replication is already caught up - when similar set of
            // messages are received from different replicas around the same time.
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
      } else {
        if (messageInfo.isDeleted()) {
          // if the key is not present locally and if the remote replica has the message in deleted state,
          // it is not considered missing locally.
          missingStoreKeys.remove(messageInfo.getStoreKey());
          logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key in deleted state remotely: {}",
              remoteNode, threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
          // A Repair event for Delete signifies that a Delete message was received from the remote and it is fired
          // as long as the Delete is guaranteed to have taken effect locally.
          if (notification != null) {
            notification.onBlobReplicaDeleted(dataNodeId.getHostname(), dataNodeId.getPort(),
                messageInfo.getStoreKey().getID(), BlobReplicaSourceType.REPAIRED);
          }
        } else if (messageInfo.isExpired()) {
          // if the key is not present locally and if the remote replica has the key as expired,
          // it is not considered missing locally.
          missingStoreKeys.remove(messageInfo.getStoreKey());
          logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key in expired state remotely {}",
              remoteNode, threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
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
   * Checks to see if we need to wait between replication iterations
   * @param replicaMetadataResponseInfo The replica metadata response from the remote node
   * @param remoteNode The remote node from which replication needs to happen
   * @param remoteReplicaInfo The remote replica that is being replicated from
   * @throws InterruptedException
   */
  private void waitIfRequired(ReplicaMetadataResponseInfo replicaMetadataResponseInfo, DataNodeId remoteNode,
      RemoteReplicaInfo remoteReplicaInfo) throws InterruptedException {
    long remoteReplicaLag = replicaMetadataResponseInfo.getRemoteReplicaLagInBytes();
    long startTime = SystemTime.getInstance().milliseconds();
    if (remoteReplicaLag < replicationConfig.replicationMaxLagForWaitTimeInBytes) {
      logger.trace("Remote node: {} Thread name: {} Remote replica: {} Remote replica lag: {} "
              + "ReplicationMaxLagForWaitTimeInBytes: {} Waiting for {} ms", remoteNode, threadName,
          remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getRemoteReplicaLagInBytes(),
          replicationConfig.replicationMaxLagForWaitTimeInBytes, replicationConfig.replicaWaitTimeBetweenReplicasMs);
      // We apply the wait time between replication from remote replicas here. Any new objects that get written
      // in the remote replica are given time to be written to the local replica and avoids failing the request
      // from the client. This is done only when the replication lag with that node is less than
      // replicationMaxLagForWaitTimeInBytes
      Thread.sleep(replicationConfig.replicaWaitTimeBetweenReplicasMs);
      waitEnabled = false;
    }
    //TODO do we need interColo metrics here?
//    if (remoteColo) {
//      replicationMetrics.interColoReplicationWaitTime.update(SystemTime.getInstance().milliseconds() - startTime);
//    } else {
//      replicationMetrics.intraColoReplicationWaitTime.update(SystemTime.getInstance().milliseconds() - startTime);
//    }
    replicationMetrics.intraColoReplicationWaitTime.update(SystemTime.getInstance().milliseconds() - startTime);
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
      GetRequest getRequest =
          new GetRequest(correlationIdGenerator.incrementAndGet(), "replication-fetch-" + dataNodeId.getHostname(),
              MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
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
        if (exchangeMetadataResponse.missingStoreKeys.size() > 0) {
          PartitionResponseInfo partitionResponseInfo =
              getResponse.getPartitionResponseInfoList().get(partitionResponseInfoIndex);
          responseHandler.onEvent(remoteReplicaInfo.getReplicaId(), partitionResponseInfo.getErrorCode());
          partitionResponseInfoIndex++;
          if (partitionResponseInfo.getPartition().compareTo(remoteReplicaInfo.getReplicaId().getPartitionId()) != 0) {
            throw new IllegalStateException(
                "The partition id from partitionResponseInfo " + partitionResponseInfo.getPartition()
                    + " and from remoteReplicaInfo " + remoteReplicaInfo.getReplicaId().getPartitionId()
                    + " are not the same");
          }
          if (partitionResponseInfo.getErrorCode() == ServerErrorCode.No_Error) {
            try {
              List<MessageInfo> messageInfoList = partitionResponseInfo.getMessageInfoList();
              logger.trace("Remote node: {} Thread name: {} Remote replica: {} Messages to fix: {} "
                      + "Partition: {} Local mount path: {}", remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                  exchangeMetadataResponse.missingStoreKeys, remoteReplicaInfo.getReplicaId().getPartitionId(),
                  remoteReplicaInfo.getLocalReplicaId().getMountPath());

              MessageFormatWriteSet writeset = null;
              if (validateMessageStream) {
                MessageSievingInputStream validMessageDetectionInputStream =
                    new MessageSievingInputStream(getResponse.getInputStream(), messageInfoList, storeKeyFactory,
                        metricRegistry);
                if (validMessageDetectionInputStream.hasInvalidMessages()) {
                  replicationMetrics.incrementInvalidMessageError(partitionResponseInfo.getPartition());
                  logger.error("Out of " + (messageInfoList.size()) + " messages, " + (messageInfoList.size()
                      - validMessageDetectionInputStream.getValidMessageInfoList().size())
                      + " invalid messages were found in message stream from " + remoteReplicaInfo.getReplicaId());
                }
                messageInfoList = validMessageDetectionInputStream.getValidMessageInfoList();
                if (messageInfoList.size() == 0) {
                  logger.error("MessageInfoList is of size 0 as all messages are invalidated ");
                } else {
                  writeset = new MessageFormatWriteSet(validMessageDetectionInputStream, messageInfoList, false);
                  remoteReplicaInfo.getLocalStore().put(writeset);
                }
              } else {
                writeset = new MessageFormatWriteSet(getResponse.getInputStream(), messageInfoList, true);
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
