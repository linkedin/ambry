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
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class to handle replication for given partitions. Responsible for the following
 * 1. Initialize class members.
 * 2. Create replica threads and distribute partitions amongst the threads
 * 3. Set up replica token persistor used to recover from shutdown/crash
 * 4. Initialize and shutdown all the components required to perform replication
 */
public abstract class ReplicationEngine {

  protected final ReplicationConfig replicationConfig;
  private final ClusterMap clusterMap;
  protected final ScheduledExecutorService scheduler;
  private final AtomicInteger correlationIdGenerator;
  private final ConnectionPool connectionPool;
  private final NotificationSystem notification;
  // RemoteReplicaInfo are managed by replicaThread.
  protected final Map<String, List<ReplicaThread>> replicaThreadPoolByDc;
  protected final Map<DataNodeId, ReplicaThread> dataNodeIdToReplicaThread;
  protected final Map<String, AtomicInteger> nextReplicaThreadIndex;
  private final StoreKeyFactory storeKeyFactory;
  private final List<String> sslEnabledDatacenters;
  private final StoreKeyConverterFactory storeKeyConverterFactory;
  private final String transformerClassName;

  protected final DataNodeId dataNodeId;
  protected final MetricRegistry metricRegistry;
  protected final ReplicationMetrics replicationMetrics;
  protected final FindTokenFactory factory;
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final Map<PartitionId, PartitionInfo> partitionToPartitionInfo;
  protected final Map<String, List<PartitionInfo>> mountPathToPartitionInfoList;
  protected ReplicaTokenPersistor persistor = null;

  protected static final short Replication_Delay_Multiplier = 5;
  protected static final String replicaTokenFileName = "replicaTokens";

  public ReplicationEngine(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, ScheduledExecutorService scheduler, DataNodeId dataNode,
      List<? extends ReplicaId> replicaIds, ConnectionPool connectionPool, MetricRegistry metricRegistry,
      NotificationSystem requestNotification, StoreKeyConverterFactory storeKeyConverterFactory,
      String transformerClassName) throws ReplicationException {
    this.replicationConfig = replicationConfig;
    this.storeKeyFactory = storeKeyFactory;
    try {
      this.factory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);
    } catch (ReflectiveOperationException e) {
      logger.error("Error on getting replicationTokenFactory", e);
      throw new ReplicationException("Error on getting replicationTokenFactory");
    }
    this.replicaThreadPoolByDc = new ConcurrentHashMap<>();
    this.replicationMetrics = new ReplicationMetrics(metricRegistry, replicaIds);
    this.mountPathToPartitionInfoList = new ConcurrentHashMap<>();
    this.partitionToPartitionInfo = new ConcurrentHashMap<>();
    this.clusterMap = clusterMap;
    this.scheduler = scheduler;
    this.correlationIdGenerator = new AtomicInteger(0);
    this.dataNodeId = dataNode;
    this.connectionPool = connectionPool;
    this.notification = requestNotification;
    this.metricRegistry = metricRegistry;
    this.dataNodeIdToReplicaThread = new ConcurrentHashMap<>();
    this.nextReplicaThreadIndex = new ConcurrentHashMap<>();
    this.sslEnabledDatacenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
    this.storeKeyConverterFactory = storeKeyConverterFactory;
    this.transformerClassName = transformerClassName;
  }

  abstract public void start() throws ReplicationException;

  /**
   * Enables/disables replication of the given {@code ids} from {@code origins}. The disabling is in-memory and
   * therefore is not valid across restarts.
   * @param ids the {@link PartitionId}s to enable/disable it on.
   * @param origins the list of datacenters from which replication should be enabled/disabled. Having an empty list
   *                disables replication from all datacenters.
   * @param enable whether to enable ({@code true}) or disable.
   * @return {@code true} if disabling succeeded, {@code false} otherwise. Disabling fails if {@code origins} is empty
   * or contains unrecognized datacenters.
   */
  public boolean controlReplicationForPartitions(Collection<PartitionId> ids, List<String> origins, boolean enable) {
    if (origins.isEmpty()) {
      origins = new ArrayList<>(replicaThreadPoolByDc.keySet());
    }
    if (!replicaThreadPoolByDc.keySet().containsAll(origins)) {
      return false;
    }
    for (String origin : origins) {
      for (ReplicaThread replicaThread : replicaThreadPoolByDc.get(origin)) {
        replicaThread.controlReplicationForPartitions(ids, enable);
      }
    }
    return true;
  }

  /**
   * Updates the total bytes read by a remote replica from local store
   * @param partitionId PartitionId to which the replica belongs to
   * @param hostName HostName of the datanode where the replica belongs to
   * @param replicaPath Replica Path of the replica interested in
   * @param totalBytesRead Total bytes read by the replica
   */
  public void updateTotalBytesReadByRemoteReplica(PartitionId partitionId, String hostName, String replicaPath,
      long totalBytesRead) {
    RemoteReplicaInfo remoteReplicaInfo = getRemoteReplicaInfo(partitionId, hostName, replicaPath);
    if (remoteReplicaInfo != null) {
      remoteReplicaInfo.setTotalBytesReadFromLocalStore(totalBytesRead);
    }
  }

  /**
   * Gets the replica lag of the remote replica with the local store
   * @param partitionId The partition to which the remote replica belongs to
   * @param hostName The hostname where the remote replica is present
   * @param replicaPath The path of the remote replica on the host
   * @return The lag in bytes that the remote replica is behind the local store
   */
  public long getRemoteReplicaLagFromLocalInBytes(PartitionId partitionId, String hostName, String replicaPath) {
    RemoteReplicaInfo remoteReplicaInfo = getRemoteReplicaInfo(partitionId, hostName, replicaPath);
    if (remoteReplicaInfo != null) {
      return remoteReplicaInfo.getRemoteLagFromLocalInBytes();
    }
    return 0;
  }

  /**
   * Gets the replica info for the remote peer replica identified by PartitionId, ReplicaPath and Hostname
   * @param partitionId PartitionId to which the replica belongs to
   * @param hostName hostname of the remote peer replica
   * @param replicaPath replica path on the remote peer replica
   * @return RemoteReplicaInfo
   */
  private RemoteReplicaInfo getRemoteReplicaInfo(PartitionId partitionId, String hostName, String replicaPath) {
    RemoteReplicaInfo foundRemoteReplicaInfo = null;

    PartitionInfo partitionInfo = partitionToPartitionInfo.get(partitionId);
    for (RemoteReplicaInfo remoteReplicaInfo : partitionInfo.getRemoteReplicaInfos()) {
      if (remoteReplicaInfo.getReplicaId().getReplicaPath().equals(replicaPath) && remoteReplicaInfo.getReplicaId()
          .getDataNodeId()
          .getHostname()
          .equals(hostName)) {
        foundRemoteReplicaInfo = remoteReplicaInfo;
        break;
      }
    }
    // TODO: replace replicaPath.contains("vcr").
    if (foundRemoteReplicaInfo == null && !replicaPath.contains(GetRequest.Cloud_Replica_Keyword)) {
      replicationMetrics.unknownRemoteReplicaRequestCount.inc();
      logger.error("ReplicaMetaDataRequest from unknown Replica {}, with path {}", hostName, replicaPath);
    }
    return foundRemoteReplicaInfo;
  }

  /**
   * Shutsdown the replication manager. Shutsdown the individual replica threads and
   * then persists all the replica tokens
   * @throws ReplicationException
   */
  public void shutdown() throws ReplicationException {
    try {
      // stop all replica threads
      for (Map.Entry<String, List<ReplicaThread>> replicaThreads : replicaThreadPoolByDc.entrySet()) {
        for (ReplicaThread replicaThread : replicaThreads.getValue()) {
          replicaThread.shutdown();
        }
      }

      // persist replica tokens
      if (persistor != null) {
        persistor.write(true);
      }
    } catch (Exception e) {
      logger.error("Error shutting down replica manager {}", e);
      throw new ReplicationException("Error shutting down replica manager");
    }
  }

  /**
   * Remove a list of {@link RemoteReplicaInfo} from each's {@link ReplicaThread}.
   * @param remoteReplicaInfos List of {@link RemoteReplicaInfo} to remote.
   */
  protected void removeRemoteReplicaInfoFromReplicaThread(List<RemoteReplicaInfo> remoteReplicaInfos) {
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfos) {
      // Thread safe with addRemoteReplicaInfoToReplicaThread.
      // For ReplicationManger, this method is not used.
      // For CloudBackUpManager with HelixVcrCluster, Helix requires acknowledgement before next message for the same
      // resource, which means methods in HelixVcrStateModel will be executed sequentially for same partition.
      // So do listener actions in addPartition() and removePartition().
      remoteReplicaInfo.getReplicaThread().removeRemoteReplicaInfo(remoteReplicaInfo);
      remoteReplicaInfo.setReplicaThread(null);
    }
  }

  /**
   * Assign {@link RemoteReplicaInfo} to a {@link ReplicaThread} for replication.
   * The assignment is based on {@link DataNodeId}. If no {@link ReplicaThread} responsible for a {@link DataNodeId},
   * a {@link ReplicaThread} will be selected by {@link ReplicationEngine#getReplicaThreadIndexToUse(String)}.
   * Create threads pool for a DC if not exists.
   * @param remoteReplicaInfos List of {@link RemoteReplicaInfo} to add.
   * @param startThread if threads need to be started when create.
   */
  protected void addRemoteReplicaInfoToReplicaThread(List<RemoteReplicaInfo> remoteReplicaInfos, boolean startThread) {
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfos) {
      DataNodeId dataNodeIdToReplicate = remoteReplicaInfo.getReplicaId().getDataNodeId();
      String datacenter = dataNodeIdToReplicate.getDatacenterName();
      createThreadPoolIfNecessary(datacenter, startThread);
      ReplicaThread replicaThread = dataNodeIdToReplicaThread.computeIfAbsent(dataNodeIdToReplicate,
          key -> replicaThreadPoolByDc.get(datacenter).get(getReplicaThreadIndexToUse(datacenter)));
      replicaThread.addRemoteReplicaInfo(remoteReplicaInfo);
      remoteReplicaInfo.setReplicaThread(replicaThread);
    }
  }

  /**
   * Select next available {@link ReplicaThread} in given datacenter.
   * @param datacenter the datacenter String.
   */
  private int getReplicaThreadIndexToUse(String datacenter) {
    return nextReplicaThreadIndex.get(datacenter).getAndIncrement() % replicaThreadPoolByDc.get(datacenter).size();
  }

  /**
   * Create thread pool for a datacenter if its thread pool doesn't exist.
   * @param datacenter The datacenter String.
   * @param startThread If thread needs to be started when create.
   */
  private void createThreadPoolIfNecessary(String datacenter, boolean startThread) {
    int numOfThreadsInPool =
        datacenter.equals(dataNodeId.getDatacenterName()) ? replicationConfig.replicationNumOfIntraDCReplicaThreads
            : replicationConfig.replicationNumOfInterDCReplicaThreads;
    replicaThreadPoolByDc.computeIfAbsent(datacenter,
        key -> createThreadPool(datacenter, numOfThreadsInPool, startThread));
  }

  /**
   * Create thread pool for a datacenter.
   * @param datacenter The datacenter String.
   * @param numberOfThreads number of threads to create for the thread pool.
   * @param startThread If thread needs to be started when create.
   */
  private List<ReplicaThread> createThreadPool(String datacenter, int numberOfThreads, boolean startThread) {
    nextReplicaThreadIndex.put(datacenter, new AtomicInteger(0));
    List<ReplicaThread> replicaThreads = new ArrayList<>();
    if (numberOfThreads <= 0) {
      logger.warn("Number of replica threads is smaller or equal to 0, not starting any replica threads for {} ",
          datacenter);
      return null;
    }
    logger.info("Number of replica threads to replicate from {}: {}", datacenter, numberOfThreads);

    ResponseHandler responseHandler = new ResponseHandler(clusterMap);
    for (int i = 0; i < numberOfThreads; i++) {
      boolean replicatingOverSsl = sslEnabledDatacenters.contains(datacenter);
      String threadIdentity =
          (startThread ? "Vcr" : "") + "ReplicaThread-" + (dataNodeId.getDatacenterName().equals(datacenter) ? "Intra-"
              : "Inter-") + i + "-" + datacenter;
      try {
        StoreKeyConverter threadSpecificKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
        Transformer threadSpecificTransformer =
            Utils.getObj(transformerClassName, storeKeyFactory, threadSpecificKeyConverter);
        ReplicaThread replicaThread =
            new ReplicaThread(threadIdentity, factory, clusterMap, correlationIdGenerator, dataNodeId, connectionPool,
                replicationConfig, replicationMetrics, notification, threadSpecificKeyConverter,
                threadSpecificTransformer, metricRegistry, replicatingOverSsl, datacenter, responseHandler,
                SystemTime.getInstance());
        replicaThreads.add(replicaThread);
        if (startThread) {
          Thread thread = Utils.newThread(replicaThread.getName(), replicaThread, false);
          thread.start();
          logger.info("Started replica thread " + thread.getName());
        }
      } catch (Exception e) {
        throw new RuntimeException("Encountered exception instantiating ReplicaThread", e);
      }
    }
    replicationMetrics.trackLiveThreadsCount(replicaThreads, datacenter);
    return replicaThreads;
  }

  /**
   * Reads the replica tokens from storage, populates the Remote replica info,
   * and re-persists the tokens if they have been reset.
   * @param mountPath The mount path where the replica tokens are stored
   * @throws ReplicationException
   * @throws IOException
   */
  protected void retrieveReplicaTokensAndPersistIfNecessary(String mountPath) throws ReplicationException, IOException {
    boolean tokenWasReset = false;
    long readStartTimeMs = SystemTime.getInstance().milliseconds();
    List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfoList = persistor.retrieve(mountPath);

    for (RemoteReplicaInfo.ReplicaTokenInfo tokenInfo : tokenInfoList) {
      String hostname = tokenInfo.getHostname();
      int port = tokenInfo.getPort();
      PartitionId partitionId = tokenInfo.getPartitionId();
      FindToken token = tokenInfo.getReplicaToken();
      // update token
      PartitionInfo partitionInfo = partitionToPartitionInfo.get(tokenInfo.getPartitionId());
      if (partitionInfo != null) {
        boolean updatedToken = false;
        for (RemoteReplicaInfo remoteReplicaInfo : partitionInfo.getRemoteReplicaInfos()) {
          DataNodeId dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
          if (dataNodeId.getHostname().equalsIgnoreCase(hostname) && dataNodeId.getPort() == port
              && remoteReplicaInfo.getReplicaId().getReplicaPath().equals(tokenInfo.getReplicaPath())) {
            logger.info("Read token for partition {} remote host {} port {} token {}", partitionId, hostname, port,
                token);
            if (!partitionInfo.getStore().isEmpty()) {
              remoteReplicaInfo.initializeTokens(token);
              remoteReplicaInfo.setTotalBytesReadFromLocalStore(tokenInfo.getTotalBytesReadFromLocalStore());
            } else {
              // if the local replica is empty, it could have been newly created. In this case, the offset in
              // every peer replica which the local replica lags from should be set to 0, so that the local
              // replica starts fetching from the beginning of the peer. The totalBytes the peer read from the
              // local replica should also be set to 0. During initialization these values are already set to 0,
              // so we let them be.
              tokenWasReset = true;
              logTokenReset(partitionId, hostname, port, token);
            }
            updatedToken = true;
            break;
          }
        }
        if (!updatedToken) {
          logger.warn("Persisted remote replica host {} and port {} not present in new cluster ", hostname, port);
        }
      } else {
        // If this partition was not found in partitionToPartitionInfo, it means that the local store corresponding
        // to this partition could not be started. In such a case, the tokens for its remote replicas should be
        // reset.
        tokenWasReset = true;
        logTokenReset(partitionId, hostname, port, token);
      }
    }
    replicationMetrics.remoteReplicaTokensRestoreTime.update(SystemTime.getInstance().milliseconds() - readStartTimeMs);

    if (tokenWasReset) {
      // We must ensure that the the token file is persisted if any of the tokens in the file got reset. We need to do
      // this before an associated store takes any writes, to avoid the case where a store takes writes and persists it,
      // before the replica token file is persisted after the reset.
      if (persistor != null) {
        persistor.write(mountPath, false);
      } else {
        logger.warn("Unable to persist after token reset, persistor is null");
      }
    }
  }

  /**
   * Update metrics and print a log message when a replica token is reset.
   * @param partitionId The replica's partition.
   * @param hostname The replica's hostname.
   * @param port The replica's port.
   * @param token The {@link FindToken} for the replica.
   */
  private void logTokenReset(PartitionId partitionId, String hostname, int port, FindToken token) {
    replicationMetrics.replicationTokenResetCount.inc();
    logger.info("Resetting token for partition {} remote host {} port {}, persisted token {}", partitionId, hostname,
        port, token);
  }
}
