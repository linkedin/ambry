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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

  private final ReplicationConfig replicationConfig;
  private final ClusterMap clusterMap;
  private final ScheduledExecutorService scheduler;
  private final AtomicInteger correlationIdGenerator;
  private final ConnectionPool connectionPool;
  private final NotificationSystem notification;
  private final Map<String, DataNodeRemoteReplicaInfos> dataNodeRemoteReplicaInfosPerDC;
  private final StoreKeyFactory storeKeyFactory;
  private final List<String> sslEnabledDatacenters;
  private final Map<String, List<ReplicaThread>> replicaThreadPools;
  private final StoreKeyConverterFactory storeKeyConverterFactory;
  private final String transformerClassName;

  protected final DataNodeId dataNodeId;
  protected final MetricRegistry metricRegistry;
  protected final ReplicationMetrics replicationMetrics;
  protected final FindTokenFactory factory;
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final Map<PartitionId, PartitionInfo> partitionsToReplicate;
  protected final Map<String, List<PartitionInfo>> partitionGroupedByMountPath;
  protected final Map<String, Integer> numberOfReplicaThreads;
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
    this.replicaThreadPools = new HashMap<>();
    this.replicationMetrics = new ReplicationMetrics(metricRegistry, replicaIds);
    this.partitionGroupedByMountPath = new HashMap<>();
    this.partitionsToReplicate = new HashMap<>();
    this.clusterMap = clusterMap;
    this.scheduler = scheduler;
    this.correlationIdGenerator = new AtomicInteger(0);
    this.dataNodeId = dataNode;
    this.connectionPool = connectionPool;
    this.notification = requestNotification;
    this.metricRegistry = metricRegistry;
    this.dataNodeRemoteReplicaInfosPerDC = new HashMap<>();
    this.sslEnabledDatacenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
    this.numberOfReplicaThreads = new HashMap<>();
    this.storeKeyConverterFactory = storeKeyConverterFactory;
    this.transformerClassName = transformerClassName;
  }

  public void start() throws ReplicationException {

    try {
      // read stored tokens
      // iterate through all mount paths and read replication info for the partitions it owns
      for (String mountPath : partitionGroupedByMountPath.keySet()) {
        // TODO: for VCR, every partition must have distinct mount path and be saved separately
        retrieveReplicaTokensAndPersistIfNecessary(mountPath);
      }
      if (dataNodeRemoteReplicaInfosPerDC.size() == 0) {
        logger.warn("Number of Datacenters to replicate from is 0, not starting any replica threads");
        return;
      }

      // divide the nodes between the replica threads if the number of replica threads is less than or equal to the
      // number of nodes. Otherwise, assign one thread to one node.
      assignReplicasToThreadPool();
      replicationMetrics.trackLiveThreadsCount(replicaThreadPools, dataNodeId.getDatacenterName());
      replicationMetrics.trackReplicationDisabledPartitions(replicaThreadPools);

      // start all replica threads
      for (List<ReplicaThread> replicaThreads : replicaThreadPools.values()) {
        for (ReplicaThread thread : replicaThreads) {
          Thread replicaThread = Utils.newThread(thread.getName(), thread, false);
          logger.info("Starting replica thread " + thread.getName());
          replicaThread.start();
        }
      }

      // start background persistent thread
      // start scheduler thread to persist replica token in the background
      if (persistor != null) {
        this.scheduler.scheduleAtFixedRate(persistor, replicationConfig.replicationTokenFlushDelaySeconds,
            replicationConfig.replicationTokenFlushIntervalSeconds, TimeUnit.SECONDS);
      }
    } catch (IOException e) {
      logger.error("IO error while starting replication", e);
    }
  }

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
      origins = new ArrayList<>(replicaThreadPools.keySet());
    }
    if (!replicaThreadPools.keySet().containsAll(origins)) {
      return false;
    }
    for (String origin : origins) {
      for (ReplicaThread replicaThread : replicaThreadPools.get(origin)) {
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

    PartitionInfo partitionInfo = partitionsToReplicate.get(partitionId);
    for (RemoteReplicaInfo remoteReplicaInfo : partitionInfo.getRemoteReplicaInfos()) {
      if (remoteReplicaInfo.getReplicaId().getReplicaPath().equals(replicaPath) && remoteReplicaInfo.getReplicaId()
          .getDataNodeId()
          .getHostname()
          .equals(hostName)) {
        foundRemoteReplicaInfo = remoteReplicaInfo;
      }
    }
    // TODO: replace replicaPath.contains("vcr").
    if (foundRemoteReplicaInfo == null && !replicaPath.contains("vcr")) {
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
      for (Map.Entry<String, List<ReplicaThread>> replicaThreads : replicaThreadPools.entrySet()) {
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
   * Updates the {@code dataNodeRemoteReplicaInfosPerDC} with the remoteReplicaInfo and also populates
   * {@code numberOfReplicaThreads}
   * @param datacenter remote datacenter name
   * @param remoteReplicaInfo The remote replica that needs to be added to the mapping
   */
  protected void updateReplicasToReplicate(String datacenter, RemoteReplicaInfo remoteReplicaInfo) {
    DataNodeRemoteReplicaInfos dataNodeRemoteReplicaInfos = dataNodeRemoteReplicaInfosPerDC.get(datacenter);
    if (dataNodeRemoteReplicaInfos != null) {
      dataNodeRemoteReplicaInfos.addRemoteReplica(remoteReplicaInfo);
    } else {
      dataNodeRemoteReplicaInfos = new DataNodeRemoteReplicaInfos(remoteReplicaInfo);
      // update numberOfReplicaThreads
      if (datacenter.equals(dataNodeId.getDatacenterName())) {
        this.numberOfReplicaThreads.put(datacenter, replicationConfig.replicationNumOfIntraDCReplicaThreads);
      } else {
        this.numberOfReplicaThreads.put(datacenter, replicationConfig.replicationNumOfInterDCReplicaThreads);
      }
    }
    dataNodeRemoteReplicaInfosPerDC.put(datacenter, dataNodeRemoteReplicaInfos);
  }

  /**
   * Partitions the list of data nodes between given set of replica threads for the given DC
   */
  private void assignReplicasToThreadPool() throws IOException {
    for (Map.Entry<String, DataNodeRemoteReplicaInfos> mapEntry : dataNodeRemoteReplicaInfosPerDC.entrySet()) {
      String datacenter = mapEntry.getKey();
      DataNodeRemoteReplicaInfos dataNodeRemoteReplicaInfos = mapEntry.getValue();
      Set<DataNodeId> dataNodesToReplicate = dataNodeRemoteReplicaInfos.getDataNodeIds();
      int dataNodesCount = dataNodesToReplicate.size();
      int replicaThreadCount = numberOfReplicaThreads.get(datacenter);
      if (replicaThreadCount <= 0) {
        logger.warn("Number of replica threads is smaller or equal to 0, not starting any replica threads for {} ",
            datacenter);
        continue;
      } else if (dataNodesCount == 0) {
        logger.warn("Number of nodes to replicate from is 0, not starting any replica threads for {} ", datacenter);
        continue;
      }

      // Divide the nodes between the replica threads if the number of replica threads is less than or equal to the
      // number of nodes. Otherwise, assign one thread to one node.
      logger.info("Number of replica threads to replicate from {}: {}", datacenter, replicaThreadCount);
      logger.info("Number of dataNodes to replicate :", dataNodesCount);

      if (dataNodesCount < replicaThreadCount) {
        logger.warn("Number of replica threads: {} is more than the number of nodes to replicate from: {}",
            replicaThreadCount, dataNodesCount);
        replicaThreadCount = dataNodesCount;
      }

      ResponseHandler responseHandler = new ResponseHandler(clusterMap);

      int numberOfNodesPerThread = dataNodesCount / replicaThreadCount;
      int remainingNodes = dataNodesCount % replicaThreadCount;

      Iterator<DataNodeId> dataNodeIdIterator = dataNodesToReplicate.iterator();

      for (int i = 0; i < replicaThreadCount; i++) {
        // create the list of nodes for the replica thread
        Map<DataNodeId, List<RemoteReplicaInfo>> replicasForThread = new HashMap<DataNodeId, List<RemoteReplicaInfo>>();
        int nodesAssignedToThread = 0;
        while (nodesAssignedToThread < numberOfNodesPerThread) {
          DataNodeId dataNodeToReplicate = dataNodeIdIterator.next();
          replicasForThread.put(dataNodeToReplicate,
              dataNodeRemoteReplicaInfos.getRemoteReplicaListForDataNode(dataNodeToReplicate));
          dataNodeIdIterator.remove();
          nodesAssignedToThread++;
        }
        if (remainingNodes > 0) {
          DataNodeId dataNodeToReplicate = dataNodeIdIterator.next();
          replicasForThread.put(dataNodeToReplicate,
              dataNodeRemoteReplicaInfos.getRemoteReplicaListForDataNode(dataNodeToReplicate));
          dataNodeIdIterator.remove();
          remainingNodes--;
        }
        boolean replicatingOverSsl = sslEnabledDatacenters.contains(datacenter);
        String threadIdentity =
            "Replica Thread-" + (dataNodeId.getDatacenterName().equals(datacenter) ? "Intra-" : "Inter") + i
                + datacenter;
        try {
          StoreKeyConverter threadSpecificKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
          Transformer threadSpecificTransformer =
              Utils.getObj(transformerClassName, storeKeyFactory, threadSpecificKeyConverter);
          ReplicaThread replicaThread =
              new ReplicaThread(threadIdentity, replicasForThread, factory, clusterMap, correlationIdGenerator,
                  dataNodeId, connectionPool, replicationConfig, replicationMetrics, notification,
                  threadSpecificKeyConverter, threadSpecificTransformer, metricRegistry, replicatingOverSsl, datacenter,
                  responseHandler, SystemTime.getInstance());
          if (replicaThreadPools.containsKey(datacenter)) {
            replicaThreadPools.get(datacenter).add(replicaThread);
          } else {
            replicaThreadPools.put(datacenter, new ArrayList<>(Arrays.asList(replicaThread)));
          }
        } catch (Exception e) {
          throw new IOException("Encountered exception instantiating ReplicaThread", e);
        }
      }
    }
  }

  /**
   * Reads the replica tokens from storage, populates the Remote replica info,
   * and re-persists the tokens if they have been reset.
   * @param mountPath The mount path where the replica tokens are stored
   * @throws ReplicationException
   * @throws IOException
   */
  private void retrieveReplicaTokensAndPersistIfNecessary(String mountPath) throws ReplicationException, IOException {
    logger.info("Reading replica tokens for mount path {}", mountPath);

    boolean tokenWasReset = false;
    long readStartTimeMs = SystemTime.getInstance().milliseconds();
    try {
      List<ReplicaTokenInfo> tokenInfoList = persistor.retrieveTokens(mountPath);

      for (ReplicaTokenInfo tokenInfo : tokenInfoList) {
        String hostname = tokenInfo.getHostname();
        int port = tokenInfo.getPort();
        PartitionId partitionId = tokenInfo.getPartitionId();
        FindToken token = tokenInfo.getReplicaToken();
        // update token
        PartitionInfo partitionInfo = partitionsToReplicate.get(tokenInfo.getPartitionId());
        if (partitionInfo != null) {
          boolean updatedToken = false;
          for (RemoteReplicaInfo remoteReplicaInfo : partitionInfo.getRemoteReplicaInfos()) {
            DataNodeId dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
            if (dataNodeId.getHostname().equalsIgnoreCase(hostname) && dataNodeId.getPort() == port && remoteReplicaInfo
                .getReplicaId()
                .getReplicaPath()
                .equals(tokenInfo.getReplicaPath())) {
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
          // If this partition was not found in partitionsToReplicate, it means that the local store corresponding
          // to this partition could not be started. In such a case, the tokens for its remote replicas should be
          // reset.
          tokenWasReset = true;
          logTokenReset(partitionId, hostname, port, token);
        }
      }
    } catch (IOException e) {
      throw new ReplicationException("IO error while reading from replica token file " + e);
    } finally {
      replicationMetrics.remoteReplicaTokensRestoreTime.update(
          SystemTime.getInstance().milliseconds() - readStartTimeMs);
    }

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

  /**
   * Holds a list of {@link DataNodeId} for a Datacenter
   * Also contains the mapping of {@link RemoteReplicaInfo} list for every {@link DataNodeId}
   */
  class DataNodeRemoteReplicaInfos {
    private Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaLists;

    DataNodeRemoteReplicaInfos(RemoteReplicaInfo remoteReplicaInfo) {
      this.dataNodeToReplicaLists = new HashMap<>();
      this.dataNodeToReplicaLists.put(remoteReplicaInfo.getReplicaId().getDataNodeId(),
          new ArrayList<>(Collections.singletonList(remoteReplicaInfo)));
    }

    void addRemoteReplica(RemoteReplicaInfo remoteReplicaInfo) {
      DataNodeId dataNodeIdToReplicate = remoteReplicaInfo.getReplicaId().getDataNodeId();
      List<RemoteReplicaInfo> replicaInfos = dataNodeToReplicaLists.get(dataNodeIdToReplicate);
      if (replicaInfos == null) {
        replicaInfos = new ArrayList<>();
      }
      replicaInfos.add(remoteReplicaInfo);
      dataNodeToReplicaLists.put(dataNodeIdToReplicate, replicaInfos);
    }

    Set<DataNodeId> getDataNodeIds() {
      return this.dataNodeToReplicaLists.keySet();
    }

    List<RemoteReplicaInfo> getRemoteReplicaListForDataNode(DataNodeId dataNodeId) {
      return dataNodeToReplicaLists.get(dataNodeId);
    }
  }
}
