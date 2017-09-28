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
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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


final class RemoteReplicaInfo {
  private final ReplicaId replicaId;
  private final ReplicaId localReplicaId;
  private final Object lock = new Object();
  private final Store localStore;
  private final Port port;
  private final Time time;
  // tracks the point up to which a node is in sync with a remote replica
  private final long tokenPersistIntervalInMs;

  // The latest known token
  private FindToken currentToken = null;
  // The token that will be safe to persist eventually
  private FindToken candidateTokenToPersist = null;
  // The time at which the candidate token is set
  private long timeCandidateSetInMs;
  // The token that is known to be safe to persist.
  private FindToken tokenSafeToPersist = null;
  private long totalBytesReadFromLocalStore;
  private long localLagFromRemoteStore = -1;

  RemoteReplicaInfo(ReplicaId replicaId, ReplicaId localReplicaId, Store localStore, FindToken token,
      long tokenPersistIntervalInMs, Time time, Port port) {
    this.replicaId = replicaId;
    this.localReplicaId = localReplicaId;
    this.totalBytesReadFromLocalStore = 0;
    this.localStore = localStore;
    this.time = time;
    this.port = port;
    this.tokenPersistIntervalInMs = tokenPersistIntervalInMs;
    initializeTokens(token);
  }

  ReplicaId getReplicaId() {
    return replicaId;
  }

  ReplicaId getLocalReplicaId() {
    return localReplicaId;
  }

  Store getLocalStore() {
    return localStore;
  }

  Port getPort() {
    return this.port;
  }

  long getRemoteLagFromLocalInBytes() {
    if (localStore != null) {
      return this.localStore.getSizeInBytes() - this.totalBytesReadFromLocalStore;
    } else {
      return 0;
    }
  }

  long getLocalLagFromRemoteInBytes() {
    return localLagFromRemoteStore;
  }

  FindToken getToken() {
    synchronized (lock) {
      return currentToken;
    }
  }

  void setTotalBytesReadFromLocalStore(long totalBytesReadFromLocalStore) {
    this.totalBytesReadFromLocalStore = totalBytesReadFromLocalStore;
  }

  void setLocalLagFromRemoteInBytes(long localLagFromRemoteStore) {
    this.localLagFromRemoteStore = localLagFromRemoteStore;
  }

  long getTotalBytesReadFromLocalStore() {
    return this.totalBytesReadFromLocalStore;
  }

  void setToken(FindToken token) {
    // reference assignment is atomic in java but we want to be completely safe. performance is
    // not important here
    synchronized (lock) {
      this.currentToken = token;
    }
  }

  void initializeTokens(FindToken token) {
    synchronized (lock) {
      this.currentToken = token;
      this.candidateTokenToPersist = token;
      this.tokenSafeToPersist = token;
      this.timeCandidateSetInMs = time.milliseconds();
    }
  }

  /*
   * The token persist logic ensures that a token corresponding to an entry in the store is never persisted in the
   * replicaTokens file before the entry itself is persisted in the store. This is done as follows. Objects of this
   * class maintain 3 tokens: tokenSafeToPersist, candidateTokenToPersist and currentToken:
   *
   * tokenSafeToPersist: this is the token that we know is safe to be persisted. The associated store entry from the
   * remote replica is guaranteed to have been persisted by the store.
   *
   * candidateTokenToPersist: this is the token that we would like to persist next. We would go ahead with this
   * only if we know for sure that the associated store entry has been persisted. We ensure safety by maintaining the
   * time at which candidateTokenToPersist was set and ensuring that tokenSafeToPersist is assigned this value only if
   * sufficient time has passed since the time we set candidateTokenToPersist.
   *
   * currentToken: this is the latest token associated with the latest record obtained from the remote replica.
   *
   * tokenSafeToPersist <= candidateTokenToPersist <= currentToken
   * (Note: when a token gets reset by the remote, the above equation may not hold true immediately after, but it should
   * eventually hold true.)
   */

  /**
   * get the token to persist. Returns either the candidate token if enough time has passed since it was
   * set, or the last token again.
   */
  FindToken getTokenToPersist() {
    synchronized (lock) {
      if (time.milliseconds() - timeCandidateSetInMs > tokenPersistIntervalInMs) {
        // candidateTokenToPersist is now safe to be persisted.
        tokenSafeToPersist = candidateTokenToPersist;
      }
      return tokenSafeToPersist;
    }
  }

  void onTokenPersisted() {
    synchronized (lock) {
      /* Only update the candidate token if it qualified as the token safe to be persisted in the previous get call.
       * If not, keep it as it is.
       */
      if (tokenSafeToPersist == candidateTokenToPersist) {
        candidateTokenToPersist = currentToken;
        timeCandidateSetInMs = time.milliseconds();
      }
    }
  }

  @Override
  public String toString() {
    return replicaId.toString();
  }
}

final class PartitionInfo {

  private final List<RemoteReplicaInfo> remoteReplicas;
  private final PartitionId partitionId;
  private final Store store;
  private final ReplicaId localReplicaId;

  public PartitionInfo(List<RemoteReplicaInfo> remoteReplicas, PartitionId partitionId, Store store,
      ReplicaId localReplicaId) {
    this.remoteReplicas = remoteReplicas;
    this.partitionId = partitionId;
    this.store = store;
    this.localReplicaId = localReplicaId;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public List<RemoteReplicaInfo> getRemoteReplicaInfos() {
    return remoteReplicas;
  }

  public Store getStore() {
    return store;
  }

  public ReplicaId getLocalReplicaId() {
    return this.localReplicaId;
  }

  @Override
  public String toString() {
    return partitionId.toString() + " " + remoteReplicas.toString();
  }
}

/**
 * Controls replication across all partitions. Responsible for the following
 * 1. Create replica threads and distribute partitions amongst the threads
 * 2. Set up replica token persistor used to recover from shutdown/crash
 * 3. Initialize and shutdown all the components required to perform replication
 */
public class ReplicationManager {

  private final Map<PartitionId, PartitionInfo> partitionsToReplicate;
  private final Map<String, List<PartitionInfo>> partitionGroupedByMountPath;
  private final ReplicationConfig replicationConfig;
  private final FindTokenFactory factory;
  private final ClusterMap clusterMap;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ReplicaTokenPersistor persistor;
  private final ScheduledExecutorService scheduler;
  private final AtomicInteger correlationIdGenerator;
  private final DataNodeId dataNodeId;
  private final ConnectionPool connectionPool;
  private final ReplicationMetrics replicationMetrics;
  private final NotificationSystem notification;
  private final Map<String, DataNodeRemoteReplicaInfos> dataNodeRemoteReplicaInfosPerDC;
  private final StoreKeyFactory storeKeyFactory;
  private final MetricRegistry metricRegistry;
  private final List<String> sslEnabledDatacenters;
  private final Map<String, List<ReplicaThread>> replicaThreadPools;
  private final Map<String, Integer> numberOfReplicaThreads;

  private static final String replicaTokenFileName = "replicaTokens";
  private static final short Crc_Size = 8;
  private static final short Replication_Delay_Multiplier = 5;

  public ReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StorageManager storageManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, ConnectionPool connectionPool,
      MetricRegistry metricRegistry, NotificationSystem requestNotification) throws ReplicationException {

    try {
      this.replicationConfig = replicationConfig;
      this.storeKeyFactory = storeKeyFactory;
      this.factory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);
      this.replicaThreadPools = new HashMap<>();
      this.replicationMetrics = new ReplicationMetrics(metricRegistry, clusterMap.getReplicaIds(dataNode));
      this.partitionGroupedByMountPath = new HashMap<>();
      this.partitionsToReplicate = new HashMap<>();
      this.clusterMap = clusterMap;
      this.scheduler = scheduler;
      this.persistor = new ReplicaTokenPersistor();
      this.correlationIdGenerator = new AtomicInteger(0);
      this.dataNodeId = dataNode;
      List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
      this.connectionPool = connectionPool;
      this.notification = requestNotification;
      this.metricRegistry = metricRegistry;
      this.dataNodeRemoteReplicaInfosPerDC = new HashMap<>();
      this.sslEnabledDatacenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
      this.numberOfReplicaThreads = new HashMap<>();

      // initialize all partitions
      for (ReplicaId replicaId : replicaIds) {
        PartitionId partition = replicaId.getPartitionId();
        Store store = storageManager.getStore(partition);
        if (store != null) {
          List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
          if (peerReplicas != null) {
            List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>(peerReplicas.size());
            for (ReplicaId remoteReplica : peerReplicas) {
              // We need to ensure that a replica token gets persisted only after the corresponding data in the
              // store gets flushed to disk. We use the store flush interval multiplied by a constant factor
              // to determine the token flush interval
              RemoteReplicaInfo remoteReplicaInfo =
                  new RemoteReplicaInfo(remoteReplica, replicaId, store, factory.getNewFindToken(),
                      storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec * Replication_Delay_Multiplier,
                      SystemTime.getInstance(), remoteReplica.getDataNodeId().getPortToConnectTo());
              replicationMetrics.addRemoteReplicaToLagMetrics(remoteReplicaInfo);
              replicationMetrics.createRemoteReplicaErrorMetrics(remoteReplicaInfo);
              remoteReplicas.add(remoteReplicaInfo);
              updateReplicasToReplicate(remoteReplica.getDataNodeId().getDatacenterName(), remoteReplicaInfo);
            }
            PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas, partition, store, replicaId);
            partitionsToReplicate.put(partition, partitionInfo);
            List<PartitionInfo> partitionInfos = partitionGroupedByMountPath.get(replicaId.getMountPath());
            if (partitionInfos == null) {
              partitionInfos = new ArrayList<>();
            }
            partitionInfos.add(partitionInfo);
            partitionGroupedByMountPath.put(replicaId.getMountPath(), partitionInfos);
          }
        } else {
          logger.error(
              "Not replicating to partition " + partition + " because an initialized store could not be found");
        }
      }
      replicationMetrics.populatePerColoMetrics(numberOfReplicaThreads.keySet());
    } catch (Exception e) {
      logger.error("Error on starting replication manager", e);
      throw new ReplicationException("Error on starting replication manager");
    }
  }

  public void start() throws ReplicationException {

    try {
      // read stored tokens
      // iterate through all mount paths and read replication info for the partitions it owns
      for (String mountPath : partitionGroupedByMountPath.keySet()) {
        readFromFileAndPersistIfNecessary(mountPath);
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
      // start scheduler thread to persist index in the background
      this.scheduler.scheduleAtFixedRate(persistor, replicationConfig.replicationTokenFlushDelaySeconds,
          replicationConfig.replicationTokenFlushIntervalSeconds, TimeUnit.SECONDS);
    } catch (IOException e) {
      logger.error("IO error while starting replication");
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
    if (foundRemoteReplicaInfo == null) {
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
      persistor.write(true);
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
  private void updateReplicasToReplicate(String datacenter, RemoteReplicaInfo remoteReplicaInfo) {
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
  private void assignReplicasToThreadPool() {
    Iterator<Map.Entry<String, DataNodeRemoteReplicaInfos>> mapIterator =
        dataNodeRemoteReplicaInfosPerDC.entrySet().iterator();
    while (mapIterator.hasNext()) {
      Map.Entry<String, DataNodeRemoteReplicaInfos> mapEntry = mapIterator.next();
      String datacenter = mapEntry.getKey();
      DataNodeRemoteReplicaInfos dataNodeRemoteReplicaInfos = mapEntry.getValue();
      Set<DataNodeId> dataNodesToReplicate = dataNodeRemoteReplicaInfos.getDataNodeIds();
      int dataNodesCount = dataNodesToReplicate.size();
      int replicaThreadCount = numberOfReplicaThreads.get(datacenter);
      if (replicaThreadCount <= 0) {
        logger.warn("Number of replica threads is smaller or equal to 0, not starting any replica threads for {} ",
            datacenter);
        return;
      } else if (dataNodesCount == 0) {
        logger.warn("Number of nodes to replicate from is 0, not starting any replica threads for {} ", datacenter);
        return;
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
        ReplicaThread replicaThread =
            new ReplicaThread(threadIdentity, replicasForThread, factory, clusterMap, correlationIdGenerator,
                dataNodeId, connectionPool, replicationConfig, replicationMetrics, notification, storeKeyFactory,
                replicationConfig.replicationValidateMessageStream, metricRegistry, replicatingOverSsl, datacenter,
                responseHandler);
        if (replicaThreadPools.containsKey(datacenter)) {
          replicaThreadPools.get(datacenter).add(replicaThread);
        } else {
          replicaThreadPools.put(datacenter, new ArrayList<>(Arrays.asList(replicaThread)));
        }
      }
    }
  }

  /**
   * Reads the replica tokens from the file and populates the Remote replica info
   * and persists the token file if necessary.
   * @param mountPath The mount path where the replica tokens are stored
   * @throws ReplicationException
   * @throws IOException
   */
  private void readFromFileAndPersistIfNecessary(String mountPath) throws ReplicationException, IOException {
    logger.info("Reading replica tokens for mount path {}", mountPath);
    long readStartTimeMs = SystemTime.getInstance().milliseconds();
    File replicaTokenFile = new File(mountPath, replicaTokenFileName);
    boolean tokenWasReset = false;
    if (replicaTokenFile.exists()) {
      CrcInputStream crcStream = new CrcInputStream(new FileInputStream(replicaTokenFile));
      DataInputStream stream = new DataInputStream(crcStream);
      try {
        short version = stream.readShort();
        switch (version) {
          case 0:
            while (stream.available() > Crc_Size) {
              // read partition id
              PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
              // read remote node host name
              String hostname = Utils.readIntString(stream);
              // read remote replica path
              String replicaPath = Utils.readIntString(stream);
              // read remote port
              int port = stream.readInt();
              // read total bytes read from local store
              long totalBytesReadFromLocalStore = stream.readLong();
              // read replica token
              FindToken token = factory.getFindToken(stream);
              // update token
              PartitionInfo partitionInfo = partitionsToReplicate.get(partitionId);
              if (partitionInfo != null) {
                boolean updatedToken = false;
                for (RemoteReplicaInfo remoteReplicaInfo : partitionInfo.getRemoteReplicaInfos()) {
                  if (remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname().equalsIgnoreCase(hostname)
                      && remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() == port
                      && remoteReplicaInfo.getReplicaId().getReplicaPath().equals(replicaPath)) {
                    logger.info("Read token for partition {} remote host {} port {} token {}", partitionId, hostname,
                        port, token);
                    if (partitionInfo.getStore().getSizeInBytes() > 0) {
                      remoteReplicaInfo.initializeTokens(token);
                      remoteReplicaInfo.setTotalBytesReadFromLocalStore(totalBytesReadFromLocalStore);
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
                  logger.warn("Persisted remote replica host {} and port {} not present in new cluster ", hostname,
                      port);
                }
              } else {
                // If this partition was not found in partitionsToReplicate, it means that the local store corresponding
                // to this partition could not be started. In such a case, the tokens for its remote replicas should be
                // reset.
                tokenWasReset = true;
                logTokenReset(partitionId, hostname, port, token);
              }
            }
            long crc = crcStream.getValue();
            if (crc != stream.readLong()) {
              throw new ReplicationException(
                  "Crc check does not match for replica token file for mount path " + mountPath);
            }
            break;
          default:
            throw new ReplicationException("Invalid version in replica token file for mount path " + mountPath);
        }
      } catch (IOException e) {
        throw new ReplicationException("IO error while reading from replica token file " + e);
      } finally {
        stream.close();
        replicationMetrics.remoteReplicaTokensRestoreTime.update(
            SystemTime.getInstance().milliseconds() - readStartTimeMs);
      }
    }

    if (tokenWasReset) {
      // We must ensure that the the token file is persisted if any of the tokens in the file got reset. We need to do
      // this before an associated store takes any writes, to avoid the case where a store takes writes and persists it,
      // before the replica token file is persisted after the reset.
      persistor.write(mountPath, false);
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

  class ReplicaTokenPersistor implements Runnable {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private final short version = 0;

    private void write(String mountPath, boolean shuttingDown) throws IOException, ReplicationException {
      long writeStartTimeMs = SystemTime.getInstance().milliseconds();
      File temp = new File(mountPath, replicaTokenFileName + ".tmp");
      File actual = new File(mountPath, replicaTokenFileName);
      FileOutputStream fileStream = new FileOutputStream(temp);
      CrcOutputStream crc = new CrcOutputStream(fileStream);
      DataOutputStream writer = new DataOutputStream(crc);
      try {
        // write the current version
        writer.writeShort(version);
        // Get all partitions for the mount path and persist the tokens for them
        for (PartitionInfo info : partitionGroupedByMountPath.get(mountPath)) {
          for (RemoteReplicaInfo remoteReplica : info.getRemoteReplicaInfos()) {
            FindToken tokenToPersist = remoteReplica.getTokenToPersist();
            if (tokenToPersist != null) {
              writer.write(info.getPartitionId().getBytes());
              writer.writeInt(remoteReplica.getReplicaId().getDataNodeId().getHostname().getBytes().length);
              writer.write(remoteReplica.getReplicaId().getDataNodeId().getHostname().getBytes());
              writer.writeInt(remoteReplica.getReplicaId().getReplicaPath().getBytes().length);
              writer.write(remoteReplica.getReplicaId().getReplicaPath().getBytes());
              writer.writeInt(remoteReplica.getReplicaId().getDataNodeId().getPort());
              writer.writeLong(remoteReplica.getTotalBytesReadFromLocalStore());
              writer.write(tokenToPersist.toBytes());
              remoteReplica.onTokenPersisted();
              if (shuttingDown) {
                logger.info("Persisting token {}", tokenToPersist);
              }
            }
          }
        }
        long crcValue = crc.getValue();
        writer.writeLong(crcValue);

        // flush and overwrite old file
        fileStream.getChannel().force(true);
        // swap temp file with the original file
        temp.renameTo(actual);
      } catch (IOException e) {
        logger.error("IO error while persisting tokens to disk {}", temp.getAbsoluteFile());
        throw new ReplicationException("IO error while persisting replica tokens to disk ");
      } finally {
        writer.close();
        replicationMetrics.remoteReplicaTokensPersistTime.update(
            SystemTime.getInstance().milliseconds() - writeStartTimeMs);
      }
      logger.debug("Completed writing replica tokens to file {}", actual.getAbsolutePath());
    }

    /**
     * Iterates through each mount path and persists all the replica tokens for the partitions on the mount
     * path to a file. The file is saved on the corresponding mount path
     * @param shuttingDown indicates whether this is being called as part of shut down
     */

    private void write(boolean shuttingDown) throws IOException, ReplicationException {
      for (String mountPath : partitionGroupedByMountPath.keySet()) {
        write(mountPath, shuttingDown);
      }
    }

    public void run() {
      try {
        write(false);
      } catch (Exception e) {
        logger.error("Error while persisting the replica tokens {}", e);
      }
    }
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
