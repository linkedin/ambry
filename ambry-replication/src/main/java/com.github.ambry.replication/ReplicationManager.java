package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.notification.NotificationSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreManager;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


final class RemoteReplicaInfo {
  private final ReplicaId replicaId;
  private final ReplicaId localReplicaId;
  private final Object lock = new Object();

  // tracks the point up to which a node is in sync with a remote replica
  private final long tokenPersistIntervalInMs;
  private FindToken currentToken = null;
  private FindToken tokenToPersist = null;
  private long timeTokenSetInMs;
  private FindToken tokenPersisted = null;
  private final Store localStore;
  private long totalBytesReadFromLocalStore;

  public RemoteReplicaInfo(ReplicaId replicaId, ReplicaId localReplicaId, Store localStore, FindToken token,
      long tokenPersistIntervalInMs) {
    this.replicaId = replicaId;
    this.localReplicaId = localReplicaId;
    this.currentToken = token;
    if (tokenToPersist == null) {
      this.tokenToPersist = token;
      timeTokenSetInMs = SystemTime.getInstance().milliseconds();
    }
    this.tokenPersistIntervalInMs = tokenPersistIntervalInMs;
    this.totalBytesReadFromLocalStore = 0;
    this.localStore = localStore;
  }

  public ReplicaId getReplicaId() {
    return replicaId;
  }

  public ReplicaId getLocalReplicaId() {
    return localReplicaId;
  }

  public Store getLocalStore() {
    return localStore;
  }

  public long getReplicaLagInBytes() {
    if (localStore != null) {
      return this.localStore.getSizeInBytes() - this.totalBytesReadFromLocalStore;
    } else {
      return 0;
    }
  }

  public FindToken getToken() {
    synchronized (lock) {
      return currentToken;
    }
  }

  public void setTotalBytesReadFromLocalStore(long totalBytesReadFromLocalStore) {
    this.totalBytesReadFromLocalStore = totalBytesReadFromLocalStore;
  }

  public long getTotalBytesReadFromLocalStore() {
    return this.totalBytesReadFromLocalStore;
  }

  public void setToken(FindToken token) {
    // reference assignment is atomic in java but we want to be completely safe. performance is
    // not important here
    synchronized (lock) {
      this.currentToken = token;
    }
  }

  public FindToken getTokenToPersist() {
    synchronized (lock) {
      if ((SystemTime.getInstance().milliseconds() - timeTokenSetInMs) > tokenPersistIntervalInMs) {
        return tokenToPersist;
      }
      return tokenPersisted;
    }
  }

  public void onTokenPersisted() {
    synchronized (lock) {
      this.tokenPersisted = tokenToPersist;
      this.tokenToPersist = currentToken;
      timeTokenSetInMs = SystemTime.getInstance().milliseconds();
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
public final class ReplicationManager {

  private final Map<PartitionId, PartitionInfo> partitionsToReplicate;
  private final Map<String, List<PartitionInfo>> partitionGroupedByMountPath;
  private final List<ReplicaThread> replicationIntraDCThreads;
  private final List<ReplicaThread> replicationInterDCThreads;
  private final ReplicationConfig replicationConfig;
  private final FindTokenFactory factory;
  private final ClusterMap clusterMap;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ReplicaTokenPersistor persistor;
  private final Scheduler scheduler;
  private final AtomicInteger correlationIdGenerator;
  private final DataNodeId dataNodeId;
  private final ConnectionPool connectionPool;
  private final ReplicationMetrics replicationMetrics;
  private final NotificationSystem notification;
  private final Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateIntraDC;
  private final Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateInterDC;

  private static final String replicaTokenFileName = "replicaTokens";
  private static final short Crc_Size = 8;
  private static final short Replication_Delay_Multiplier = 5;

  public ReplicationManager(ReplicationConfig replicationConfig, StoreConfig storeConfig, StoreManager storeManager,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, Scheduler scheduler, DataNodeId dataNode,
      ConnectionPool connectionPool, MetricRegistry metricRegistry, NotificationSystem requestNotification)
      throws ReplicationException {

    try {
      this.replicationConfig = replicationConfig;
      this.factory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);
      this.replicationIntraDCThreads =
          new ArrayList<ReplicaThread>(replicationConfig.replicationNumOfIntraDCReplicaThreads);
      this.replicationInterDCThreads =
          new ArrayList<ReplicaThread>(replicationConfig.replicationNumOfInterDCReplicaThreads);
      this.replicationMetrics =
          new ReplicationMetrics(metricRegistry, replicationIntraDCThreads, replicationInterDCThreads);
      this.partitionGroupedByMountPath = new HashMap<String, List<PartitionInfo>>();
      this.partitionsToReplicate = new HashMap<PartitionId, PartitionInfo>();
      this.clusterMap = clusterMap;
      this.scheduler = scheduler;
      this.persistor = new ReplicaTokenPersistor();
      this.correlationIdGenerator = new AtomicInteger(0);
      this.dataNodeId = dataNode;
      List<ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
      this.connectionPool = connectionPool;
      this.notification = requestNotification;
      this.replicasToReplicateIntraDC = new HashMap<DataNodeId, List<RemoteReplicaInfo>>();
      this.replicasToReplicateInterDC = new HashMap<DataNodeId, List<RemoteReplicaInfo>>();

      // initialize all partitions
      for (ReplicaId replicaId : replicaIds) {
        List<ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
        if (peerReplicas != null) {
          List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>(peerReplicas.size());
          for (ReplicaId remoteReplica : peerReplicas) {
            // We need to ensure that a replica token gets persisted only after the corresponding data in the
            // store gets flushed to disk. We use the store flush interval multiplied by a constant factor
            // to determine the token flush interval
            RemoteReplicaInfo remoteReplicaInfo =
                new RemoteReplicaInfo(remoteReplica, replicaId, storeManager.getStore(replicaId.getPartitionId()),
                    factory.getNewFindToken(), storeConfig.storeDataFlushIntervalSeconds *
                    SystemTime.MsPerSec * Replication_Delay_Multiplier);
            replicationMetrics.addRemoteReplicaToLagMetrics(remoteReplicaInfo);
            replicationMetrics.createRemoteReplicaErrorMetrics(remoteReplicaInfo);
            remoteReplicas.add(remoteReplicaInfo);
            if (dataNodeId.getDatacenterName().compareToIgnoreCase(remoteReplica.getDataNodeId().getDatacenterName())
                == 0) {
              updateReplicasToReplicate(replicasToReplicateIntraDC, remoteReplicaInfo);
            } else {
              updateReplicasToReplicate(replicasToReplicateInterDC, remoteReplicaInfo);
            }
          }
          PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas, replicaId.getPartitionId(),
              storeManager.getStore(replicaId.getPartitionId()), replicaId);
          partitionsToReplicate.put(replicaId.getPartitionId(), partitionInfo);
          List<PartitionInfo> partitionInfos = partitionGroupedByMountPath.get(replicaId.getMountPath());
          if (partitionInfos == null) {
            partitionInfos = new ArrayList<PartitionInfo>();
          }
          partitionInfos.add(partitionInfo);
          partitionGroupedByMountPath.put(replicaId.getMountPath(), partitionInfos);
        }
      }
    } catch (Exception e) {
      logger.error("Error on starting replication manager", e);
      throw new ReplicationException("Error on starting replication manager");
    }
  }

  public void start()
      throws ReplicationException {

    try {
      // read stored tokens
      // iterate through all mount paths and read replication info for the partitions it owns
      for (String mountPath : partitionGroupedByMountPath.keySet()) {
        readFromFile(mountPath);
      }
      // divide the nodes between the replica threads if the number of replica threads is less than or equal to the
      // number of nodes. Otherwise, assign one thread to one node.
      logger.info("Number of intra DC replica threads: " + replicationConfig.replicationNumOfIntraDCReplicaThreads);
      logger.info("Number of intra DC nodes to replicate from: " + replicasToReplicateIntraDC.size());
      assignReplicasToThreads(replicasToReplicateIntraDC, replicationConfig.replicationNumOfIntraDCReplicaThreads,
        replicationIntraDCThreads, "Intra DC");

      logger.info("Number of inter DC replica threads: " + replicationConfig.replicationNumOfInterDCReplicaThreads);
      logger.info("Number of inter DC nodes to replicate from: " + replicasToReplicateInterDC.size());
      assignReplicasToThreads(replicasToReplicateInterDC, replicationConfig.replicationNumOfInterDCReplicaThreads,
        replicationInterDCThreads, "Inter DC");

      // start all replica threads
      for (ReplicaThread thread : replicationIntraDCThreads) {
        Thread replicaThread = Utils.newThread(thread.getName(), thread, false);
        logger.info("Starting intra DC replica thread " + thread.getName());
        replicaThread.start();
      }

      // start all replica threads
      for (ReplicaThread thread : replicationInterDCThreads) {
        Thread replicaThread = Utils.newThread(thread.getName(), thread, false);
        logger.info("Starting inter DC replica thread " + thread.getName());
        replicaThread.start();
      }

      // start background persistent thread
      // start scheduler thread to persist index in the background
      this.scheduler.schedule("replica token persistor", persistor, replicationConfig.replicationTokenFlushDelaySeconds,
          replicationConfig.replicationTokenFlushIntervalSeconds, TimeUnit.SECONDS);
    } catch (IOException e) {
      logger.error("IO error while starting replication");
    }
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
  public long getRemoteReplicaLagInBytes(PartitionId partitionId, String hostName, String replicaPath) {
    RemoteReplicaInfo remoteReplicaInfo = getRemoteReplicaInfo(partitionId, hostName, replicaPath);
    if (remoteReplicaInfo != null) {
      return remoteReplicaInfo.getReplicaLagInBytes();
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
          .getDataNodeId().getHostname().equals(hostName)) {
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
  public void shutdown()
      throws ReplicationException {
    try {
      // stop all replica threads
      for (ReplicaThread replicaThread : replicationIntraDCThreads) {
        replicaThread.shutdown();
      }
      for (ReplicaThread replicaThread : replicationInterDCThreads) {
        replicaThread.shutdown();
      }
      // persist replica tokens
      persistor.write();
    } catch (Exception e) {
      logger.error("Error shutting down replica manager {}", e);
      throw new ReplicationException("Error shutting down replica manager");
    }
  }

  /**
   * Updates the replicasToReplicateMap with the remoteReplicaInfo.
   * @param replicasToReplicateMap The map that contains mapping between data nodes and the remote replicas
   * @param remoteReplicaInfo The remote replica that needs to be added to the map
   */
  private void updateReplicasToReplicate(Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateMap,
      RemoteReplicaInfo remoteReplicaInfo) {
    List<RemoteReplicaInfo> replicaListForNode =
        replicasToReplicateMap.get(remoteReplicaInfo.getReplicaId().getDataNodeId());
    if (replicaListForNode == null) {
      replicaListForNode = new ArrayList<RemoteReplicaInfo>();
      replicaListForNode.add(remoteReplicaInfo);
      replicasToReplicateMap.put(remoteReplicaInfo.getReplicaId().getDataNodeId(), replicaListForNode);
    } else {
      replicaListForNode.add(remoteReplicaInfo);
    }
  }

  /**
   * Partitions the list of data node to remote replica list mapping between given set of replica threads
   * @param replicasToReplicate Map of data nodes to remote replicas
   * @param numberOfReplicaThreads The total number of replica threads between which the partition needs to be done
   * @param replicaThreadList The list of replica threads
   * @param threadIdentity The identity that uniquely identifies the group of threads
   */
  private void assignReplicasToThreads(Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate,
      int numberOfReplicaThreads, List<ReplicaThread> replicaThreadList, String threadIdentity) {
    if (replicasToReplicate.size() < numberOfReplicaThreads) {
      numberOfReplicaThreads = replicasToReplicate.size();
    }
    int numberOfNodesPerThread = replicasToReplicate.size() / numberOfReplicaThreads;
    int remainingNodes = replicasToReplicate.size() % numberOfReplicaThreads;

    Iterator<Map.Entry<DataNodeId, List<RemoteReplicaInfo>>> mapIterator = replicasToReplicate.entrySet().iterator();

    for (int i = 0; i < numberOfReplicaThreads; i++) {
      // create the list of nodes for the replica thread
      Map<DataNodeId, List<RemoteReplicaInfo>> replicasForThread = new HashMap<DataNodeId, List<RemoteReplicaInfo>>();
      int nodesAssignedToThread = 0;
      while (nodesAssignedToThread < numberOfNodesPerThread) {
        Map.Entry<DataNodeId, List<RemoteReplicaInfo>> mapEntry = mapIterator.next();
        replicasForThread.put(mapEntry.getKey(), mapEntry.getValue());
        mapIterator.remove();
        nodesAssignedToThread++;
      }
      if (remainingNodes > 0) {
        Map.Entry<DataNodeId, List<RemoteReplicaInfo>> mapEntry = mapIterator.next();
        replicasForThread.put(mapEntry.getKey(), mapEntry.getValue());
        mapIterator.remove();
        remainingNodes--;
      }
      ReplicaThread replicaThread =
          new ReplicaThread("Replica Thread-" + threadIdentity + "-" + i, replicasForThread, factory, clusterMap,
              correlationIdGenerator, dataNodeId, connectionPool, replicationConfig, replicationMetrics, notification);
      replicaThreadList.add(replicaThread);
    }
  }

  /**
   * Reads the replica tokens from the file and populates the Remote replica info
   * @param mountPath The mount path where the replica tokens are stored
   * @throws ReplicationException
   * @throws IOException
   */
  private void readFromFile(String mountPath)
      throws ReplicationException, IOException {
    logger.info("Reading replica tokens for mount path {}", mountPath);
    long readStartTimeMs = SystemTime.getInstance().milliseconds();
    File replicaTokenFile = new File(mountPath, replicaTokenFileName);
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
              boolean updatedToken = false;
              for (RemoteReplicaInfo info : partitionInfo.getRemoteReplicaInfos()) {
                if (info.getReplicaId().getDataNodeId().getHostname().equalsIgnoreCase(hostname)
                    && info.getReplicaId().getDataNodeId().getPort() == port && info.getReplicaId().getReplicaPath()
                    .equals(replicaPath)) {
                  info.setToken(token);
                  info.setTotalBytesReadFromLocalStore(totalBytesReadFromLocalStore);
                  logger
                      .info("Read token for partition {} remote host {} port {} token {}", partitionId, hostname, port,
                          token);
                  updatedToken = true;
                  break;
                }
              }
              if (!updatedToken) {
                logger.warn("Persisted remote replica host {} and port {} not present in new cluster ", hostname, port);
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
        replicationMetrics.remoteReplicaTokensRestoreTime
            .update(SystemTime.getInstance().milliseconds() - readStartTimeMs);
      }
    }
  }

  class ReplicaTokenPersistor implements Runnable {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private final short version = 0;

    /**
     * Iterates through each mount path and persists all the replica tokens for the partitions on the mount
     * path to a file. The file is saved on the corresponding mount path
     */
    public void write()
        throws IOException, ReplicationException {
      long writeStartTimeMs = SystemTime.getInstance().milliseconds();
      for (String mountPath : partitionGroupedByMountPath.keySet()) {
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
                writer.write(remoteReplica.getTokenToPersist().toBytes());
                remoteReplica.onTokenPersisted();
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
          replicationMetrics.remoteReplicaTokensPersistTime
              .update(SystemTime.getInstance().milliseconds() - writeStartTimeMs);
        }
        logger.debug("Completed writing replica tokens to file {}", actual.getAbsolutePath());
      }
    }

    public void run() {
      try {
        write();
      } catch (Exception e) {
        logger.info("Error while persisting the replica tokens {}", e);
      }
    }
  }
}
