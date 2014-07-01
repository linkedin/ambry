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
import com.github.ambry.shared.ConnectionPool;
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
  private final Object lock = new Object();

  // tracks the point up to which a node is in sync with a remote replica
  private final long tokenPersistIntervalInMs;
  private FindToken currentToken = null;
  private FindToken tokenToPersist = null;
  private long timeTokenSetInMs;
  private FindToken tokenPersisted = null;
  private final Store localStore;
  private long totalBytesReadFromLocalStore;

  public RemoteReplicaInfo(ReplicaId replicaId, Store localStore, FindToken token, long tokenPersistIntervalInMs) {
    this.replicaId = replicaId;
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

  public List<RemoteReplicaInfo> getRemoteReplicaInfo() {
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
  private final List<ReplicaThread> replicaThreads;
  private final Map<String, List<PartitionInfo>> partitionGroupedByMountPath;
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
      this.replicaThreads = new ArrayList<ReplicaThread>(replicationConfig.replicationNumReplicaThreads);
      this.replicationMetrics =
          new ReplicationMetrics("replication-" + dataNode.getHostname() + "-" + dataNode.getPort(), metricRegistry,
              replicaThreads);
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

      // initialize all partitions
      for (ReplicaId replicaId : replicaIds) {
        List<ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
        if (peerReplicas != null) {
          List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>(peerReplicas.size());
          for (ReplicaId remoteReplica : peerReplicas) {
            // We need to ensure that replica tokens gets persisted only after the corresponding data in the
            // store gets flushed to disk. We use the store flush interval multiplied by a constant factor
            // to determine the token flush interval
            RemoteReplicaInfo remoteReplicaInfo =
                new RemoteReplicaInfo(remoteReplica, storeManager.getStore(replicaId.getPartitionId()),
                    factory.getNewFindToken(), storeConfig.storeDataFlushIntervalSeconds *
                    SystemTime.MsPerSec * Replication_Delay_Multiplier);
            replicationMetrics.addRemoteReplicaToLagMetrics(remoteReplicaInfo);
            remoteReplicas.add(remoteReplicaInfo);
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

      // start replica threads and divide the partitions between them
      // if the number of replica threads is less than or equal to the number of mount paths
      logger.info("replica threads " + replicationConfig.replicationNumReplicaThreads);
      if (partitionGroupedByMountPath.size() >= replicationConfig.replicationNumReplicaThreads) {
        logger.info("Number of replica threads is less than or equal to the number of mount paths");
        int numberOfMountPathPerThread =
            partitionGroupedByMountPath.size() / replicationConfig.replicationNumReplicaThreads;
        int remainingMountPaths = partitionGroupedByMountPath.size() % replicationConfig.replicationNumReplicaThreads;
        Iterator<Map.Entry<String, List<PartitionInfo>>> mountPathEntries =
            partitionGroupedByMountPath.entrySet().iterator();
        for (int i = 0; i < replicationConfig.replicationNumReplicaThreads; i++) {
          // create the list of partition info for the replica thread
          List<PartitionInfo> partitionInfoList = new ArrayList<PartitionInfo>();
          int mountPathAssignedToThread = 0;
          while (mountPathAssignedToThread < numberOfMountPathPerThread) {
            partitionInfoList.addAll(mountPathEntries.next().getValue());
            mountPathAssignedToThread++;
          }
          if (remainingMountPaths > 0) {
            partitionInfoList.addAll(mountPathEntries.next().getValue());
            remainingMountPaths--;
          }
          ReplicaThread replicaThread =
              new ReplicaThread("Replica Thread " + i, partitionInfoList, factory, clusterMap, correlationIdGenerator,
                  dataNodeId, connectionPool, replicationConfig, replicationMetrics, notification);
          replicaThreads.add(replicaThread);
        }
      } else {

      }
      // start all replica threads
      for (ReplicaThread thread : replicaThreads) {
        Thread replicaThread = Utils.newThread(thread.getName(), thread, false);
        logger.info("Starting replica thread " + thread.getName());
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
   * Used to fetch the remote replica from the local store with the given PartitionId, ReplicaPath and Hostname
   * @param partitionId PartitionId to which the replica belongs to
   * @param hostName HostName of the datanode where the replica belongs to
   * @param replicaPath Replica Path of the replica interested in
   * @return RemoteReplicaInfo
   */
  private RemoteReplicaInfo getRemoteReplicaInfo(PartitionId partitionId, String hostName, String replicaPath) {
    RemoteReplicaInfo remoteReplicaInfo = null;

    PartitionInfo partitionInfo = partitionsToReplicate.get(partitionId);
    for (RemoteReplicaInfo tempRemoteReplicaInfo : partitionInfo.getRemoteReplicaInfo()) {
      if (tempRemoteReplicaInfo.getReplicaId().getReplicaPath().equals(replicaPath) && tempRemoteReplicaInfo
          .getReplicaId().getDataNodeId().getHostname().equals(hostName)) {
        remoteReplicaInfo = tempRemoteReplicaInfo;
      }
    }
    if (remoteReplicaInfo == null) {
      replicationMetrics.unknownRemoteReplicaRequestCount.inc();
      logger.error("ReplicaMetaDataRequest from unknown Replica {}, with path {}", hostName, replicaPath);
    }
    return remoteReplicaInfo;
  }

  public void shutdown()
      throws ReplicationException {
    try {
      // stop all replica threads
      for (ReplicaThread replicaThread : replicaThreads) {
        replicaThread.shutdown();
      }
      // persist replica tokens
      persistor.write();
    } catch (Exception e) {
      logger.error("Error shutting down replica manager {}", e);
      throw new ReplicationException("Error shutting down replica manager");
    }
  }

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
              // read replicaPath
              String replicaPath = Utils.readIntString(stream);
              // read remote port
              int port = stream.readInt();
              // read totalBytesreadFromLocalStore
              long totalBytesReadFromLocalStore = stream.readLong();
              // read replica token
              FindToken token = factory.getFindToken(stream);
              // update token
              PartitionInfo partitionInfo = partitionsToReplicate.get(partitionId);
              boolean updatedToken = false;
              for (RemoteReplicaInfo info : partitionInfo.getRemoteReplicaInfo()) {
                if (info.getReplicaId().getDataNodeId().getHostname().equalsIgnoreCase(hostname)
                    && info.getReplicaId().getDataNodeId().getPort() == port
                    && info.getReplicaId().getReplicaPath().equals(replicaPath)) {
                  info.setToken(token);
                  info.setTotalBytesReadFromLocalStore(totalBytesReadFromLocalStore);
                  logger
                      .trace("Read token for partition {} remote host {} port {} token {}", partitionId, hostname, port,
                          token);
                  updatedToken = true;
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
            for (RemoteReplicaInfo remoteReplica : info.getRemoteReplicaInfo()) {
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
