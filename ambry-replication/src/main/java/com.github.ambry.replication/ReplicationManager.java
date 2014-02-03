package com.github.ambry.replication;

import com.github.ambry.clustermap.*;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.store.*;
import com.github.ambry.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class RemoteReplicaInfo {
  private final ReplicaId replicaId;
  private final Object lock = new Object();
  private final long tokenPersistInterval;
  private FindToken currentToken = null;
  private FindToken tokenToPersist = null;
  private long timeTokenSet;
  private FindToken tokenPersisted = null;

  public RemoteReplicaInfo(ReplicaId replicaId, FindToken token, long tokenPersistInterval) {
    this.replicaId = replicaId;
    this.currentToken = token;
    if (tokenToPersist == null) {
      this.tokenToPersist = token;
      timeTokenSet = SystemTime.getInstance().milliseconds();
    }
    this.tokenPersistInterval = tokenPersistInterval;
  }

  public ReplicaId getReplicaId() {
    return replicaId;
  }

  public FindToken getToken() {
    synchronized (lock) {
      return currentToken;
    }
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
      if (SystemTime.getInstance().milliseconds() - timeTokenSet > tokenPersistInterval) {
        return tokenToPersist;
      }
      return tokenPersisted;
    }
  }

  public void onTokenPersisted() {
    synchronized (lock) {
      this.tokenPersisted = tokenToPersist;
      this.tokenToPersist = currentToken;
      timeTokenSet = SystemTime.getInstance().milliseconds();
    }
  }
}

final class PartitionInfo {

  private final List<RemoteReplicaInfo> remoteReplicas;
  private final PartitionId partitionId;
  private final Store store;

  public PartitionInfo(List<RemoteReplicaInfo> remoteReplicas, PartitionId partitionId, Store store) {
    this.remoteReplicas = remoteReplicas;
    this.partitionId = partitionId;
    this.store = store;
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

  private static final String replicaTokenFileName = "replicaTokens";
  private static final short Crc_Size = 8;
  private static final short Replication_Delay = 5;



  public ReplicationManager(ReplicationConfig replicationConfig,
                            StoreConfig storeConfig,
                            StoreManager storeManager,
                            StoreKeyFactory storeKeyFactory,
                            ClusterMap clusterMap,
                            Scheduler scheduler,
                            DataNodeId dataNode,
                            ConnectionPool connectionPool) throws ReplicationException {

    try {
      this.replicationConfig = replicationConfig;
      this.factory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);
      this.replicaThreads = new ArrayList<ReplicaThread>(replicationConfig.replicationNoOfReplicaThreads);
      this.partitionGroupedByMountPath = new HashMap<String, List<PartitionInfo>>();
      this.partitionsToReplicate = new HashMap<PartitionId, PartitionInfo>();
      this.clusterMap = clusterMap;
      this.scheduler = scheduler;
      this.persistor = new ReplicaTokenPersistor();
      this.correlationIdGenerator = new AtomicInteger(0);
      this.dataNodeId = dataNode;
      List<ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
      this.connectionPool = connectionPool;

      // initialize all partitions
      for (ReplicaId replicaId : replicaIds) {
        List<ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
        if (peerReplicas != null) {
          List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>(peerReplicas.size());
          for (ReplicaId remoteReplica : replicaId.getPeerReplicaIds()) {
            RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(remoteReplica,
                                                                        factory.getNewFindToken(),
                                                                        storeConfig.storeDataFlushIntervalSeconds * Replication_Delay);
            remoteReplicas.add(remoteReplicaInfo);
          }
          PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas,
                                                          replicaId.getPartitionId(),
                                                          storeManager.getStore(replicaId.getPartitionId()));
          partitionsToReplicate.put(replicaId.getPartitionId(), partitionInfo);
          List<PartitionInfo> partitionInfos = partitionGroupedByMountPath.get(replicaId.getMountPath());
          if (partitionInfos == null) {
            partitionInfos = new ArrayList<PartitionInfo>();
          }
          partitionInfos.add(partitionInfo);
          partitionGroupedByMountPath.put(replicaId.getMountPath(), partitionInfos);
        }
      }
    }
    catch (Exception e) {
      logger.error("Error on starting replication {}", e);
      throw new ReplicationException("Error on starting replication");
    }
  }

  public void start() throws ReplicationException {

    try {
      // read stored tokens
      // iterate through all mount paths and read replication info for the partitions it owns
      for (String mountPath : partitionGroupedByMountPath.keySet()) {
        readFromFile(mountPath);
      }

      // start replica threads and divide the partitions between them
      // if the number of replica threads is less than the number of mount paths
      if (partitionGroupedByMountPath.size() > replicaThreads.size()) {
        int numberOfMountPathPerThread = partitionGroupedByMountPath.size() / replicaThreads.size();
        int remainingMountPaths = partitionGroupedByMountPath.size() % replicaThreads.size();
        Iterator<Map.Entry<String, List<PartitionInfo>>> mountPathEntries =
                partitionGroupedByMountPath.entrySet().iterator();
        int currentMountPath = 0;
        for (int i = 0; i < replicaThreads.size(); i++) {
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
          ReplicaThread replicaThread = new ReplicaThread(partitionInfoList,
                                                          factory,
                                                          clusterMap,
                                                          correlationIdGenerator,
                                                          dataNodeId,
                                                          connectionPool,
                                                          replicationConfig.replicationConnectionPoolCheckoutTimeoutMs);
          replicaThreads.add(replicaThread);
        }
      }
      else {

      }
      // start all replica threads
      for (ReplicaThread thread : replicaThreads)
        thread.run();

      // start background persistent thread
      // start scheduler thread to persist index in the background
      this.scheduler.schedule("replica token persistor",
                              persistor,
                              replicationConfig.replicationTokenFlushDelaySeconds,
                              replicationConfig.replicationTokenFlushIntervalSeconds,
                              TimeUnit.SECONDS);

    }
    catch (IOException e) {
      logger.error("IO error while starting replication");
    }
  }

  public void shutdown() throws ReplicationException {
    try {
      // stop all replica threads
      for (ReplicaThread replicaThread : replicaThreads) {
        replicaThread.shutdown();
      }
      // persist replica tokens
      persistor.write();
    }
    catch (Exception e) {
      logger.error("Error shutting down replica manager {}", e);
      throw new ReplicationException("Error shuting down replica manager");
    }
  }

  private void readFromFile(String mountPath) throws ReplicationException, IOException {
    logger.info("Reading replica tokens for mount path {}", mountPath);
    CrcInputStream crcStream = new CrcInputStream(new FileInputStream(new File(mountPath, replicaTokenFileName)));
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
            // read remote port
            int port = Integer.parseInt(Utils.readIntString(stream));
            // read replica token
            FindToken token = factory.getFindToken(stream);
            // update token
            PartitionInfo partitionInfo = partitionsToReplicate.get(partitionId);
            boolean updatedToken = false;
            for (RemoteReplicaInfo info : partitionInfo.getRemoteReplicaInfo()) {
              if (info.getReplicaId().getDataNodeId().getHostname().equalsIgnoreCase(hostname) &&
                  info.getReplicaId().getDataNodeId().getPort() == port) {
                info.setToken(token);
                updatedToken = true;
              }
            }
            if (!updatedToken)
              logger.warn("Persisted remote replica host {} and port {} not present in new cluster ", hostname, port);
          }
          long crc = crcStream.getValue();
          if (crc != stream.readLong()) {
            throw new ReplicationException("Crc check does not match for replica token file for mount path " + mountPath);
          }
          break;
        default:
          throw new ReplicationException("Invalid version in replica token file for mount path " + mountPath);
      }
    }
    catch (IOException e) {
      throw new ReplicationException("IO error while reading from replica token file " +  e);
    }
    finally {
      stream.close();
    }
  }

  class ReplicaTokenPersistor implements Runnable {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private final short version = 1;
    // TODO add metric to indicate existence of thread
    // TODO add metric to indicate time taken

    /**
     * Iterates through each mount path and persists all the replica tokens for the partitions on the mount
     * path to a file. The file is saved on the corresponding mount path
     */
    public void write() throws IOException, ReplicationException {
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
              writer.write(info.getPartitionId().getBytes());
              writer.writeInt(remoteReplica.getReplicaId().getDataNodeId().getHostname().length());
              writer.write(remoteReplica.getReplicaId().getDataNodeId().getHostname().getBytes());
              writer.writeInt(remoteReplica.getReplicaId().getDataNodeId().getPort());
              writer.write(remoteReplica.getTokenToPersist().toBytes());
            }
          }
          long crcValue = crc.getValue();
          writer.writeLong(crcValue);

          // flush and overwrite old file
          fileStream.getChannel().force(true);
          // swap temp file with the original file
          temp.renameTo(actual);
        }
        catch (IOException e) {
          logger.error("IO error while persisting tokens to disk {}", temp.getAbsoluteFile());
          throw new ReplicationException("IO error while persisting replica tokens to disk ");
        }
        finally {
          writer.close();
        }
        logger.debug("Completed writing replica tokens to file {}", actual.getAbsolutePath());
      }
    }

    public void run() {
      try {
        write();
      }
      catch (Exception e) {
        logger.info("Error while persisting the replica tokens {}", e);
      }
    }
  }
}
