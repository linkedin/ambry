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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaTokenInfo;
import com.github.ambry.replication.ReplicaTokenPersistor;
import com.github.ambry.replication.ReplicaTokenSerde;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.FindToken;
import com.github.ambry.utils.SystemTime;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link CloudTokenPersistor} persists replication token to a cloud storage.
 */
public class CloudTokenPersistor implements ReplicaTokenPersistor {

  private static final Logger logger = LoggerFactory.getLogger(CloudTokenPersistor.class);
  private final String replicaTokenFileName;
  private final Map<String, List<PartitionInfo>> partitionGroupedByMountPath;
  private final ReplicationMetrics replicationMetrics;
  private final ReplicaTokenSerde replicaTokenSerde;
  private final CloudDestination cloudDestination;

  /**
   * Constructor for {@link CloudTokenPersistor}.
   * @param replicaTokenFileName the token's file name.
   * @param partitionGroupedByMountPath A map between mount path and list of partitions under this mount path.
   * @param replicationMetrics metrics including token persist time.
   */
  public CloudTokenPersistor(String replicaTokenFileName, Map<String, List<PartitionInfo>> partitionGroupedByMountPath,
      ReplicationMetrics replicationMetrics, ReplicaTokenSerde replicaTokenSerde, CloudDestination cloudDestination) {
    this.replicaTokenFileName = replicaTokenFileName;
    this.partitionGroupedByMountPath = partitionGroupedByMountPath;
    this.replicationMetrics = replicationMetrics;
    this.replicaTokenSerde = replicaTokenSerde;
    this.cloudDestination = cloudDestination;
  }

  @Override
  public void write(String mountPath, boolean shuttingDown) throws IOException, ReplicationException {
    long writeStartTimeMs = SystemTime.getInstance().milliseconds();
    File temp = new File(mountPath, replicaTokenFileName + ".tmp");
    File actual = new File(mountPath, replicaTokenFileName);
    FileOutputStream fileStream = new FileOutputStream(temp);

    // Get all partitions for the mount path and persist the tokens for them
    List<RemoteReplicaInfo> replicasWithTokensPersisted = new ArrayList<>();
    List<ReplicaTokenInfo> tokenInfoList = new ArrayList<>();
    for (PartitionInfo info : partitionGroupedByMountPath.get(mountPath)) {
      for (RemoteReplicaInfo remoteReplica : info.getRemoteReplicaInfos()) {
        FindToken tokenToPersist = remoteReplica.getTokenToPersist();
        if (tokenToPersist != null) {
          replicasWithTokensPersisted.add(remoteReplica);
          DataNodeId dataNodeId = remoteReplica.getReplicaId().getDataNodeId();
          tokenInfoList.add(new ReplicaTokenInfo(info.getPartitionId(), dataNodeId.getHostname(),
              remoteReplica.getReplicaId().getReplicaPath(), dataNodeId.getPort(),
              remoteReplica.getTotalBytesReadFromLocalStore(), tokenToPersist));
        }
      }
    }

    try {
      replicaTokenSerde.write(tokenInfoList, fileStream);

      //TODO: cloudDestination.uploadTokenFile(partitionId, tokenFileName, inputStream);

      // flush and overwrite old file
      fileStream.getChannel().force(true);
      // swap temp file with the original file
      temp.renameTo(actual);
    } catch (IOException e) {
      logger.error("IO error while persisting tokens to disk {}", temp.getAbsoluteFile());
      throw new ReplicationException("IO error while persisting replica tokens to disk ");
    } finally {
      // TODO: could move this out
      replicationMetrics.remoteReplicaTokensPersistTime.update(
          SystemTime.getInstance().milliseconds() - writeStartTimeMs);
    }
    logger.debug("Completed writing replica tokens to file {}", actual.getAbsolutePath());

    if (shuttingDown) {
      for (RemoteReplicaInfo remoteReplica : replicasWithTokensPersisted) {
        logger.info("Persisted token {}", remoteReplica.getTokenToPersist());
        remoteReplica.onTokenPersisted();
      }
    }
  }

  @Override
  public void write(boolean shuttingDown) throws IOException, ReplicationException {
    for (String mountPath : partitionGroupedByMountPath.keySet()) {
      write(mountPath, shuttingDown);
    }
  }

  @Override
  public List<ReplicaTokenInfo> read(String mountPath) throws IOException, ReplicationException {
    File replicaTokenFile = new File(mountPath, replicaTokenFileName);
    if (replicaTokenFile.exists()) {
      List<ReplicaTokenInfo> tokenInfoList;
      try {
        return replicaTokenSerde.read(new FileInputStream(replicaTokenFile));
      } catch (IOException e) {
        throw new ReplicationException("IO error while reading from replica token file at mount path " + mountPath, e);
      }
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public void run() {
    try {
      write(false);
    } catch (Exception e) {
      logger.error("Error while persisting the replica tokens {}", e);
    }
  }
}

