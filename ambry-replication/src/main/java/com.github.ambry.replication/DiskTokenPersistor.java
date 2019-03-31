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

import com.github.ambry.store.FindToken;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.SystemTime;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link DiskTokenPersistor} persists replication token to disk.
 */
public class DiskTokenPersistor implements ReplicaTokenPersistor {

  private static final Logger logger = LoggerFactory.getLogger(DiskTokenPersistor.class);
  private final short version = 0;
  private final String replicaTokenFileName;
  private final Map<String, List<PartitionInfo>> partitionGroupedByMountPath;
  private final ReplicationMetrics replicationMetrics;

  /**
   * Constructor for {@link DiskTokenPersistor}.
   * @param replicaTokenFileName the token's file name.
   * @param partitionGroupedByMountPath A map between mount path and list of partitions under this mount path.
   * @param replicationMetrics metrics including token persist time.
   */
  public DiskTokenPersistor(String replicaTokenFileName, Map<String, List<PartitionInfo>> partitionGroupedByMountPath,
      ReplicationMetrics replicationMetrics) {
    this.replicaTokenFileName = replicaTokenFileName;
    this.partitionGroupedByMountPath = partitionGroupedByMountPath;
    this.replicationMetrics = replicationMetrics;
  }

  @Override
  public void write(String mountPath, boolean shuttingDown) throws IOException, ReplicationException {
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

  @Override
  public void write(boolean shuttingDown) throws IOException, ReplicationException {
    for (String mountPath : partitionGroupedByMountPath.keySet()) {
      write(mountPath, shuttingDown);
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

