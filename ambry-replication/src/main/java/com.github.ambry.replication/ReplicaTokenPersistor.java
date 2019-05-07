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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.replication.RemoteReplicaInfo.*;


/**
 * {@link ReplicaTokenPersistor} is used in {@link ReplicationEngine} to persist replication token.
 */
public abstract class ReplicaTokenPersistor implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DiskTokenPersistor.class);
  protected final Map<String, List<PartitionInfo>> partitionGroupedByMountPath;
  protected final ReplicationMetrics replicationMetrics;
  protected final ReplicaTokenSerde replicaTokenSerde;

  public ReplicaTokenPersistor(Map<String, List<PartitionInfo>> partitionGroupedByMountPath,
      ReplicationMetrics replicationMetrics, ClusterMap clusterMap, FindTokenFactory tokenfactory) {
    this.partitionGroupedByMountPath = partitionGroupedByMountPath;
    this.replicationMetrics = replicationMetrics;
    this.replicaTokenSerde = new ReplicaTokenSerde(clusterMap, tokenfactory);
  }

  /**
   * Iterates through each mount path and persists all the replica tokens for the partitions on the mount
   * path to a file. The file is saved on the corresponding mount path.
   * @param shuttingDown indicates whether this is being called as part of shut down.
   */
  public final void write(boolean shuttingDown) throws IOException, ReplicationException {
    for (String mountPath : partitionGroupedByMountPath.keySet()) {
      write(mountPath, shuttingDown);
    }
  }

  /**
   * Method to persist the token of partition(s) under the same mountPath.
   * @param mountPath The mouth path of the partition(s).
   * @param shuttingDown indicates whether this is being called as part of shut down.
   */
  public final void write(String mountPath, boolean shuttingDown) throws IOException, ReplicationException {
    long writeStartTimeMs = SystemTime.getInstance().milliseconds();

    // Get all partitions for the mount path and persist their tokens
    List<ReplicaTokenInfo> tokensPersisted = new ArrayList<>();
    for (PartitionInfo info : partitionGroupedByMountPath.get(mountPath)) {
      for (RemoteReplicaInfo remoteReplica : info.getRemoteReplicaInfos()) {
        FindToken tokenToPersist = remoteReplica.getTokenToPersist();
        if (tokenToPersist != null) {
          tokensPersisted.add(new ReplicaTokenInfo(remoteReplica));
        }
      }
    }

    try {
      persist(mountPath, tokensPersisted);
    } finally {
      // TODO: could move this out
      replicationMetrics.remoteReplicaTokensPersistTime.update(
          SystemTime.getInstance().milliseconds() - writeStartTimeMs);
    }

    for (ReplicaTokenInfo replicaTokenInfo : tokensPersisted) {
      replicaTokenInfo.getReplicaInfo().onTokenPersisted();
      if (shuttingDown) {
        logger.info("Persisted token {}", replicaTokenInfo.getReplicaToken());
      }
    }
  }

  /**
   * Persist the list of tokens in the specified mount path.
   * @param mountPath
   * @param tokenInfoList
   * @throws IOException
   */
  protected abstract void persist(String mountPath, List<ReplicaTokenInfo> tokenInfoList)
      throws IOException, ReplicationException;

  /**
   * Retrieve the tokens under the same mountPath.
   * @param mountPath The mouth path of the partition(s).
   * @return A list of {@link ReplicaTokenInfo}.
   */
  protected abstract List<ReplicaTokenInfo> retrieve(String mountPath) throws IOException, ReplicationException;

  @Override
  public void run() {
    try {
      write(false);
    } catch (Exception e) {
      logger.error("Error while persisting the replica tokens {}", e);
    }
  }

  /**
   * Class to serialize and deserialize replica tokens
   */
  public static class ReplicaTokenSerde {
    private static final short Crc_Size = 8;
    private final ClusterMap clusterMap;
    private final FindTokenFactory tokenfactory;
    private final short version = 0;

    // Map<Sting,FindToken>
    public ReplicaTokenSerde(ClusterMap clusterMap, FindTokenFactory tokenfactory) {
      this.clusterMap = clusterMap;
      this.tokenfactory = tokenfactory;
    }

    public void serializeTokens(List<ReplicaTokenInfo> tokenInfoList, OutputStream outputStream) throws IOException {
      CrcOutputStream crcOutputStream = new CrcOutputStream(outputStream);
      DataOutputStream writer = new DataOutputStream(crcOutputStream);
      try {
        // write the current version
        writer.writeShort(version);
        for (ReplicaTokenInfo replicaTokenInfo : tokenInfoList) {
          writer.write(replicaTokenInfo.getPartitionId().getBytes());
          // Write hostname
          writer.writeInt(replicaTokenInfo.getHostname().getBytes().length);
          writer.write(replicaTokenInfo.getHostname().getBytes());
          // Write replica path
          writer.writeInt(replicaTokenInfo.getReplicaPath().getBytes().length);
          writer.write(replicaTokenInfo.getReplicaPath().getBytes());
          // Write port
          writer.writeInt(replicaTokenInfo.getPort());
          writer.writeLong(replicaTokenInfo.getTotalBytesReadFromLocalStore());
          writer.write(replicaTokenInfo.getReplicaToken().toBytes());
        }
        long crcValue = crcOutputStream.getValue();
        writer.writeLong(crcValue);
      } catch (IOException e) {
        logger.error("IO error while serializing replica tokens");
        throw e;
      } finally {
        if (outputStream instanceof FileOutputStream) {
          // flush and overwrite file
          ((FileOutputStream) outputStream).getChannel().force(true);
        }
        writer.close();
      }
    }

    public List<ReplicaTokenInfo> deserializeTokens(InputStream inputStream) throws IOException, ReplicationException {
      CrcInputStream crcStream = new CrcInputStream(inputStream);
      DataInputStream stream = new DataInputStream(crcStream);
      List<ReplicaTokenInfo> tokenInfoList = new ArrayList<>();
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
              FindToken token = tokenfactory.getFindToken(stream);
              tokenInfoList.add(
                  new ReplicaTokenInfo(partitionId, hostname, replicaPath, port, totalBytesReadFromLocalStore, token));
            }

            long computedCrc = crcStream.getValue();
            long readCrc = stream.readLong();
            if (computedCrc != readCrc) {
              throw new ReplicationException(
                  "Crc mismatch during replica token deserialization, computed " + computedCrc + ", read " + readCrc);
            }
            return tokenInfoList;
          default:
            throw new ReplicationException("Invalid version found during replica token deserialization: " + version);
        }
      } catch (IOException e) {
        throw new ReplicationException("IO error deserializing replica tokens", e);
      } finally {
        stream.close();
      }
    }
  }
}
