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
import com.github.ambry.clustermap.ReplicaType;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.replication.RemoteReplicaInfo.*;


/**
 * {@link ReplicaTokenPersistor} is used in {@link ReplicationEngine} to persist replication token.
 */
public abstract class ReplicaTokenPersistor implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ReplicaTokenPersistor.class);
  protected final Map<String, Set<PartitionInfo>> partitionGroupedByMountPath;
  protected final Set<String> mountPathsToSkip = new HashSet<>();
  protected final ReplicationMetrics replicationMetrics;
  protected final ReplicaTokenSerde replicaTokenSerde;

  public ReplicaTokenPersistor(Map<String, Set<PartitionInfo>> partitionGroupedByMountPath,
      ReplicationMetrics replicationMetrics, ClusterMap clusterMap, FindTokenHelper findTokenHelper) {
    this.partitionGroupedByMountPath = partitionGroupedByMountPath;
    this.replicationMetrics = replicationMetrics;
    this.replicaTokenSerde = new ReplicaTokenSerde(clusterMap, findTokenHelper);
  }

  /**
   * Iterates through each mount path and persists all the replica tokens for the partitions on the mount
   * path to a file. The file is saved on the corresponding mount path.
   * @param shuttingDown indicates whether this is being called as part of shut down.
   */
  public final void write(boolean shuttingDown) throws IOException, ReplicationException {
    for (String mountPath : partitionGroupedByMountPath.keySet()) {
      if (!mountPathsToSkip.contains(mountPath)) {
        write(mountPath, shuttingDown);
      }
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
   * @return a set of mount paths that should be skipped when persisting replica tokens.
   */
  Set<String> getMountPathsToSkip() {
    return Collections.unmodifiableSet(mountPathsToSkip);
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
  public abstract List<ReplicaTokenInfo> retrieve(String mountPath) throws ReplicationException;

  @Override
  public void run() {
    try {
      write(false);
    } catch (Exception e) {
      logger.error("Error while persisting the replica tokens ", e);
    }
  }

  /**
   * Class to serialize and deserialize replica tokens
   */
  public static class ReplicaTokenSerde {
    private static final short Crc_Size = 8;
    private final ClusterMap clusterMap;
    private final FindTokenHelper tokenHelper;
    private static final short VERSION_0 = 0;
    private static final short VERSION_1 = 1;
    private static final short CURRENT_VERSION = VERSION_1;

    // Map<Sting,FindToken>
    public ReplicaTokenSerde(ClusterMap clusterMap, FindTokenHelper tokenHelper) {
      this.clusterMap = clusterMap;
      this.tokenHelper = tokenHelper;
    }

    public void serializeTokens(List<ReplicaTokenInfo> tokenInfoList, OutputStream outputStream) throws IOException {
      CrcOutputStream crcOutputStream = new CrcOutputStream(outputStream);
      DataOutputStream writer = new DataOutputStream(crcOutputStream);
      try {
        // write the current version
        writer.writeShort(CURRENT_VERSION);
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
          // Write total bytes read from local store
          writer.writeLong(replicaTokenInfo.getTotalBytesReadFromLocalStore());
          // Write replica type
          writer.writeShort((short) replicaTokenInfo.getReplicaInfo().getReplicaId().getReplicaType().ordinal());
          // Write replica token
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
        if (version < VERSION_0 || version > VERSION_1) {
          throw new ReplicationException("Invalid version found during replica token deserialization: " + version);
        }
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
          //read replica type; prior to VERSION_1 all the replicas were DISK_BACKED only
          ReplicaType replicaType = ReplicaType.DISK_BACKED;
          if (version > VERSION_0) {
            replicaType = ReplicaType.values()[stream.readShort()];
          }
          // read replica token
          FindTokenFactory findTokenFactory = tokenHelper.getFindTokenFactoryFromReplicaType(replicaType);
          FindToken token = findTokenFactory.getFindToken(stream);

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
      } catch (IOException e) {
        throw new ReplicationException("IO error deserializing replica tokens", e);
      } finally {
        stream.close();
      }
    }
  }
}
