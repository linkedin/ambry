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
package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains the token, hostname, replicapath for a local partition. This is used
 * by replica metadata request to specify token in a partition
 */
public class ReplicaMetadataRequestInfo {
  private FindToken token;
  private String hostName;
  private String replicaPath;
  private ReplicaType replicaType;
  private PartitionId partitionId;

  private static final int ReplicaPath_Field_Size_In_Bytes = 4;
  private static final int HostName_Field_Size_In_Bytes = 4;
  private static final int ReplicaType_Size_In_Bytes = Short.BYTES;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public ReplicaMetadataRequestInfo(PartitionId partitionId, FindToken token, String hostName, String replicaPath,
      ReplicaType replicaType) {
    if (partitionId == null || token == null || hostName == null || replicaPath == null) {
      throw new IllegalArgumentException(
          "A parameter in the replica metadata request is null: " + "[Partition: " + partitionId + ", token: " + token
              + ", hostName: " + hostName + ", replicaPath: " + replicaPath);
    }
    this.partitionId = partitionId;
    this.token = token;
    this.hostName = hostName;
    this.replicaPath = replicaPath;
    this.replicaType = replicaType;
  }

  public static ReplicaMetadataRequestInfo readFrom(DataInputStream stream, ClusterMap clusterMap,
      FindTokenHelper findTokenHelper) throws IOException {
    String hostName = Utils.readIntString(stream);
    String replicaPath = Utils.readIntString(stream);
    ReplicaType replicaType = ReplicaType.values()[stream.readShort()];
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    FindTokenFactory findTokenFactory = findTokenHelper.getFindTokenFactoryFromReplicaType(replicaType);
    FindToken token = findTokenFactory.getFindToken(stream);
    return new ReplicaMetadataRequestInfo(partitionId, token, hostName, replicaPath, replicaType);
  }

  public void writeTo(ByteBuffer buffer) {
    Utils.serializeString(buffer, hostName, Charset.defaultCharset());
    Utils.serializeString(buffer, replicaPath, Charset.defaultCharset());
    buffer.putShort((short) replicaType.ordinal());
    buffer.put(partitionId.getBytes());
    buffer.put(token.toBytes());
  }

  public long sizeInBytes() {
    return HostName_Field_Size_In_Bytes + hostName.getBytes().length + ReplicaPath_Field_Size_In_Bytes
        + replicaPath.getBytes().length + +partitionId.getBytes().length + ReplicaType_Size_In_Bytes
        + token.toBytes().length;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[Token=").append(token);
    sb.append(", ").append(" PartitionId=").append(partitionId);
    sb.append(", ").append(" HostName=").append(hostName);
    sb.append(", ").append(" ReplicaPath=").append(replicaPath);
    sb.append(", ").append(" ReplicaType=").append(replicaType).append("]");
    return sb.toString();
  }

  public FindToken getToken() {
    return token;
  }

  public String getHostName() {
    return this.hostName;
  }

  public String getReplicaPath() {
    return this.replicaPath;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public ReplicaType getReplicaType() {
    return replicaType;
  }
}
