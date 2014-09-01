package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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
  private PartitionId partitionId;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final int ReplicaPath_Field_Size_In_Bytes = 4;
  private static final int HostName_Field_Size_In_Bytes = 4;

  public ReplicaMetadataRequestInfo(PartitionId partitionId, FindToken token, String hostName, String replicaPath) {
    if (partitionId == null || token == null || hostName == null || replicaPath == null) {
      throw new IllegalArgumentException("no parameters of replica metadata request info can be null");
    }
    this.partitionId = partitionId;
    this.token = token;
    this.hostName = hostName;
    this.replicaPath = replicaPath;
  }

  public static ReplicaMetadataRequestInfo readFrom(DataInputStream stream, ClusterMap clusterMap,
      FindTokenFactory factory)
      throws IOException {
    String hostName = Utils.readIntString(stream);
    String replicaPath = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    FindToken token = factory.getFindToken(stream);
    return new ReplicaMetadataRequestInfo(partitionId, token, hostName, replicaPath);
  }

  public void writeTo(ByteBuffer buffer) {
    logger.info("writing hostName length " + hostName.getBytes().length + " to buffer");
    buffer.putInt(hostName.getBytes().length);
    logger.info("writing hostName " + hostName + " to buffer");
    buffer.put(hostName.getBytes());
    logger.info("writing replicaPath length " + replicaPath.getBytes().length + " to buffer");
    buffer.putInt(replicaPath.getBytes().length);
    logger.info("writing replicaPath " + replicaPath + " to buffer");
    buffer.put(replicaPath.getBytes());
    logger.info("writing partitionId " + partitionId + " to buffer");
    buffer.put(partitionId.getBytes());
    logger.info("writing token " + token + " to buffer");
    buffer.put(token.toBytes());
  }

  public long sizeInBytes() {
    return HostName_Field_Size_In_Bytes + hostName.getBytes().length + ReplicaPath_Field_Size_In_Bytes + replicaPath
        .getBytes().length +
        + partitionId.getBytes().length + token.toBytes().length;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Token=").append(token);
    sb.append(", ").append("PartitionId=").append(partitionId);
    sb.append(", ").append("HostName=").append(hostName);
    sb.append(", ").append("ReplicaPath=").append(replicaPath);
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
}
