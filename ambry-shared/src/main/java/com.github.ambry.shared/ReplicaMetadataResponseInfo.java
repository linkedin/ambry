package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * Contains the token, messageInfoList, remoteReplicaLag for a local partition. This is used
 * by replica metadata response to specify information for a partition
 */
public class ReplicaMetadataResponseInfo {
  private FindToken token;
  private MessageInfoListSerde messageInfoListSerDe;
  private final int messageInfoListSize;
  private final long remoteReplicaLagInBytes;
  private PartitionId partitionId;

  private static final int Remote_Replica_Lag_Size_In_Bytes = 8;

  public ReplicaMetadataResponseInfo(PartitionId partitionId, FindToken findToken, List<MessageInfo> messageInfoList,
      long remoteReplicaLagInBytes) {
    if (partitionId == null || findToken == null || messageInfoList == null) {
      throw new IllegalArgumentException("Invalid partition or token or message info list");
    }
    this.partitionId = partitionId;
    this.remoteReplicaLagInBytes = remoteReplicaLagInBytes;
    messageInfoListSerDe = new MessageInfoListSerde(messageInfoList);
    messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
    this.token = findToken;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoListSerDe.getMessageInfoList();
  }

  public FindToken getFindToken() {
    return token;
  }

  public long getRemoteReplicaLagInBytes() {
    return remoteReplicaLagInBytes;
  }

  public static ReplicaMetadataResponseInfo readFrom(DataInputStream stream, FindTokenFactory factory,
      ClusterMap clusterMap)
      throws IOException {
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    FindToken token = factory.getFindToken(stream);
    List<MessageInfo> messageInfoList = MessageInfoListSerde.deserializeMessageInfoList(stream, clusterMap);
    long remoteReplicaLag = stream.readLong();
    return new ReplicaMetadataResponseInfo(partitionId, token, messageInfoList, remoteReplicaLag);
  }

  public void writeTo(ByteBuffer buffer) {
    buffer.put(partitionId.getBytes());
    if (token != null) {
      buffer.put(token.toBytes());
      messageInfoListSerDe.serializeMessageInfoList(buffer);
    }
    buffer.putLong(remoteReplicaLagInBytes);
  }

  public long sizeInBytes() {
    return messageInfoListSize + (token == null ? 0 : token.toBytes().length) + Remote_Replica_Lag_Size_In_Bytes
        + partitionId.getBytes().length;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(partitionId);
    if (token != null) {
      sb.append(" Token=").append(token);
    }
    sb.append(" RemoteReplicaLagInBytes=").append(remoteReplicaLagInBytes);
    return sb.toString();
  }
}
