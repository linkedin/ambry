package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.Replica;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Replica metadata request to get new entries for replication
 */
public class ReplicaMetadataRequest extends RequestOrResponse {
  private FindToken token;
  private String hostName;
  private String replicaPath;
  private PartitionId partitionId;
  private long maxTotalSizeOfEntriesInBytes;

  private static final int Max_Entries_Size_In_Bytes = 8;

  private static final int ReplicaPath_Field_Size_In_Bytes = 4;

  public ReplicaMetadataRequest(int correlationId, String clientId, PartitionId partitionId, FindToken token,
      String hostName, String replicaPath, long maxTotalSizeOfEntriesInBytes) {
    super(RequestResponseType.ReplicaMetadataRequest, Request_Response_Version, correlationId, clientId);
    this.token = token;
    this.hostName = hostName;
    this.replicaPath = replicaPath;
    this.partitionId = partitionId;
    this.maxTotalSizeOfEntriesInBytes = maxTotalSizeOfEntriesInBytes;
  }

  public static ReplicaMetadataRequest readFrom(DataInputStream stream, ClusterMap clusterMap, FindTokenFactory factory)
      throws IOException {
    RequestResponseType type = RequestResponseType.ReplicaMetadataRequest;
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    String hostName = Utils.readIntString(stream);
    String replicaPath = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    FindToken token = factory.getFindToken(stream);
    long maxTotalSizeOfEntries = stream.readLong();
    // ignore version for now
    return new ReplicaMetadataRequest(correlationId, clientId, partitionId, token, hostName, replicaPath,
        maxTotalSizeOfEntries);
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

  public long getMaxTotalSizeOfEntriesInBytes() {
    return maxTotalSizeOfEntriesInBytes;
  }

  @Override
  public void writeTo(WritableByteChannel channel)
      throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putInt(hostName.getBytes().length);
      bufferToSend.put(hostName.getBytes());
      bufferToSend.putInt(replicaPath.getBytes().length);
      bufferToSend.put(replicaPath.getBytes());
      bufferToSend.put(partitionId.getBytes());
      bufferToSend.put(token.toBytes());
      bufferToSend.putLong(maxTotalSizeOfEntriesInBytes);
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      channel.write(bufferToSend);
    }
  }

  @Override
  public boolean isSendComplete() {
    return bufferToSend != null && bufferToSend.remaining() == 0;
  }

  @Override
  public long sizeInBytes() {
    // header + hostName + replicaPath +  partitionId + token
    return super.sizeInBytes() + ReplicaPath_Field_Size_In_Bytes + hostName.getBytes().length +
        replicaPath.getBytes().length + partitionId.getBytes().length + token.toBytes().length
        + Max_Entries_Size_In_Bytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReplicaMetadataRequest[");
    sb.append("Token=").append(token);
    sb.append(", ").append("PartitionId=").append(partitionId);
    sb.append(", ").append("HostName=").append(hostName);
    sb.append(", ").append("ReplicaPath=").append(replicaPath);
    sb.append(", ").append("maxTotalSizeOfEntriesInBytes=").append(maxTotalSizeOfEntriesInBytes);
    sb.append("]");
    return sb.toString();
  }
}
