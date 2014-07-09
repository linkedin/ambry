package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
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
  private PartitionId partitionId;
  private long maxTotalSizeOfEntriesInBytes;

  private static int Max_Entries_Size_In_Bytes = 8;

  public ReplicaMetadataRequest(int correlationId, String clientId, PartitionId partitionId, FindToken token,
      long maxTotalSizeOfEntriesInBytes) {
    super(RequestOrResponseType.ReplicaMetadataRequest, Request_Response_Version, correlationId, clientId);
    this.token = token;
    this.partitionId = partitionId;
    this.maxTotalSizeOfEntriesInBytes = maxTotalSizeOfEntriesInBytes;
  }

  public static ReplicaMetadataRequest readFrom(DataInputStream stream, ClusterMap clusterMap, FindTokenFactory factory)
      throws IOException {
    RequestOrResponseType type = RequestOrResponseType.ReplicaMetadataRequest;
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    FindToken token = factory.getFindToken(stream);
    long maxTotalSizeOfEntries = stream.readLong();
    // ignore version for now
    return new ReplicaMetadataRequest(correlationId, clientId, partitionId, token, maxTotalSizeOfEntries);
  }

  public FindToken getToken() {
    return token;
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
    // header + partitionId + token
    return super.sizeInBytes() + partitionId.getBytes().length + token.toBytes().length + Max_Entries_Size_In_Bytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReplicaMetadataRequest[");
    sb.append("Token=").append(token);
    sb.append(", ").append("PartitionId=").append(partitionId);
    sb.append(", ").append("maxTotalSizeOfEntriesInBytes=").append(maxTotalSizeOfEntriesInBytes);
    sb.append("]");
    return sb.toString();
  }
}
