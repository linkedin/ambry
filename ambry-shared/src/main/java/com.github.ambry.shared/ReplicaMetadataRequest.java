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

  public ReplicaMetadataRequest(int correlationId, String clientId, PartitionId partitionId, FindToken token) {
    super(RequestResponseType.ReplicaMetadataRequest, Request_Response_Version, correlationId, clientId);
    this.token = token;
    this.partitionId = partitionId;
  }

  public static ReplicaMetadataRequest readFrom(DataInputStream stream, ClusterMap clusterMap, FindTokenFactory factory) throws IOException {
    RequestResponseType type = RequestResponseType.ReplicaMetadataRequest;
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    FindToken token = factory.getFindToken(stream);
    // ignore version for now
    return new ReplicaMetadataRequest(correlationId, clientId, partitionId, token);
  }

  public FindToken getToken() {
    return token;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.put(partitionId.getBytes());
      bufferToSend.put(token.toBytes());
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
    return super.sizeInBytes() + partitionId.getBytes().length + token.toBytes().length;
  }
}
