package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * GetRequest to fetch data
 */
public class GetRequest extends RequestOrResponse {

  private MessageFormatFlags flags;
  private PartitionId partitionId;
  private ArrayList<BlobId> blobIds;
  private int sizeSent;
  private long totalIdSize;

  private static final int MessageFormat_Size_InBytes = 2;
  private static final int Blob_Id_Count_Size_InBytes = 4;

  public GetRequest(int correlationId, String clientId, MessageFormatFlags flags, PartitionId partitionId,
                    ArrayList<BlobId> blobIds) {
    super(RequestResponseType.GetRequest, Request_Response_Version, correlationId, clientId);

    this.flags = flags;
    this.partitionId = partitionId;
    if (partitionId == null) {
      throw new IllegalArgumentException("No blob IDs specified in GetRequest");
    }
    this.blobIds = blobIds;
    this.sizeSent = 0;
    totalIdSize = 0;
    for (BlobId id : blobIds) {
      totalIdSize += id.sizeInBytes();
      if (!partitionId.equals(id.getPartition())) {
        throw new IllegalArgumentException("Not all blob IDs in GetRequest are from the same partition.");
      }
    }
  }

  public MessageFormatFlags getMessageFormatFlag() {
    return flags;
  }

  public PartitionId getPartition() {
    return partitionId;
  }

  public List<? extends StoreKey> getBlobIds() {
    return blobIds;
  }

  public static GetRequest readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    RequestResponseType type = RequestResponseType.GetRequest;
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    MessageFormatFlags messageType = MessageFormatFlags.values()[stream.readShort()];
    int blobCount = stream.readInt();
    ArrayList<BlobId> ids = new ArrayList<BlobId>(blobCount);
    PartitionId partitionId = null;
    while (blobCount > 0) {
      BlobId id = new BlobId(stream, clusterMap);
      if (partitionId == null) {
        partitionId = id.getPartition();
      }
      ids.add(id);
      blobCount--;
    }
    // ignore version for now
    return new GetRequest(correlationId, clientId, messageType, partitionId, ids);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int)sizeInBytes());
      writeHeader();
      bufferToSend.putShort((short)flags.ordinal());
      bufferToSend.putInt(blobIds.size());
      for (BlobId blobId : blobIds) {
        bufferToSend.put(blobId.toBytes());
      }
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      int written = channel.write(bufferToSend);
      sizeSent += written;
    }
  }

  @Override
  public boolean isSendComplete() {
    return sizeSent == sizeInBytes();
  }

  @Override
  public long sizeInBytes() {
    // header + error
    return super.sizeInBytes() + MessageFormat_Size_InBytes + Blob_Id_Count_Size_InBytes + totalIdSize;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("GetRequest[");
    sb.append("ListOfBlobIDs=").append(blobIds);
    if(partitionId!=null) {
      sb.append(", ").append("PartitionId=").append(partitionId);
    } else {
      sb.append(", ").append("PartitionId=Null");
    }
    if(flags!=null) {
      sb.append(", ").append("MessageFormatFlags=").append(flags);
    } else {
      sb.append(", ").append("MessageFormatFlags=Null");
    }
    sb.append(", ").append("TotalIdSize=").append(totalIdSize);
    sb.append("]");
    return sb.toString();
  }

}
