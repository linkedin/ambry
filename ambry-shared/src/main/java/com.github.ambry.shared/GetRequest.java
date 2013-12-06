package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Utils;
import java.util.List;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

/**
 * GetRequest to fetch data
 */
public class GetRequest extends RequestOrResponse {

  private MessageFormatFlags flags;
  private ArrayList<BlobId> ids;
  private int sizeSent;
  private long totalIdSize;
  private PartitionId partition;

  private static final int MessageFormat_Size_InBytes = 2;
  private static final int Blob_Id_Count_Size_InBytes = 4;

  public GetRequest(PartitionId partition, int correlationId, String clientId, MessageFormatFlags flags, ArrayList<BlobId> ids) {
    super(RequestResponseType.GetRequest, Request_Response_Version, correlationId,clientId);

    this.flags = flags;
    this.ids = ids;
    this.sizeSent = 0;
    this.partition = partition;
    totalIdSize = 0;
    for (BlobId id : ids) {
      totalIdSize += id.sizeInBytes();
    }
  }

  public MessageFormatFlags getMessageFormatFlag() {
    return flags;
  }

  public List<? extends StoreKey> getBlobIds() {
    return ids;
  }

  public PartitionId getPartition() {
    return partition;
  }

  public static GetRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    RequestResponseType type = RequestResponseType.GetRequest;
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    MessageFormatFlags messageType = MessageFormatFlags.values()[stream.readShort()];
    PartitionId partitionId = map.getPartitionIdFromStream(stream);
    int blobCount = stream.readInt();
    ArrayList<BlobId> ids = new ArrayList<BlobId>(blobCount);
    while (blobCount > 0) {
      BlobId id = new BlobId(stream, map);
      ids.add(id);
      blobCount--;
    }
    // ignore version for now
    return new GetRequest(partitionId, correlationId, clientId, messageType, ids);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putShort((short) flags.ordinal());
      bufferToSend.put(partition.getBytes());
      bufferToSend.putInt(ids.size());
      for (BlobId id : ids) {
        bufferToSend.put(id.toBytes());
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
    // header + messageformat type + partitionId + blobids
    return super.sizeInBytes() + MessageFormat_Size_InBytes + partition.getBytes().length +
           Blob_Id_Count_Size_InBytes + totalIdSize;
  }
}
