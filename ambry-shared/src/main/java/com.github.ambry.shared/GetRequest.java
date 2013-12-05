package com.github.ambry.shared;

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
  private long partitionId;

  private static final int MessageFormat_Size_InBytes = 2;
  private static final int Blob_Id_Count_Size_InBytes = 4;

  public GetRequest(int correlationId, String clientId, MessageFormatFlags flags, long partitionId, ArrayList<BlobId> ids) {
    super(RequestResponseType.GetRequest, Request_Response_Version, correlationId,clientId);

    this.flags = flags;
    this.ids = ids;
    this.sizeSent = 0;
    this.partitionId = partitionId;
    totalIdSize = 0;
    for (BlobId id : ids) {
      totalIdSize += Blob_Id_Size_In_Bytes + id.sizeInBytes();
    }
  }

  public MessageFormatFlags getMessageFormatFlag() {
    return flags;
  }

  public List<? extends StoreKey> getBlobIds() {
    return ids;
  }

  public long getPartitionId() {
    return partitionId;
  }

  public static GetRequest readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.GetRequest;
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    MessageFormatFlags messageType = MessageFormatFlags.values()[stream.readShort()];
    long partition = stream.readLong();
    int blobCount = stream.readInt();
    ArrayList<BlobId> ids = new ArrayList<BlobId>(blobCount);
    while (blobCount > 0) {
      BlobId id = new BlobId(stream);
      ids.add(id);
      blobCount--;
    }
    // ignore version for now
    return new GetRequest(correlationId, clientId, messageType, partition, ids);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putShort((short) flags.ordinal());
      bufferToSend.putLong(partitionId);
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
    // header + error
    return super.sizeInBytes() + MessageFormat_Size_InBytes + PartitionId_Size_In_Bytes +
           Blob_Id_Count_Size_InBytes + totalIdSize;
  }
}
