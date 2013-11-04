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

  private static final int MessageFormat_Size_InBytes = 2;
  private static final int Blob_Id_Size_InBytes = 2;
  private static final int Blob_Id_Count_Size_InBytes = 4;

  public GetRequest(short versionId, int correlationId, MessageFormatFlags flags, ArrayList<BlobId> ids) {
    super(RequestResponseType.GetRequest, versionId, correlationId);

    this.flags = flags;
    this.ids = ids;
    this.sizeSent = 0;
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

  public static GetRequest readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.values()[stream.readShort()];
    if (type != RequestResponseType.GetRequest) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    MessageFormatFlags messageType = MessageFormatFlags.values()[stream.readShort()];
    int blobCount = stream.readInt();
    ArrayList<BlobId> ids = new ArrayList<BlobId>(blobCount);
    while (blobCount > 0) {
      ByteBuffer blobIdBytes = Utils.readShortBuffer(stream);
      BlobId id = new BlobId(blobIdBytes);
      ids.add(id);
      blobCount--;
    }
    return new GetRequest(versionId, correlationId, messageType, ids);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putShort((short) flags.ordinal());
      bufferToSend.putInt(ids.size());
      for (BlobId id : ids) {
        ByteBuffer buf = id.toBytes();
        bufferToSend.putShort((short)id.sizeInBytes());
        bufferToSend.put(buf.array());
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
    return super.sizeInBytes() + MessageFormat_Size_InBytes + Blob_Id_Size_InBytes +
            Blob_Id_Count_Size_InBytes + totalIdSize;
  }
}
