package com.github.ambry.shared;

import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * GetRequest to fetch data
 */
public class GetRequest extends RequestOrResponse {

  private MessageFormatFlags flags;
  private BlobId id;

  private static final int MessageFormat_Size_InBytes = 2;
  private static final int Blob_Id_Size_InBytes = 2;

  public GetRequest(short versionId, int correlationId, MessageFormatFlags flags, BlobId id) {
    super(RequestResponseType.GetRequest, versionId, correlationId);

    this.flags = flags;
    this.id = id;
  }

  public MessageFormatFlags getMessageFormatFlag() {
    return flags;
  }

  public BlobId getBlobId() {
    return id;
  }

  public static GetRequest readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.values()[stream.readShort()];
    if (type != RequestResponseType.GetRequest) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    MessageFormatFlags messageType = MessageFormatFlags.values()[stream.readShort()];
    ByteBuffer blobIdBytes = Utils.readShortBuffer(stream);
    BlobId id = new BlobId(blobIdBytes);
    return new GetRequest(versionId, correlationId, messageType, id);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putShort((short) flags.ordinal());
      ByteBuffer buf = id.toBytes();
      bufferToSend.putShort((short)id.sizeInBytes());
      bufferToSend.put(buf.array());
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      channel.write(bufferToSend);
    }
  }

  @Override
  public boolean isSendComplete() {
    return bufferToSend.remaining() == 0;
  }

  @Override
  public long sizeInBytes() {
    // header + error
    return super.sizeInBytes() + MessageFormat_Size_InBytes + Blob_Id_Size_InBytes + id.sizeInBytes();
  }
}
