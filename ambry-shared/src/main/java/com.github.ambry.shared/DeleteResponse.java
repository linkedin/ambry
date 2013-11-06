package com.github.ambry.shared;

import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Response of delete request
 */
public class DeleteResponse extends RequestOrResponse {

  private short error;
  private static final int Error_Size_InBytes = 2;

  public DeleteResponse(int correlationId, String clientId, short error) {
    super(RequestResponseType.DeleteResponse, (short)1, correlationId, clientId);
    this.error = error;
  }

  public short getError() {
    return error;
  }

  public static DeleteResponse readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.values()[stream.readShort()];
    if (type != RequestResponseType.DeleteResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    Short error = stream.readShort();
    // ignore version for now
    return new DeleteResponse(correlationId, clientId, error);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putShort(error);
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
    return super.sizeInBytes() + Error_Size_InBytes;
  }
}
