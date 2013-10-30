package com.github.ambry.shared;


import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.*;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/14/13
 * Time: 10:57 AM
 * To change this template use File | Settings | File Templates.
 */
public class PutResponse extends RequestOrResponse {

  private short error;

  public PutResponse(short versionId, int correlationId, short error) {
    super(RequestResponseType.PutReponse, versionId, correlationId);
    this.error = error;
  }

  public short getError() {
    return error;
  }

  public static PutResponse readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.values()[stream.readShort()];
    if (type != RequestResponseType.PutReponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    Short error = stream.readShort();
    return new PutResponse(versionId, correlationId, error);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int)sizeInBytes());
      writeHeader();
      bufferToSend.putShort(error);
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      channel.write(bufferToSend);
    }
  }

  @Override
  public boolean isComplete() {
    return bufferToSend.remaining() == 0;
  }

  @Override
  public long sizeInBytes() {
    // header + error
    return super.sizeInBytes() + 2;
  }
}
