package com.github.ambry.protocol;

import com.github.ambry.server.ServerErrorCode;
import io.netty.buffer.ByteBuf;
import java.util.zip.CRC32;


public class FileChunkResponse extends Response{
  String partitionName;
  String fileName;
  long startOffSet;
  long sizeInBytes;

  CRC32 crc32;
  ByteBuf data;
  public FileChunkResponse(RequestOrResponseType type, short requestResponseVersion, int correlationId, String clientId,
      ServerErrorCode error) {
    super(type, requestResponseVersion, correlationId, clientId, error);
  }
}
