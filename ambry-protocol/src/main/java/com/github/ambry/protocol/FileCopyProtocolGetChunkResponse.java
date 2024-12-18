package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;


public class FileCopyProtocolGetChunkResponse extends Response{
  private PartitionId partitionId;
  private String fileName;
  protected ByteBuf chunkData;
  private final long startOffset;
  private final long chunkSizeInBytes;
  private final boolean isLastChunk;
  private static final int File_Name_Field_Size_In_Bytes = 4;

  public FileCopyProtocolGetChunkResponse(short versionId, int correlationId, String clientId, ServerErrorCode errorCode,
      PartitionId partitionId, String fileName, ByteBuf chunkData, long startOffset, long chunkSizeInBytes, boolean isLastChunk) {
    super(RequestOrResponseType.FileCopyProtocolGetChunkResponse, versionId, correlationId, clientId, errorCode);
    this.partitionId = partitionId;
    this.fileName = fileName;
    this.chunkData = chunkData;
    this.startOffset = startOffset;
    this.chunkSizeInBytes = chunkSizeInBytes;
    this.isLastChunk = isLastChunk;
  }

  public static FileCopyProtocolGetChunkResponse readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.FileCopyProtocolGetChunkResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode errorCode = ServerErrorCode.values()[stream.readShort()];

    if(errorCode != ServerErrorCode.No_Error){
      return new FileCopyProtocolGetChunkResponse(versionId, correlationId, clientId, errorCode,
          null, null, null, -1, -1, false);
    }

    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    String fileName = Utils.readIntString(stream);
    long startOffset = stream.readLong();
    long sizeInBytes = stream.readLong();
    boolean isLastChunk = stream.readBoolean();
    int chunkLengthInBytes = stream.readInt();
    ByteBuf chunk = Unpooled.buffer(chunkLengthInBytes);
    for(int i = 0; i < chunkLengthInBytes; i++){
      chunk.writeByte(stream.readByte());
    }
    return new FileCopyProtocolGetChunkResponse(versionId, correlationId, clientId, errorCode, partitionId, fileName, chunk, startOffset, sizeInBytes, isLastChunk);
  }
  public long sizeInBytes() {
    return super.sizeInBytes() + partitionId.getBytes().length + File_Name_Field_Size_In_Bytes + fileName.length() + Long.BYTES + Long.BYTES + 1 + Integer.BYTES + chunkData.readableBytes();
  }
  public void prepareBuffer(){
    super.prepareBuffer();
    bufferToSend.writeBytes(partitionId.getBytes());
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    bufferToSend.writeLong(startOffset);
    bufferToSend.writeLong(chunkSizeInBytes);
    bufferToSend.writeBoolean(isLastChunk);
    bufferToSend.writeInt(chunkData.readableBytes());
    bufferToSend.writeBytes(chunkData);
  }
  public PartitionId getPartitionId() { return partitionId; }
  public String getFileName() { return fileName; }
  public ByteBuf getChunk() { return chunkData; }
  public long getStartOffset() { return startOffset; }
  public long getChunkSizeInBytes() { return chunkSizeInBytes; }
  public boolean isLastChunk() { return isLastChunk; }
}
