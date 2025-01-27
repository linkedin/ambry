package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;


public class FileCopyGetChunkResponse extends Response {
  private final DataInputStream chunkStream;
  private final boolean isLastChunk;
  private final long startOffset;
  private final long chunkSizeInBytes;
  private final PartitionId partitionId;
  private final String fileName;

  private static final int File_Name_Field_Size_In_Bytes = 4;
  public static final short File_Copy_Chunk_Response_Version_V1 = 1;

  static short CURRENT_VERSION = File_Copy_Chunk_Response_Version_V1;

  public FileCopyGetChunkResponse(short versionId, int correlationId, String clientId, ServerErrorCode errorCode,
      PartitionId partitionId, String fileName, DataInputStream chunkStream,
      long startOffset, long chunkSizeInBytes, boolean isLastChunk) {
    super(RequestOrResponseType.FileCopyGetChunkResponse, versionId, correlationId, clientId, errorCode);

    validateVersion(versionId);

    this.chunkStream = chunkStream;
    this.startOffset = startOffset;
    this.chunkSizeInBytes = chunkSizeInBytes;
    this.isLastChunk = isLastChunk;
    this.partitionId = partitionId;
    this.fileName = fileName;
  }

  public FileCopyGetChunkResponse(int correlationId, String clientId, ServerErrorCode errorCode) {
    this(CURRENT_VERSION, correlationId, clientId, errorCode, null, null, null, -1, -1, false);
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public String getFileName() {
    return fileName;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public boolean isLastChunk() {
    return isLastChunk;
  }

  public long getChunkSizeInBytes() {
    return chunkSizeInBytes;
  }

  public long sizeInBytes() {
    try {
      return super.sizeInBytes() + partitionId.getBytes().length + File_Name_Field_Size_In_Bytes +
          fileName.length() + Long.BYTES + Long.BYTES + 1 + Integer.BYTES + chunkStream.available();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static FileCopyGetChunkResponse readFrom(DataInputStream chunkResponseStream, ClusterMap clusterMap) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[chunkResponseStream.readShort()];
    if (type != RequestOrResponseType.FileCopyGetChunkResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short versionId = chunkResponseStream.readShort();
    validateVersion(versionId);

    int correlationId = chunkResponseStream.readInt();
    String clientId = Utils.readIntString(chunkResponseStream);

    ServerErrorCode errorCode = ServerErrorCode.values()[chunkResponseStream.readShort()];
    if(errorCode != ServerErrorCode.No_Error){
      return new FileCopyGetChunkResponse(correlationId, clientId, errorCode);
    }
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(chunkResponseStream);
    String fileName = Utils.readIntString(chunkResponseStream);
    long startOffset = chunkResponseStream.readLong();
    long sizeInBytes = chunkResponseStream.readLong();
    boolean isLastChunk = chunkResponseStream.readBoolean();

    return new FileCopyGetChunkResponse(versionId, correlationId, clientId, errorCode, partitionId, fileName,
        chunkResponseStream, startOffset, sizeInBytes, isLastChunk);
  }

  public void prepareBuffer(){
    super.prepareBuffer();
    bufferToSend.writeBytes(partitionId.getBytes());
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    bufferToSend.writeLong(startOffset);
    bufferToSend.writeLong(chunkSizeInBytes);
    bufferToSend.writeBoolean(isLastChunk);
    try {
      bufferToSend.writeInt(chunkStream.available());
      bufferToSend.writeBytes(chunkStream, chunkStream.available());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    try {
      sb.append("FileCopyGetChunkResponse[")
          .append("PartitionId=").append(partitionId.getId())
          .append(", FileName=").append(fileName)
          .append(", SizeInBytes=").append(chunkStream.available())
          .append(", isLastChunk=").append(isLastChunk)
          .append("]");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  public DataInputStream getChunkStream() {
    return chunkStream;
  }

  private static void validateVersion(short version) {
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileCopyGetChunkResponse: " + version);
    }
  }
}
