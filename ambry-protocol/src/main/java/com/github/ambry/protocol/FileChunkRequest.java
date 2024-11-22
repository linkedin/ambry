package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;


public class FileChunkRequest extends RequestOrResponse{

  private String hostName;
  private PartitionId partitionId;
  private String fileName;
  private long startOffset;
  private long size;

  private static final short File_Chunk_Request_Version_V1 = 1;

  private static final int HostName_Field_Size_In_Bytes = 4;
  private static final int FileName_Field_Size_In_Bytes = 4;

  public FileChunkRequest(short versionId, int correlationId, String clientId,PartitionId partitionId, String hostName, String fileName, long startOffset, long size) {
    super(RequestOrResponseType.FileChunkRequest, versionId, correlationId, clientId);
    this.hostName = hostName;
    this.partitionId = partitionId;
    this.fileName = fileName;
    this.startOffset = startOffset;
    this.size = size;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FileChunkRequest[").append(", HostName=").append(hostName).append("PartitionId=").append(partitionId)
        .append(", FileName=").append(fileName).append(", StartOffset=").append(startOffset).append(", Size=").append(size).append("]");
    return sb.toString();
  }

  public String getHostName() {
    return hostName;
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

  public long getSize() {
    return size;
  }

  public long sizeInBytes() {
    return super.sizeInBytes() + HostName_Field_Size_In_Bytes + hostName.length() + partitionId.getBytes().length
        + FileName_Field_Size_In_Bytes + fileName.length() + Long.BYTES + Long.BYTES;
  }

  protected void prepareBuffer() {
    super.prepareBuffer();

    Utils.serializeString(bufferToSend, hostName, Charset.defaultCharset());
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    bufferToSend.writeBytes(partitionId.getBytes());
    bufferToSend.writeLong(startOffset);
    bufferToSend.writeLong(size);
  }

  protected static FileChunkRequest readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    Short versionId = stream.readShort();
    validateVersion(versionId);
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    String hostName = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    String fileName = Utils.readIntString(stream);
    long startOffset = stream.readLong();
    long size = stream.readLong();
    return new FileChunkRequest(versionId, correlationId, clientId, partitionId, hostName, fileName, startOffset, size);
  }

  static void validateVersion(short version) {
    if (version != File_Chunk_Request_Version_V1) {
      throw new IllegalArgumentException("Unknown version for FileChunkRequest: " + version);
    }
  }
}
