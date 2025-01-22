package com.github.ambry.protocol;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.server.ServerErrorCode;
import java.io.FileInputStream;


public class FileCopyGetChunkResponse extends Response {

  private final PartitionId partitionId;
  private final String fileName;
  private final long sizeInBytes;
  private final FileInputStream fileInputStream;
  public static short File_Copy_Protocol_Chunk_Response_Version_V1 = 1;

  static short CURRENT_VERSION = File_Copy_Protocol_Chunk_Response_Version_V1;


  public FileCopyGetChunkResponse(short versionId, int correlationId, String clientId,
      PartitionId partitionId, String fileName, long sizeInBytes, FileInputStream fileInputStream,
      ServerErrorCode errorCode) {
    super(RequestOrResponseType.FileCopyGetChunkResponse, versionId, correlationId, clientId, errorCode);

    this.partitionId = partitionId;
    this.fileName = fileName;
    this.sizeInBytes = sizeInBytes;
    this.fileInputStream = fileInputStream;
  }

  public FileCopyGetChunkResponse(int correlationId, String clientId, ServerErrorCode serverErrorCode) {
    super(RequestOrResponseType.FileCopyGetChunkResponse, CURRENT_VERSION, correlationId, clientId, serverErrorCode);
  }
}
