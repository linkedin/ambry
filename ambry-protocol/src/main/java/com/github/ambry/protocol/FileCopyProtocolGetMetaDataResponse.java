package com.github.ambry.protocol;

import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


public class FileCopyProtocolGetMetaDataResponse extends Response {
  private final int numberOfLogfiles;

  private final String hostName;
  private final List<LogInfo> logInfoList;
  private static final short File_Copy_Protocol_Metadata_Response_Version_V1 = 1;
  private static final int HostName_Field_Size_In_Bytes = 4;

  public FileCopyProtocolGetMetaDataResponse(short versionId, int correlationId, String clientId, int numberOfLogfiles,
      List<LogInfo> logInfoList, ServerErrorCode errorCode, String hostName) {
    super(RequestOrResponseType.FileCopyProtocolGetMetaDataResponse, versionId, correlationId, clientId, errorCode);
    this.numberOfLogfiles = numberOfLogfiles;
    this.logInfoList = logInfoList;
    this.hostName = hostName;
  }

  public static FileCopyProtocolGetMetaDataResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.FileCopyProtocolGetMetaDataResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode errorCode = ServerErrorCode.values()[stream.readShort()];

    if(errorCode != ServerErrorCode.No_Error) {
      return new FileCopyProtocolGetMetaDataResponse(versionId, correlationId, clientId, -1, new ArrayList<>(), errorCode, null);
    }

    int numberOfLogfiles = stream.readInt();
    String hostName = Utils.readIntString(stream);
    int logInfoListSize = stream.readInt();
    List<LogInfo> logInfoList = new ArrayList<>();
    for (int i = 0; i < logInfoListSize; i++) {
      logInfoList.add(LogInfo.readFrom(stream));
    }
    return new FileCopyProtocolGetMetaDataResponse(versionId, correlationId, clientId, numberOfLogfiles, logInfoList, errorCode, hostName);
  }
  protected void prepareBuffer() {
    super.prepareBuffer();
    Utils.serializeString(bufferToSend, hostName, Charset.defaultCharset());
    bufferToSend.writeInt(numberOfLogfiles);
    bufferToSend.writeInt(logInfoList.size());
    for (LogInfo logInfo : logInfoList) {
      logInfo.writeTo(bufferToSend);
    }
  }

  public long sizeInBytes() {
    return super.sizeInBytes() + Integer.BYTES + HostName_Field_Size_In_Bytes + hostName.length() + Integer.BYTES + logInfoList.stream().mapToLong(LogInfo::sizeInBytes).sum();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FileMetaDataResponse[NumberOfLogfiles=").append(numberOfLogfiles).append(", logInfoList").append(logInfoList.toString()).append("]");
    return sb.toString();
  }

  public int getNumberOfLogfiles() {
    return numberOfLogfiles;
  }

  public List<LogInfo> getLogInfoList() {
    return logInfoList;
  }

  static void validateVersion(short version) {
    if (version != File_Copy_Protocol_Metadata_Response_Version_V1) {
      throw new IllegalArgumentException("Unknown version for FileCopyProtocolMetaDataResponse: " + version);
    }
  }
}
