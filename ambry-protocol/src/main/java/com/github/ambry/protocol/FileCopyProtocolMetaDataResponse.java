package com.github.ambry.protocol;

import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FileCopyProtocolMetaDataResponse extends Response {
  private int numberOfLogfiles;
  private List<LogInfo> logInfoList;

  public FileCopyProtocolMetaDataResponse(short versionId, int correlationId, String clientId, int numberOfLogfiles,
      List<LogInfo> logInfoList, ServerErrorCode errorCode) {
    super(RequestOrResponseType.FileCopyMetaDataRequest, versionId, correlationId, clientId, errorCode);
    this.numberOfLogfiles = numberOfLogfiles;
    this.logInfoList = logInfoList;
  }

  public static FileCopyProtocolMetaDataResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.FileCopyMetaDataRequest) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode errorCode = ServerErrorCode.values()[stream.readShort()];
    int numberOfLogfiles = stream.readInt();
    int logInfoListSize = stream.readInt();
    List<LogInfo> logInfoList = new ArrayList<>();
    for (int i = 0; i < logInfoListSize; i++) {
      logInfoList.add(LogInfo.readFrom(stream));
    }
    return new FileCopyProtocolMetaDataResponse(versionId, correlationId, clientId, numberOfLogfiles, logInfoList, errorCode);
  }
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeInt(numberOfLogfiles);
    bufferToSend.writeInt(logInfoList.size());
    for (LogInfo logInfo : logInfoList) {
      logInfo.writeTo(bufferToSend);
    }
  }

  public long sizeInBytes() {
    return super.sizeInBytes() + Integer.BYTES + Integer.BYTES + logInfoList.stream().mapToLong(LogInfo::sizeInBytes).sum();
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
}
