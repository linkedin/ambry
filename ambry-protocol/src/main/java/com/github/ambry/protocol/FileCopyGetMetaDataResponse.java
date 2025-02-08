/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.protocol;

import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;


public class FileCopyGetMetaDataResponse extends Response {
  private final int numberOfLogfiles;
  private final List<LogInfo> logInfos;
  public static final short File_Copy_Protocol_Metadata_Response_Version_V1 = 1;

  static short CURRENT_VERSION = File_Copy_Protocol_Metadata_Response_Version_V1;

  public FileCopyGetMetaDataResponse(short versionId, int correlationId, String clientId, int numberOfLogfiles,
      @Nonnull List<LogInfo> logInfos, @Nonnull ServerErrorCode errorCode) {
    super(RequestOrResponseType.FileCopyGetMetaDataResponse, versionId, correlationId, clientId, errorCode);

    validateVersion(versionId);
    this.numberOfLogfiles = numberOfLogfiles;
    this.logInfos = logInfos;
  }

  public FileCopyGetMetaDataResponse(int correlationId, String clientId, ServerErrorCode serverErrorCode) {
    this(CURRENT_VERSION, correlationId, clientId, 0, new ArrayList<>(), serverErrorCode);
  }

  public static FileCopyGetMetaDataResponse readFrom(
      @Nonnull DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.FileCopyGetMetaDataResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible. Expected : {}, Actual : {}" +
          RequestOrResponseType.FileCopyGetMetaDataResponse + type);
    }
    short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode errorCode = ServerErrorCode.values()[stream.readShort()];

    if(errorCode != ServerErrorCode.No_Error) {
      //Setting the number of logfiles to 0 as there are no logfiles to be read.
      return new FileCopyGetMetaDataResponse(versionId, correlationId, clientId, 0, new ArrayList<>(), errorCode);
    }
    int numberOfLogfiles = stream.readInt();
    List<LogInfo> logInfos = new ArrayList<>();
    for (int i = 0; i < numberOfLogfiles; i++) {
      logInfos.add(LogInfo.readFrom(stream));
    }
    return new FileCopyGetMetaDataResponse(versionId, correlationId, clientId, numberOfLogfiles, logInfos, errorCode);
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeInt(numberOfLogfiles);
    for (LogInfo logInfo : logInfos) {
      logInfo.writeTo(bufferToSend);
    }
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + Integer.BYTES + logInfos.stream().mapToLong(LogInfo::sizeInBytes).sum();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb
      .append("FileCopyGetMetaDataResponse[NumberOfLogfiles=")
      .append(numberOfLogfiles)
      .append(", logInfoList")
      .append(logInfos.toString())
      .append("]");
    return sb.toString();
  }

  public int getNumberOfLogfiles() {
    return numberOfLogfiles;
  }

  public List<LogInfo> getLogInfos() {
    return Collections.unmodifiableList(logInfos);
  }

  static void validateVersion(short version) {
    if (version != File_Copy_Protocol_Metadata_Response_Version_V1) {
      throw new IllegalArgumentException("Unknown version for FileCopyProtocolMetaDataResponse: " + version);
    }
  }
}
