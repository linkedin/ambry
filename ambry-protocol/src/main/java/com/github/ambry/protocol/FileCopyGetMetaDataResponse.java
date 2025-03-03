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
import com.github.ambry.store.LogInfo;
import com.github.ambry.store.StoreLogInfo;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Protocol class representing response to get metadata of a file.
 */
public class FileCopyGetMetaDataResponse extends Response {
  /**
   * The number of log files.
   */
  private final int numberOfLogfiles;

  /**
   * The list of log files.
   */
  private final List<LogInfo> logInfos;

  /**
   * The version of the response.
   */
  public static final short FILE_COPY_PROTOCOL_METADATA_RESPONSE_VERSION_V_1 = 1;

  /**
   * The current version of the response.
   */
  static short CURRENT_VERSION = FILE_COPY_PROTOCOL_METADATA_RESPONSE_VERSION_V_1;

  /**
   * Constructor for FileCopyGetMetaDataResponse
   * @param versionId The version of the response.
   * @param correlationId The correlation id of the response.
   * @param clientId The client id of the response.
   * @param numberOfLogfiles The number of log files.
   * @param logInfos The list of log files.
   * @param errorCode The error code of the response.
   */
  public FileCopyGetMetaDataResponse(short versionId, int correlationId, String clientId, int numberOfLogfiles,
      @Nonnull List<LogInfo> logInfos, ServerErrorCode errorCode) {
    super(RequestOrResponseType.FileCopyGetMetaDataResponse, versionId, correlationId, clientId, errorCode);
    Objects.requireNonNull(logInfos, "logInfos must not be null");

    validateVersion(versionId);
    this.numberOfLogfiles = numberOfLogfiles;
    this.logInfos = logInfos;
  }

  /**
   * Constructor for FileCopyGetMetaDataResponse
   * @param correlationId The correlation id of the response.
   * @param clientId The client id of the response.
   * @param serverErrorCode The error code of the response.
   */
  public FileCopyGetMetaDataResponse(int correlationId, String clientId, ServerErrorCode serverErrorCode) {
    this(CURRENT_VERSION, correlationId, clientId, 0, new ArrayList<>(), serverErrorCode);
  }

  /**
   * Constructor for FileCopyGetMetaDataResponse
   * @param serverErrorCode The error code of the response.
   */
  public FileCopyGetMetaDataResponse(ServerErrorCode serverErrorCode) {
    this(CURRENT_VERSION, -1, "", 0, new ArrayList<>(), serverErrorCode);
  }

  /**
   * Deserialize a FileCopyGetMetaDataResponse
   * @param stream The stream to read from
   * @return The FileCopyGetMetaDataResponse
   */
  public static FileCopyGetMetaDataResponse readFrom(
      @Nonnull DataInputStream stream) throws IOException {
    Objects.requireNonNull(stream, "stream should not be null");

    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.FileCopyGetMetaDataResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible. Expected : {}, Actual : {}" +
          RequestOrResponseType.FileCopyGetMetaDataResponse + type);
    }
    short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode errorCode = ServerErrorCode.values()[stream.readShort()];

    if (errorCode != ServerErrorCode.NoError) {
      //Setting the number of logfiles to 0 as there are no logfiles to be read.
      return new FileCopyGetMetaDataResponse(versionId, correlationId, clientId, 0, new ArrayList<>(), errorCode);
    }
    int numberOfLogfiles = stream.readInt();
    List<LogInfo> logInfos = new ArrayList<>();
    for (int i = 0; i < numberOfLogfiles; i++) {
      logInfos.add(StoreLogInfo.readFrom(stream));
    }
    return new FileCopyGetMetaDataResponse(versionId, correlationId, clientId, numberOfLogfiles, logInfos, errorCode);
  }

  /**
   * Write the response to the buffer
   */
  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeInt(numberOfLogfiles);
    for (LogInfo logInfo : logInfos) {
      logInfo.writeTo(bufferToSend);
    }
  }

  /**
   * Get the size of the response in bytes
   */
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

  /**
   * Get the number of log files
   */
  public int getNumberOfLogfiles() {
    return numberOfLogfiles;
  }

  /**
   * Get the list of log files
   */
  public List<LogInfo> getLogInfos() {
    return Collections.unmodifiableList(logInfos);
  }

  /**
   * Validate the version of the response
   */
  static void validateVersion(short version) {
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileCopyProtocolMetaDataResponse: " + version);
    }
  }
}
