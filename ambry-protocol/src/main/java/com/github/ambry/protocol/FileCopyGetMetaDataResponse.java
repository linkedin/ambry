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
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Utils;
import io.netty.util.ReferenceCountUtil;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
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
   * The stream to read metadata from.
   */
  private final DataInputStream stream;

  /**
   * The number of log files.
   */
  private final int numberOfLogfiles;

  /**
   * The list of log files.
   */
  private final List<LogInfo> logInfos;

  /**
   * The snapshot id of the partition on serving node.
   * This is returned as part of {@link FileCopyGetMetaDataResponse} and is used to ensure that a bootstrapping request
   * doesn't clash with an ongoing compaction.
   */
  private final String snapshotId;

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
   * @param snapshotId The snapshot id of the partition on serving node.
   * @param errorCode The error code of the response.
   * @param stream The stream to read metadata from.
   */
  public FileCopyGetMetaDataResponse(short versionId, int correlationId, String clientId, int numberOfLogfiles,
      @Nonnull List<LogInfo> logInfos, String snapshotId, ServerErrorCode errorCode, DataInputStream stream) {
    super(RequestOrResponseType.FileCopyGetMetaDataResponse, versionId, correlationId, clientId, errorCode);
    Objects.requireNonNull(logInfos, "logInfos must not be null");

    validateVersion(versionId);
    this.numberOfLogfiles = numberOfLogfiles;
    this.logInfos = logInfos;
    this.snapshotId = snapshotId;
    this.stream = stream;
  }

  /**
   * Constructor for FileCopyGetMetaDataResponse
   * @param correlationId The correlation id of the response.
   * @param clientId The client id of the response.
   * @param serverErrorCode The error code of the response.
   */
  public FileCopyGetMetaDataResponse(int correlationId, String clientId, ServerErrorCode serverErrorCode) {
    this(CURRENT_VERSION, correlationId, clientId, 0, new ArrayList<>(), null, serverErrorCode, null);
  }

  /**
   * Constructor for FileCopyGetMetaDataResponse
   * @param serverErrorCode The error code of the response.
   */
  public FileCopyGetMetaDataResponse(ServerErrorCode serverErrorCode) {
    this(CURRENT_VERSION, -1, "", 0, new ArrayList<>(), null, serverErrorCode, null);
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
      return new FileCopyGetMetaDataResponse(versionId, correlationId, clientId, 0, new ArrayList<>(), null, errorCode, stream);
    }
    int numberOfLogfiles = stream.readInt();
    List<LogInfo> logInfos = new ArrayList<>();
    for (int i = 0; i < numberOfLogfiles; i++) {
      logInfos.add(StoreLogInfo.readFrom(stream));
    }
    String snapshotId = Utils.readIntString(stream);

    return new FileCopyGetMetaDataResponse(versionId, correlationId, clientId, numberOfLogfiles, logInfos, snapshotId, errorCode, stream);
  }

  /**
   * Override the release method from {@link RequestOrResponse}.
   */
  @Override
  public boolean release() {
    if (bufferToSend != null) {
      ReferenceCountUtil.safeRelease(bufferToSend);
      bufferToSend = null;
    }
    // If the DataInputStream is NettyByteBufDataInputStream based, it's time to release its buffer.
    if (stream != null && stream instanceof NettyByteBufDataInputStream) {
      ReferenceCountUtil.safeRelease(((NettyByteBufDataInputStream) stream).getBuffer());
    }
    return false;
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
    Utils.serializeString(bufferToSend, snapshotId, Charset.defaultCharset());
  }

  /**
   * Get the size of the response in bytes
   */
  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + Integer.BYTES + logInfos.stream().mapToLong(LogInfo::sizeInBytes).sum() + snapshotId.length();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb
      .append("FileCopyGetMetaDataResponse[")
      .append("NumberOfLogfiles=").append(numberOfLogfiles)
      .append(", logInfoList").append(logInfos.toString()).append("], ")
      .append("SnapshotId=").append(snapshotId);
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
   * Get the snapshot id of the partition on serving node
   */
  public String getSnapshotId() {
    return snapshotId;
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
