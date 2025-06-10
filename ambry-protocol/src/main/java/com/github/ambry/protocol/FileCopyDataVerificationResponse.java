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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;


public class FileCopyDataVerificationResponse extends Response {
  private final List<String> checksums;

  private static final int CHECKSUMS_COUNT_FIELD_SIZE_IN_BYTES = 4;

  /**
   * The version of the request.
   */
  public static final short FILE_COPY_DATA_VERIFICATION_RESPONSE_VERSION_V_1 = 1;

  /**
   * The current version of the response.
   */
  static short CURRENT_VERSION = FILE_COPY_DATA_VERIFICATION_RESPONSE_VERSION_V_1;

  public FileCopyDataVerificationResponse(short versionId, int correlationId, String clientId, ServerErrorCode errorCode,
      @Nonnull List<String> checksums) {
    super(RequestOrResponseType.FileCopyDataVerificationResponse, versionId, correlationId, clientId, errorCode);

    this.checksums = Objects.requireNonNull(checksums, "checksums must not be null");
  }

  public FileCopyDataVerificationResponse(int correlationId, String clientId, ServerErrorCode serverErrorCode) {
    this(CURRENT_VERSION, correlationId, clientId, serverErrorCode, new ArrayList<>());
  }

  public FileCopyDataVerificationResponse(ServerErrorCode serverErrorCode) {
    this(-1, "", serverErrorCode);
  }

  public static FileCopyDataVerificationResponse readFrom(@Nonnull DataInputStream stream) throws IOException {
    Objects.requireNonNull(stream, "stream should not be null");

    short versionId = stream.readShort();
    validateVersion(versionId);

    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);

    ServerErrorCode errorCode = ServerErrorCode.values()[stream.readShort()];

    if (errorCode != ServerErrorCode.NoError) {
      return new FileCopyDataVerificationResponse(versionId, correlationId, clientId, errorCode, new ArrayList<>());
    }

    int checksumCount = stream.readInt();
    List<String> checksums = new java.util.ArrayList<>(checksumCount);
    for (int i = 0; i < checksumCount; i++) {
      checksums.add(Utils.readIntString(stream));
    }
    return new FileCopyDataVerificationResponse(versionId, correlationId, clientId, errorCode, checksums);
  }

  @Override
  public long sizeInBytes() {
    long size = super.sizeInBytes() + CHECKSUMS_COUNT_FIELD_SIZE_IN_BYTES;
    for (String checksum : checksums) {
      size += checksum.getBytes().length;
    }
    return size;
  }

  public String toString() {
    return "FileCopyDataVerificationResponse[checksums=" + checksums + "]";
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeInt(checksums.size());
    for (String checksum : checksums) {
      Utils.serializeString(bufferToSend, checksum, java.nio.charset.StandardCharsets.UTF_8);
    }
  }

  public List<String> getChecksums() {
    return checksums;
  }

  /**
   * Validate the version of the request.
   */
  static void validateVersion(short version) {
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileCopyDataVerificationResponse: " + version);
    }
  }
}
