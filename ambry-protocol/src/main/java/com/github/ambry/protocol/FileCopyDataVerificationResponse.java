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

/**
 * Response for file copy data verification requests.
 * This response contains a list of checksums for the copied data.
 */
public class FileCopyDataVerificationResponse extends Response {
  /**
   * The list of checksums for the copied data.
   * This is a list of strings, where each string represents a checksum.
   */
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

  /**
   * Constructs a FileCopyDataVerificationResponse with the specified parameters.
   * @param versionId the version of the response
   * @param correlationId the correlation ID for this response
   * @param clientId the client ID that made the request
   * @param errorCode the error code indicating success or failure
   * @param checksums the list of checksums for the copied data
   */
  public FileCopyDataVerificationResponse(short versionId, int correlationId, String clientId, ServerErrorCode errorCode,
      @Nonnull List<String> checksums) {
    super(RequestOrResponseType.FileCopyDataVerificationResponse, versionId, correlationId, clientId, errorCode);

    this.checksums = Objects.requireNonNull(checksums, "checksums must not be null");
  }

  /**
   * Constructs a FileCopyDataVerificationResponse with the specified parameters.
   * @param correlationId the correlation ID for this response
   * @param clientId the client ID that made the request
   * @param serverErrorCode the error code indicating success or failure
   */
  public FileCopyDataVerificationResponse(int correlationId, String clientId, ServerErrorCode serverErrorCode) {
    this(CURRENT_VERSION, correlationId, clientId, serverErrorCode, new ArrayList<>());
  }

  /**
   * Constructs a FileCopyDataVerificationResponse with the specified error code.
   * This constructor is used when there is an error and no checksums are provided.
   * @param serverErrorCode the error code indicating the failure
   */
  public FileCopyDataVerificationResponse(ServerErrorCode serverErrorCode) {
    this(-1, "", serverErrorCode);
  }

  /**
   * Reads a FileCopyDataVerificationResponse from the provided DataInputStream.
   * @param stream the DataInputStream to read from
   * @return a FileCopyDataVerificationResponse object
   * @throws IOException if an I/O error occurs while reading from the stream
   */
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

  /**
   * Returns the size in bytes of this response.
   * This includes the size of the superclass and the size of the checksums.
   * @return the size in bytes
   */
  @Override
  public long sizeInBytes() {
    long size = super.sizeInBytes() + CHECKSUMS_COUNT_FIELD_SIZE_IN_BYTES;
    for (String checksum : checksums) {
      size += checksum.getBytes().length;
    }
    return size;
  }

  @Override
  public String toString() {
    return "FileCopyDataVerificationResponse[checksums=" + checksums + "]";
  }

  /**
   * Prepares the buffer to be sent over the network.
   * This method serializes the checksums into the buffer.
   */
  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeInt(checksums.size());
    for (String checksum : checksums) {
      Utils.serializeString(bufferToSend, checksum, java.nio.charset.StandardCharsets.UTF_8);
    }
  }

  /**
   * Returns the list of checksums for the copied data.
   * @return the list of checksums
   */
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
