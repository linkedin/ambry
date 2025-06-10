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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;


public class FileCopyDataVerificationRequest extends RequestOrResponse {

  // The replica in which the requested log segment resides
  private final PartitionId partitionId;

  // The logsegment fileName
  private final String fileName;

  // List of byte ranges to checksum
  private final List<Pair<Integer, Integer>> ranges;

  private static final int FILE_NAME_FIELD_SIZE_IN_BYTES = 4;

  /**
   * The version of the request.
   */
  public static final short FILE_COPY_DATA_VERIFICATION_REQUEST_VERSION_V_1 = 1;

  /**
   * The current version of the response.
   */
  static short CURRENT_VERSION = FILE_COPY_DATA_VERIFICATION_REQUEST_VERSION_V_1;

  public FileCopyDataVerificationRequest(short versionId, int correlationId, @Nonnull  String clientId,
      @Nonnull PartitionId replicaId, @Nonnull String fileName, @Nonnull List<Pair<Integer, Integer>> ranges) {
    super(RequestOrResponseType.FileCopyDataVerificationRequest, versionId, correlationId, clientId);

    this.partitionId = Objects.requireNonNull(replicaId, "replicaId must not be null");
    this.fileName = Objects.requireNonNull(fileName, "fileName must not be null");

    if (ranges.isEmpty()) {
      throw new IllegalArgumentException("ranges must not be empty");
    }
    this.ranges = Objects.requireNonNull(ranges, "ranges must not be null");
  }

  public static FileCopyDataVerificationRequest readFrom(@Nonnull DataInputStream stream, @Nonnull ClusterMap clusterMap)
      throws IOException {
    Objects.requireNonNull(stream, "stream must not be null");
    Objects.requireNonNull(clusterMap, "clusterMap must not be null");

    short version = stream.readShort();
    validateVersion(version);

    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    String fileName = Utils.readIntString(stream);

    int rangeCount = stream.readInt();
    List<Pair<Integer, Integer>> ranges = new ArrayList<>(rangeCount);

    for (int i = 0; i < rangeCount; i++) {
      long start = stream.readLong();
      long end = stream.readLong();
      if (start < 0 || end < 0 || start > end) {
        throw new IllegalArgumentException("Invalid byte range: [" + start + ", " + end + "]");
      }
      ranges.add(new Pair<>((int) start, (int) end));
    }
    return new FileCopyDataVerificationRequest(version, correlationId, clientId, partitionId, fileName, ranges);
  }

  /**
   * Get the size of the request in bytes.
   */
  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + FILE_NAME_FIELD_SIZE_IN_BYTES + fileName.length() + partitionId.getBytes().length +
        Integer.BYTES + ((long) ranges.size() * Long.BYTES * 2); // each range has two longs (start and end);
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    bufferToSend.writeInt(ranges.size());
    for (Pair<Integer, Integer> range : ranges) {
      bufferToSend.writeLong(range.getFirst());
      bufferToSend.writeLong(range.getSecond());
    }
    bufferToSend.writeBytes(partitionId.getBytes());
  }

  /**
   * Validate the version of the request.
   */
  static void validateVersion(short version) {
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileCopyDataVerificationRequest: " + version);
    }
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public String getFileName() {
    return fileName;
  }

  public List<Pair<Integer, Integer>> getRanges() {
    return new ArrayList<>(ranges);
  }
}