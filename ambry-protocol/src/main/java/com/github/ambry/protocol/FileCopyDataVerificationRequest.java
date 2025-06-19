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

/**
 * Request to verify the data integrity of a file copy operation by checking specific byte ranges in a log segment.
 * This request is used to ensure that the copied data matches the expected data in the source log segment.
 */
public class FileCopyDataVerificationRequest extends RequestOrResponse {

  // The Partition in which the requested log segment resides
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
      @Nonnull PartitionId partitionId, @Nonnull String fileName, @Nonnull List<Pair<Integer, Integer>> ranges) {
    super(RequestOrResponseType.FileCopyDataVerificationRequest, versionId, correlationId, clientId);
    validateVersion(versionId);
    Objects.requireNonNull(clientId, "clientId must not be null");

    this.partitionId = Objects.requireNonNull(partitionId, "partitionId must not be null");

    Objects.requireNonNull(fileName, "fileName must not be null");
    if (fileName.isEmpty()) {
      throw new IllegalArgumentException("fileName must not be empty");
    }
    this.fileName = fileName;

    Objects.requireNonNull(ranges, "ranges must not be null");
    if (ranges.isEmpty()) {
      throw new IllegalArgumentException("ranges must not be empty");
    }
    for (Pair<Integer, Integer> range : ranges) {
      if (range.getFirst() < 0 || range.getSecond() < 0 || range.getFirst() > range.getSecond()) {
        throw new IllegalArgumentException("Invalid byte range: [" + range.getFirst() + ", " + range.getSecond() + "]");
      }
    }
    this.ranges = ranges;
  }

  /**
   * Reads a {@link FileCopyDataVerificationRequest} from the provided {@link DataInputStream}.
   * @param stream the input stream to read from.
   * @param clusterMap the cluster map to resolve partition IDs.
   * @return a {@link FileCopyDataVerificationRequest} object.
   * @throws IOException if an I/O error occurs while reading from the stream.
   */
  public static FileCopyDataVerificationRequest readFrom(@Nonnull DataInputStream stream, @Nonnull ClusterMap clusterMap)
      throws IOException {
    Objects.requireNonNull(stream, "stream must not be null");
    Objects.requireNonNull(clusterMap, "clusterMap must not be null");

    short version = stream.readShort();
    validateVersion(version);

    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    String fileName = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);

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

  /**
   * Prepares the buffer to be sent over the network.
   */
  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    bufferToSend.writeBytes(partitionId.getBytes());
    bufferToSend.writeInt(ranges.size());
    for (Pair<Integer, Integer> range : ranges) {
      bufferToSend.writeLong(range.getFirst());
      bufferToSend.writeLong(range.getSecond());
    }
  }

  @Override
  public void accept(RequestVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * Validate the version of the request.
   */
  static void validateVersion(short version) {
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileCopyDataVerificationRequest: " + version);
    }
  }

  /**
   * Get the partition ID of the log segment to verify.
   * @return the partition ID.
   */
  public PartitionId getPartitionId() {
    return partitionId;
  }

  /**
   * Get the file name of the log segment to verify.
   * @return the file name.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Get the list of byte ranges to verify.
   * @return a list of byte ranges, each represented as a pair of start and end offsets.
   */
  public List<Pair<Integer, Integer>> getRanges() {
    return new ArrayList<>(ranges);
  }
}