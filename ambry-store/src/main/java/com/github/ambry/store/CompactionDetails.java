/*
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Contains all the details required for a compaction cycle.
 */
class CompactionDetails {
  private static final short VERSION_0 = 0;
  private static final int VERSION_SIZE = 2;
  private static final int REFERENCE_TIME_SIZE = 8;
  private static final int SEGMENT_COUNT_SIZE = 4;
  private static final int SEGMENT_NAME_LENGTH_SIZE = 4;

  private final long referenceTimeMs;
  private final List<String> logSegmentsUnderCompaction;

  /**
   * Construct a representation of all the details required for a compaction cycle.
   * @param referenceTimeMs the epoch time to use to get the same results from the {@link BlobStore} as when these
   *                        details were created.
   * @param logSegmentsUnderCompaction the names of the {@link LogSegment} under compaction.
   * @throws IllegalArgumentException if {@code referenceTimeMs} < 0 or if {@code logSegmentsUnderCompaction} has no
   * elements.
   */
  CompactionDetails(long referenceTimeMs, List<String> logSegmentsUnderCompaction) {
    if (referenceTimeMs < 0 || logSegmentsUnderCompaction.size() == 0) {
      throw new IllegalArgumentException(
          "Illegal arguments provided. Ref time: [" + referenceTimeMs + "]. " + "Segments under compaction size: ["
              + logSegmentsUnderCompaction.size() + "]");
    }
    this.referenceTimeMs = referenceTimeMs;
    this.logSegmentsUnderCompaction = logSegmentsUnderCompaction;
  }

  /**
   * @param stream the serialized version of the {@link CompactionDetails} that has to be loaded.
   * @return a representation of all the details required for compaction from the given {@code stream}.
   * @throws IllegalArgumentException if the version is not recognized
   * @throws IOException if there is an I/O error reading from the stream
   */
  static CompactionDetails fromBytes(DataInputStream stream) throws IOException {
    CompactionDetails details;
    short version = stream.readShort();
    switch (version) {
      case VERSION_0:
        long referenceTime = stream.readLong();
        int segmentCount = stream.readInt();
        List<String> logSegmentsUnderCompaction = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
          logSegmentsUnderCompaction.add(Utils.readIntString(stream));
        }
        details = new CompactionDetails(referenceTime, logSegmentsUnderCompaction);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized version: " + version);
    }
    return details;
  }

  /**
   * @return the epoch time to use to get the same results from the {@link BlobStore} as when the details were created.
   * Guaranteed to be >= 0.
   */
  long getReferenceTimeMs() {
    return referenceTimeMs;
  }

  /**
   * @return the names of the segments under compaction. Guaranteed to contain at least one element.
   */
  List<String> getLogSegmentsUnderCompaction() {
    return logSegmentsUnderCompaction;
  }

  /**
   * @return serialized representation of this object.
   */
  byte[] toBytes() {
    /*
      Description of format

      version
      referenceTimeMs
      size of the logSegmentsUnderCompaction list
      segment_1_name_length segment_1_name
      segment_2_name_length segment_2_name
      ....
     */

    List<byte[]> segmentNameBytesList = new ArrayList<>();
    int size = VERSION_SIZE + REFERENCE_TIME_SIZE + SEGMENT_COUNT_SIZE;
    for (String segmentName : logSegmentsUnderCompaction) {
      byte[] segmentNameBytes = segmentName.getBytes();
      segmentNameBytesList.add(segmentNameBytes);
      size += SEGMENT_NAME_LENGTH_SIZE + segmentNameBytes.length;
    }

    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    // version
    bufWrap.putShort(VERSION_0);
    // reference time
    bufWrap.putLong(referenceTimeMs);
    // size of logSegmentsUnderCompaction
    bufWrap.putInt(logSegmentsUnderCompaction.size());
    // log segments under compaction
    for (byte[] segmentNameBytes : segmentNameBytesList) {
      bufWrap.putInt(segmentNameBytes.length);
      bufWrap.put(segmentNameBytes);
    }
    return buf;
  }

  @Override
  public String toString() {
    return "(Delete Ref Time [" + referenceTimeMs + "] Segments to compact [" + logSegmentsUnderCompaction + "])";
  }
}
