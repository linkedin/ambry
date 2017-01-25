/**
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
  private static final byte[] ZERO_LENGTH_ARRAY = new byte[0];

  private static final short VERSION_0 = 0;
  private static final int VERSION_SIZE = 2;
  private static final int REFERENCE_TIME_SIZE = 8;
  private static final int SEGMENT_COUNT_SIZE = 4;
  private static final int SEGMENT_NAME_LENGTH_SIZE = 4;
  private static final int SWAP_SPACE_COUNT_SIZE = 4;

  private final long referenceTime;
  private final List<String> logSegmentsUnderCompaction;
  private final String extraSegmentName;
  private final int swapSpaceCount;

  /**
   * Construct a representation of all the details required for a compaction cycle.
   * @param referenceTime the epoch time to use to get the same results from the {@link BlobStore} as when these details
   *                      were created.
   * @param logSegmentsUnderCompaction the names of the {@link LogSegment} under compaction.
   * @param extraSegmentName the name of the segment which will provide the "extra" data. Should be {@code null} if no
   *                         extra data is required.
   * @param swapSpaceCount the number of swap spaces that will be required for compaction.
   */
  CompactionDetails(long referenceTime, List<String> logSegmentsUnderCompaction, String extraSegmentName,
      int swapSpaceCount) {
    this.referenceTime = referenceTime;
    this.logSegmentsUnderCompaction = logSegmentsUnderCompaction;
    this.extraSegmentName = extraSegmentName;
    this.swapSpaceCount = swapSpaceCount;
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
        String extraLogSegmentName = Utils.readIntString(stream);
        if (extraLogSegmentName.isEmpty()) {
          extraLogSegmentName = null;
        }
        int swapSpaceCount = stream.readInt();
        details = new CompactionDetails(referenceTime, logSegmentsUnderCompaction, extraLogSegmentName, swapSpaceCount);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized version: " + version);
    }
    return details;
  }

  /**
   * @return the epoch time to use to get the same results from the {@link BlobStore} as when the details were created
   */
  long getReferenceTime() {
    return referenceTime;
  }

  /**
   * @return the names of the segments under compaction.
   */
  List<String> getLogSegmentsUnderCompaction() {
    return logSegmentsUnderCompaction;
  }

  /**
   * @return the name of the segment which will provide the "extra" data. Can be {@code null} or empty if no extra data
   * is required.
   */
  String getExtraSegmentName() {
    return extraSegmentName;
  }

  /**
   * @return the number of swap spaces that will be required for compaction.
   */
  int getSwapSpaceCount() {
    return swapSpaceCount;
  }

  /**
   * @return serialized representation of this object.
   */
  byte[] toBytes() {
    /*
      Description of format

      version
      referenceTime
      size of the logSegmentsUnderCompaction list
      segment_1_name_length segment_1_name
      segment_2_name_length segment_2_name
      ....
      extra_segment_name_length extra_segment_name
      swapSpaceCount
     */

    List<byte[]> segmentNameBytesList = new ArrayList<>();
    int size = VERSION_SIZE + REFERENCE_TIME_SIZE + SEGMENT_COUNT_SIZE;
    for (String segmentName : logSegmentsUnderCompaction) {
      byte[] segmentNameBytes = segmentName.getBytes();
      segmentNameBytesList.add(segmentNameBytes);
      size += SEGMENT_NAME_LENGTH_SIZE + segmentNameBytes.length;
    }
    byte[] extraSegmentNameBytes = extraSegmentName == null ? ZERO_LENGTH_ARRAY : extraSegmentName.getBytes();
    size += SEGMENT_NAME_LENGTH_SIZE + extraSegmentNameBytes.length + SWAP_SPACE_COUNT_SIZE;

    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    // version
    bufWrap.putShort(VERSION_0);
    // reference time
    bufWrap.putLong(referenceTime);
    // size of logSegmentsUnderCompaction
    bufWrap.putInt(logSegmentsUnderCompaction.size());
    // log segments under compaction
    for (byte[] segmentNameBytes : segmentNameBytesList) {
      bufWrap.putInt(segmentNameBytes.length);
      bufWrap.put(segmentNameBytes);
    }
    // extra log segment name
    bufWrap.putInt(extraSegmentNameBytes.length);
    bufWrap.put(extraSegmentNameBytes);
    // swap space count
    bufWrap.putInt(swapSpaceCount);
    return buf;
  }
}
