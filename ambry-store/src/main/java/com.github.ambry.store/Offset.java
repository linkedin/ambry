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
import java.nio.charset.StandardCharsets;


/**
 * Denotes an offset inside the log.
 */
public class Offset implements Comparable<Offset> {
  private final String name;
  private final long offset;

  private static final short CURRENT_VERSION = 0;
  private static final int VERSION_SIZE = 2;
  private static final int NAME_LENGTH_SIZE = 4;
  private static final int OFFSET_SIZE = 8;

  /**
   * Construct an Offset that refers to a position in Log.
   * @param name the name of segment being referred to.
   * @param offset the offset within the segment.
   * @throws IllegalArgumentException if {@code name} is {@code null} or {@code offset} < 0.
   */
  Offset(String name, long offset) {
    if (name == null || offset < 0) {
      throw new IllegalArgumentException("Name [" + name + "] is null or offset [" + offset + "] < 0");
    }
    this.name = name;
    this.offset = offset;
  }

  /**
   * Constructs an Offset from a stream.
   * @param stream the {@link DataInputStream} that will contain the serialized form of this object.
   * @throws IllegalArgumentException if {@code name} is {@code null} or {@code offset} < 0 or if the version
   * of the record is not recognized.
   * @throws IOException if there are I/O problems reading from the stream.
   */
  public static Offset fromBytes(DataInputStream stream) throws IOException {
    String name;
    long offset;
    int version = stream.readShort();
    switch (version) {
      case 0:
        name = Utils.readIntString(stream, StandardCharsets.UTF_8);
        offset = stream.readLong();
        break;
      default:
        throw new IllegalArgumentException("Unrecognized version [" + version + "] of Offset");
    }
    return new Offset(name, offset);
  }

  /**
   * @return the name of the log segment for which the offset provided by {@link #getOffset()} is valid. Guaranteed to
   * be non-null and non-empty.
   */
  public String getName() {
    return name;
  }

  /**
   * @return the offset in the log segment with name provided by {@link #getName()}. Guaranteed to be >= 0.
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Converts this object into its byte representation.
   * @return the byte representation of this object.
   */
  byte[] toBytes() {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    int size = VERSION_SIZE + NAME_LENGTH_SIZE + nameBytes.length + OFFSET_SIZE;
    byte[] bytes = new byte[size];
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    buf.putShort(CURRENT_VERSION);
    buf.putInt(nameBytes.length);
    buf.put(nameBytes);
    buf.putLong(offset);
    return bytes;
  }

  @Override
  public int compareTo(Offset o) {
    int compare = LogSegmentNameHelper.COMPARATOR.compare(name, o.name);
    return compare == 0 ? Long.compare(offset, o.offset) : compare;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Offset other = (Offset) o;
    return compareTo(other) == 0;
  }

  @Override
  public int hashCode() {
    int result = LogSegmentNameHelper.hashcode(name);
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "Name = [" + name + "] Offset = [" + offset + "]";
  }
}
