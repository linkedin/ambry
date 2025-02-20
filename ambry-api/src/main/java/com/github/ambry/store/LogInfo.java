/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;


/**
 * Represents the info for a log segment saved in store.
 * Contains basic info like log segment, index segments and bloom filters.
 */
public interface LogInfo {

  /**
   * Get the log segment
   * @return Info about the log segment of type FileInfo
   */
  FileInfo getLogSegment();

  /**
   * Get the index segments
   * @return Info about the index segments of type FileInfo
   */
  List<FileInfo> getIndexSegments();

  /**
   * Get the bloom filters
   * @return Info about the bloom filters of type FileInfo
   */
  List<FileInfo> getBloomFilters();

  static LogInfo readFrom(DataInputStream stream) throws IOException {
    return null;
  }

  /**
   * Write the LogInfo to the buffer
   * @param bufferToSend the buffer to write to of type ByteBuf
   */
  void writeTo(ByteBuf bufferToSend);

  /**
   * Get the size of the LogInfo in bytes
   * This is to be used for SerDe purposes
   * @return Long
   */
  long sizeInBytes();
}
