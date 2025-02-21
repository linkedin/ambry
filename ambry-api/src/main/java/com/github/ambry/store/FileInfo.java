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


/**
 * Represents a file info for a file saved in store.
 */
public interface FileInfo {

  /**
   * Get the name of the file
   * @return String
   */
  String getFileName();

  /**
   * Get the size of the file in bytes
   * @return Long
   */
  Long getFileSize();

  /**
   * Read FileInfo from the stream
   * @param stream the input stream of type DataInputStream
   * @return FileInfo
   * @throws IOException if an I/O error occurs
   */
  static FileInfo readFrom(DataInputStream stream) throws IOException {
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
