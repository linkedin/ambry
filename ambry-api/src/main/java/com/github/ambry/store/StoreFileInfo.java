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

import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;


/**
 * Represents a file saved in store.
 * Contains basic info like fileName and fileSize
 */
public class StoreFileInfo implements FileInfo {
  /**
   * Name of the file
   */
  private final String fileName;

  /**
   * Size of the file in bytes
   */
  private final long fileSize;

  /**
   * The size of the file name field in bytes.
   */
  private static final int FILE_NAME_FIELD_SIZE_IN_BYTES = 4;

  /**
   * The size of the file size field in bytes.
   */
  private static final int FILE_SIZE_FIELD_SIZE_IN_BYTES = 8;

  /**
   * Constructor to create a FileInfo
   * @param fileName name of the file
   * @param fileSize file size in bytes
   */
  public StoreFileInfo(String fileName, Long fileSize) {
    this.fileName = fileName;
    this.fileSize = fileSize;
  }

  /**
   * Get the name of the file
   */
  @Override
  public String getFileName() {
    return fileName;
  }

  /**
   * Get the size of the file
   */
  @Override
  public Long getFileSize() {
    return fileSize;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FileInfo{")
      .append("fileName='").append(fileName).append('\'')
      .append(", fileSize=").append(fileSize)
      .append('}');
    return sb.toString();
  }

  /**
   * Returns the size of the FileInfo object in bytes.
   */
  public long sizeInBytes() {
    return FILE_NAME_FIELD_SIZE_IN_BYTES + fileName.length() + FILE_SIZE_FIELD_SIZE_IN_BYTES;
  }

  /**
   * Reads a FileInfo object from the given input stream.
   * @param stream The input stream to read from.
   * @return The FileInfo object read from the input stream.
   */
  public static FileInfo readFrom(DataInputStream stream) throws IOException {
    Objects.requireNonNull(stream, "stream should not be null");

    String fileName = Utils.readIntString(stream);
    long fileSize = stream.readLong();
    return new StoreFileInfo(fileName, fileSize);
  }

  /**
   * Writes the FileInfo object to the given output stream.
   * @param buf The output stream to write to.
   */
  public void writeTo(ByteBuf buf) {
    Objects.requireNonNull(buf, "buf should not be null");

    Utils.serializeString(buf, fileName, Charset.defaultCharset());
    buf.writeLong(fileSize);
  }
}
