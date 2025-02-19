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
package com.github.ambry.protocol;

import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * Contains the fileName and fileSizeInBytes for a local partition. This is used
 * by LogInfo as part of filecopy metadata request.
 */
public class FileInfo {
  /**
   * The name of the file.
   */
  private final String fileName;

  /**
   * The size of the file in bytes.
   */
  private final long fileSizeInBytes;

  /**
   * The size of the file name field in bytes.
   */
  private static final int FileName_Field_Size_In_Bytes = 4;

  /**
   * The size of the file size field in bytes.
   */
  private static final int FileSize_Field_Size_In_Bytes = 8;

  /**
   * Constructs a FileInfo object with the given file name and size.
   * @param fileName The name of the file.
   * @param fileSize The size of the file in bytes.
   */
  public FileInfo(
      String fileName,
      long fileSize) {
    this.fileName = fileName;
    this.fileSizeInBytes = fileSize;
  }

  /**
   * Returns the size of the FileInfo object in bytes.
   */
  public long sizeInBytes() {
    return FileName_Field_Size_In_Bytes + fileName.length() + FileSize_Field_Size_In_Bytes;
  }

  /**
   * Reads a FileInfo object from the given input stream.
   * @param stream The input stream to read from.
   * @return The FileInfo object read from the input stream.
   */
  public static FileInfo readFrom(@Nonnull DataInputStream stream) throws IOException {
    Objects.requireNonNull(stream, "stream should not be null");

    String fileName = Utils.readIntString(stream);
    long fileSize = stream.readLong();
    return new FileInfo(fileName, fileSize);
  }

  /**
   * Writes the FileInfo object to the given output stream.
   * @param buf The output stream to write to.
   */
  public void writeTo(@Nonnull ByteBuf buf) {
    Objects.requireNonNull(buf, "buf should not be null");

    Utils.serializeString(buf, fileName, Charset.defaultCharset());
    buf.writeLong(fileSizeInBytes);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FileInfo[")
        .append("FileName=").append(fileName)
        .append(", FileSizeInBytes=").append(fileSizeInBytes)
      .append("]");
    return sb.toString();
  }

  /**
   * Returns the name of the file.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Returns the size of the file in bytes.
   */
  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }
}
