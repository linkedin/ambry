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
package com.github.ambry.filetransfer;

import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * FileChunkInfo contains the information required to copy a chunk of a file from one node to another.
 */
public class FileChunkInfo {
  /**
   * The name of the file to be copied
   */
  private final String fileName;

  /**
   * The start offset of the file to be copied
   */
  private final long startOffset;

  /**
   * The length of the chunk to be copied
   */
  private final long chunkLengthInBytes;

  /**
   * Whether the file is chunked or not
   */
  private final boolean isChunked;

  /**
   * Constructor to create FileChunkInfo
   * @param fileName The name of the file to be copied
   * @param startOffset The start offset of the file to be copied
   * @param chunkLengthInBytes The length of the chunk to be copied
   * @param isChunked Whether the file is chunked or not
   */
  public FileChunkInfo(@Nonnull String fileName, long startOffset, long chunkLengthInBytes, boolean isChunked) {
    Objects.requireNonNull(fileName, "fileName cannot be null");

    this.fileName = fileName;
    this.startOffset = startOffset;
    this.chunkLengthInBytes = chunkLengthInBytes;
    this.isChunked = isChunked;
  }

  /**
   * Get the name of the file to be copied
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Get the start offset of the file to be copied
   */
  public long getStartOffset() {
    return startOffset;
  }

  /**
   * Get the length of the chunk to be copied
   */
  public long getChunkLengthInBytes() {
    return chunkLengthInBytes;
  }

  /**
   * Get whether the file is chunked or not
   */
  public boolean isChunked() {
    return isChunked;
  }
}
