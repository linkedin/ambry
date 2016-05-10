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

/**
 * Represents a portion of a file. Provides the start and end offset of a file
 */
public class FileSpan {
  private long fileStartOffset;
  private long fileEndOffset;

  public FileSpan(long fileStartOffset, long fileEndOffset) {
    if (fileEndOffset < fileStartOffset) {
      throw new IllegalArgumentException("File span needs to be positive");
    }
    this.fileStartOffset = fileStartOffset;
    this.fileEndOffset = fileEndOffset;
  }

  public long getStartOffset() {
    return fileStartOffset;
  }

  public long getEndOffset() {
    return fileEndOffset;
  }
}
