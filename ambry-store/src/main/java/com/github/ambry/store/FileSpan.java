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
 * Represents a portion of a log. Provides the start and end offset of a log
 */
class FileSpan {
  private Offset startOffset;
  private Offset endOffset;

  /**
   * Creates a file span with the given start and end offsets.
   * @param startOffset the start {@link Offset} of the FileSpan.
   * @param endOffset the end {@link Offset} of the FileSpan.
   * @throws IllegalArgumentException if {@code endOffset} < {@code startOffset}
   */
  FileSpan(Offset startOffset, Offset endOffset) {
    if (endOffset.compareTo(startOffset) < 0) {
      throw new IllegalArgumentException("File span needs to be positive");
    }
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  /**
   * @return the start {@link Offset} represented by this FileSpan. Guaranteed to be <= {@link #getEndOffset()}.
   */
  Offset getStartOffset() {
    return startOffset;
  }

  /**
   * @return the end {@link Offset} represented by this FileSpan. Guaranteed to be >= {@link #getStartOffset()}.
   */
  Offset getEndOffset() {
    return endOffset;
  }

  /**
   * @param offset the {@link Offset} to check
   * @return {@code true} if {@code offset} is in this {@link FileSpan} (start and end offsets are considered inclusive)
   */
  boolean inSpan(Offset offset) {
    return offset.compareTo(startOffset) >= 0 && offset.compareTo(endOffset) <= 0;
  }

  @Override
  public String toString() {
    return "StartOffset=[" + startOffset + "], EndOffset=[" + endOffset + "]";
  }
}
