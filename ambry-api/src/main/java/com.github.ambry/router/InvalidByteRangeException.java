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

package com.github.ambry.router;

public class InvalidByteRangeException extends Exception {
  public InvalidByteRangeException(ByteRange range, long totalSize) {
    super("The provided byte offset range: " + range + " exceeds the total size: " + totalSize + " of the blob");
  }

  public InvalidByteRangeException(long startOffset, long endOffset, ByteRange.ByteRangeType type) {
    super(buildMessage(startOffset, endOffset, type));
  }

  private static String buildMessage(long startOffset, long endOffset, ByteRange.ByteRangeType type) {
    StringBuilder sb = new StringBuilder("Invalid range offsets provided for ByteRange type=").append(type);
    switch (type) {
      case LAST_N_BYTES:
        sb.append("; lastNBytes=").append(startOffset);
      case FROM_START_OFFSET:
        sb.append("; startOffset=").append(startOffset);
      case OFFSET_RANGE:
        sb.append("; startOffset=").append(startOffset).append(", endOffset=").append(endOffset);
    }
    return sb.toString();
  }
}
