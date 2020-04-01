/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.utils;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Objects;


/**
 * ByteBufferDataInputStream wraps a {@link ByteBuffer} within a {@link DataInputStream}.
 */
public class ByteBufferDataInputStream extends DataInputStream {
  private final ByteBuffer buffer;

  /**
   * The constructor to create a {@link ByteBufferDataInputStream}.
   * @param buffer The buffer from which {@link DataInputStream} will be created upon.
   */
  public ByteBufferDataInputStream(ByteBuffer buffer) {
    super(new ByteBufferInputStream(buffer));
    Objects.requireNonNull(buffer);
    this.buffer = buffer;
  }

  /**
   * Return the underlying {@link ByteBuffer}.
   * @return The underlying {@link ByteBuffer}.
   */
  public ByteBuffer getBuffer() {
    return buffer;
  }
}
