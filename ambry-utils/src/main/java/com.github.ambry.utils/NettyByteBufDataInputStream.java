/**
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import java.io.DataInputStream;


/**
 * NettyByteBufDataInputStream wraps a {@link ByteBuf} within a {@link DataInputStream}.
 */
public class NettyByteBufDataInputStream extends DataInputStream {
  private final ByteBuf buffer;

  /**
   * The constructor to create a {@link NettyByteBufDataInputStream}.
   * @param buffer The buffer from which {@link DataInputStream} will be created upon.
   */
  public NettyByteBufDataInputStream(ByteBuf buffer) {
    super(new ByteBufInputStream(buffer));
    this.buffer = buffer;
  }

  /**
   * Return the underlying {@link ByteBuf}.
   * @return The underlying {@link ByteBuf}.
   */
  public ByteBuf getBuffer() {
    return this.buffer;
  }
}
