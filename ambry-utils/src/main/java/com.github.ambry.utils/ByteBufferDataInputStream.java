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
