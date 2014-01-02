package com.github.ambry.store;

import java.nio.ByteBuffer;
import java.io.IOException;

/**
 * Provides a read interface into the underlying storage layer
 */
public interface Read {

  /**
   * Read from the underlying store(file) into the buffer starting at the given position in the store
   * @param buffer The buffer into which the read needs to write to
   * @param position The position to start the read from
   * @throws IOException
   */
  void readInto(ByteBuffer buffer , long position) throws IOException;

  /**
   * Specifies the complete length of the underlying store
   * @return The complete length of the store represented by this read interface
   */
  long totalLength() throws IOException;
}
