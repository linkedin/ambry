package com.github.ambry.store;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * A factory interface to create the FindToken
 */
public interface FindTokenFactory {
  /**
   * The find token created using the input stream provided
   * @param stream The stream that is used to create the find token
   * @return The find token created from the stream
   */
  FindToken getFindToken(DataInputStream stream) throws IOException;
}
