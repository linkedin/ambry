package com.github.ambry.network;

import java.io.IOException;


/**
 *  Factory to create a {@link ConnectionManager}.
 */
public interface ConnectionManagerFactory {
  /**
   * Gets a {@link ConnectionManager}
   * @return The {@link ConnectionManager} that was created
   */
  public ConnectionManager getConnectionManager()
      throws IOException;
}
