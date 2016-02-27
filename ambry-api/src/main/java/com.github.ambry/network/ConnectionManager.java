package com.github.ambry.network;

import java.io.IOException;


/**
 * An interface that manages all network connections to a set of nodes (host-port). Provides methods to check out and
 * check in connections to the nodes and to remove them.
 */
public interface ConnectionManager {
  /**
   * Check out a connection to the host:port provided
   * @param host The host to connect to.
   * @param port The port on the host to connect to.
   * @return connectionId, if there is one available to use, null otherwise.
   * @throws IOException if an attempt to initiate a connection as a result of this call fails.
   */
  public String checkOutConnection(String host, Port port)
      throws IOException;

  /**
   * Check in a connection. This could be a previously checked out connection or a newly available connection to be
   * managed by this connection manager.
   * It is illegal to call this method with an invalid connection id.
   * @param connectionId the id of the previously checked out connection.
   */
  public void checkInConnection(String connectionId);

  /**
   * Remove the connection associated with the given connectionId. Calling this method with an invalid connection id
   * may result in unexpected behavior.
   * @param connectionId connection to remove.
   */
  public void removeConnection(String connectionId);

  /**
   * Close the ConnectionManager.
   * Any subsequent {@link #checkOutConnection(String, Port)} will result in an exception.
   */
  public void close();
}
