package com.github.ambry.network;

import java.io.IOException;
import java.util.List;


/**
 * An interface that manages all network connections to a set of nodes (host-port). Provides methods to check out and
 * check in connections to the nodes and to send requests and receive responses.
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
   * Check in a previously checked out connection.
   * @param connectionId the id of the previously checked out connection.
   */
  public void checkInConnection(String connectionId);

  /**
   * Destroy the connection associated with the given connectionId.
   * @param connectionId connection to destroy.
   */
  public void destroyConnection(String connectionId);

  /**
   * Initiate sends of the given network requests and polls the ConnectionManager for results from prior requests.
   * @param timeoutMs the timeout for poll in milliseconds.
   * @param sends the list of Network requests to send.
   * @return A {@link ConnectionManagerPollResponse} containing the result/status of prior requests,
   * if any were received.
   * @throws IOException if an error is encountered.
   */
  public ConnectionManagerPollResponse sendAndPoll(long timeoutMs, List<NetworkSend> sends)
      throws IOException;

  /**
   * Get the total number of connections managed by this ConnectionManager.
   * @return the total number of connections.
   */
  public int getTotalConnectionsCount();

  /**
   * Get the number of connections managed by this ConnectionManager that are available.
   * @return return the count of available connections.
   */
  public int getAvailableConnectionsCount();
  /**
   * Close the ConnectionManager.
   * Any subsequent {@link #checkOutConnection(String, Port)} will result in an exception.
   */
  public void close();
}
