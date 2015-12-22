package com.github.ambry.router;

import java.util.concurrent.ConcurrentHashMap;


/**
 * The ConnectionManager keeps track of current connections to datanodes, and provides methods
 * to checkout and checkin connections. This class also ensures maxConnectionsPerHostPort is not
 * exceeded.
 */

class ConnectionManager {
  public void addToAvailableConnections(Connection conn) {
  }

  /** Attempts to check out a connection to the host:port provided, or returns null if none available. In the
   * latter case, initiates a connection to the host:port unless max connections to it has been reached.
   *
   * @param host
   * @param port
   * @return
   */
  public Connection checkOut(String host, int port) {
    // if any available, give that
    // else if max connections to hostport is reached, return null
    // else initiate a new connection and return null
  }

  public void removeFromAvailableConnections() {

  }
}
