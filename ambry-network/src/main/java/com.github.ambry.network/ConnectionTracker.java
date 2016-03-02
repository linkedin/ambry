package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.LinkedList;


/**
 * The ConnectionTracker keeps track of current connections to datanodes, and provides methods to check out and
 * check in connections.
 */

class ConnectionTracker {
  private final HashMap<String, HostPortPoolManager> hostPortToPoolManager;
  private final HashMap<String, HostPortPoolManager> connectionIdToPoolManager;
  private final int maxConnectionsPerPortPlainText;
  private final int maxConnectionsPerPortSsl;
  private final Time time;
  private int totalManagedConnectionsCount;

  /**
   * Instantiates a ConnectionTracker
   * @param maxConnectionsPerPortPlainText the connection pool limit for plain text connections to a (host, port)
   * @param maxConnectionsPerPortPlainSsl the connection pool limit for ssl connections to a (host, port)
   * @param time The Time instance to use.
   */
  ConnectionTracker(int maxConnectionsPerPortPlainText, int maxConnectionsPerPortPlainSsl, Time time) {
    hostPortToPoolManager = new HashMap<String, HostPortPoolManager>();
    connectionIdToPoolManager = new HashMap<String, HostPortPoolManager>();
    totalManagedConnectionsCount = 0;
    this.maxConnectionsPerPortPlainText = maxConnectionsPerPortPlainText;
    this.maxConnectionsPerPortSsl = maxConnectionsPerPortPlainSsl;
    this.time = time;
  }

  /**
   * Returns true if a new connection may be created for the given hostPort, that is if the number of connections for
   * the given hostPort has not reached the pool limit.
   * @param host the host
   * @param port the port
   * @return true if a new connection may be created, false otherwise.
   */
  boolean mayCreateNewConnection(String host, Port port) {
    return !getHostPortPoolManager(host, port).hasReachedPoolLimit();
  }

  /**
   * Start tracking a new connection id associated with the given host and port.
   * @param host the host
   * @param port the port
   * @param connId the connection id
   */
  void addNewConnection(String host, Port port, String connId) {
    HostPortPoolManager hostPortPoolManager = getHostPortPoolManager(host, port);
    hostPortPoolManager.incrementPoolCount();
    connectionIdToPoolManager.put(connId, hostPortPoolManager);
    totalManagedConnectionsCount++;
  }

  /**
   * Attempts to check out an existing connection to the hostPort provided, or returns null if none available.
   * @param host The host to connect to.
   * @param port The port on the host to connect to.
   * @return connectionId, if there is one available to use, null otherwise.
   */
  String checkOutConnection(String host, Port port) {
    return getHostPortPoolManager(host, port).checkOutConnection();
  }

  /**
   * Add connection to available pool.
   * @param connectionId the id of the newly established or previously checked out connection.
   */
  void checkInConnection(String connectionId) {
    connectionIdToPoolManager.get(connectionId).checkInConnection(connectionId);
  }

  /**
   * Remove and stop tracking the given connection id.
   * @param connectionId connection to remove.
   */
  void removeConnection(String connectionId) {
    HostPortPoolManager hostPortPoolManager = connectionIdToPoolManager.remove(connectionId);
    hostPortPoolManager.removeConnection(connectionId);
    totalManagedConnectionsCount--;
  }

  /**
   * Return the total number of connections that are managed by this connection tracker.
   * @return the total number of initiated and/or established connections.
   */
  int getTotalConnectionsCount() {
    return totalManagedConnectionsCount;
  }

  /**
   * Return the total available connections across all hostPortPoolManagers.
   * @return total established and available connections.
   */
  int getAvailableConnectionsCount() {
    int count = 0;
    for (HostPortPoolManager hostPortPoolManager : hostPortToPoolManager.values()) {
      count += hostPortPoolManager.getAvailableConnectionsCount();
    }
    return count;
  }

  /**
   * Returns the {@link HostPortPoolManager} associated with the (host, port) pair. Creates one if not available
   * already.
   * @param host The hostname
   * @param port The port
   * @return the HostPortPoolManager for the associated (host, port) pair.
   */
  private HostPortPoolManager getHostPortPoolManager(String host, Port port) {
    String lookupStr = host + ":" + Integer.toString(port.getPort());
    HostPortPoolManager poolManager = hostPortToPoolManager.get(lookupStr);
    if (poolManager == null) {
      poolManager = new HostPortPoolManager(
          port.getPortType() == PortType.SSL ? maxConnectionsPerPortSsl : maxConnectionsPerPortPlainText);
      hostPortToPoolManager.put(lookupStr, poolManager);
    }
    return poolManager;
  }

  /**
   * HostPortPoolManager manages all the connections to a specific (host,
   * port) pair. The  {@link ConnectionTracker} creates one for every (host, port) pair it knows of.
   */
  private class HostPortPoolManager {
    private final int maxConnectionsToHostPort;
    private final LinkedList<String> availableConnections;
    private int poolCount;

    /**
     * Instantiate a HostPortPoolManager
     * @param poolLimit the max connections allowed for this hostPort.
     */
    HostPortPoolManager(int poolLimit) {
      this.poolCount = 0;
      this.maxConnectionsToHostPort = poolLimit;
      availableConnections = new LinkedList<String>();
    }

    /**
     * Return true if this manager has reached the pool limit.
     * @return true if this manager has reached the pool limit
     */
    boolean hasReachedPoolLimit() {
      return poolCount == maxConnectionsToHostPort;
    }

    /**
     * Increment the pool count.
     */
    void incrementPoolCount() {
      poolCount++;
    }

    /**
     * Attempts to check out a connection to the (host, port) associated with this manager.
     * @return returns a connection id, if there is one; null otherwise.
     */
    String checkOutConnection() {
      return availableConnections.poll();
    }

    /**
     * Add connection to available pool.
     * @param connectionId the connection id of the connection.
     */
    void checkInConnection(String connectionId) {
      availableConnections.add(connectionId);
    }

    /**
     * Remove a connection managed by this manager. This connection id could be either a checked out connection or a
     * connection that was previously available to be checked out.
     * @param connectionId the connection id of the connection.
     */
    void removeConnection(String connectionId) {
      availableConnections.remove(connectionId);
      poolCount--;
    }

    /**
     * Return the number of available connections to this hostPort
     * @return number of available connections
     */
    int getAvailableConnectionsCount() {
      return availableConnections.size();
    }
  }
}
