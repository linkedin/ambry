/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network;

import com.github.ambry.clustermap.DataNodeId;
import java.util.HashMap;
import java.util.LinkedList;


/**
 * The ConnectionTracker keeps track of current connections to datanodes, and provides methods to check out and
 * check in connections.
 *
 * This class is not thread safe.
 */

class ConnectionTracker {
  private final HashMap<String, HostPortPoolManager> hostPortToPoolManager;
  private final HashMap<String, HostPortPoolManager> connectionIdToPoolManager;
  private final int maxConnectionsPerPortPlainText;
  private final int maxConnectionsPerPortSsl;
  private int totalManagedConnectionsCount;

  /**
   * Instantiates a ConnectionTracker
   * @param maxConnectionsPerPortPlainText the connection pool limit for plain text connections to a (host, port)
   * @param maxConnectionsPerPortSsl the connection pool limit for ssl connections to a (host, port)
   */
  ConnectionTracker(int maxConnectionsPerPortPlainText, int maxConnectionsPerPortSsl) {
    hostPortToPoolManager = new HashMap<String, HostPortPoolManager>();
    connectionIdToPoolManager = new HashMap<String, HostPortPoolManager>();
    totalManagedConnectionsCount = 0;
    this.maxConnectionsPerPortPlainText = maxConnectionsPerPortPlainText;
    this.maxConnectionsPerPortSsl = maxConnectionsPerPortSsl;
  }

  /**
   * Returns true if a new connection may be created for the given hostPort, that is if the number of connections for
   * the given hostPort has not reached the pool limit.
   * @param host the host associated with this check.
   * @param port the port associated with this check.
   * @param dataNodeId the {@link DataNodeId} associated with this check.
   * @return true if a new connection may be created, false otherwise.
   */
  boolean mayCreateNewConnection(String host, Port port, DataNodeId dataNodeId) {
    // TODO refactor methods in this class to remove host and port because both info can be parsed from dataNodeId
    return !getHostPortPoolManager(host, port, dataNodeId).hasReachedPoolLimit();
  }

  /**
   * Start tracking a new connection id associated with the given host and port. Note that this connection will not
   * be made available for checking out until a {@link #checkInConnection(String)} is called on it.
   * @param host the host to which this connection belongs.
   * @param port the port on the host to which this connection belongs.
   * @param connId the connection id of the connection.
   * @param dataNodeId the {@link DataNodeId} associated with this connection
   */
  void startTrackingInitiatedConnection(String host, Port port, String connId, DataNodeId dataNodeId) {
    HostPortPoolManager hostPortPoolManager = getHostPortPoolManager(host, port, dataNodeId);
    hostPortPoolManager.incrementPoolCount();
    connectionIdToPoolManager.put(connId, hostPortPoolManager);
    totalManagedConnectionsCount++;
  }

  /**
   * Attempts to check out an existing connection to the hostPort provided, or returns null if none available.
   * @param host The host to connect to.
   * @param port The port on the host to connect to.
   * @param dataNodeId The {@link DataNodeId} to connect to.
   * @return connectionId, if there is one available to use, null otherwise.
   */
  String checkOutConnection(String host, Port port, DataNodeId dataNodeId) {
    return getHostPortPoolManager(host, port, dataNodeId).checkOutConnection();
  }

  /**
   * Add connection to available pool.
   * @param connectionId the id of the newly established or previously checked out connection.
   * @throws {@link IllegalArgumentException} if the passed in connection id is invalid.
   */
  void checkInConnection(String connectionId) {
    HostPortPoolManager hostPortPoolManager = connectionIdToPoolManager.get(connectionId);
    if (hostPortPoolManager == null) {
      throw new IllegalArgumentException("Invalid connection id passed in");
    }
    hostPortPoolManager.checkInConnection(connectionId);
  }

  /**
   * Remove and stop tracking the given connection id.
   * @param connectionId connection to remove.
   * @throws {@link IllegalArgumentException} if the passed in connection id is invalid.
   * @return {@link DataNodeId} associated with this connection.
   */
  DataNodeId removeConnection(String connectionId) {
    HostPortPoolManager hostPortPoolManager = connectionIdToPoolManager.remove(connectionId);
    if (hostPortPoolManager == null) {
      throw new IllegalArgumentException("Invalid connection id passed in");
    }
    DataNodeId dataNodeId = hostPortPoolManager.removeConnection(connectionId);
    totalManagedConnectionsCount--;
    return dataNodeId;
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
   * @param dataNodeId The {@link DataNodeId} associated with (host, port) pair
   * @return the HostPortPoolManager for the associated (host, port) pair.
   */
  private HostPortPoolManager getHostPortPoolManager(String host, Port port, DataNodeId dataNodeId) {
    String lookupStr = host + ":" + port.getPort();
    HostPortPoolManager poolManager = hostPortToPoolManager.get(lookupStr);
    if (poolManager == null) {
      poolManager = new HostPortPoolManager(
          port.getPortType() == PortType.SSL ? maxConnectionsPerPortSsl : maxConnectionsPerPortPlainText, dataNodeId);
      hostPortToPoolManager.put(lookupStr, poolManager);
    }
    return poolManager;
  }

  /**
   * Returns max number of connections allowed for a plain text port.
   */
  int getMaxConnectionsPerPortPlainText() {
    return maxConnectionsPerPortPlainText;
  }

  /**
   * Returns max number of connections allowed for a ssl port.
   */
  int getMaxConnectionsPerPortSsl() {
    return maxConnectionsPerPortSsl;
  }

  /**
   * HostPortPoolManager manages all the connections to a specific (host,
   * port) pair. The  {@link ConnectionTracker} creates one for every (host, port) pair it knows of.
   */
  private class HostPortPoolManager {
    private final int maxConnectionsToHostPort;
    private final LinkedList<String> availableConnections;
    private final DataNodeId dataNodeId;
    private int poolCount;

    /**
     * Instantiate a HostPortPoolManager
     * @param poolLimit the max connections allowed for this hostPort.
     * @param dataNodeId the {@link DataNodeId} associated with this {@link HostPortPoolManager}.
     */
    HostPortPoolManager(int poolLimit, DataNodeId dataNodeId) {
      poolCount = 0;
      maxConnectionsToHostPort = poolLimit;
      availableConnections = new LinkedList<String>();
      this.dataNodeId = dataNodeId;
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
     * @return {@link DataNodeId} associated with this manager and this connection.
     */
    DataNodeId removeConnection(String connectionId) {
      availableConnections.remove(connectionId);
      poolCount--;
      return dataNodeId;
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
