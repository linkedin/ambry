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
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ConnectionTracker keeps track of current connections to datanodes, and provides methods to check out and
 * check in connections.
 *
 * This class is not thread safe.
 */

class ConnectionTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionTracker.class);
  private final HashMap<Pair<String, Port>, HostPortPoolManager> hostPortToPoolManager = new HashMap<>();
  private final HashMap<String, HostPortPoolManager> connectionIdToPoolManager = new HashMap<>();
  private final HashSet<HostPortPoolManager> poolManagersBelowMinActiveConnections = new HashSet<>();
  private int totalManagedConnectionsCount = 0;
  private final int maxConnectionsPerPortPlainText;
  private final int maxConnectionsPerPortSsl;

  /**
   * Instantiates a ConnectionTracker
   * @param maxConnectionsPerPortPlainText the connection pool limit for plain text connections to a (host, port)
   * @param maxConnectionsPerPortSsl the connection pool limit for ssl connections to a (host, port)
   */
  ConnectionTracker(int maxConnectionsPerPortPlainText, int maxConnectionsPerPortSsl) {
    this.maxConnectionsPerPortPlainText = maxConnectionsPerPortPlainText;
    this.maxConnectionsPerPortSsl = maxConnectionsPerPortSsl;
  }

  /**
   * Returns true if a new connection may be created for the given hostPort, that is if the number of connections for
   * the given (host, port) has not reached the pool limit.
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
   * Configure the connection tracker to keep a specified percentage of connections to this data node ready for use.
   * @param dataNodeId the {@link DataNodeId} to configure the connection pool for.
   * @param minActiveConnectionsPercentage percentage of max connections to this data node that should be kept ready
   *                                       for use. The minimum connection number will be rounded down to the nearest
   *                                       whole number.
   */
  void setMinimumActiveConnectionsPercentage(DataNodeId dataNodeId, int minActiveConnectionsPercentage) {
    HostPortPoolManager hostPortPoolManager =
        getHostPortPoolManager(dataNodeId.getHostname(), dataNodeId.getPortToConnectTo(), dataNodeId);
    hostPortPoolManager.setMinActiveConnections(minActiveConnectionsPercentage * hostPortPoolManager.poolLimit / 100);
    if (!hostPortPoolManager.hasMinActiveConnections()) {
      poolManagersBelowMinActiveConnections.add(hostPortPoolManager);
    }
  }

  /**
   * For (host, port) pools that are below the minimum number of active connections, initiate new connections to each
   * host until they meet it.
   * @param connectionFactory the {@link ConnectionFactory} for interfacing with the networking layer.
   * @param maxNewConnectionsPerHost the max number of connections to be initiated in this call for each host.
   * @return the number of connections initiated.
   */
  int replenishConnections(ConnectionFactory connectionFactory, int maxNewConnectionsPerHost) {
    int newConnections = 0;
    Iterator<HostPortPoolManager> iter = poolManagersBelowMinActiveConnections.iterator();
    while (iter.hasNext()) {
      HostPortPoolManager poolManager = iter.next();
      try {
        // avoid continuously attempting to connect to down nodes.
        if (poolManager.dataNodeId.getState() == HardwareState.AVAILABLE) {
          int newConnectionsToHost = 0;
          while (newConnectionsToHost < maxNewConnectionsPerHost && !poolManager.hasMinActiveConnections()) {
            String connId = connectionFactory.connect(poolManager.host, poolManager.port);
            poolManager.incrementPoolCount();
            connectionIdToPoolManager.put(connId, poolManager);
            totalManagedConnectionsCount++;
            newConnections++;
            newConnectionsToHost++;
          }
          if (poolManager.hasMinActiveConnections()) {
            iter.remove();
          }
        }
      } catch (IOException e) {
        LOGGER.warn("Encountered exception while replenishing connections to {}:{}.", poolManager.host,
            poolManager.port.getPort(), e);
      }
    }
    return newConnections;
  }

  /**
   * Initiate a new connection using the provided {@link ConnectionFactory} and start tracking a new connection id
   * associated with the given host and port. Note that this connection will not be made available for checking out
   * until a {@link #checkInConnection} is called on it.
   * @param connectionFactory the {@link ConnectionFactory} for interfacing with the networking layer.
   * @param host the host to which this connection belongs.
   * @param port the port on the host to which this connection belongs.
   * @return the connection id of the connection returned by {@link ConnectionFactory#connect}.
   * @param dataNodeId the {@link DataNodeId} associated with this connection
   */
  String connectAndTrack(ConnectionFactory connectionFactory, String host, Port port, DataNodeId dataNodeId)
      throws IOException {
    String connId = connectionFactory.connect(host, port);
    HostPortPoolManager hostPortPoolManager = getHostPortPoolManager(host, port, dataNodeId);
    hostPortPoolManager.incrementPoolCount();
    connectionIdToPoolManager.put(connId, hostPortPoolManager);
    totalManagedConnectionsCount++;
    if (hostPortPoolManager.hasMinActiveConnections()) {
      poolManagersBelowMinActiveConnections.remove(hostPortPoolManager);
    }
    return connId;
  }

  /**
   * Attempts to check out an existing connection to the (host, port) provided, or returns null if none available.
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
    if (!hostPortPoolManager.hasMinActiveConnections()) {
      poolManagersBelowMinActiveConnections.add(hostPortPoolManager);
    }
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
   * Return the total available connections across all {@link HostPortPoolManager}s.
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
    return hostPortToPoolManager.computeIfAbsent(new Pair<>(host, port), k -> new HostPortPoolManager(host, port,
        port.getPortType() == PortType.SSL ? maxConnectionsPerPortSsl : maxConnectionsPerPortPlainText, dataNodeId));
  }

  /**
   * {@link HostPortPoolManager} manages all the connections to a specific (host, port) pair. The
   * {@link ConnectionTracker} creates one for every (host, port) pair it knows of.
   */
  private static class HostPortPoolManager {
    private final LinkedList<String> availableConnections = new LinkedList<>();
    private final DataNodeId dataNodeId;
    private int minActiveConnections = 0;
    private int poolCount = 0;
    final String host;
    final Port port;
    final int poolLimit;

    /**
     * Instantiate a HostPortPoolManager
     * @param host the destination host for this pool.
     * @param port the destination port for this pool.
     * @param poolLimit the max connections allowed for this (host, port).
     * @param dataNodeId the {@link DataNodeId} associated with this {@link HostPortPoolManager}.
     */
    HostPortPoolManager(String host, Port port, int poolLimit, DataNodeId dataNodeId) {
      this.host = host;
      this.port = port;
      this.poolLimit = poolLimit;
      this.dataNodeId = dataNodeId;
    }

    /**
     * @return true if this manager has at least {@link #minActiveConnections}.
     */
    boolean hasMinActiveConnections() {
      return poolCount >= minActiveConnections;
    }

    /**
     * @return true if this manager has reached the pool limit
     */
    boolean hasReachedPoolLimit() {
      return poolCount == poolLimit;
    }

    /**
     * @param minActiveConnections the minimum number of connections to this (host, port) to keep ready for use.
     */
    void setMinActiveConnections(int minActiveConnections) {
      this.minActiveConnections = Math.min(poolLimit, minActiveConnections);
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
     * Return the number of available connections to this (host, port)
     * @return number of available connections
     */
    int getAvailableConnectionsCount() {
      return availableConnections.size();
    }
  }

  /**
   * Used to signal to the networking layer to initiate a new connection.
   */
  interface ConnectionFactory {
    /**
     * Initiate a new connection to the given (host, port). This method can return before the connection is ready for
     * sending requests. Once it is ready, {@link #checkInConnection} should be called.
     * @param host the hostname to connect to.
     * @param port the port to connect to.
     * @return a unique connection ID to represent the (future) connection.
     * @throws IOException if the connection could not be initiated.
     */
    String connect(String host, Port port) throws IOException;
  }
}
