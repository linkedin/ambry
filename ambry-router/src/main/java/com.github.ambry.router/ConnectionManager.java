package com.github.ambry.router;

import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestResponseHandler;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * The ConnectionManager keeps track of current connections to datanodes, and provides methods
 * to checkout and checkin connections.
 */

class ConnectionManager {
  private final ConcurrentHashMap<String, HostPortPoolManager> hostPortToPoolManager;
  private final ConcurrentHashMap<String, HostPortPoolManager> connectionIdToPoolManager;
  private final RequestResponseHandler requestResponseHandler;
  private final RouterConfig routerConfig;

  ConnectionManager(RequestResponseHandler requestResponseHandler, RouterConfig routerConfig) {
    hostPortToPoolManager = new ConcurrentHashMap<String, HostPortPoolManager>();
    connectionIdToPoolManager = new ConcurrentHashMap<String, HostPortPoolManager>();
    this.requestResponseHandler = requestResponseHandler;
    this.routerConfig = routerConfig;
  }

  private static String getHostPortString(String host, int port) {
    return host + ":" + Integer.toString(port);
  }

  /**
   * Returns the {@link HostPortPoolManager} associated with the (host, port) pair. Creates one if not available
   * already.
   * @param host The hostname
   * @param port The port
   * @return a HostPortPoolManager for the associated (host, port) pair.
   */
  private HostPortPoolManager getHostPortPoolManager(String host, Port port) {
    String lookupStr = getHostPortString(host, port.getPort());
    HostPortPoolManager poolManager = hostPortToPoolManager.get(lookupStr);
    if (poolManager == null) {
      // @todo: maxConnections and port type to be obtained by looking up host and port.
      HostPortPoolManager newPoolManager = new HostPortPoolManager(host, port);
      poolManager = hostPortToPoolManager.putIfAbsent(lookupStr, newPoolManager);
      if (poolManager == null) {
        poolManager = newPoolManager;
      }
    }
    return poolManager;
  }

  /**
   * Attempts to check out a connection to the host:port provided, or returns null if none available. In the
   * latter case, initiates a connection to the host:port unless max connections to it has been reached.
   * @param host The host to connect to.
   * @param port The port on the host to connect to.
   * @return connectionId, if there is one available to use, null otherwise.
   */
  String checkOutConnection(String host, Port port)
      throws IOException {
    // if any available, give that
    // else if max connections to hostport is reached, return null
    // else initiate a new connection and return null
    return getHostPortPoolManager(host, port).checkOutConnection();
  }

  /**
   * Check in a previously checked out connection.
   * @param connectionId the id of the previously checked out connection.
   */
  void checkInConnection(String connectionId) {
    connectionIdToPoolManager.get(connectionId).checkInConnection(connectionId);
  }

  /**
   * Adds a connectionId as an available connection that can be checked out in the future.
   * @param connectionId The connection id to add to the list of available connections.
   */
  void addToAvailablePool(String connectionId) {
    connectionIdToPoolManager.get(connectionId).addToAvailablePool(connectionId);
  }

  /**
   * Removes the given connectionId from the list of available connections.
   * @param connectionId The connection id to remove from the list of available connections.
   */
  void destroyConnection(String connectionId) {
    connectionIdToPoolManager.get(connectionId).destroyConnection(connectionId);
    connectionIdToPoolManager.remove(connectionId);
  }

  /**
   * HostPortPoolManager manages all the connections to a specific (host, port) pair. The {@link ConnectionManager}
   * creates one for every (host, port) pair it knows of.
   */
  private class HostPortPoolManager {
    final String host;
    final Port port;
    final int maxConnectionsToHostPort;
    final ConcurrentLinkedQueue<String> availableConnections;
    final AtomicInteger poolCount = new AtomicInteger(0);

    HostPortPoolManager(String host, Port port) {
      this.host = host;
      this.port = port;
      if (port.getPortType() == PortType.SSL) {
        this.maxConnectionsToHostPort = routerConfig.routerMaxConnectionsPerPortSSL;
      } else {
        this.maxConnectionsToHostPort = routerConfig.routerMaxConnectionsPerPortPlainText;
      }
      availableConnections = new ConcurrentLinkedQueue<String>();
    }

    String checkOutConnection()
        throws IOException {
      String connectionId = availableConnections.poll();
      if (connectionId == null) {
        if (poolCount.incrementAndGet() <= maxConnectionsToHostPort) {
          connectionIdToPoolManager.put(requestResponseHandler.connect(host, port), this);
        } else {
          poolCount.decrementAndGet();
        }
      }
      return connectionId;
    }

    void checkInConnection(String connectionId) {
      availableConnections.add(connectionId);
    }

    void addToAvailablePool(String connectionId) {
      availableConnections.add(connectionId);
    }

    void destroyConnection(String connectionId) {
      availableConnections.remove(connectionId);
      poolCount.decrementAndGet();
    }
  }
}

