package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * The ConnectionManager keeps track of current connections to datanodes, and provides methods
 * to checkout and checkin connections.
 */

public class ConnectionManager {
  private final ConcurrentHashMap<String, HostPortPoolManager> hostPortToPoolManager;
  private final ConcurrentHashMap<String, HostPortPoolManager> connectionIdToPoolManager;
  private final Selector selector;
  private final NetworkConfig networkConfig;
  private final int maxConnectionsPerPortPlainText;
  private final int maxConnectionsPerPortSsl;

  /**
   * Instantiates a ConnectionManager
   * @param selector The {@link Selector} to be used to make connections.
   * @param networkConfig The {@link NetworkConfig} containing the config for the Network.
   * @param maxConnectionsPerPortPlainText the connection pool limit for plain text connections to a (host, port)
   * @param maxConnectionsPerPortPlainSsl the conneciton pool limit for ssl connections to a (host, port)
   */
  public ConnectionManager(Selector selector, NetworkConfig networkConfig, int maxConnectionsPerPortPlainText,
      int maxConnectionsPerPortPlainSsl) {
    hostPortToPoolManager = new ConcurrentHashMap<String, HostPortPoolManager>();
    connectionIdToPoolManager = new ConcurrentHashMap<String, HostPortPoolManager>();
    this.selector = selector;
    this.networkConfig = networkConfig;
    this.maxConnectionsPerPortPlainText = maxConnectionsPerPortPlainText;
    this.maxConnectionsPerPortSsl = maxConnectionsPerPortPlainSsl;
  }

  /**
   * Construct a host port string from the given host and port.
   * @param host the host
   * @param port the port on the host.
   * @return returns the hostPortString.
   */
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
  public String checkOutConnection(String host, Port port)
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
  public void checkInConnection(String connectionId) {
    connectionIdToPoolManager.get(connectionId).checkInConnection(connectionId);
  }

  /**
   * Removes the given connectionId from the list of available connections. This connection id could be either a
   * checked out connection or a connection that was available to be checked out. Attempting to destroy the same
   * connection more than once, or attempting to destroy an invalid connection will result in unexpected behavior.
   * @param connectionId the connection id of the connection.
   */
  public void destroyConnection(String connectionId) {
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

    /**
     * Instantiate a HostPortPoolManager
     * @param host the host associated with this manager
     * @param port the port associated with this manager
     */
    HostPortPoolManager(String host, Port port) {
      this.host = host;
      this.port = port;
      if (port.getPortType() == PortType.SSL) {
        this.maxConnectionsToHostPort = maxConnectionsPerPortSsl;
      } else {
        this.maxConnectionsToHostPort = maxConnectionsPerPortPlainText;
      }
      availableConnections = new ConcurrentLinkedQueue<String>();
    }

    /**
     * Attempts to check out a connection to the (host, port) associated with this manager.
     * If no connections are available initalizes one if it can.
     * @return returns a connection id, if there is one; null otherwise.
     * @throws IOException
     */
    String checkOutConnection()
        throws IOException {
      String connectionId = availableConnections.poll();
      if (connectionId == null) {
        if (poolCount.incrementAndGet() <= maxConnectionsToHostPort) {
          connectionIdToPoolManager.put(selector
              .connect(new InetSocketAddress(host, port.getPort()), networkConfig.socketSendBufferBytes,
                  networkConfig.socketReceiveBufferBytes, port.getPortType()), this);
        } else {
          poolCount.decrementAndGet();
        }
      }
      return connectionId;
    }

    /**
     * Check in a previously checked out connection.
     * @param connectionId the connection id of the connection.
     */
    void checkInConnection(String connectionId) {
      availableConnections.add(connectionId);
    }

    /** Destroy a connection managed by this manager. This connection id could be either a checked out connection or a
     * connection that was available to be checked out. Attempting to destroy the same connection more than once will
     * result in unexpected behavior.
     * @param connectionId the connection id of the connection.
     */
    void destroyConnection(String connectionId) {
      availableConnections.remove(connectionId);
      poolCount.decrementAndGet();
    }
  }
}

