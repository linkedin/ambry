package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A {@link ConnectionManager} implementation that keeps track of current connections to datanodes, and provides methods
 * to checkout and checkin connections.
 *
 * This class is not thread safe.
 */

public class NonBlockingConnectionManager implements ConnectionManager {
  private final ConcurrentHashMap<String, HostPortPoolManager> hostPortToPoolManager;
  private final ConcurrentHashMap<String, HostPortPoolManager> connectionIdToPoolManager;
  private final Selector selector;
  private final NetworkConfig networkConfig;
  private final int maxConnectionsPerPortPlainText;
  private final int maxConnectionsPerPortSsl;
  private final Time time;
  private int connectionIdCount;

  /**
   * Instantiates a ConnectionManager
   * @param selector the {@link Selector} to use to make connections.
   * @param networkConfig The {@link NetworkConfig} containing the config for the Network.
   * @param maxConnectionsPerPortPlainText the connection pool limit for plain text connections to a (host, port)
   * @param maxConnectionsPerPortPlainSsl the connection pool limit for ssl connections to a (host, port)
   */
  public NonBlockingConnectionManager(Selector selector, NetworkConfig networkConfig,
      int maxConnectionsPerPortPlainText, int maxConnectionsPerPortPlainSsl, Time time) {
    if (selector == null || networkConfig == null) {
      throw new IllegalArgumentException("Invalid inputs passed in, Selector: " + selector +
          " NetworkConfig: " + networkConfig);
    }
    hostPortToPoolManager = new ConcurrentHashMap<String, HostPortPoolManager>();
    connectionIdToPoolManager = new ConcurrentHashMap<String, HostPortPoolManager>();
    connectionIdCount = 0;
    this.selector = selector;
    this.networkConfig = networkConfig;
    this.maxConnectionsPerPortPlainText = maxConnectionsPerPortPlainText;
    this.maxConnectionsPerPortSsl = maxConnectionsPerPortPlainSsl;
    this.time = time;
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
   * @throws IOException if an attempt to initiate a connection as a result of this call fails.
   */
  @Override
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
  @Override
  public void checkInConnection(String connectionId) {
    connectionIdToPoolManager.get(connectionId).checkInConnection(connectionId);
  }

  /**
   * Destroy the connection associated with the given connectionId. The connection maybe a checked out connection,
   * or a connection that is not checked out. The connection could also have already been closed by the selector.
   * @param connectionId connection to destroy.
   */
  @Override
  public void destroyConnection(String connectionId) {
    HostPortPoolManager hostPortPoolManager = connectionIdToPoolManager.remove(connectionId);
    if (hostPortPoolManager != null) {
      connectionIdCount--;
      hostPortPoolManager.removeConnection(connectionId);
      selector.close(connectionId);
    }
  }

  /**
   * Return the total number of connections that are initiated and/or established but not destroyed.
   * @return the total number of initiated and/or established connections.
   */
  @Override
  public int getTotalConnectionsCount() {
    return connectionIdCount;
  }

  /**
   * Return the total established and available connections across all hostPortPoolManagers.
   * @return total established and available connections.
   */
  @Override
  public int getAvailableConnectionsCount() {
    int count = 0;
    for (HostPortPoolManager hostPortPoolManager : hostPortToPoolManager.values()) {
      count += hostPortPoolManager.getAvailableConnectionsCount();
    }
    return count;
  }

  /**
   * Close the ConnectionManager.
   */
  @Override
  public void close() {
  }

  /**
   * HostPortPoolManager manages all the connections to a specific (host, port) pair. The {@link ConnectionManager}
   * creates one for every (host, port) pair it knows of.
   */
  private class HostPortPoolManager {
    private final String host;
    private final Port port;
    private final int maxConnectionsToHostPort;
    private final ConcurrentLinkedQueue<String> availableConnections;
    private final AtomicInteger poolCount = new AtomicInteger(0);
    private final AtomicInteger availableCount = new AtomicInteger(0);

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
     * @throws IOException if an attempt to initiate a connection as a result of this call fails.
     */
    String checkOutConnection()
        throws IOException {
      String connectionId = availableConnections.poll();
      if (connectionId == null) {
        if (poolCount.incrementAndGet() <= maxConnectionsToHostPort) {
          connectionIdToPoolManager.put(selector
              .connect(new InetSocketAddress(host, port.getPort()), networkConfig.socketSendBufferBytes,
                  networkConfig.socketReceiveBufferBytes, port.getPortType()), this);
          connectionIdCount++;
        } else {
          poolCount.decrementAndGet();
        }
      } else {
        availableCount.decrementAndGet();
      }
      return connectionId;
    }

    /**
     * Check in a previously checked out connection.
     * @param connectionId the connection id of the connection.
     */
    void checkInConnection(String connectionId) {
      availableConnections.add(connectionId);
      availableCount.incrementAndGet();
    }

    /**
     * Remove a connection managed by this manager. This connection id could be either a checked out connection or a
     * connection that was previously available to be checked out.
     * @param connectionId the connection id of the connection.
     */
    void removeConnection(String connectionId) {
      if (availableConnections.remove(connectionId)) {
        availableCount.decrementAndGet();
      }
      poolCount.decrementAndGet();
    }

    /**
     * Return the number of available connections to this hostPort
     * @return number of available connections
     */
    int getAvailableConnectionsCount() {
      return availableCount.get();
    }
  }
}

