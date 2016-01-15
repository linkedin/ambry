package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * The HostPortPoolManager manages all the connections to a specific (host, port) pair.
 */
class HostPortPoolManager {
  String host;
  int port;
  PortType portType;
  private final int maxConnectionsToHostPort;
  private final ConcurrentLinkedDeque<String> availableConnections;
  private final Set<String> pendingConnections;
  AtomicInteger poolCount;
  private final Object newConnectionsLock;
  private final Selector selector;
  private static int BUFFER_SIZE = 4 * 1024;

  public HostPortPoolManager(String host, int port, PortType portType, Selector selector) {
    this.host = host;
    this.port = port;
    this.portType = portType;
    //@todo get it from configs.
    this.maxConnectionsToHostPort = 5;
    this.selector = selector;
    availableConnections = new ConcurrentLinkedDeque<String>();
    pendingConnections = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    newConnectionsLock = new Object();
  }

  String checkOut()
      throws IOException {
    String connectionId = availableConnections.poll();
    if (connectionId == null) {
      boolean initiateNewConnection = false;
      if (poolCount.get() < maxConnectionsToHostPort) {
        synchronized (newConnectionsLock) {
          if (poolCount.get() < maxConnectionsToHostPort) {
            poolCount.incrementAndGet();
            initiateNewConnection = true;
          }
        }
        if (initiateNewConnection) {
          pendingConnections
              .add(selector.connect(new InetSocketAddress(host, port), BUFFER_SIZE, BUFFER_SIZE, portType));
        }
      }
    }
    return connectionId;
  }

  void checkIn(String connectionId) {
    availableConnections.add(connectionId);
  }

  void addToAvailablePool(String connectionId) {
    availableConnections.add(connectionId);
    pendingConnections.remove(connectionId);
  }

  void removeFromAvailablePool(String connectionId) {
    availableConnections.remove(connectionId);
    poolCount.decrementAndGet();
  }
}

/**
 * The ConnectionManager keeps track of current connections to datanodes, and provides methods
 * to checkout and checkin connections.
 */

public class ConnectionManager {
  // "host:port" to HostPortPoolManager.
  private ConcurrentHashMap<String, HostPortPoolManager> hostPortToPoolManager;
  // connectionId to HostPortPoolManager.
  private ConcurrentHashMap<String, HostPortPoolManager> connectionIdToPoolManager;
  // The selector that is used for connecting and sending requests.
  private Selector selector;
  private NetworkMetrics metrics;

  public ConnectionManager(Time time, SSLFactory sslFactory)
      throws IOException {
    //@todo NetworkMetrics class needs to be separated out. There will not be any acceptors
    // or processors at the client side.
    metrics = null;
    hostPortToPoolManager = new ConcurrentHashMap<String, HostPortPoolManager>();
    connectionIdToPoolManager = new ConcurrentHashMap<String, HostPortPoolManager>();
    selector = new Selector(metrics, time, sslFactory);
  }

  public static String getHostPortString(String host, int port) {
    return host + ":" + Integer.toString(port);
  }

  /**
   * Returns the HostPortPoolManager associated with the (host, port) pair. Creates one if one is not available already.
   * @param host The hostname
   * @param port The port
   * @return a HostPortPoolManager for the associated (host, port) pair.
   */
  HostPortPoolManager getHostPortPoolManager(String host, int port) {
    String lookupStr = getHostPortString(host, port);
    HostPortPoolManager poolManager = hostPortToPoolManager.get(lookupStr);
    if (poolManager == null) {
      // @todo: maxConnections and port type to be obtained by looking up host and port.
      HostPortPoolManager newPoolManager = new HostPortPoolManager(host, port, PortType.PLAINTEXT, selector);
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
  public String checkOut(String host, int port)
      throws IOException {
    // if any available, give that
    // else if max connections to hostport is reached, return null
    // else initiate a new connection and return null
    return getHostPortPoolManager(host, port).checkOut();
  }

  /**
   * Adds a connectionId as an available connection that can be checked out in the future.
   * @param connectionId The connection id to add to the list of available connections.
   */
  public void addToAvailablePool(String connectionId) {
    connectionIdToPoolManager.get(connectionId).addToAvailablePool(connectionId);
  }

  /**
   * Removes the given connectionId from the list of available connections.
   * @param connectionId The connection id to remove from the list of available connections.
   */
  public void removeFromAvailablePool(String connectionId) {
    connectionIdToPoolManager.get(connectionId).removeFromAvailablePool(connectionId);
    connectionIdToPoolManager.remove(connectionId);
  }
}
