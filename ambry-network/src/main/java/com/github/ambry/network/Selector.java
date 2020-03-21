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

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.Time;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A selector doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit network requests and responses.
 * <p>
 * A connection can be added to the selector by doing
 *
 * <pre>
 * selector.connect("connectionId", new InetSocketAddress(&quot;linkedin.com&quot;, server.port), 64000, 64000);
 * </pre>
 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established. The
 * call on return provides a unique id that identifies this connection
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 *
 * <pre>
 * List&lt;NetworkSend&gt; requestsToSend = Arrays.asList(new NetworkSend(0, bytes), new NetworkSend(1, otherBytes));
 * selector.poll(TIMEOUT_MS, requestsToSend);
 * </pre>
 *
 * The selector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 *
 * This class is not thread safe!
 */
public class Selector implements Selectable {
  private static final Logger logger = LoggerFactory.getLogger(Selector.class);

  private final java.nio.channels.Selector nioSelector;
  private final Map<String, SelectionKey> keyMap;
  private final List<NetworkSend> completedSends;
  private final List<NetworkReceive> completedReceives;
  private final List<String> disconnected;
  private final List<String> closedConnections;
  private final List<String> connected;
  private final Set<String> unreadyConnections;
  private final Time time;
  private final NetworkMetrics metrics;
  private final AtomicLong idGenerator;
  private final AtomicLong numActiveConnections;
  private final SSLFactory sslFactory;
  private final ExecutorService executorPool;
  private final NetworkConfig networkConfig;

  /**
   * Create a new selector
   */
  public Selector(NetworkMetrics metrics, Time time, SSLFactory sslFactory, NetworkConfig networkConfig)
      throws IOException {
    this.nioSelector = java.nio.channels.Selector.open();
    this.time = time;
    this.keyMap = new HashMap<>();
    this.completedSends = new ArrayList<>();
    this.completedReceives = new ArrayList<>();
    this.connected = new ArrayList<>();
    this.disconnected = new ArrayList<>();
    this.closedConnections = new ArrayList<>();
    this.metrics = metrics;
    this.sslFactory = sslFactory;
    idGenerator = new AtomicLong(0);
    numActiveConnections = new AtomicLong(0);
    unreadyConnections = new HashSet<>();
    this.networkConfig = networkConfig;
    if (networkConfig.selectorExecutorPoolSize > 0) {
      executorPool = Executors.newFixedThreadPool(networkConfig.selectorExecutorPoolSize);
    } else {
      executorPool = null;
    }
    metrics.registerSelectorActiveConnections(numActiveConnections);
    metrics.registerSelectorUnreadyConnections(unreadyConnections);
  }

  /**
   * Generate an unique connection id
   * @param channel The channel between two hosts
   * @return The id for the connection that was created
   */
  private String generateConnectionId(SocketChannel channel) {
    Socket socket = channel.socket();
    String localHost = socket.getLocalAddress().getHostAddress();
    int localPort = socket.getLocalPort();
    String remoteHost = socket.getInetAddress().getHostAddress();
    int remotePort = socket.getPort();
    long connectionIdSuffix = idGenerator.getAndIncrement();
    return localHost + ":" + localPort + "-" + remoteHost + ":" + remotePort + "_" + connectionIdSuffix;
  }

  /**
   * Begin connecting to the given address and add the connection to this selector and returns an id that identifies
   * the connection
   * <p>
   * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
   * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
   * @param address The address to connect to
   * @param sendBufferSize The networkSend buffer size for the new connection
   * @param receiveBufferSize The receive buffer size for the new connection
   * @param portType {@link PortType} which represents the type of connection to establish
   * @return The id for the connection that was created
   * @throws IllegalStateException if there is already a connection for that id
   * @throws IOException if DNS resolution fails on the hostname or if the server is down
   */
  @Override
  public String connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize, PortType portType)
      throws IOException {
    SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(false);
    Socket socket = channel.socket();
    socket.setKeepAlive(true);
    socket.setSendBufferSize(sendBufferSize);
    socket.setReceiveBufferSize(receiveBufferSize);
    socket.setTcpNoDelay(true);
    try {
      channel.connect(address);
    } catch (UnresolvedAddressException e) {
      channel.close();
      throw new IOException("Can't resolve address: " + address, e);
    } catch (IOException e) {
      channel.close();
      throw e;
    }
    String connectionId = generateConnectionId(channel);
    SelectionKey key = channel.register(this.nioSelector, SelectionKey.OP_CONNECT);
    Transmission transmission;
    try {
      transmission = createTransmission(connectionId, key, address.getHostName(), address.getPort(), portType,
          SSLFactory.Mode.CLIENT);
    } catch (IOException e) {
      logger.error("IOException on transmission creation " + e);
      channel.socket().close();
      channel.close();
      throw e;
    }
    key.attach(transmission);
    this.keyMap.put(connectionId, key);
    numActiveConnections.set(this.keyMap.size());
    return connectionId;
  }

  /**
   * Register the nioSelector with an existing channel
   * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
   * Note that we are not checking if the connection id is valid - since the connection already exists
   */
  public String register(SocketChannel channel, PortType portType) throws IOException {
    Socket socket = channel.socket();
    String connectionId = generateConnectionId(channel);
    SelectionKey key = channel.register(nioSelector, SelectionKey.OP_READ);
    Transmission transmission;
    try {
      transmission =
          createTransmission(connectionId, key, socket.getInetAddress().getHostName(), socket.getPort(), portType,
              SSLFactory.Mode.SERVER);
    } catch (IOException e) {
      logger.error("IOException on transmission creation ", e);
      socket.close();
      channel.close();
      throw e;
    }
    key.attach(transmission);
    this.keyMap.put(connectionId, key);
    numActiveConnections.set(this.keyMap.size());
    return connectionId;
  }

  /**
   * Disconnect any connections for the given id (if there are any). The disconnection is asynchronous and will not be
   * processed until the next {@link #poll(long) poll()} call.
   */
  @Override
  public void disconnect(String connectionId) {
    SelectionKey key = this.keyMap.get(connectionId);
    if (key != null) {
      key.cancel();
    }
  }

  /**
   * Interrupt the selector if it is blocked waiting to do I/O.
   */
  @Override
  public void wakeup() {
    nioSelector.wakeup();
  }

  /**
   * Close this selector and all associated connections
   */
  @Override
  public void close() {
    for (SelectionKey key : this.nioSelector.keys()) {
      close(key);
    }
    try {
      this.nioSelector.close();
    } catch (IOException e) {
      metrics.selectorNioCloseErrorCount.inc();
      logger.error("Exception closing nioSelector:", e);
    }
  }

  /**
   * Tells whether or not this selector is open.
   *
   * @return <tt>true</tt> if, and only if, this selector is open
   */
  @Override
  public boolean isOpen() {
    return nioSelector.isOpen();
  }

  /**
   * Queue the given request for sending in the subsequent {@link #poll(long)} calls
   * @param networkSend The NetworkSend that is ready to be sent
   */
  public void send(NetworkSend networkSend) {
    SelectionKey key = keyForId(networkSend.getConnectionId());
    if (key == null) {
      throw new IllegalStateException("Attempt to send data to a null key");
    }
    Transmission transmission = getTransmission(key);
    try {
      transmission.setNetworkSend(networkSend);
    } catch (CancelledKeyException e) {
      logger.debug("Ignoring response for closed socket.");
      close(key);
    }
  }

  /**
   * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
   * disconnections, initiating new sends, or making progress on in-progress sends or receives.
   * <p>
   *
   * When this call is completed the user can check for completed sends, receives, connections or disconnects using
   * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
   * lists will be cleared at the beginning of each {@link #poll(long)} call and repopulated by the call if any
   * completed I/O.
   *
   * @param timeoutMs The amount of time to wait, in milliseconds. If negative, wait indefinitely.
   *
   * @throws IOException If a send is given for which we have no existing connection or for which there is
   *         already an in-progress send
   */
  @Override
  public void poll(long timeoutMs) throws IOException {
    poll(timeoutMs, null);
  }

  /**
   * Firstly initiate the provided sends. Then do whatever I/O can be done on each connection without blocking.
   * This includes completing connections, completing disconnections, initiating new sends,
   * or making progress on in-progress sends or receives.
   * <p>
   *
   * When this call is completed the user can check for completed sends, receives, connections or disconnects using
   * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
   * lists will be cleared at the beginning of each {@link #poll(long, List)} call and repopulated by the call if any
   * completed I/O.
   *
   * @param timeoutMs The amount of time to wait, in milliseconds. If negative, wait indefinitely.
   * @param sends The list of new sends to initiate.
   *
   * @throws IOException If a send is given for which we have no existing connection or for which there is
   *         already an in-progress send
   */
  @Override
  public void poll(long timeoutMs, List<NetworkSend> sends) throws IOException {
    long startTime = time.milliseconds();
    if (executorPool != null) {
      pollWithExecutorPool(timeoutMs, sends);
    } else {
      pollOnMainThread(timeoutMs, sends);
    }
    logger.trace("Selector poll total time: {} ms.", time.milliseconds() - startTime);
  }

  /**
   * Process read/write events on current thread.
   */
  private void pollOnMainThread(long timeoutMs, List<NetworkSend> sends) throws IOException {
    clear();

    // register for write interest on any new sends
    if (sends != null) {
      for (NetworkSend networkSend : sends) {
        send(networkSend);
      }
    }

    // check ready keys
    long startSelect = time.milliseconds();
    int readyKeys = select(timeoutMs);
    this.metrics.selectorSelectCount.inc();

    if (readyKeys > 0) {
      long endSelect = time.milliseconds();
      metrics.selectorSelectTime.update(endSelect - startSelect);
      Set<SelectionKey> keys = nioSelector.selectedKeys();
      metrics.selectorReadyKeyCount.inc(keys.size());
      Iterator<SelectionKey> iter = keys.iterator();
      int keyProcessed = 0;
      while (iter.hasNext()) {
        if (networkConfig.selectorMaxKeyToProcess != -1 && keyProcessed >= networkConfig.selectorMaxKeyToProcess) {
          break;
        }
        SelectionKey key = iter.next();
        iter.remove();
        keyProcessed++;

        Transmission transmission = getTransmission(key);
        try {
          if (key.isConnectable()) {
            transmission.finishConnect();
            if (transmission.ready()) {
              connected.add(transmission.getConnectionId());
              metrics.selectorConnectionCreated.inc();
            } else {
              unreadyConnections.add(transmission.getConnectionId());
            }
          }

          /* if channel is not ready, finish prepare */
          if (transmission.isConnected() && !transmission.ready()) {
            transmission.prepare();
            continue;
          }

          if (key.isReadable() && transmission.ready()) {
            metrics.selectorReadKeyCount.inc();
            NetworkReceive networkReceive = read(key, transmission);
            if (networkReceive == null) {
              // Exception happened in read.
              close(key);
            } else if (networkReceive.getReceivedBytes().isReadComplete()) {
              this.completedReceives.add(networkReceive);
            }
          } else if (key.isWritable() && transmission.ready()) {
            metrics.selectorWriteKeyCount.inc();
            NetworkSend networkSend = write(key, transmission);
            if (networkSend == null) {
              // Exception happened in write.
              close(key);
            } else if (networkSend.getPayload().isSendComplete()) {
              this.completedSends.add(networkSend);
            }
          } else if (!key.isValid()) {
            close(key);
          }
        } catch (IOException e) {
          handleReadWriteIOException(e, key);
          close(key);
        } catch (Exception e) {
          metrics.selectorKeyOperationErrorCount.inc();
          logger.error("closing key on exception remote host {}", channel(key).socket().getRemoteSocketAddress(), e);
          close(key);
        }
      }
      checkUnreadyConnectionsStatus();
      metrics.selectorIOCount.inc();
      metrics.selectorIOTime.update(time.milliseconds() - endSelect);
    }
    disconnected.addAll(closedConnections);
    closedConnections.clear();
  }

  /**
   * Use {@link ExecutorService} to process read/write events.
   */
  private void pollWithExecutorPool(long timeoutMs, List<NetworkSend> sends) throws IOException {
    List<Future<NetworkSend>> completedSendsFutures = new ArrayList<>();
    List<Future<NetworkReceive>> completedReceivesFutures = new ArrayList<>();
    List<Future<SelectionKey>> completedPrepareFutures = new ArrayList<>();
    Set<SelectionKey> processingKeySet = new HashSet<>();
    clear();

    // register for write interest on any new sends
    if (sends != null) {
      for (NetworkSend networkSend : sends) {
        send(networkSend);
      }
    }

    // check ready keys
    long startSelect = time.milliseconds();
    int readyKeys = select(timeoutMs);
    this.metrics.selectorSelectCount.inc();

    if (readyKeys > 0) {
      long endSelect = time.milliseconds();
      this.metrics.selectorSelectTime.update(endSelect - startSelect);
      Set<SelectionKey> keys = nioSelector.selectedKeys();
      metrics.selectorReadyKeyCount.inc(keys.size());
      Iterator<SelectionKey> iter = keys.iterator();
      while (iter.hasNext()) {
        if (networkConfig.selectorMaxKeyToProcess != -1
            && processingKeySet.size() >= networkConfig.selectorMaxKeyToProcess) {
          break;
        }
        SelectionKey key = iter.next();
        iter.remove();

        Transmission transmission = getTransmission(key);
        try {
          if (key.isConnectable()) {
            transmission.finishConnect();
            if (transmission.ready()) {
              connected.add(transmission.getConnectionId());
              metrics.selectorConnectionCreated.inc();
            } else {
              unreadyConnections.add(transmission.getConnectionId());
            }
          }

          /* if channel is not ready, finish prepare */
          if (transmission.isConnected() && !transmission.ready()) {
            metrics.selectorPrepareKeyCount.inc();
            completedPrepareFutures.add(executorPool.submit(() -> prepare(key, transmission)));
            processingKeySet.add(key);
          } else if (key.isReadable() && transmission.ready()) {
            metrics.selectorReadKeyCount.inc();
            completedReceivesFutures.add(executorPool.submit(() -> read(key, transmission)));
            processingKeySet.add(key);
          } else if (key.isWritable() && transmission.ready()) {
            metrics.selectorWriteKeyCount.inc();
            completedSendsFutures.add(executorPool.submit(() -> write(key, transmission)));
            processingKeySet.add(key);
          } else if (!key.isValid()) {
            close(key);
          }
        } catch (IOException e) {
          // handles IOException from transmission.finishConnect()
          handleReadWriteIOException(e, key);
          close(key);
        } catch (Exception e) {
          close(key);
          metrics.selectorKeyOperationErrorCount.inc();
          logger.error("closing key on exception remote host {}", channel(key).socket().getRemoteSocketAddress(), e);
        }
      }

      for (Future<SelectionKey> future : completedPrepareFutures) {
        try {
          SelectionKey returnKey = future.get();
          if (returnKey != null) {
            processingKeySet.remove(returnKey);
          }
        } catch (InterruptedException | ExecutionException e) {
          logger.error("Hit Unexpected exception on selector prepare, ", e);
        }
      }

      for (Future<NetworkReceive> future : completedReceivesFutures) {
        try {
          NetworkReceive networkReceive = future.get();
          if (networkReceive != null) {
            processingKeySet.remove(keyForId(networkReceive.getConnectionId()));
            if (networkReceive.getReceivedBytes().isReadComplete()) {
              this.completedReceives.add(networkReceive);
            }
          }
        } catch (InterruptedException | ExecutionException e) {
          logger.error("Hit Unexpected exception on selector read, ", e);
        }
      }

      for (Future<NetworkSend> future : completedSendsFutures) {
        try {
          NetworkSend networkSend = future.get();
          if (networkSend != null) {
            processingKeySet.remove(keyForId(networkSend.getConnectionId()));
            if (networkSend.getPayload().isSendComplete()) {
              this.completedSends.add(networkSend);
            }
          }
        } catch (ExecutionException | InterruptedException e) {
          logger.error("Hit Unexpected exception on selector write, ", e);
        }
      }
      // Keys hit exception should be closed.
      for (SelectionKey keyWithError : processingKeySet) {
        close(keyWithError);
      }
      checkUnreadyConnectionsStatus();
      long selectorIOTime = time.milliseconds() - endSelect;
      if (selectorIOTime >= 50) {
        // Some selectors may not have enough work to do, so counting their time is misleading.
        metrics.selectorIOCount.inc();
        metrics.selectorIOTime.update(time.milliseconds() - endSelect);
      }
    }
    disconnected.addAll(closedConnections);
    closedConnections.clear();
  }

  /**
   * Check readiness for unready connections and add to completed list if ready
   */
  private void checkUnreadyConnectionsStatus() {
    Iterator<String> iterator = unreadyConnections.iterator();
    while (iterator.hasNext()) {
      String connId = iterator.next();
      if (isChannelReady(connId)) {
        connected.add(connId);
        iterator.remove();
        metrics.selectorConnectionCreated.inc();
      }
    }
  }

  /**
   * Generate the description for a SocketChannel
   */
  private String socketDescription(SocketChannel channel) {
    Socket socket = channel.socket();
    if (socket == null) {
      return "[unconnected socket]";
    } else if (socket.getInetAddress() != null) {
      return socket.getInetAddress().toString();
    } else {
      return socket.getLocalAddress().toString();
    }
  }

  /**
   * Returns {@code true} if channel is ready to send or receive data, {@code false} otherwise
   * @param connectionId upon which readiness is checked for
   * @return true if channel is ready to accept reads/writes, false otherwise
   */
  public boolean isChannelReady(String connectionId) {
    Transmission transmission = getTransmission(keyForId(connectionId));
    return transmission.ready();
  }

  @Override
  public List<NetworkSend> completedSends() {
    return this.completedSends;
  }

  @Override
  public List<NetworkReceive> completedReceives() {
    return this.completedReceives;
  }

  @Override
  public List<String> disconnected() {
    return this.disconnected;
  }

  @Override
  public List<String> connected() {
    return this.connected;
  }

  /**
   * Create a new {@link Transmission} to represent a live connection. This method is exposed so that it can be
   * unit tests can override it.
   * @param connectionId a unique ID for this connection.
   * @param key the {@link SelectionKey} used to communicate socket events.
   * @param hostname the remote hostname for the connection, used for SSL host verification.
   * @param portType used to select between a plaintext or SSL transmission.
   * @param mode for SSL transmissions, whether to operate in client or server mode.
   * @return either a {@link Transmission} or {@link SSLTransmission}.
   * @throws IOException
   */
  protected Transmission createTransmission(String connectionId, SelectionKey key, String hostname, int port,
      PortType portType, SSLFactory.Mode mode) throws IOException {
    Transmission transmission;
    if (portType == PortType.PLAINTEXT) {
      transmission = new PlainTextTransmission(connectionId, channel(key), key, time, metrics, networkConfig);
    } else if (portType == PortType.SSL) {
      try {
        transmission =
            new SSLTransmission(sslFactory, connectionId, channel(key), key, hostname, port, time, metrics, mode,
                networkConfig);
        metrics.sslTransmissionInitializationCount.inc();
      } catch (IOException e) {
        metrics.sslTransmissionInitializationErrorCount.inc();
        logger.error("SSLTransmission initialization error ", e);
        throw e;
      }
    } else {
      throw new IllegalArgumentException("Unsupported portType " + portType + " passed in");
    }
    return transmission;
  }

  /**
   * Clear the results from the prior poll
   */
  private void clear() {
    completedSends.clear();
    completedReceives.clear();
    connected.clear();
    disconnected.clear();
  }

  /**
   * Check for data, waiting up to the given timeout.
   *
   * @param ms Length of time to wait, in milliseconds. If negative, wait indefinitely.
   * @return The number of keys ready
   * @throws IOException
   */
  private int select(long ms) throws IOException {
    if (ms == 0L) {
      return this.nioSelector.selectNow();
    } else if (ms < 0L) {
      return this.nioSelector.select();
    } else {
      return this.nioSelector.select(ms);
    }
  }

  /**
   * Begin closing this connection by given connection id
   */
  @Override
  public void close(String connectionId) {
    SelectionKey key = keyForId(connectionId);
    if (key == null) {
      metrics.selectorCloseKeyErrorCount.inc();
      logger.error("Attempt to close socket for which there is no open connection. Connection id {}", connectionId);
    } else {
      close(key);
    }
  }

  /**
   * Begin closing this connection by given key
   */
  private void close(SelectionKey key) {
    Transmission transmission = getTransmission(key);
    if (transmission != null) {
      logger.debug("Closing connection from {}", transmission.getConnectionId());
      closedConnections.add(transmission.getConnectionId());
      keyMap.remove(transmission.getConnectionId());
      numActiveConnections.set(keyMap.size());
      unreadyConnections.remove(transmission.getConnectionId());
      try {
        transmission.close();
      } catch (IOException e) {
        logger.error("IOException thrown during closing of transmission with connectionId {} :",
            transmission.getConnectionId(), e);
      }
    } else {
      key.attach(null);
      key.cancel();
      SocketAddress address = null;
      try {
        SocketChannel socketChannel = channel(key);
        address = socketChannel.socket().getRemoteSocketAddress();
        socketChannel.socket().close();
        socketChannel.close();
      } catch (IOException e) {
        metrics.selectorCloseSocketErrorCount.inc();
        logger.error("Exception closing connection to remote host {} :", address, e);
      }
    }
    this.metrics.selectorConnectionClosed.inc();
  }

  /**
   * Get the selection key associated with this numeric id
   */
  private SelectionKey keyForId(String id) {
    return this.keyMap.get(id);
  }

  /**
   * Handle read/write IOException
   */
  private void handleReadWriteIOException(IOException e, SelectionKey key) {
    String socketDescription = socketDescription(channel(key));
    if (e instanceof EOFException || e instanceof ConnectException) {
      metrics.selectorDisconnectedErrorCount.inc();
      logger.error("Connection {} disconnected", socketDescription, e);
    } else {
      metrics.selectorIOErrorCount.inc();
      logger.warn("Error in I/O with connection to {}", socketDescription, e);
    }
  }

  /**
   * A wrapper to call transmission.prepare().
   */
  private SelectionKey prepare(SelectionKey key, Transmission transmission) {
    long startTimeInMs = time.milliseconds();
    try {
      transmission.prepare();
      return key;
    } catch (IOException e) {
      handleReadWriteIOException(e, key);
      return null;
    } finally {
      long prepareTime = time.milliseconds() - startTimeInMs;
      logger.trace("SocketServer time spent on prepare {} = {}ms", transmission.getConnectionId(), prepareTime);
    }
  }

  /**
   * Process reads from ready sockets
   * @return the {@link NetworkReceive} if no IOException during read().
   */
  private NetworkReceive read(SelectionKey key, Transmission transmission) {
    long startTimeToReadInMs = time.milliseconds();
    try {
      boolean readComplete = transmission.read();
      NetworkReceive networkReceive = transmission.getNetworkReceive();
      if (readComplete) {
        transmission.onReceiveComplete();
        transmission.clearReceive();
      }
      return networkReceive;
    } catch (IOException e) {
      // We have key information if we log IOException here.
      handleReadWriteIOException(e, key);
      return null;
    } finally {
      long readTime = time.milliseconds() - startTimeToReadInMs;
      logger.trace("SocketServer time spent on read per key {} = {}", transmission.getConnectionId(), readTime);
    }
  }

  /**
   * Process writes to ready sockets
   * @return the {@link NetworkSend} if no IOException during write().
   */
  private NetworkSend write(SelectionKey key, Transmission transmission) {
    long startTimeToWriteInMs = time.milliseconds();
    try {
      boolean sendComplete = transmission.write();
      NetworkSend networkSend = transmission.getNetworkSend();
      if (sendComplete) {
        logger.trace("Finished writing, registering for read on connection {}", transmission.getRemoteSocketAddress());
        transmission.onSendComplete();
        metrics.sendInFlight.dec();
        transmission.clearSend();
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE | SelectionKey.OP_READ);
      }
      return networkSend;
    } catch (IOException e) {
      // We have key information if we log IOException here.
      handleReadWriteIOException(e, key);
      return null;
    } finally {
      long writeTime = time.milliseconds() - startTimeToWriteInMs;
      logger.trace("SocketServer time spent on write per key {} = {}", transmission.getConnectionId(), writeTime);
    }
  }

  /**
   * Get the Transmission for the given connection
   */
  private Transmission getTransmission(SelectionKey key) {
    return (Transmission) key.attachment();
  }

  /**
   * Get the socket channel associated with this selection key
   */
  private SocketChannel channel(SelectionKey key) {
    return (SocketChannel) key.channel();
  }
}
