package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final List<String> connected;
  private final Time time;
  private final NetworkMetrics metrics;
  private final AtomicLong IdGenerator;
  private AtomicLong activeConnections;

  /**
   * Create a new selector
   */
  public Selector(NetworkMetrics metrics, Time time)
      throws IOException {
    this.nioSelector = java.nio.channels.Selector.open();
    this.time = time;
    this.keyMap = new HashMap<String, SelectionKey>();
    this.completedSends = new ArrayList<NetworkSend>();
    this.completedReceives = new ArrayList<NetworkReceive>();
    this.connected = new ArrayList<String>();
    this.disconnected = new ArrayList<String>();
    this.metrics = metrics;
    this.IdGenerator = new AtomicLong(0);
    this.activeConnections = new AtomicLong(0);
    this.metrics.initializeSelectorMetricsIfRequired(activeConnections);
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
    long connectionIdSuffix = IdGenerator.getAndIncrement();
    StringBuilder connectionIdBuilder = new StringBuilder();
    connectionIdBuilder.append(localHost).append(":").append(localPort).append("-").append(remoteHost).append(":")
        .append(remotePort).append("_").append(connectionIdSuffix);
    return connectionIdBuilder.toString();
  }

  /**
   * Begin connecting to the given address and add the connection to this selector and returns an id that identifies
   * the connection
   * <p>
   * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
   * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
   * @param address The address to connect to
   * @param sendBufferSize The send buffer for the new connection
   * @param receiveBufferSize The receive buffer for the new connection
   * @return The id for the connection that was created
   * @throws IllegalStateException if there is already a connection for that id
   * @throws IOException if DNS resolution fails on the hostname or if the server is down
   */
  @Override
  public String connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize)
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
    key.attach(new Transmissions(connectionId, address.getHostName(), address.getPort()));
    this.keyMap.put(connectionId, key);
    activeConnections.set(this.keyMap.size());
    return connectionId;
  }

  /**
   * Register the nioSelector with an existing channel
   * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
   * Note that we are not checking if the connection id is valid - since the connection already exists
   */
  public String register(SocketChannel channel)
      throws ClosedChannelException {
    Socket socket = channel.socket();
    String remoteHost = socket.getInetAddress().getHostAddress();
    int remotePort = socket.getPort();
    String connectionId = generateConnectionId(channel);
    SelectionKey key = channel.register(nioSelector, SelectionKey.OP_READ);
    key.attach(new Transmissions(connectionId, remoteHost, remotePort));
    this.keyMap.put(connectionId, key);
    activeConnections.set(this.keyMap.size());
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
   * Queue the given request for sending in the subsequent {@poll(long)} calls
   * @param networkSend The NetworkSend that is ready to be sent
   */
  public void send(NetworkSend networkSend) {
    SelectionKey key = keyForId(networkSend.getConnectionId());
    if (key == null) {
      throw new IllegalStateException("Attempt to send data to a null key");
    }
    Transmissions transmissions = transmissions(key);
    if (transmissions.hasSend()) {
      throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
    }
    transmissions.send = networkSend;
    metrics.sendInFlight.inc();
    try {
      key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
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
  public void poll(long timeoutMs)
      throws IOException {
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
   * @param sends The list of new sends to begin
   *
   * @throws IOException If a send is given for which we have no existing connection or for which there is
   *         already an in-progress send
   */
  @Override
  public void poll(long timeoutMs, List<NetworkSend> sends)
      throws IOException {
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
    long endSelect = time.milliseconds();
    this.metrics.selectorSelectTime.update(endSelect - startSelect);
    this.metrics.selectorSelectRate.inc();

    if (readyKeys > 0) {
      Set<SelectionKey> keys = nioSelector.selectedKeys();
      Iterator<SelectionKey> iter = keys.iterator();
      while (iter.hasNext()) {
        SelectionKey key = iter.next();
        iter.remove();

        Transmissions transmissions = transmissions(key);
        // register all per-node metrics at once
        //metrics.initializeSelectorNodeMetricIfRequired(transmissions.remoteHostName, transmissions.remotePort);
        try {
          if (key.isConnectable()) {
            handleConnect(key, transmissions);
          } else if (key.isReadable()) {
            read(key, transmissions);
          } else if (key.isWritable()) {
            write(key, transmissions);
          } else if (!key.isValid()) {
            close(key);
          } else {
            throw new IllegalStateException("Unrecognized key state for processor thread.");
          }
        } catch (IOException e) {
          String socketDescription = socketDescription(channel(key));
          if (e instanceof EOFException || e instanceof ConnectException) {
            metrics.selectorDisconnectedErrorCount.inc();
            logger.error("Connection {} disconnected", socketDescription, e);
          } else {
            metrics.selectorIOErrorCount.inc();
            logger.warn("Error in I/O with connection to {}", socketDescription, e);
          }
          close(key);
        } catch (Exception e) {
          metrics.selectorKeyOperationErrorCount.inc();
          logger.error("closing key on exception remote host {}", channel(key).socket().getRemoteSocketAddress(), e);
          close(key);
        }
      }
      this.metrics.selectorIORate.inc();
    }
    long endIo = time.milliseconds();
    this.metrics.selectorIOTime.update(endIo - endSelect);
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

  public long getActiveConnections() {
    return activeConnections.get();
  }

  /**
   * Clear the results from the prior poll
   */
  private void clear() {
    this.completedSends.clear();
    this.completedReceives.clear();
    this.connected.clear();
    this.disconnected.clear();
  }

  /**
   * Check for data, waiting up to the given timeout.
   *
   * @param ms Length of time to wait, in milliseconds. If negative, wait indefinitely.
   * @return The number of keys ready
   * @throws IOException
   */
  private int select(long ms)
      throws IOException {
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
    SocketChannel socketChannel = channel(key);
    Transmissions transmissions = transmissions(key);
    if (transmissions != null) {
      logger.debug("Closing connection from {}", transmissions.connectionId);
      this.disconnected.add(transmissions.connectionId);
      this.keyMap.remove(transmissions.connectionId);
      activeConnections.set(this.keyMap.size());
      transmissions.clearReceive();
      transmissions.clearSend();
    }
    key.attach(null);
    key.cancel();
    try {
      socketChannel.socket().close();
      socketChannel.close();
    } catch (IOException e) {
      metrics.selectorCloseSocketErrorCount.inc();
      logger.error("Exception closing connection to node {}:", transmissions.connectionId, e);
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
   * Process connections that have finished their handshake
   */
  private void handleConnect(SelectionKey key, Transmissions transmissions)
      throws IOException {
    SocketChannel socketChannel = channel(key);
    socketChannel.finishConnect();
    key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
    this.connected.add(transmissions.getConnectionId());
    this.metrics.selectorConnectionCreated.inc();
    //this.metrics.initializeSelectorNodeMetricIfRequired(transmissions.remoteHostName, transmissions.remotePort);
  }

  /**
   * Process reads from ready sockets
   */
  private void read(SelectionKey key, Transmissions transmissions)
      throws IOException {
    long startTimeToReadInMs = time.milliseconds();
    try {
      if (!transmissions.hasReceive()) {
        transmissions.receive =
            new NetworkReceive(transmissions.getConnectionId(), new BoundedByteBufferReceive(), time);
      }

      SocketChannel socketChannel = channel(key);
      long bytesRead = transmissions.receive.getReceivedBytes().readFrom(socketChannel);
      if (bytesRead == -1) {
        close(key);
        return;
      }
      metrics.selectorBytesReceived.update(bytesRead);
      metrics.selectorBytesReceivedCount.inc(bytesRead);

      if (transmissions.receive.getReceivedBytes().isReadComplete()) {
        this.completedReceives.add(transmissions.receive);
        //metrics.updateNodeReceiveMetric(transmissions.remoteHostName, transmissions.remotePort,
        //    transmissions.receive.getReceivedBytes().getPayload().limit(),
        //    time.milliseconds() - transmissions.receive.getReceiveStartTimeInMs());
        transmissions.clearReceive();
      }
    } finally {
      long readTime = time.milliseconds() - startTimeToReadInMs;
      logger.trace("SocketServer time spent on read per key {} = {}", transmissions.connectionId, readTime);
    }
  }

  /**
   * Process writes to ready sockets
   */
  private void write(SelectionKey key, Transmissions transmissions)
      throws IOException {
    long startTimeToWriteInMs = time.milliseconds();
    try {
      SocketChannel socketChannel = channel(key);
      NetworkSend networkSend = transmissions.send;
      Send send = networkSend.getPayload();
      if (send == null) {
        throw new IllegalStateException("Registered for write interest but no response attached to key.");
      }
      send.writeTo(socketChannel);
      logger.trace("Bytes written to {} using key {}", socketChannel.socket().getRemoteSocketAddress(),
          transmissions.connectionId);

      if (send.isSendComplete()) {
        logger.trace("Finished writing, registering for read on connection {}",
            socketChannel.socket().getRemoteSocketAddress());
        networkSend.onSendComplete();
        this.completedSends.add(networkSend);
        metrics.sendInFlight.dec();
        //metrics.updateNodeSendMetric(transmissions.remoteHostName, transmissions.remotePort, send.sizeInBytes(),
        //    time.milliseconds() - networkSend.getSendStartTimeInMs());
        transmissions.clearSend();
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE | SelectionKey.OP_READ);
      }
    } finally {
      long writeTime = time.milliseconds() - startTimeToWriteInMs;
      logger.trace("SocketServer time spent on write per key {} = {}", transmissions.connectionId, writeTime);
    }
  }

  /**
   * Get the transmissions for the given connection
   */
  private Transmissions transmissions(SelectionKey key) {
    return (Transmissions) key.attachment();
  }

  /**
   * Get the socket channel associated with this selection key
   */
  private SocketChannel channel(SelectionKey key) {
    return (SocketChannel) key.channel();
  }

  /**
   * The id, hostname, port and in-progress send and receive associated with a connection
   */
  private static class Transmissions {
    private String connectionId;
    private String remoteHostName;
    private int remotePort;
    private NetworkSend send = null;
    private NetworkReceive receive = null;

    private Transmissions(String connectionId, String remoteHostName, int remotePort) {
      this.connectionId = connectionId;
      this.remoteHostName = remoteHostName;
      this.remotePort = remotePort;
    }

    private String getConnectionId() {
      return connectionId;
    }

    private boolean hasSend() {
      return send != null;
    }

    private void clearSend() {
      send = null;
    }

    private boolean hasReceive() {
      return receive != null;
    }

    private void clearReceive() {
      receive = null;
    }
  }
}
