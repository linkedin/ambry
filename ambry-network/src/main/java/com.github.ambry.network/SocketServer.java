package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Iterator;
import java.util.Set;


/**
 * A NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
public class SocketServer implements NetworkServer {

  private final String host;
  private final int port;
  private final int numProcessorThreads;
  private final int maxQueuedRequests;
  private final int sendBufferSize;
  private final int recvBufferSize;
  private final int maxRequestSize;
  private final ArrayList<Processor> processors;
  private volatile Acceptor acceptor;
  private final SocketRequestResponseChannel requestResponseChannel;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final NetworkMetrics metrics;

  public SocketServer(NetworkConfig config, MetricRegistry registry) {
    this.host = config.hostName;
    this.port = config.port;
    this.numProcessorThreads = config.numIoThreads;
    this.maxQueuedRequests = config.queuedMaxRequests;
    this.sendBufferSize = config.socketSendBufferBytes;
    this.recvBufferSize = config.socketReceiveBufferBytes;
    this.maxRequestSize = config.socketRequestMaxBytes;
    processors = new ArrayList<Processor>(numProcessorThreads);
    requestResponseChannel = new SocketRequestResponseChannel(numProcessorThreads, maxQueuedRequests);
    metrics = new NetworkMetrics(requestResponseChannel, registry);
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getNumProcessorThreads() {
    return numProcessorThreads;
  }

  public int getMaxQueuedRequests() {
    return maxQueuedRequests;
  }

  public int getSendBufferSize() {
    return sendBufferSize;
  }

  public int getRecvBufferSize() {
    return recvBufferSize;
  }

  public int getMaxRequestSize() {
    return maxRequestSize;
  }

  @Override
  public RequestResponseChannel getRequestResponseChannel() {
    return requestResponseChannel;
  }

  public void start()
      throws IOException, InterruptedException {
    logger.info("Starting {} processor threads", numProcessorThreads);
    for (int i = 0; i < numProcessorThreads; i++) {
      processors.add(i, new Processor(i, maxRequestSize, requestResponseChannel, metrics));
      Utils.newThread("ambry-processor-" + port + " " + i, processors.get(i), false).start();
    }

    requestResponseChannel.addResponseListener(new ResponseListener() {
      @Override
      public void onResponse(int processorId) {
        processors.get(processorId).wakeup();
      }
    });

    // start accepting connections
    logger.info("Starting acceptor thread");
    this.acceptor = new Acceptor(host, port, processors, sendBufferSize, recvBufferSize);
    Utils.newThread("ambry-acceptor", acceptor, false).start();
    acceptor.awaitStartup();
    logger.info("Started server");
  }

  public void shutdown() {
    try {
      logger.info("Shutting down server");
      if (acceptor != null) {
        acceptor.shutdown();
      }
      for (Processor processor : processors) {
        processor.shutdown();
      }
      logger.info("Shutdown completed");
    } catch (Exception e) {
      logger.error("Error shutting down socket server {}", e);
    }
  }
}

/**
 * A base class with some helper variables and methods
 */
abstract class AbstractServerThread implements Runnable {
  private final CountDownLatch startupLatch;
  private final CountDownLatch shutdownLatch;
  private final AtomicBoolean alive;
  protected Logger logger = LoggerFactory.getLogger(getClass());

  public AbstractServerThread()
      throws IOException {
    startupLatch = new CountDownLatch(1);
    shutdownLatch = new CountDownLatch(1);
    alive = new AtomicBoolean(false);
  }

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  public void shutdown()
      throws InterruptedException {
    alive.set(false);
    shutdownLatch.await();
  }

  /**
   * Wait for the thread to completely start up
   */
  public void awaitStartup()
      throws InterruptedException {
    startupLatch.await();
  }

  /**
   * Record that the thread startup is complete
   */
  protected void startupComplete() {
    alive.set(true);
    startupLatch.countDown();
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected void shutdownComplete() {
    shutdownLatch.countDown();
  }

  /**
   * Is the server still running?
   */
  protected boolean isRunning() {
    return alive.get();
  }
}

/**
 * Thread that accepts and configures new connections. There is only need for one of these
 */
class Acceptor extends AbstractServerThread {
  private final String host;
  private final int port;
  private final ArrayList<Processor> processors;
  private final int sendBufferSize;
  private final int recvBufferSize;
  private final ServerSocketChannel serverChannel;
  private final java.nio.channels.Selector nioSelector;
  private static final long selectTimeOutMs = 500;
  protected Logger logger = LoggerFactory.getLogger(getClass());

  public Acceptor(String host, int port, ArrayList<Processor> processors, int sendBufferSize, int recvBufferSize)
      throws IOException {
    this.host = host;
    this.port = port;
    this.processors = processors;
    this.sendBufferSize = sendBufferSize;
    this.recvBufferSize = recvBufferSize;
    this.serverChannel = openServerSocket(this.host, this.port);
    this.nioSelector = java.nio.channels.Selector.open();
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  public void run() {
    try {
      serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
      startupComplete();
      int currentProcessor = 0;
      while (isRunning()) {
        int ready = nioSelector.select(selectTimeOutMs);
        if (ready > 0) {
          Set<SelectionKey> keys = nioSelector.selectedKeys();
          Iterator<SelectionKey> iter = keys.iterator();
          while (iter.hasNext() && isRunning()) {
            SelectionKey key = null;
            try {
              key = iter.next();
              iter.remove();
              if (key.isAcceptable()) {
                accept(key, processors.get(currentProcessor));
              } else {
                throw new IllegalStateException("Unrecognized key state for acceptor thread.");
              }

              // round robin to the next processor thread
              currentProcessor = (currentProcessor + 1) % processors.size();
            } catch (Exception e) {
              // throw
            }
          }
        }
      }
      logger.debug("Closing server socket and selector.");
      serverChannel.close();
      nioSelector.close();
      shutdownComplete();
    } catch (Exception e) {
      logger.error("Error during shutdown of acceptor thread {}", e);
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private ServerSocketChannel openServerSocket(String host, int port)
      throws IOException {
    InetSocketAddress address = null;
    if (host == null || host.trim().isEmpty()) {
      address = new InetSocketAddress(port);
    } else {
      address = new InetSocketAddress(host, port);
    }
    ServerSocketChannel serverChannel = ServerSocketChannel.open();
    serverChannel.configureBlocking(false);
    serverChannel.socket().bind(address);
    logger.info("Awaiting socket connections on {}:{}", address.getHostName(), port);
    return serverChannel;
  }

  /*
   * Accept a new connection
   */
  private void accept(SelectionKey key, Processor processor)
      throws SocketException, IOException {
    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
    serverSocketChannel.socket().setReceiveBufferSize(recvBufferSize);
    SocketChannel socketChannel = serverSocketChannel.accept();
    socketChannel.configureBlocking(false);
    socketChannel.socket().setTcpNoDelay(true);
    socketChannel.socket().setSendBufferSize(sendBufferSize);
    logger.trace("Accepted connection from {} on {}. sendBufferSize "
            + "[actual|requested]: [{}|{}] recvBufferSize [actual|requested]: [{}|{}]",
        socketChannel.socket().getInetAddress(), socketChannel.socket().getLocalSocketAddress(),
        socketChannel.socket().getSendBufferSize(), sendBufferSize, socketChannel.socket().getReceiveBufferSize(),
        recvBufferSize);
    processor.accept(socketChannel);
  }

  public void shutdown()
      throws InterruptedException {
    nioSelector.wakeup();
    super.shutdown();
  }
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
class Processor extends AbstractServerThread {
  private final int maxRequestSize;
  private final SocketRequestResponseChannel channel;
  private final int id;
  private final Time time;
  private final ConcurrentLinkedQueue<SocketChannel> newConnections = new ConcurrentLinkedQueue<SocketChannel>();
  private final Selector selector;
  private static final long pollTimeoutMs = 300;

  Processor(int id, int maxRequestSize, RequestResponseChannel channel, NetworkMetrics metrics)
      throws IOException {
    this.maxRequestSize = maxRequestSize;
    this.channel = (SocketRequestResponseChannel) channel;
    this.id = id;
    this.time = SystemTime.getInstance();
    selector = new Selector(metrics, time);
  }

  public void run() {
    try {
      startupComplete();
      while (isRunning()) {
        // setup any new connections that have been queued up
        configureNewConnections();
        // register any new responses for writing
        processNewResponses();
        selector.poll(pollTimeoutMs);

        // handle completed receives
        List<NetworkReceive> completedReceives = selector.completedReceives();
        for (NetworkReceive networkReceive : completedReceives) {
          String connectionId = networkReceive.getConnectionId();
          SocketServerRequest req = new SocketServerRequest(id, connectionId,
              new ByteBufferInputStream(networkReceive.getReceivedBytes().getPayload()));
          channel.sendRequest(req);
        }
      }
    } catch (Exception e) {
      logger.error("Error in processor thread {}", e);
    } finally {
      logger.debug("Closing server socket and selector.");
      closeAll();
      shutdownComplete();
    }
  }

  private void processNewResponses()
      throws InterruptedException, IOException {
    SocketServerResponse curr = (SocketServerResponse) channel.receiveResponse(id);
    while (curr != null) {
      curr.onDequeueFromResponseQueue();
      SocketServerRequest request = (SocketServerRequest) curr.getRequest();
      String connectionId = request.getConnectionId();
      try {
        if (curr.getPayload() == null) {
          // We should never need to send an empty response. If the payload is empty, we will assume error
          // and close the connection
          logger.trace("Socket server received no response and hence closing the connection");
          selector.close(connectionId);
        } else {
          logger.trace("Socket server received response to send, registering for write: {}", curr);
          NetworkSend networkSend = new NetworkSend(connectionId, curr.getPayload(), curr.getMetrics(), time);
          selector.send(networkSend);
        }
      } finally {
        curr = (SocketServerResponse) channel.receiveResponse(id);
      }
    }
  }

  /**
   * Queue up a new connection for reading
   */
  public void accept(SocketChannel socketChannel) {
    newConnections.add(socketChannel);
    wakeup();
  }

  /**
   * Close all open connections
   */
  private void closeAll() {
    selector.close();
  }

  /**
   * Register any new connections that have been queued up
   */
  private void configureNewConnections()
      throws ClosedChannelException {
    while (newConnections.size() > 0) {
      SocketChannel channel = newConnections.poll();
      logger.debug("Processor {} listening to new connection from {}", id, channel.socket().getRemoteSocketAddress());
      selector.register(channel);
    }
  }

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  public void shutdown()
      throws InterruptedException {
    selector.wakeup();
    super.shutdown();
  }

  /**
   * Wakes up the thread for selection.
   */
  public void wakeup() {
    selector.wakeup();
  }
}
