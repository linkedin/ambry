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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
public class SocketServer implements NetworkServer {

  private final NetworkConfig networkConfig;
  private final String host;
  private final int port;
  private final int numProcessorThreads;
  private final int maxQueuedRequests;
  private final int sendBufferSize;
  private final int recvBufferSize;
  private final int maxRequestSize;
  private final ArrayList<Processor> processors;
  private volatile ArrayList<Acceptor> acceptors;
  private final SocketRequestResponseChannel requestResponseChannel;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final ServerNetworkMetrics metrics;
  private final HashMap<PortType, Port> ports;
  private SSLFactory sslFactory;

  public SocketServer(NetworkConfig config, SSLConfig sslConfig, MetricRegistry registry, ArrayList<Port> portList) {
    this.networkConfig = config;
    this.host = config.hostName;
    this.port = config.port;
    this.numProcessorThreads = config.numIoThreads;
    this.maxQueuedRequests = config.queuedMaxRequests;
    this.sendBufferSize = config.socketSendBufferBytes;
    this.recvBufferSize = config.socketReceiveBufferBytes;
    this.maxRequestSize = config.socketRequestMaxBytes;
    processors = new ArrayList<Processor>(numProcessorThreads);
    requestResponseChannel = new SocketRequestResponseChannel(numProcessorThreads, maxQueuedRequests);
    metrics = new ServerNetworkMetrics(requestResponseChannel, registry, processors);
    this.acceptors = new ArrayList<Acceptor>();
    this.ports = new HashMap<PortType, Port>();
    this.validatePorts(portList);
    this.initializeSSLFactory(sslConfig);
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getSSLPort() {
    Port sslPort = ports.get(PortType.SSL);
    if (sslPort != null) {
      return sslPort.getPort();
    }
    throw new IllegalStateException("No SSL Port Exists for Server " + host + ":" + port);
  }

  private void initializeSSLFactory(SSLConfig sslConfig) {
    if (ports.get(PortType.SSL) != null) {
      try {
        this.sslFactory = SSLFactory.getNewInstance(sslConfig);
        metrics.sslFactoryInitializationCount.inc();
      } catch (Exception e) {
        metrics.sslFactoryInitializationErrorCount.inc();
        throw new IllegalStateException("Exception thrown during initialization of SSLFactory ", e);
      }
    }
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

  private void validatePorts(ArrayList<Port> portList) {
    HashSet<PortType> portTypeSet = new HashSet<PortType>();
    for (Port port : portList) {
      if (portTypeSet.contains(port.getPortType())) {
        throw new IllegalArgumentException("Not more than one port of same type is allowed : " + port.getPortType());
      } else {
        portTypeSet.add(port.getPortType());
        this.ports.put(port.getPortType(), port);
      }
    }
  }

  public void start() throws IOException, InterruptedException {
    logger.info("Starting {} processor threads", numProcessorThreads);
    for (int i = 0; i < numProcessorThreads; i++) {
      processors.add(i, new Processor(i, maxRequestSize, requestResponseChannel, metrics, sslFactory, networkConfig));
      Utils.newThread("ambry-processor-" + port + " " + i, processors.get(i), false).start();
    }

    requestResponseChannel.addResponseListener(new ResponseListener() {
      @Override
      public void onResponse(int processorId) {
        processors.get(processorId).wakeup();
      }
    });

    // start accepting connections
    logger.info("Starting acceptor threads on port {}", port);
    Acceptor plainTextAcceptor = new Acceptor(port, processors, sendBufferSize, recvBufferSize, metrics);
    this.acceptors.add(plainTextAcceptor);
    Utils.newThread("ambry-acceptor", plainTextAcceptor, false).start();

    Port sslPort = ports.get(PortType.SSL);
    if (sslPort != null) {
      SSLAcceptor sslAcceptor = new SSLAcceptor(sslPort.getPort(), processors, sendBufferSize, recvBufferSize, metrics);
      acceptors.add(sslAcceptor);
      Utils.newThread("ambry-sslacceptor", sslAcceptor, false).start();
    }
    for (Acceptor acceptor : acceptors) {
      acceptor.awaitStartup();
    }
    logger.info("Started server");
  }

  public void shutdown() {
    try {
      logger.info("Shutting down server");
      for (Acceptor acceptor : acceptors) {
        if (acceptor != null) {
          acceptor.shutdown();
        }
      }
      for (Processor processor : processors) {
        processor.shutdown();
      }
      logger.info("Shutdown completed");
      requestResponseChannel.shutdown();
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

  public AbstractServerThread() throws IOException {
    startupLatch = new CountDownLatch(1);
    shutdownLatch = new CountDownLatch(1);
    alive = new AtomicBoolean(false);
  }

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  public void shutdown() throws InterruptedException {
    alive.set(false);
    shutdownLatch.await();
  }

  /**
   * Wait for the thread to completely start up
   */
  public void awaitStartup() throws InterruptedException {
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
 * Thread that accepts and configures new connections.
 */
class Acceptor extends AbstractServerThread {
  private final ArrayList<Processor> processors;
  private final int sendBufferSize;
  private final int recvBufferSize;
  private final ServerSocketChannel serverChannel;
  private final java.nio.channels.Selector nioSelector;
  private static final long selectTimeOutMs = 500;
  private final ServerNetworkMetrics metrics;
  protected Logger logger = LoggerFactory.getLogger(getClass());

  public Acceptor(int port, ArrayList<Processor> processors, int sendBufferSize, int recvBufferSize,
      ServerNetworkMetrics metrics) throws IOException {
    this.processors = processors;
    this.sendBufferSize = sendBufferSize;
    this.recvBufferSize = recvBufferSize;
    this.serverChannel = openServerSocket(port);
    this.nioSelector = java.nio.channels.Selector.open();
    this.metrics = metrics;
  }

  /**
   * Accept loop that checks for new connection attempts for a plain text port
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
              key.cancel();
              metrics.acceptConnectionErrorCount.inc();
              logger.debug("Error in accepting new connection", e);
            }
          }
        }
      }
      logger.debug("Closing server socket and selector.");
      serverChannel.close();
      nioSelector.close();
      shutdownComplete();
      super.shutdown();
    } catch (Exception e) {
      metrics.acceptorShutDownErrorCount.inc();
      logger.error("Error during shutdown of acceptor thread", e);
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private ServerSocketChannel openServerSocket(int port) throws IOException {
    InetSocketAddress address = new InetSocketAddress(port);
    ServerSocketChannel serverChannel = ServerSocketChannel.open();
    serverChannel.configureBlocking(false);
    serverChannel.socket().bind(address);
    logger.info("Awaiting socket connections on {}:{}", address.getHostName(), port);
    return serverChannel;
  }

  /*
   * Accept a new connection
   */
  protected void accept(SelectionKey key, Processor processor) throws SocketException, IOException {
    SocketChannel socketChannel = acceptConnection(key);
    processor.accept(socketChannel, PortType.PLAINTEXT);
  }

  protected SocketChannel acceptConnection(SelectionKey key) throws SocketException, IOException {
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
    return socketChannel;
  }

  public void shutdown() throws InterruptedException {
    nioSelector.wakeup();
    super.shutdown();
  }
}

/**
 * Thread that accepts and configures new connections for an SSL Port
 */
class SSLAcceptor extends Acceptor {

  public SSLAcceptor(int port, ArrayList<Processor> processors, int sendBufferSize, int recvBufferSize,
      ServerNetworkMetrics metrics) throws IOException {
    super(port, processors, sendBufferSize, recvBufferSize, metrics);
  }

  /*
   * Accept a new connection
   */
  @Override
  protected void accept(SelectionKey key, Processor processor) throws SocketException, IOException {
    SocketChannel socketChannel = acceptConnection(key);
    processor.accept(socketChannel, PortType.SSL);
  }
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
class Processor extends AbstractServerThread {
  private final SocketRequestResponseChannel channel;
  private final int id;
  private final Time time;
  private final ConcurrentLinkedQueue<SocketChannelPortTypePair> newConnections =
      new ConcurrentLinkedQueue<SocketChannelPortTypePair>();
  private final Selector selector;
  private final ServerNetworkMetrics metrics;
  private static final long pollTimeoutMs = 300;
  private final NetworkConfig networkConfig;

  Processor(int id, int maxRequestSize, RequestResponseChannel channel, ServerNetworkMetrics metrics,
      SSLFactory sslFactory, NetworkConfig networkConfig) throws IOException {
    this.channel = (SocketRequestResponseChannel) channel;
    this.id = id;
    this.time = SystemTime.getInstance();
    selector = new Selector(metrics, time, sslFactory, networkConfig);
    this.metrics = metrics;
    this.networkConfig = networkConfig;
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
          ByteBuf buffer = networkReceive.getReceivedBytes().content();
          SocketServerRequest req = new SocketServerRequest(id, connectionId, buffer);
          channel.sendRequest(req);
        }
      }
    } catch (Exception e) {
      logger.error("Error in processor thread", e);
    } finally {
      logger.debug("Closing server socket and selector.");
      try {
        closeAll();
        shutdownComplete();
        super.shutdown();
      } catch (InterruptedException ie) {
        metrics.processorShutDownErrorCount.inc();
        logger.error("InterruptedException on processor shutdown ", ie);
      }
    }
  }

  private void processNewResponses() throws InterruptedException, IOException {
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
      } catch (IllegalStateException e) {
        metrics.processNewResponseErrorCount.inc();
        logger.debug("Error in processing new responses", e);
      } finally {
        curr = (SocketServerResponse) channel.receiveResponse(id);
      }
    }
  }

  /**
   * Queue up a new connection for reading
   */
  public void accept(SocketChannel socketChannel, PortType portType) {
    newConnections.add(new SocketChannelPortTypePair(socketChannel, portType));
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
  private void configureNewConnections() throws ClosedChannelException, IOException {
    while (newConnections.size() > 0) {
      SocketChannelPortTypePair socketChannelPortTypePair = newConnections.poll();
      logger.debug("Processor {} listening to new connection from {}", id,
          socketChannelPortTypePair.getSocketChannel().socket().getRemoteSocketAddress());
      try {
        selector.register(socketChannelPortTypePair.getSocketChannel(), socketChannelPortTypePair.getPortType());
      } catch (IOException e) {
        logger.error("Error on registering new connection ", e);
      }
    }
  }

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  public void shutdown() throws InterruptedException {
    selector.wakeup();
    super.shutdown();
  }

  /**
   * Wakes up the thread for selection.
   */
  public void wakeup() {
    selector.wakeup();
  }

  class SocketChannelPortTypePair {
    private SocketChannel socketChannel;
    private PortType portType;

    public SocketChannelPortTypePair(SocketChannel socketChannel, PortType portType) {
      this.socketChannel = socketChannel;
      this.portType = portType;
    }

    public PortType getPortType() {
      return portType;
    }

    public SocketChannel getSocketChannel() {
      return this.socketChannel;
    }
  }
}
