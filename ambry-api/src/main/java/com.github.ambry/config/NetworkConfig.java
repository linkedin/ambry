package com.github.ambry.config;


/**
 * The configs for network layer
 */
public class NetworkConfig {

  private final VerifiableProperties props;


  /**
   * The number of network threads that the server uses for handling network requests
   */
  @Config("num.network.threads")
  @Default("3")
  public final int numNetworkThreads;

  /**
   * The number of io threads that the server uses for carrying out network requests
   */
  @Config("num.io.threads")
  @Default("8")
  public final int numIoThreads;

  /**
   * The number of queued requests allowed before blocking the network threads
   */
  @Config("queued.max.requests")
  @Default("500")
  public final int queuedMaxRequests;

  /**
   * The port to listen and accept connections on
   */
  @Config("port")
  @Default("6667")
  public final int port;

  /**
   * Hostname of broker. If this is set, it will only bind to this address. If this is not set,
   * it will bind to all interfaces, and publish one to ZK
   */
  @Config("host.name")
  @Default("localhost")
  public final String hostName;

  /**
   * The SO_SNDBUFF buffer of the socket sever sockets
   */
  @Config("socket.send.buffer.bytes")
  @Default("102400")
  public final int socketSendBufferBytes;

  /**
   * The SO_RCVBUFF buffer of the socket sever sockets
   */
  @Config("socket.receive.buffer.bytes")
  @Default("102400")
  public final int socketReceiveBufferBytes;

  /**
   * The maximum number of bytes in a socket request
   */
  @Config("socket.request.max.bytes")
  @Default("104857600")
  public final int socketRequestMaxBytes;

  public NetworkConfig(VerifiableProperties properties) {
    this.props = properties;
    numNetworkThreads = props.getIntInRange("num.network.threads", 3, 1, Integer.MAX_VALUE);
    numIoThreads = props.getIntInRange("num.io.threads", 8, 1, Integer.MAX_VALUE);
    port = props.getInt("port", 6667);
    hostName = props.getString("host.name", null);
    socketSendBufferBytes = props.getInt("socket.send.buffer.bytes", 100*1024);
    socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", 100*1024);
    socketRequestMaxBytes = props.getIntInRange("socket.request.max.bytes", 100*1024*1024, 1, Integer.MAX_VALUE);
    queuedMaxRequests = props.getIntInRange("queued.max.requests", 500, 1, Integer.MAX_VALUE);
  }




}
