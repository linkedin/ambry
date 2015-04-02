package com.github.ambry.config;

/**
 * The configs for connection pool.
 */
public class ConnectionPoolConfig {

  /**
   * The read buffer size in bytes for a connection.
   */
  @Config("connectionpool.read.buffer.size.bytes")
  @Default("1048576")
  public final int connectionPoolReadBufferSizeBytes;

  /**
   * The write buffer size in bytes for a connection.
   */
  @Config("connectionpool.write.buffer.size.bytes")
  @Default("1048576")
  public final int connectionPoolWriteBufferSizeBytes;

  /**
   * Read timeout in milliseconds for a connection.
   */
  @Config("connectionpool.read.timeout.ms")
  @Default("1500")
  public final int connectionPoolReadTimeoutMs;

  /**
   * Connect timeout in milliseconds for a connection.
   */
  @Config("connectionpool.connect.timeout.ms")
  @Default("800")
  public final int connectionPoolConnectTimeoutMs;

  /**
   * The max connections allowed per host
   */
  @Config("connectionpool.max.connections.per.host")
  @Default("5")
  public final int connectionPoolMaxConnectionsPerHost;

  public ConnectionPoolConfig(VerifiableProperties verifiableProperties) {
    connectionPoolReadBufferSizeBytes =
        verifiableProperties.getIntInRange("connectionpool.read.buffer.size.bytes", 1048576, 1, 1024 * 1024 * 1024);
    connectionPoolWriteBufferSizeBytes =
        verifiableProperties.getIntInRange("connectionpool.write.buffer.size.bytes", 1048576, 1, 1024 * 1024 * 1024);
    connectionPoolReadTimeoutMs = verifiableProperties.getIntInRange("connectionpool.read.timeout.ms", 1500, 1, 100000);
    connectionPoolConnectTimeoutMs =
        verifiableProperties.getIntInRange("connectionpool.connect.timeout.ms", 800, 1, 100000);
    connectionPoolMaxConnectionsPerHost =
        verifiableProperties.getIntInRange("connectionpool.max.connections.per.host", 5, 1, 20);
  }
}
