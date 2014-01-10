package com.github.ambry.config;

/**
 * The configs for connection pool.
 */
public class ConnectionPoolConfig {

  /**
   * The read buffer size in bytes for a connection.
   */
  @Config("connectionpool.read.buffer.size.bytes")
  @Default("10000")
  public final int readBufferSizeBytes;

  /**
   * The write buffer size in bytes for a connection.
   */
  @Config("connectionpool.write.buffer.size.bytes")
  @Default("10000")
  public final int writeBufferSizeBytes;

  /**
   * Read timeout in milliseconds for a connection.
   */
  @Config("connectionpool.read.timeout.ms")
  @Default("10000")
  public final int readTimeoutMs;

  public ConnectionPoolConfig(VerifiableProperties verifiableProperties) {
    readBufferSizeBytes =
            verifiableProperties.getIntInRange("connectionpool.read.buffer.size.bytes", 10000, 1, Integer.MAX_VALUE);
    writeBufferSizeBytes =
            verifiableProperties.getIntInRange("connectionpool.write.buffer.size.bytes", 10000, 1, Integer.MAX_VALUE);
    readTimeoutMs = verifiableProperties.getIntInRange("connectionpool.read.timeout.ms", 10000, 1, Integer.MAX_VALUE);
  }
}
