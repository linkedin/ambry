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
   * The max connections allowed per host per port for plain text
   */
  @Config("connectionpool.max.connections.per.port.plain.text")
  @Default("5")
  public final int connectionPoolMaxConnectionsPerPortPlainText;

  /**
   * The max connections allowed per host per port for ssl
   */
  @Config("connectionpool.max.connections.per.port.ssl")
  @Default("2")
  public final int connectionPoolMaxConnectionsPerPortSSL;

  /**
   * Use TCP RST instead of FIN on replication socket error
   */
  @Config("connectionpool.socket.reset.on.error")
  @Default("true")
  public final boolean connectionPoolSocketResetOnError;

  /**
   * Enable TCP_NODELAY socket option
   */
  @Config("connectionpool.socket.enable.tcp.no.delay")
  @Default("true")
  public final boolean connectionPoolSocketEnableTcpNoDelay;

  public ConnectionPoolConfig(VerifiableProperties verifiableProperties) {
    connectionPoolReadBufferSizeBytes =
        verifiableProperties.getIntInRange("connectionpool.read.buffer.size.bytes", 1048576, 1, 1024 * 1024 * 1024);
    connectionPoolWriteBufferSizeBytes =
        verifiableProperties.getIntInRange("connectionpool.write.buffer.size.bytes", 1048576, 1, 1024 * 1024 * 1024);
    connectionPoolReadTimeoutMs = verifiableProperties.getIntInRange("connectionpool.read.timeout.ms", 1500, 1, 100000);
    connectionPoolConnectTimeoutMs =
        verifiableProperties.getIntInRange("connectionpool.connect.timeout.ms", 800, 1, 100000);
    connectionPoolMaxConnectionsPerPortPlainText =
        verifiableProperties.getIntInRange("connectionpool.max.connections.per.port.plain.text", 5, 1, 20);
    connectionPoolMaxConnectionsPerPortSSL =
        verifiableProperties.getIntInRange("connectionpool.max.connections.per.port.ssl", 2, 1, 20);
    connectionPoolSocketResetOnError = verifiableProperties.getBoolean("connectionpool.socket.reset.on.error", true);
    connectionPoolSocketEnableTcpNoDelay = verifiableProperties.getBoolean("connectionpool.socket.enable.tcp.no.delay", true);
  }
}
