/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 * The configs for HTTP/2 Client
 */
public class Http2ClientConfig {

  public static final String HTTP2_MIN_CONNECTION_PER_PORT = "http2.min.connection.per.port";
  public static final String HTTP2_MAX_STREAMS_PER_CONNECTION = "http2.max.streams.per.connection";
  public static final String HTTP2_NETTY_EVENT_LOOP_GROUP_THREADS = "http2.netty.event.loop.group.threads";
  public static final String HTTP2_IDLE_CONNECTION_TIMEOUT = "http2.idle.connection.timeout";

  /**
   * HTTP/2 connection idle time before we close it. -1 means no idle close.
   */
  @Config(HTTP2_IDLE_CONNECTION_TIMEOUT)
  @Default("-1")
  public final Long idleConnectionTimeout;

  /**
   * Minimum number of http2 connection per port we want to keep.
   */
  @Config(HTTP2_MIN_CONNECTION_PER_PORT)
  @Default("4")
  public final int http2MinConnectionPerPort;

  /**
   * Maximum number of streams allowed per HTTP/2 connection.
   */
  @Config(HTTP2_MAX_STREAMS_PER_CONNECTION)
  @Default("Long.Max_VALUE")
  public final Long http2MaxStreamsPerConnection;

  /**
   * Number of threads in a Netty event loop group. 0 means Netty will decide the number.
   */
  @Config(HTTP2_NETTY_EVENT_LOOP_GROUP_THREADS)
  @Default("0")
  public final int http2NettyEventLoopGroupThreads;

  public Http2ClientConfig(VerifiableProperties verifiableProperties) {
    idleConnectionTimeout = verifiableProperties.getLong(HTTP2_IDLE_CONNECTION_TIMEOUT, -1);
    http2MinConnectionPerPort = verifiableProperties.getInt(HTTP2_MIN_CONNECTION_PER_PORT, 4);
    http2MaxStreamsPerConnection = verifiableProperties.getLong(HTTP2_MAX_STREAMS_PER_CONNECTION, Long.MAX_VALUE);
    http2NettyEventLoopGroupThreads = verifiableProperties.getInt(HTTP2_NETTY_EVENT_LOOP_GROUP_THREADS, 0);
  }
}
