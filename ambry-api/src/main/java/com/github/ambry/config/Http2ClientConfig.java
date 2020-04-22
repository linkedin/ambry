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
  public static final String HTTP2_MAX_CONCURRENT_STREAMS_PER_CONNECTION =
      "http2.max.concurrent.streams.per.connection";
  public static final String HTTP2_NETTY_EVENT_LOOP_GROUP_THREADS = "http2.netty.event.loop.group.threads";
  public static final String HTTP2_IDLE_CONNECTION_TIMEOUT_MS = "http2.idle.connection.timeout.ms";
  public static final String HTTP2_MAX_CONTENT_LENGTH = "http2.max.content.length";
  public static final String HTTP2_FRAME_MAX_SIZE = "http2.frame.max.size";
  public static final String HTTP2_INITIAL_WINDOW_SIZE = "http2.initial.window.size";
  public static final String NETTY_RECEIVE_BUFFER_SIZE = "netty.receive.buffer.size";
  public static final String NETTY_SEND_BUFFER_SIZE = "netty.send.buffer.size";

  /**
   * HTTP/2 connection idle time before we close it. -1 means no idle close.
   */
  @Config(HTTP2_IDLE_CONNECTION_TIMEOUT_MS)
  @Default("-1")
  public final Long idleConnectionTimeoutMs;

  /**
   * Minimum number of http2 connection per port we want to keep.
   * Based on initial perf test, number of HTTP/2 connection is not a significant performance factor.
   * 2 is used by default, in case of one connection died.
   */
  @Config(HTTP2_MIN_CONNECTION_PER_PORT)
  @Default("2")
  public final int http2MinConnectionPerPort;

  /**
   * Maximum concurrent number of streams allowed per HTTP/2 connection.
   */
  @Config(HTTP2_MAX_CONCURRENT_STREAMS_PER_CONNECTION)
  @Default("Integer.MAX_VALUE")
  public final int http2MaxConcurrentStreamsPerConnection;

  /**
   * Number of threads in a Netty event loop group. 0 means Netty will decide the number.
   */
  @Config(HTTP2_NETTY_EVENT_LOOP_GROUP_THREADS)
  @Default("0")
  public final int http2NettyEventLoopGroupThreads;

  /**
   * Maximum content length for a full HTTP/2 content. Used in HttpObjectAggregator.
   * In HttpObjectAggregator, maxContentLength is not used to preallocate buffer,
   * but it throws exception if content length great than maxContentLength
   * TODO: Link this with blob chunk size.
   */
  @Config(HTTP2_MAX_CONTENT_LENGTH)
  @Default("25 * 1024 * 1024")
  public final int http2MaxContentLength;

  /**
   * The maximum allowed http2 frame size. This value is used to represent
   * <a href="https://tools.ietf.org/html/rfc7540#section-6.5.2">SETTINGS_MAX_FRAME_SIZE</a>.
   */
  @Config(HTTP2_FRAME_MAX_SIZE)
  @Default("5 * 1024 * 1024")
  public final int http2FrameMaxSize;


  /**
   * The initial window size used in http streams. This allows sender send big frame.
   */
  @Config(HTTP2_INITIAL_WINDOW_SIZE)
  @Default("5 * 1024 * 1024")
  public final int http2InitialWindowSize;


  /**
   * The socket receive buffer size for netty http2 channel.
   */
  @Config(NETTY_RECEIVE_BUFFER_SIZE)
  @Default("1024 * 1024")
  public final int nettyReceiveBufferSize;

  /**
   * The socket send buffer size for netty http2 channel.
   */
  @Config(NETTY_SEND_BUFFER_SIZE)
  @Default("1024 * 1024")
  public final int nettySendBufferSize;

  public Http2ClientConfig(VerifiableProperties verifiableProperties) {
    idleConnectionTimeoutMs = verifiableProperties.getLong(HTTP2_IDLE_CONNECTION_TIMEOUT_MS, -1);
    http2MinConnectionPerPort = verifiableProperties.getInt(HTTP2_MIN_CONNECTION_PER_PORT, 2);
    http2MaxConcurrentStreamsPerConnection =
        verifiableProperties.getIntInRange(HTTP2_MAX_CONCURRENT_STREAMS_PER_CONNECTION, Integer.MAX_VALUE, 1,
            Integer.MAX_VALUE);
    http2NettyEventLoopGroupThreads = verifiableProperties.getInt(HTTP2_NETTY_EVENT_LOOP_GROUP_THREADS, 0);
    http2MaxContentLength = verifiableProperties.getInt(HTTP2_MAX_CONTENT_LENGTH, 25 * 1024 * 1024);
    http2FrameMaxSize = verifiableProperties.getInt(HTTP2_FRAME_MAX_SIZE, 5 * 1024 * 1024);
    http2InitialWindowSize = verifiableProperties.getInt(HTTP2_INITIAL_WINDOW_SIZE, 5 * 1024 * 1024);

    nettyReceiveBufferSize = verifiableProperties.getInt(NETTY_RECEIVE_BUFFER_SIZE, 1024 * 1024);
    nettySendBufferSize = verifiableProperties.getInt(NETTY_SEND_BUFFER_SIZE, 1024 * 1024);
  }
}
