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

import com.github.ambry.rest.RestRequestService;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * Configuration parameters required by Netty.
 */
public class NettyConfig {
  public static final String NETTY_SERVER_BOSS_THREAD_COUNT = "netty.server.boss.thread.count";
  public static final String NETTY_SERVER_IDLE_TIME_SECONDS = "netty.server.idle.time.seconds";
  public static final String NETTY_SERVER_PORT = "netty.server.port";
  public static final String NETTY_SERVER_SSL_PORT = "netty.server.ssl.port";
  public static final String NETTY_SERVER_ENABLE_SSL = "netty.server.enable.ssl";
  public static final String NETTY_SERVER_SO_BACKLOG = "netty.server.so.backlog";
  public static final String NETTY_SERVER_WORKER_THREAD_COUNT = "netty.server.worker.thread.count";
  public static final String NETTY_SERVER_MAX_INITIAL_LINE_LENGTH = "netty.server.max.initial.line.length";
  public static final String NETTY_SERVER_MAX_HEADER_SIZE = "netty.server.max.header.size";
  public static final String NETTY_SERVER_MAX_CHUNK_SIZE = "netty.server.max.chunk.size";
  public static final String NETTY_SERVER_REQUEST_BUFFER_WATERMARK = "netty.server.request.buffer.watermark";
  public static final String NETTY_SERVER_BLACKLISTED_QUERY_PARAMS = "netty.server.blacklisted.query.params";
  public static final String NETTY_MULTIPART_POST_MAX_SIZE_BYTES = "netty.multipart.post.max.size.bytes";
  public static final String SSL_FACTORY_KEY = "netty.server.ssl.factory";
  public static final String NETTY_METRICS_REFRESH_INTERVAL_SECONDS = "netty.metrics.refresh.interval.seconds";
  public static final String NETTY_METRICS_STOP_WAIT_TIMEOUT_SECONDS = "netty.metrics.stop.wait.timeout.seconds";

  /**
   * Number of netty boss threads.
   */
  @Config(NETTY_SERVER_BOSS_THREAD_COUNT)
  @Default("1")
  public final int nettyServerBossThreadCount;

  /**
   * The amount of time a channel is allowed to be idle before it's closed. 0 to disable.
   */
  @Config(NETTY_SERVER_IDLE_TIME_SECONDS)
  @Default("60")
  public final int nettyServerIdleTimeSeconds;

  /**
   * Port on which to run netty server for plaintext connections.
   */
  @Config(NETTY_SERVER_PORT)
  @Default("1174")
  public final int nettyServerPort;

  /**
   * Port on which to run netty server for SSL connections.
   */
  @Config(NETTY_SERVER_SSL_PORT)
  @Default("1175")
  public final int nettyServerSSLPort;

  /**
   * Enable the netty server SSL port.
   */
  @Config(NETTY_SERVER_ENABLE_SSL)
  @Default("false")
  public final boolean nettyServerEnableSSL;

  /**
   * Socket backlog size. Defines the number of connections that can wait in queue to be accepted.
   */
  @Config(NETTY_SERVER_SO_BACKLOG)
  @Default("100")
  public final int nettyServerSoBacklog;

  /**
   * Number of netty worker threads.
   */
  @Config(NETTY_SERVER_WORKER_THREAD_COUNT)
  @Default("1")
  public final int nettyServerWorkerThreadCount;

  /**
   * The maximum length of the initial line in a request (in bytes).
   */
  @Config(NETTY_SERVER_MAX_INITIAL_LINE_LENGTH)
  @Default("4096")
  public final int nettyServerMaxInitialLineLength;

  /**
   * The maximum size of a header in a request (in bytes).
   */
  @Config(NETTY_SERVER_MAX_HEADER_SIZE)
  @Default("8192")
  public final int nettyServerMaxHeaderSize;

  /**
   * The maximum size of a chunk that is prepared for processing (in bytes).
   */
  @Config(NETTY_SERVER_MAX_CHUNK_SIZE)
  @Default("8192")
  public final int nettyServerMaxChunkSize;

  /**
   * The threshold of the size of buffered data at which reading from a client channel will be suspended. If the size
   * drops below the threshold, reading will be resumed. This value is respected on a per-request basis.
   * Note that the actual amount of data buffered may be >= this number.
   * If this is <=0, it is assumed that there is no limit on the size of buffered data.
   */
  @Config(NETTY_SERVER_REQUEST_BUFFER_WATERMARK)
  @Default("32 * 1024 * 1024")
  public final int nettyServerRequestBufferWatermark;

  /**
   * A comma separated list of query parameters that should not be honored when forwarded to the
   * {@link RestRequestService} layer.
   */
  @Config(NETTY_SERVER_BLACKLISTED_QUERY_PARAMS)
  @Default("")
  public final Set<String> nettyBlacklistedQueryParams;

  /**
   * The threshold (in bytes) for POSTs via multipart/form-data.
   * <p/>
   * The current netty implementation cannot stream POSTs that come as multipart/form-data. It is useful to set this to
   * reasonable number to ensure that memory usage is kept in check (i.e. protect against large blob uploads via
   * multipart/form-data).
   */
  @Config(NETTY_MULTIPART_POST_MAX_SIZE_BYTES)
  @Default("20 * 1024 * 1024")
  public final long nettyMultipartPostMaxSizeBytes;

  /**
   * If set, use this implementation of {@link com.github.ambry.commons.SSLFactory} to use for the netty HTTP server.
   * Otherwise, share the factory instance with the router.
   */
  @Config(SSL_FACTORY_KEY)
  @Default("")
  public final String nettyServerSslFactory;

  /**
   * The interval to update netty metrics in the collecting thread.
   */
  @Config(NETTY_METRICS_REFRESH_INTERVAL_SECONDS)
  @Default("30")
  public final int nettyMetricsRefreshIntervalSeconds;

  /**
   * The duration to wait for netty metrics collector to stop before forcelly shutting it down.
   */
  @Config(NETTY_METRICS_STOP_WAIT_TIMEOUT_SECONDS)
  @Default("1")
  public final int nettyMetricsStopWaitTimeoutSeconds;

  public NettyConfig(VerifiableProperties verifiableProperties) {
    nettyServerBossThreadCount = verifiableProperties.getInt(NETTY_SERVER_BOSS_THREAD_COUNT, 1);
    nettyServerIdleTimeSeconds = verifiableProperties.getInt(NETTY_SERVER_IDLE_TIME_SECONDS, 60);
    nettyServerPort = verifiableProperties.getInt(NETTY_SERVER_PORT, 1174);
    nettyServerSSLPort = verifiableProperties.getInt(NETTY_SERVER_SSL_PORT, 1175);
    nettyServerEnableSSL = verifiableProperties.getBoolean(NETTY_SERVER_ENABLE_SSL, false);
    nettyServerSoBacklog = verifiableProperties.getInt(NETTY_SERVER_SO_BACKLOG, 100);
    nettyServerWorkerThreadCount = verifiableProperties.getInt(NETTY_SERVER_WORKER_THREAD_COUNT, 1);
    nettyServerMaxInitialLineLength = verifiableProperties.getInt(NETTY_SERVER_MAX_INITIAL_LINE_LENGTH, 4096);
    nettyServerMaxHeaderSize = verifiableProperties.getInt(NETTY_SERVER_MAX_HEADER_SIZE, 8192);
    nettyServerMaxChunkSize = verifiableProperties.getInt(NETTY_SERVER_MAX_CHUNK_SIZE, 8192);
    nettyServerRequestBufferWatermark =
        verifiableProperties.getIntInRange(NETTY_SERVER_REQUEST_BUFFER_WATERMARK, 32 * 1024 * 1024, 1,
            Integer.MAX_VALUE);
    nettyBlacklistedQueryParams = new HashSet<>(
        Arrays.asList(verifiableProperties.getString(NETTY_SERVER_BLACKLISTED_QUERY_PARAMS, "").split(",")));
    nettyMultipartPostMaxSizeBytes =
        verifiableProperties.getLongInRange(NETTY_MULTIPART_POST_MAX_SIZE_BYTES, 20 * 1024 * 1024, 0, Long.MAX_VALUE);
    nettyServerSslFactory = verifiableProperties.getString(SSL_FACTORY_KEY, "");
    nettyMetricsRefreshIntervalSeconds =
        verifiableProperties.getIntInRange(NETTY_METRICS_REFRESH_INTERVAL_SECONDS, 30, 0, Integer.MAX_VALUE);
    nettyMetricsStopWaitTimeoutSeconds =
        verifiableProperties.getIntInRange(NETTY_METRICS_STOP_WAIT_TIMEOUT_SECONDS, 1, 0, Integer.MAX_VALUE);
  }
}
