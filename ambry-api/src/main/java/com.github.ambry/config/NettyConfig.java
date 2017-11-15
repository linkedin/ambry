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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * Configuration parameters required by Netty.
 */
public class NettyConfig {
  /**
   * Number of netty boss threads.
   */
  @Config("netty.server.boss.thread.count")
  @Default("1")
  public final int nettyServerBossThreadCount;

  /**
   * The amount of time a channel is allowed to be idle before it's closed. 0 to disable.
   */
  @Config("netty.server.idle.time.seconds")
  @Default("60")
  public final int nettyServerIdleTimeSeconds;

  /**
   * Port on which to run netty server for plaintext connections.
   */
  @Config("netty.server.port")
  @Default("1174")
  public final int nettyServerPort;

  /**
   * Port on which to run netty server for SSL connections.
   */
  @Config("netty.server.ssl.port")
  @Default("1175")
  public final int nettyServerSSLPort;

  /**
   * Enable the netty server SSL port.
   */
  @Config("netty.server.enable.ssl")
  @Default("false")
  public final boolean nettyServerEnableSSL;

  /**
   * Socket backlog size. Defines the number of connections that can wait in queue to be accepted.
   */
  @Config("netty.server.so.backlog")
  @Default("100")
  public final int nettyServerSoBacklog;

  /**
   * Number of netty worker threads.
   */
  @Config("netty.server.worker.thread.count")
  @Default("1")
  public final int nettyServerWorkerThreadCount;

  /**
   * The maximum length of the initial line in a request (in bytes).
   */
  @Config("netty.server.max.initial.line.length")
  @Default("4096")
  public final int nettyServerMaxInitialLineLength;

  /**
   * The maximum size of a header in a request (in bytes).
   */
  @Config("netty.server.max.header.size")
  @Default("8192")
  public final int nettyServerMaxHeaderSize;

  /**
   * The maximum size of a chunk that is prepared for processing (in bytes).
   */
  @Config("netty.server.max.chunk.size")
  @Default("8192")
  public final int nettyServerMaxChunkSize;

  /**
   * The threshold of the size of buffered data at which reading from a client channel will be suspended. If the size
   * drops below the threshold, reading will be resumed. This value is respected on a per-request basis.
   * Note that the actual amount of data buffered may be >= this number.
   * If this is <=0, it is assumed that there is no limit on the size of buffered data.
   */
  @Config("netty.server.request.buffer.watermark")
  @Default("32 * 1024 * 1024")
  public final int nettyServerRequestBufferWatermark;

  /**
   * A comma separated list of query parameters that should not be honored when forwarded to the
   * {@link com.github.ambry.rest.BlobStorageService} layer.
   */
  @Config("netty.server.blacklisted.query.params")
  @Default("")
  public final Set<String> nettyBlacklistedQueryParams;

  /**
   * The threshold (in bytes) for POSTs via multipart/form-data.
   * <p/>
   * The current netty implementation cannot stream POSTs that come as multipart/form-data. It is useful to set this to
   * reasonable number to ensure that memory usage is kept in check (i.e. protect against large blob uploads via
   * multipart/form-data).
   */
  @Config("netty.multipart.post.max.size.bytes")
  @Default("20 * 1024 * 1024")
  public final long nettyMultipartPostMaxSizeBytes;

  public NettyConfig(VerifiableProperties verifiableProperties) {
    nettyServerBossThreadCount = verifiableProperties.getInt("netty.server.boss.thread.count", 1);
    nettyServerIdleTimeSeconds = verifiableProperties.getInt("netty.server.idle.time.seconds", 60);
    nettyServerPort = verifiableProperties.getInt("netty.server.port", 1174);
    nettyServerSSLPort = verifiableProperties.getInt("netty.server.ssl.port", 1175);
    nettyServerEnableSSL = verifiableProperties.getBoolean("netty.server.enable.ssl", false);
    nettyServerSoBacklog = verifiableProperties.getInt("netty.server.so.backlog", 100);
    nettyServerWorkerThreadCount = verifiableProperties.getInt("netty.server.worker.thread.count", 1);
    nettyServerMaxInitialLineLength = verifiableProperties.getInt("netty.server.max.initial.line.length", 4096);
    nettyServerMaxHeaderSize = verifiableProperties.getInt("netty.server.max.header.size", 8192);
    nettyServerMaxChunkSize = verifiableProperties.getInt("netty.server.max.chunk.size", 8192);
    nettyServerRequestBufferWatermark =
        verifiableProperties.getIntInRange("netty.server.request.buffer.watermark", 32 * 1024 * 1024, 1,
            Integer.MAX_VALUE);
    nettyBlacklistedQueryParams = new HashSet<>(
        Arrays.asList(verifiableProperties.getString("netty.server.blacklisted.query.params", "").split(",")));
    nettyMultipartPostMaxSizeBytes =
        verifiableProperties.getLongInRange("netty.multipart.post.max.size.bytes", 20 * 1024 * 1024, 0, Long.MAX_VALUE);
  }
}
