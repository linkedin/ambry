package com.github.ambry.rest;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Configuration parameters required by Netty.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for Netty and presents them for retrieval through defined APIs.
 */
class NettyConfig {
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
   * Port on which to run netty server.
   */
  @Config("netty.server.port")
  @Default("1174")
  public final int nettyServerPort;

  /**
   * Socket backlog size. Defines the number of connections that can wait in queue to be accepted.
   */
  @Config("netty.server.so.backlog")
  @Default("100")
  public final int nettyServerSoBacklog;

  /**
   * Startup wait time (in seconds). If the netty server does not start up within this time, the startup is considered
   * failed.
   */
  @Config("netty.server.startup.wait.seconds")
  @Default("30")
  public final long nettyServerStartupWaitSeconds;

  /**
   * Number of netty worker threads.
   */
  @Config("netty.server.worker.thread.count")
  @Default("1")
  public final int nettyServerWorkerThreadCount;

  public NettyConfig(VerifiableProperties verifiableProperties) {
    nettyServerBossThreadCount = verifiableProperties.getInt("netty.server.boss.thread.count", 1);
    nettyServerIdleTimeSeconds = verifiableProperties.getInt("netty.server.idle.time.seconds", 60);
    nettyServerPort = verifiableProperties.getInt("netty.server.port", 1174);
    nettyServerSoBacklog = verifiableProperties.getInt("netty.server.sobacklog", 100);
    nettyServerStartupWaitSeconds = verifiableProperties.getLong("netty.server.startup.wait.seconds", 30);
    nettyServerWorkerThreadCount = verifiableProperties.getInt("netty.server.worker.thread.count", 1);
  }
}
