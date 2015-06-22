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

  protected static String BOSS_THREAD_COUNT_KEY = "netty.server.boss.thread.count";
  protected static String IDLE_TIME_SECONDS_KEY = "netty.server.idle.time.seconds";
  protected static String PORT_KEY = "netty.server.port";
  protected static String SO_BACKLOG_KEY = "netty.server.sobacklog";
  protected static String STARTUP_WAIT_SECONDS = "netty.server.startup.wait.seconds";
  protected static String WORKER_THREAD_COUNT_KEY = "netty.server.worker.thread.count";

  /**
   * Number of netty boss threads.
   */
  @Config("netty.server.boss.thread.count")
  @Default("1")
  private final int bossThreadCount;

  /**
   * The amount of time a channel is allowed to be idle before it's closed. 0 to disable.
   */
  @Config("netty.server.idle.time.seconds")
  @Default("60")
  private final int idleTimeSeconds;

  /**
   * Port on which to run netty server.
   */
  @Config("netty.server.port")
  @Default("8088")
  private final int port;

  /**
   * Socket backlog size. Defines the number of connections that can wait in queue to be accepted.
   */
  @Config("netty.server.sobacklog")
  @Default("100")
  private final int soBacklog;

  /**
   * Startup wait time (in seconds). If the netty server does not start up within this time, the startup is considered
   * failed.
   */
  @Config("netty.server.startup.wait.seconds")
  @Default("30")
  private final long startupWaitSeconds;

  /**
   * Number of netty worker threads.
   */
  @Config("netty.server.worker.thread.count")
  @Default("1")
  private final int workerThreadCount;

  public int getBossThreadCount() {
    return bossThreadCount;
  }

  public int getIdleTimeSeconds() {
    return idleTimeSeconds;
  }

  public int getPort() {
    return port;
  }

  public int getSoBacklog() {
    return soBacklog;
  }

  public long getStartupWaitSeconds() {
    return startupWaitSeconds;
  }

  public int getWorkerThreadCount() {
    return workerThreadCount;
  }

  public NettyConfig(VerifiableProperties verifiableProperties) {
    bossThreadCount = verifiableProperties.getInt(BOSS_THREAD_COUNT_KEY, 1);
    idleTimeSeconds = verifiableProperties.getInt(IDLE_TIME_SECONDS_KEY, 60);
    port = verifiableProperties.getInt(PORT_KEY, 8088);
    soBacklog = verifiableProperties.getInt(SO_BACKLOG_KEY, 100);
    startupWaitSeconds = verifiableProperties.getLong(STARTUP_WAIT_SECONDS, 30);
    workerThreadCount = verifiableProperties.getInt(WORKER_THREAD_COUNT_KEY, 1);
  }
}
