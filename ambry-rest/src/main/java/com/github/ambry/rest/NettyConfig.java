package com.github.ambry.rest;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Netty specific config
 */
public class NettyConfig extends RestServerConfig {

  public static String BOSS_THREADCOUNT_KEY = "netty.server.boss.threadcount";
  public static String IDLETIME_SECONDS_KEY = "netty.server.idletime.seconds";
  public static String PORT_KEY = "netty.server.port";
  public static String SO_BACKLOG_KEY = "netty.server.sobacklog";
  public static String WORKER_THREADCOUNT_KEY = "netty.server.worker.threadcount";

  /**
   *  Number of netty boss threads
   */
  @Config("bossThreadCount")
  @Default("1")
  private final int bossThreadCount;

  /**
   *  The amount of time a channel is allowed to be idle before its closed. 0 to disable
   */
  @Config("idleTimeSeconds")
  @Default("60")
  private final int idleTimeSeconds;

  /**
   *  Port to run netty server on
   */
  @Config("port")
  @Default("8088")
  private final int port;

  /**
   *  Socket backlog size
   */
  @Config("soBacklog")
  @Default("100")
  private final int soBacklog;

  /**
   *  Number of netty worker threads
   */
  @Config("workerThreadCount")
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

  public int getWorkerThreadCount() {
    return workerThreadCount;
  }

  public NettyConfig(VerifiableProperties verifiableProperties) {
    super(verifiableProperties);

    bossThreadCount = verifiableProperties.getInt(BOSS_THREADCOUNT_KEY, 1);
    port = verifiableProperties.getInt(PORT_KEY, 8088);
    soBacklog = verifiableProperties.getInt(SO_BACKLOG_KEY, 100);
    workerThreadCount = verifiableProperties.getInt(WORKER_THREADCOUNT_KEY, 1);
    idleTimeSeconds = verifiableProperties.getInt(IDLETIME_SECONDS_KEY, 60);
  }
}
