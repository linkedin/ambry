package com.github.ambry.config;

/**
 * The configs for the server
 */

public class ServerConfig {

  /**
   * The number of request handler threads used by the server to process requests
   */
  @Config("server.request.handler.num.of.threads")
  @Default("7")
  public final int serverRequestHandlerNumOfThreads;

  /**
   * The number of scheduler threads the server will use to perform background tasks
   */
  @Config("server.scheduler.num.of.threads")
  @Default("10")
  public final int serverSchedulerNumOfthreads;

  /**
   * The max time a write from a put request can take before timing out
   */
  @Config("server.max.put.write.time.ms")
  @Default("10000")
  public final int serverMaxPutWriteTimeMs;

  /**
   * The max time a write from a delete request can take before timing out
   */
  @Config("server.max.delete.write.time.ms")
  @Default("5000")
  public final int serverMaxDeleteWriteTimeMs;

  public ServerConfig(VerifiableProperties verifiableProperties) {
    serverRequestHandlerNumOfThreads = verifiableProperties.getInt("server.request.handler.num.of.threads", 7);
    serverSchedulerNumOfthreads = verifiableProperties.getInt("server.scheduler.num.of.threads", 10);
    serverMaxPutWriteTimeMs = verifiableProperties.getInt("server.max.put.write.time.ms", 10000);
    serverMaxDeleteWriteTimeMs = verifiableProperties.getInt("server.max.delete.write.time.ms", 5000);
  }
}
