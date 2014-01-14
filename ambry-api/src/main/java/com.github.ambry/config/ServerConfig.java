package com.github.ambry.config;

/**
 * The configs for the server
 */

public class ServerConfig {

  /**
   * The max size of the index that can reside in memory in bytes for a single store
   */
  @Config("server.request.handler.num.of.threads")
  @Default("7")
  public final int serverRequestHandlerNumOfThreads;

  public ServerConfig(VerifiableProperties verifiableProperties) {
    serverRequestHandlerNumOfThreads = verifiableProperties.getInt("server.request.handler.num.of.threads", 7);
  }
}
