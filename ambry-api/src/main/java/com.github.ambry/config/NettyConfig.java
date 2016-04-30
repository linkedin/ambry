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
    nettyServerWorkerThreadCount = verifiableProperties.getInt("netty.server.worker.thread.count", 1);
  }
}
