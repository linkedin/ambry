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
   * The number of scheduler threads the server will use to perform background tasks (store, replication)
   */
  @Config("server.scheduler.num.of.threads")
  @Default("10")
  public final int serverSchedulerNumOfthreads;

  /**
   * The option to enable or disable publishing stats locally.
   */
  @Config("server.stats.publish.enabled")
  @Default("false")
  public final boolean serverStatsPublishEnabled;

  /**
   * The option to enable or disable publishing stats via Helix Health Report
   */
  @Config("server.stats.health.report.enabled")
  @Default("false")
  public final boolean serverStatsHealthReportEnabled;

  /**
   * The period in minutes which cluster wide quota stats is aggregated.
   */
  @Config("server.quota.stats.aggregate.interval.in.minutes")
  @Default("60")
  public final long serverQuotaStatsAggregateIntervalInMinutes;

  public ServerConfig(VerifiableProperties verifiableProperties) {
    serverRequestHandlerNumOfThreads = verifiableProperties.getInt("server.request.handler.num.of.threads", 7);
    serverSchedulerNumOfthreads = verifiableProperties.getInt("server.scheduler.num.of.threads", 10);
    serverStatsPublishEnabled = verifiableProperties.getBoolean("server.stats.publish.enabled", false);
    serverStatsHealthReportEnabled = verifiableProperties.getBoolean("server.stats.health.report.enabled", false);
    serverQuotaStatsAggregateIntervalInMinutes =
        verifiableProperties.getLong("server.quota.stats.aggregate.interval.in.minutes", 60);
  }
}
