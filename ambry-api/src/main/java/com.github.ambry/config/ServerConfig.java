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

import com.github.ambry.utils.Utils;
import java.util.List;


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
  @Config("server.stats.publish.local.enabled")
  @Default("false")
  public final boolean serverStatsPublishLocalEnabled;

  /**
   * The option to enable or disable publishing stats via Health Reports
   */
  @Config("server.stats.publish.health.report.enabled")
  @Default("false")
  public final boolean serverStatsPublishHealthReportEnabled;

  /**
   * The frequency in mins at which cluster wide quota stats will be aggregated
   */
  @Config("server.quota.stats.aggregate.interval.in.minutes")
  @Default("60")
  public final long serverQuotaStatsAggregateIntervalInMinutes;

  /**
   * The option to enable data prefetch for GET request and don't do zero copy.
   */
  @Config("server.enable.store.data.prefetch")
  @Default("false")
  public final boolean serverEnableStoreDataPrefetch;

  /**
   * Implementation class for StoreKeyConverterFactory
   */
  @Config("server.store.key.converter.factory")
  @Default("com.github.ambry.store.StoreKeyConverterFactoryImpl")
  public final String serverStoreKeyConverterFactory;

  /**
   * Implementation for message transformation.
   */
  @Config("server.message.transformer")
  @Default("com.github.ambry.messageformat.ValidatingTransformer")
  public final String serverMessageTransformer;

  /**
   * The comma separated list of stats reports to publish in Helix.
   */
  @Config("server.stats.reports.to.publish")
  @Default("")
  public final List<String> serverStatsReportsToPublish;

  /**
   * The option to enable or disable validating request based on store state.
   */
  @Config("server.validate.request.based.on.store.state")
  @Default("false")
  public final boolean serverValidateRequestBasedOnStoreState;
  
  /**
   * True to enable ambry server handling undelete requests.
   */
  @Config("server.handle.undelete.request.enabled")
  @Default("false")
  public final boolean serverHandleUndeleteRequestEnabled;

  public ServerConfig(VerifiableProperties verifiableProperties) {
    serverRequestHandlerNumOfThreads = verifiableProperties.getInt("server.request.handler.num.of.threads", 7);
    serverSchedulerNumOfthreads = verifiableProperties.getInt("server.scheduler.num.of.threads", 10);
    serverStatsPublishLocalEnabled = verifiableProperties.getBoolean("server.stats.publish.local.enabled", false);
    serverStatsPublishHealthReportEnabled =
        verifiableProperties.getBoolean("server.stats.publish.health.report.enabled", false);
    serverQuotaStatsAggregateIntervalInMinutes =
        verifiableProperties.getLong("server.quota.stats.aggregate.interval.in.minutes", 60);
    serverEnableStoreDataPrefetch = verifiableProperties.getBoolean("server.enable.store.data.prefetch", false);
    serverStoreKeyConverterFactory = verifiableProperties.getString("server.store.key.converter.factory",
        "com.github.ambry.store.StoreKeyConverterFactoryImpl");
    serverMessageTransformer = verifiableProperties.getString("server.message.transformer",
        "com.github.ambry.messageformat.ValidatingTransformer");
    serverStatsReportsToPublish =
        Utils.splitString(verifiableProperties.getString("server.stats.reports.to.publish", ""), ",");
    serverValidateRequestBasedOnStoreState =
        verifiableProperties.getBoolean("server.validate.request.based.on.store.state", false);
    serverHandleUndeleteRequestEnabled =
        verifiableProperties.getBoolean("server.handle.undelete.request.enabled", false);
  }
}
