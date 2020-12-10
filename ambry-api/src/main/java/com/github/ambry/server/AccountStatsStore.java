/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server;

import java.util.Map;


/**
 * The interface that stores and fetches account stats, aggregated account stats.
 * TODO: Add methods to support partition class stats.
 */
public interface AccountStatsStore {
  /**
   * Store stats in the {@link StatsWrapper}. The StatsWrapper's {@link StatsSnapshot} should include partition, account
   * and container. This method will be used for individual ambry server to store local stats.
   * @param statsWrapper The {@link StatsWrapper} that contains stats and other metadata.
   */
  void storeStats(StatsWrapper statsWrapper);

  /**
   * Store aggregated stats in the {@link StatsSnapshot}. The StatsSnapshot should include account and container. This
   * method will be used for aggregation task to store aggregated stats.
   * @param snapshot The {@link StatsSnapshot} that contains aggregated container usage.
   * @throws Exception
   */
  void storeAggregatedStats(StatsSnapshot snapshot) throws Exception;

  /**
   * Return individual ambry server's stats for the given {@code clusterName} and {@code hostname}. This is the stats
   * stored by method {@link #storeStats}.
   * @param clusterName The clusterName.
   * @param hostname The hostname.
   * @return {@link StatsWrapper} of given {@code hostname} in the {@code clusterName}.
   * @throws Exception
   */
  StatsWrapper queryStatsOf(String clusterName, String hostname) throws Exception;

  /**
   * Return the aggregated stats for the given {@code clusterName}. This is the stats stored by method {@link #storeAggregatedStats}.
   * Since the {@link StatsSnapshot} passed to {@link #storeAggregatedStats} only have account and container stats, this
   * method returns a map back to the caller. The key of the outer map is the account id in string format, and the key of
   * the inner map is the container id in string format, the value of the inner map is the storage usage for this container.
   * <pre>
   *   {
   *     "1001": {
   *       "1": 10456,
   *       "2": 75637292
   *     },
   *     "1002": {
   *       "8": 1785385436
   *     }
   *   }
   * </pre>
   * @param clusterName The clusterName.
   * @return The map that represents the container storage usage.
   * @throws Exception
   */
  Map<String, Map<String, Long>> queryAggregatedStats(String clusterName) throws Exception;

  /**
   * Return the monthly aggregated stats. This method returns a map in the same format as the {@link #queryAggregatedStats}.
   * The only difference these two methods have is that this method's returned value only changes in the beginning of each
   * month. For every new month(in local zone offset), an aggregated stats will be written to storage and a snapshot will
   * be created. This method will return current snapshot. This method doesn't require a month value to fetch the snapshot
   * as this new snapshot will be override the old ones.
   * @param clusterName The clusterName.
   * @return The map thtat represents the container storage usage.
   * @throws Exception
   */
  Map<String, Map<String, Long>> queryMonthlyAggregatedStats(String clusterName) throws Exception;

  /**
   * Return the month value of the current container storage snapshot.
   * @param clusterName The clusterName.
   * @return The month value for current snapshot, like "2020-01"
   * @throws Exception
   */
  String queryRecordedMonth(String clusterName) throws Exception;

  /**
   * Taking a snapshot of current aggregated stats and update the month value.
   * @param clusterName The clusterName.
   * @param monthValue The month in string format, like "2020-01".
   * @throws Exception
   */
  void takeSnapshotOfAggregatedStatsAndUpdateMonth(String clusterName, String monthValue) throws Exception;
}
