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
package com.github.ambry.accountstats;

import com.github.ambry.server.HostAccountStorageStatsWrapper;
import com.github.ambry.server.HostPartitionClassStorageStatsWrapper;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.AggregatedPartitionClassStorageStats;
import com.github.ambry.server.storagestats.HostAccountStorageStats;
import java.util.Map;
import java.util.Set;


/**
 * The interface that stores and fetches account stats, aggregated account stats.
 */
public interface AccountStatsStore {
  /**
   * Store stats in the {@link HostAccountStorageStatsWrapper}. This method will be used for individual ambry server to store local stats.
   * @param statsWrapper The {@link HostAccountStorageStatsWrapper} that contains stats and other metadata.
   */
  void storeHostAccountStorageStats(HostAccountStorageStatsWrapper statsWrapper) throws Exception;

  /**
   * Delete {@link HostAccountStorageStats} for the given host.
   * @param hostname The hostname
   * @param port The port
   * @throws Exception
   */
  void deleteHostAccountStorageStatsForHost(String hostname, int port) throws Exception;

  /**
   * Store aggregated storage stats in the {@link AggregatedAccountStorageStats}. This method will be used for aggregation
   * task to store aggregated stats.
   * @param stats The {@link AggregatedAccountStorageStats} that contains aggregated container usage.
   * @throws Exception
   */
  void storeAggregatedAccountStorageStats(AggregatedAccountStorageStats stats) throws Exception;

  /**
   * Delete aggregated account stats for the given {@code accountId} and {@code containerId}.
   * @param accountId The account id
   * @param containerId The container id
   * @throws Exception
   */
  void deleteAggregatedAccountStatsForContainer(short accountId, short containerId) throws Exception;

  /**
   * Return individual ambry server's storage stats for the given {@code hostname}. This is the stats stored by method
   * {@link #storeHostAccountStorageStats}.
   * @param hostname The hostname.
   * @param port The port number.
   * @return {@link HostAccountStorageStatsWrapper} of given {@code hostname}.
   * @throws Exception
   */
  HostAccountStorageStatsWrapper queryHostAccountStorageStatsByHost(String hostname, int port) throws Exception;

  /**
   * Returns the aggregated account storage stats in {@link AggregatedAccountStorageStats}.
   * @return An {@link AggregatedAccountStorageStats} represents the aggregated account stats.
   * @throws Exception
   */
  AggregatedAccountStorageStats queryAggregatedAccountStorageStats() throws Exception;

  /**
   * Same as {@link #queryAggregatedAccountStorageStats()}. The only difference is that it takes {@code clusterName} as
   * the first parameter and return the aggregated account storage stats for the given {@code clusterName}.
   * @param clusterName The ambry cluster name.
   * @return An {@link AggregatedAccountStorageStats} represents the aggregated account stats.
   * @throws Exception
   */
  AggregatedAccountStorageStats queryAggregatedAccountStorageStatsByClusterName(String clusterName) throws Exception;

  /**
   * Return the monthly aggregated storage stats. The only difference between this method and {@link #queryAggregatedAccountStorageStats()}
   * is that this method's returned value only changes in the beginning of each month. For every new month(in local zone
   * offset), an aggregated storage stats will be written to storage and a snapshot will be created. This method will
   * return current snapshot. This method doesn't require a month value to fetch the snapshot as this new snapshot will
   * be override the old ones.
   * @return The {@link AggregatedAccountStorageStats}that represents the monthly aggregated storage stats
   * @throws Exception
   */
  AggregatedAccountStorageStats queryMonthlyAggregatedAccountStorageStats() throws Exception;

  /**
   * Return the month value of the current container storage snapshot.
   * @return The month value for current snapshot, like "2020-01". Empty string will be returned if there is no record.
   * @throws Exception
   */
  String queryRecordedMonth() throws Exception;

  /**
   * Taking a snapshot of current aggregated stats and update the month value.
   * @param monthValue The month in string format, like "2020-01".
   * @throws Exception
   */
  void takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(String monthValue) throws Exception;

  /**
   * Delete the snapshot of latest aggregated account stats. This is usually used to prepare for the new snapshot.
   * @throws Exception
   */
  void deleteSnapshotOfAggregatedAccountStats() throws Exception;

  /**
   * Return all the partition class names and their corresponding partition ids in a set.
   * @return A map whose key is the partition class name and the value is a set of a partition ids.
   * @throws Exception
   */
  Map<String, Set<Integer>> queryPartitionNameAndIds() throws Exception;

  /**
   * Store host partition class storage stats in {@link HostPartitionClassStorageStatsWrapper}. This is the same data as
   * account storage stats and we store account stats with method {@link #storeHostAccountStorageStats(HostAccountStorageStatsWrapper)}
   * much more often then partition class storage stats, in this method, we don't have to store the same container storage usage data again.
   * @param statsWrapper
   * @throws Exception
   */
  void storeHostPartitionClassStorageStats(HostPartitionClassStorageStatsWrapper statsWrapper) throws Exception;

  /**
   * Return the per host partition class stats for given {@code hostname}. The {@code partitionNameAndIds} are the return
   * value from {@link #queryPartitionNameAndIds()}.
   * @param hostname The hostname
   * @param port The port number
   * @param partitionNameAndIds the return value of {@link #queryPartitionNameAndIds()}.
   * @return The {@link HostPartitionClassStorageStatsWrapper}.
   * @throws Exception
   */
  HostPartitionClassStorageStatsWrapper queryHostPartitionClassStorageStatsByHost(String hostname, int port,
      Map<String, Set<Integer>> partitionNameAndIds) throws Exception;

  /**
   * Store the aggregated partition class storage stats in the {@link AggregatedPartitionClassStorageStats}.
   * @param aggregatedPartitionClassStorageStats The {@link AggregatedPartitionClassStorageStats} that contains aggregated
   *                                             partition class container storage stats.
   * @throws Exception
   */
  void storeAggregatedPartitionClassStorageStats(
      AggregatedPartitionClassStorageStats aggregatedPartitionClassStorageStats) throws Exception;

  /**
   * Delete aggregated partition class stats for the given {@code partitionClassName} and the {@code accountId} and {@code containerId}.
   * @param partitionClassName The partition class name.
   * @param accountId The account id
   * @param containerId The container id
   * @throws Exception
   */
  void deleteAggregatedPartitionClassStatsForAccountContainer(String partitionClassName, short accountId,
      short containerId) throws Exception;

  AggregatedPartitionClassStorageStats queryAggregatedPartitionClassStorageStats() throws Exception;

  AggregatedPartitionClassStorageStats queryAggregatedPartitionClassStorageStatsByClusterName(String clusterName)
      throws Exception;

  /**
   * Close all underlying connections and release all resources.
   */
  void shutdown();
}
