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

import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import java.util.Map;
import java.util.Set;


/**
 * The interface that stores and fetches account stats, aggregated account stats.
 */
public interface AccountStatsStore {
  /**
   * Store stats in the {@link StatsWrapper}. The StatsWrapper's {@link StatsSnapshot} should include partition, account
   * and container. This method will be used for individual ambry server to store local stats.
   * @param statsWrapper The {@link StatsWrapper} that contains stats and other metadata.
   */
  void storeAccountStats(StatsWrapper statsWrapper) throws Exception;

  /**
   * Store aggregated stats in the {@link StatsSnapshot}. The StatsSnapshot should include account and container. This
   * method will be used for aggregation task to store aggregated stats.
   * @param snapshot The {@link StatsSnapshot} that contains aggregated container usage.
   * @throws Exception
   */
  void storeAggregatedAccountStats(StatsSnapshot snapshot) throws Exception;

  /**
   * Delete aggregated account stats for the given {@code accountId} and {@code containerId}.
   * @param accountId The account id
   * @param containerId The container id
   * @throws Exception
   */
  void deleteAggregatedAccountStatsForContainer(short accountId, short containerId) throws Exception;

  /**
   * Return individual ambry server's stats for the given {@code hostname}. This is the stats stored by method {@link #storeAccountStats}.
   * @param hostname The hostname.
   * @param port The port number.
   * @return {@link StatsWrapper} of given {@code hostname}.
   * @throws Exception
   */
  StatsWrapper queryAccountStatsByHost(String hostname, int port) throws Exception;

  /**
   * Return the aggregated stats. This is the stats stored by method {@link #storeAggregatedAccountStats}.
   * Since the {@link StatsSnapshot} passed to {@link #storeAggregatedAccountStats} only have account and container stats, this
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
   * @return The map that represents the container storage usage.
   * @throws Exception
   */
  Map<String, Map<String, Long>> queryAggregatedAccountStats() throws Exception;

  /**
   * Returns the aggregated account stats for the given {@code clusterName} in {@link StatsSnapshot}. The returned value
   * is the same data set as {@link #queryAggregatedAccountStats()}, just in different format. It also returns null when
   * the {@code clusterName} doesn't exist.
   * @param clusterName The clusterName.
   * @return A {@link StatsSnapshot} represents the aggregated account stats.
   * @throws Exception
   */
  StatsSnapshot queryAggregatedAccountStatsByClusterName(String clusterName) throws Exception;

  /**
   * Return the monthly aggregated stats. This method returns a map in the same format as the {@link #queryAggregatedAccountStats}.
   * The only difference these two methods have is that this method's returned value only changes in the beginning of each
   * month. For every new month(in local zone offset), an aggregated stats will be written to storage and a snapshot will
   * be created. This method will return current snapshot. This method doesn't require a month value to fetch the snapshot
   * as this new snapshot will be override the old ones.
   * @return The map that represents the container storage usage.
   * @throws Exception
   */
  Map<String, Map<String, Long>> queryMonthlyAggregatedAccountStats() throws Exception;

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
   * Store partition class stats in {@link StatsWrapper}. This stats are per host stats. The stats is first grouped
   * by partition class name. Within each partition class name stats, it's then grouped by partition id. Within each
   * partition id, it's then grouped by the AccountID___ContainerId. It looks like this:
   * <pre>
   *   {
   *     "default-partition-class": {
   *        "Partition[10]": {
   *          "A[1]___C[1]: { "v" : 1000},
   *          "A[1]___C[2]: { "v" : 2000},
   *          "A[5]___C[1]: { "v" : 1000}
   *        },
   *        "Partition[11]": {
   *          "A[1]___C[1]: { "v": 1200 }
   *        }
   *     },
   *     "new-partition-class": {
   *        "Partition[20]": {
   *          "A[1]___C[1]: { "v" : 1000},
   *          "A[1]___C[2]: { "v" : 2000},
   *          "A[5]___C[1]: { "v" : 1000}
   *        },
   *        "Partition[21]": {
   *          "A[1]___C[1]: { "v": 1200 }
   *        }
   *     }
   *   }
   * </pre>
   * This is the same data as account stats and we store account stats with method {@link #storeAccountStats} much more often
   * then partition class stats, in this method, we have have to store the same container storage usage data again.
   * @param statsWrapper
   * @throws Exception
   */
  void storePartitionClassStats(StatsWrapper statsWrapper) throws Exception;

  /**
   * Return the per host partition class stats for given {@code hostname}. The {@code partitionNameAndIds} are the return
   * value from {@link #queryPartitionNameAndIds()}. The returned StatsWrapper is constructed in the same way as it's
   * passed to method {@link #storePartitionClassStats}.
   * @param hostname The hostname
   * @param port The port number
   * @param partitionNameAndIds the return value of {@link #queryPartitionNameAndIds()}.
   * @return A {@link StatsWrapper} represents the per host partition class stats.
   * @throws Exception
   */
  StatsWrapper queryPartitionClassStatsByHost(String hostname, int port, Map<String, Set<Integer>> partitionNameAndIds)
      throws Exception;

  /**
   * Store aggregated partition class stats in the {@link StatsWrapper}. The stats looks a bit different then the per host
   * stats. It's constructed like this:
   * <pre>
   *   {
   *     "default-partition-class": {
   *       "A[1]___C[1]": { "v": 2200},
   *       "A[1]___C[2]": { "v": 2000},
   *       "A[5]___C[2]": { "v": 1000},
   *     },
   *     "new-partition-class": {
   *       "A[1]___C[1]": { "v": 2200},
   *       "A[1]___C[2]": { "v": 2000},
   *       "A[5]___C[2]": { "v": 1000},
   *     }
   *   }
   * </pre>
   * @param statsSnapshot The {@link StatsSnapshot} that contains aggregated partition class container usage stats
   * @throws Exception
   */
  void storeAggregatedPartitionClassStats(StatsSnapshot statsSnapshot) throws Exception;

  /**
   * Delete aggregated partition class stats for the given {@code partitionClassName} and the {@code accountContainerKey}.
   * The {@code accountContainerKey} is the key in the PartitionClass StatsSnapshot. It includes account id and container id.
   * The account id and container id can be extracted from this key.
   * @param partitionClassName The partition class name.
   * @param accountContainerKey The key that contains account id and container id.
   * @throws Exception
   */
  void deleteAggregatedPartitionClassStatsForAccountContainer(String partitionClassName, String accountContainerKey)
      throws Exception;

  /**
   * Return the aggregated partition class stats for given. The returned StatsSnapshot is constructed in the same way
   * as it's passed to method {@link #storeAggregatedPartitionClassStats}.
   * @return A {@link StatsSnapshot} represents the aggregated partition class stats.
   * @throws Exception
   */
  StatsSnapshot queryAggregatedPartitionClassStats() throws Exception;

  /**
   * Same as {@link #queryAggregatedPartitionClassStats()}, the only difference is that it takes {@code clusterName}
   * as the first parameter and return the aggregated partition class stats for the given {@code clusterName}. It returns
   * null if the clusterName doesn't exist.
   * @param clusterName the clusterName
   * @return A {@link StatsSnapshot} represents the aggregated partition class stats.
   * @throws Exception
   */
  StatsSnapshot queryAggregatedPartitionClassStatsByClusterName(String clusterName) throws Exception;

  /**
   * Helper method to close the active connection, if there is one. Connection should be closed when there is no
   * action to be performed for a while.
   */
  void closeConnection();
}
