/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsMySqlStore;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreStats;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;
import static com.github.ambry.store.StoreStats.*;
import static com.github.ambry.utils.Utils.*;


/**
 * The stats manager is responsible for periodic aggregation of node level stats and expose/publish such stats to
 * potential consumers.
 */
class StatsManager {
  private static final Logger logger = LoggerFactory.getLogger(StatsManager.class);

  private final StorageManager storageManager;
  private final File statsOutputFile;
  private final StatsManagerMetrics metrics;
  private final Time time;
  private final ObjectMapper mapper = new ObjectMapper();
  private final AccountStatsMySqlStore accountStatsMySqlStore;
  private final List<Short> healthReportExcludeAccountIds;
  private final List<Short> publishExcludeAccountIds;
  private ScheduledExecutorService scheduler = null;
  private AccountStatsPublisher accountsStatsPublisher = null;
  private PartitionClassStatsPublisher partitionClassStatsPublisher = null;
  private final StatsManagerConfig config;
  private long expiredDeleteTombstoneCount = 0;
  private long expiredDeleteTombstoneTotalSize = 0;
  private long permanentDeleteTombstoneCount = 0;
  private long permanentDeleteTombstoneTotalSize = 0;
  private AtomicReference<AggregatedDeleteTombstoneStats> aggregatedDeleteTombstoneStats =
      new AtomicReference<>(new AggregatedDeleteTombstoneStats());
  final ConcurrentMap<PartitionId, ReplicaId> partitionToReplicaMap;

  /**
   * Constructs a {@link StatsManager}.
   * @param storageManager the {@link StorageManager} to be used to fetch the {@link Store}s
   * @param replicaIds a {@link List} of {@link ReplicaId}s that are going to be fetched
   * @param registry the {@link MetricRegistry} to be used for {@link StatsManagerMetrics}
   * @param config the {@link StatsManagerConfig} to be used to configure the output file path and publish period
   * @param time the {@link Time} instance to be used for reporting
   * @param clusterParticipant the {@link ClusterParticipant} to register state change listener.
   * @param accountStatsMySqlStore the {@link AccountStatsMySqlStore} to publish stats to mysql database.
   * @param accountService the {@link AccountService} to get account ids from account names.
   */
  StatsManager(StorageManager storageManager, List<? extends ReplicaId> replicaIds, MetricRegistry registry,
      StatsManagerConfig config, Time time, ClusterParticipant clusterParticipant,
      AccountStatsMySqlStore accountStatsMySqlStore, AccountService accountService) {
    this.storageManager = storageManager;
    this.config = config;
    statsOutputFile = new File(config.outputFilePath);
    metrics = new StatsManagerMetrics(registry, aggregatedDeleteTombstoneStats);
    partitionToReplicaMap =
        replicaIds.stream().collect(Collectors.toConcurrentMap(ReplicaId::getPartitionId, Function.identity()));
    this.time = time;
    if (clusterParticipant != null) {
      clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.StatsManagerListener,
          new PartitionStateChangeListenerImpl());
      logger.info("Stats Manager's state change listener registered!");
    }
    this.accountStatsMySqlStore = accountStatsMySqlStore;
    Function<List<String>, List<Short>> convertAccountNamesToIds = names -> names.stream()
        .map(name -> accountService.getAccountByName(name))
        .filter(Objects::nonNull)
        .map(Account::getId)
        .collect(Collectors.toList());
    this.healthReportExcludeAccountIds = convertAccountNamesToIds.apply(config.healthReportExcludeAccountNames);
    this.publishExcludeAccountIds = convertAccountNamesToIds.apply(config.publishExcludeAccountNames);
  }

  /**
   * Start the stats manager by scheduling the periodic task that collect, aggregate and publish stats.
   */
  void start() {
    scheduler = Utils.newScheduler(1, false);
    accountsStatsPublisher = new AccountStatsPublisher(accountStatsMySqlStore);
    int actualDelay = config.initialDelayUpperBoundInSecs > 0 ? ThreadLocalRandom.current()
        .nextInt(config.initialDelayUpperBoundInSecs) : 0;
    logger.info("Scheduling account stats publishing job with an initial delay of {} secs", actualDelay);
    scheduler.scheduleAtFixedRate(accountsStatsPublisher, actualDelay, config.publishPeriodInSecs, TimeUnit.SECONDS);

    if (config.publishPartitionClassReportPeriodInSecs != 0) {
      partitionClassStatsPublisher = new PartitionClassStatsPublisher(accountStatsMySqlStore);
      long initialDelay = ThreadLocalRandom.current().nextLong(config.publishPartitionClassReportPeriodInSecs / 2)
          + config.publishPartitionClassReportPeriodInSecs / 2;
      logger.info("Scheduling partition class stats publishing job with an initial delay of {} secs", initialDelay);
      scheduler.scheduleAtFixedRate(partitionClassStatsPublisher, initialDelay,
          config.publishPartitionClassReportPeriodInSecs, TimeUnit.SECONDS);
    }
  }

  /**
   * Stops the periodic task that is collecting, aggregating and publishing stats.
   */
  void shutdown() {
    if (accountsStatsPublisher != null) {
      accountsStatsPublisher.cancel();
    }
    if (partitionClassStatsPublisher != null) {
      partitionClassStatsPublisher.cancel();
    }
    if (scheduler != null) {
      shutDownExecutorService(scheduler, 30, TimeUnit.SECONDS);
    }
  }

  /**
   * Return the {@link #healthReportExcludeAccountIds}. Only for test.
   * @return
   */
  List<Short> getHealthReportExcludeAccountIds() {
    return Collections.unmodifiableList(healthReportExcludeAccountIds);
  }

  /**
   * Return the {@link #publishExcludeAccountIds}. Only for test.
   * @return
   */
  List<Short> getPublishExcludeAccountIds() {
    return Collections.unmodifiableList(publishExcludeAccountIds);
  }

  /**
   * Publishes stats to a local file in JSON format.
   * @param statsWrapper the {@link StatsWrapper} to be published
   * @throws IOException
   */
  void publish(StatsWrapper statsWrapper) throws IOException {
    File tempFile = new File(statsOutputFile.getAbsolutePath() + ".tmp");
    if (tempFile.createNewFile()) {
      mapper.writerWithDefaultPrettyPrinter().writeValue(tempFile, statsWrapper);
      if (!tempFile.renameTo(statsOutputFile)) {
        throw new IOException(
            "Failed to rename " + tempFile.getAbsolutePath() + " to " + statsOutputFile.getAbsolutePath());
      }
    } else {
      throw new IOException("Temporary file creation failed when publishing stats " + tempFile.getAbsolutePath());
    }
  }

  /**
   * Fetch and aggregate account stats from a given {@link Store}
   * @param aggregatedSnapshot the {@link StatsSnapshot} to hold the aggregated result
   * @param partitionId specifies the {@link Store} to be fetched from
   * @param unreachablePartitions a {@link List} containing partition Ids that were unable to successfully fetch from
   */
  void collectAndAggregateAccountStats(StatsSnapshot aggregatedSnapshot, PartitionId partitionId,
      List<PartitionId> unreachablePartitions) {
    Store store = storageManager.getStore(partitionId, false);
    if (store == null) {
      unreachablePartitions.add(partitionId);
    } else {
      try {
        long fetchAndAggregatePerStoreStartTimeMs = time.milliseconds();
        StoreStats storeStats = store.getStoreStats();
        Map<StatsReportType, StatsSnapshot> snapshotsByType =
            storeStats.getStatsSnapshots(EnumSet.of(StatsReportType.ACCOUNT_REPORT), time.milliseconds(),
                publishExcludeAccountIds);
        aggregatedSnapshot.getSubMap().put(partitionId.toString(), snapshotsByType.get(StatsReportType.ACCOUNT_REPORT));
        metrics.fetchAndAggregateTimePerStoreMs.update(time.milliseconds() - fetchAndAggregatePerStoreStartTimeMs);
        // update delete tombstone stats
        updateDeleteTombstoneStats(storeStats);
      } catch (StoreException e) {
        unreachablePartitions.add(partitionId);
      }
    }
  }

  /**
   * Fetch and aggregate partition class stats from a given {@link Store}
   * @param aggregatedSnapshot the {@link StatsSnapshot} to hold the aggregated result
   * @param partitionId specifies the {@link Store} to be fetched from
   * @param unreachablePartitions a {@link List} containing partition Ids that were unable to successfully fetch from
   */
  void collectAndAggregatePartitionClassStats(StatsSnapshot aggregatedSnapshot, PartitionId partitionId,
      List<PartitionId> unreachablePartitions) {
    Store store = storageManager.getStore(partitionId, false);
    if (store == null) {
      unreachablePartitions.add(partitionId);
    } else {
      try {
        long fetchAndAggregatePerStoreStartTimeMs = time.milliseconds();
        StoreStats storeStats = store.getStoreStats();
        Map<StatsReportType, StatsSnapshot> snapshotsByType =
            storeStats.getStatsSnapshots(EnumSet.of(StatsReportType.PARTITION_CLASS_REPORT), time.milliseconds(),
                publishExcludeAccountIds);
        StatsSnapshot statsSnapshot = snapshotsByType.get(StatsReportType.PARTITION_CLASS_REPORT);
        StatsSnapshot partitionClassSnapshot = aggregatedSnapshot.getSubMap()
            .getOrDefault(partitionId.getPartitionClass(), new StatsSnapshot(0L, new HashMap<>()));
        partitionClassSnapshot.setValue(partitionClassSnapshot.getValue() + statsSnapshot.getValue());
        partitionClassSnapshot.getSubMap().put(partitionId.toString(), statsSnapshot);
        aggregatedSnapshot.getSubMap().put(partitionId.getPartitionClass(), partitionClassSnapshot);
        metrics.fetchAndAggregateTimePerStoreMs.update(time.milliseconds() - fetchAndAggregatePerStoreStartTimeMs);
      } catch (StoreException e) {
        unreachablePartitions.add(partitionId);
      }
    }
  }

  /**
   * Fetch the {@link StatsSnapshot} for the given {@link PartitionId}.
   * @param partitionId the {@link PartitionId} to try to fetch the {@link StatsSnapshot} from
   * @param unreachablePartitions a list of partitionIds to keep track of the unreachable partitions
   * @return the generated {@link StatsSnapshot}
   */
  StatsSnapshot fetchSnapshot(PartitionId partitionId, List<PartitionId> unreachablePartitions,
      StatsReportType reportType) {
    StatsSnapshot statsSnapshot = null;
    Store store = storageManager.getStore(partitionId, false);
    if (store == null) {
      unreachablePartitions.add(partitionId);
    } else {
      try {
        Map<StatsReportType, StatsSnapshot> snapshotsByType = store.getStoreStats()
            .getStatsSnapshots(EnumSet.of(reportType), time.milliseconds(), healthReportExcludeAccountIds);
        statsSnapshot = snapshotsByType.get(reportType);
      } catch (StoreException e) {
        String reportTypeStr = reportType.toString();
        logger.error("StoreException on fetching {} stats snapshot for store {}",
            reportTypeStr.substring(0, reportTypeStr.lastIndexOf('_')), store, e);
        unreachablePartitions.add(partitionId);
      }
    }
    return statsSnapshot;
  }

  /**
   * Add new {@link ReplicaId} to StatsManager and thus stats of this store will be collected during next round of aggregation.
   * @param id the {@link ReplicaId} associated with store to be added
   * @return {@code true} if adding replica was successful. {@code false} if not.
   */
  boolean addReplica(ReplicaId id) {
    boolean success = partitionToReplicaMap.putIfAbsent(id.getPartitionId(), id) == null;
    if (success) {
      logger.info("Partition {} is added into StatsManager", id.getPartitionId());
    } else {
      logger.error("Failed to add partition {} because it is already in StatsManager", id.getPartitionId());
    }
    return success;
  }

  /**
   * Remove {@link ReplicaId} from StatsManager and thus it won't be checked during next round of aggregation.
   * @param id the {@link ReplicaId} associated with store to be removed
   * @return {@code true} if removing replica was successful. {@code false} if the replica doesn't exist.
   */
  boolean removeReplica(ReplicaId id) {
    boolean success = partitionToReplicaMap.remove(id.getPartitionId()) != null;
    if (success) {
      logger.info("Partition {} is removed from StatsManager", id.getPartitionId());
    } else {
      logger.error("Failed to remove partition {} because it doesn't exist in StatsManager", id.getPartitionId());
    }
    return success;
  }

  /**
   * Get the combined {@link StatsSnapshot} of all partitions in this node. This json will contain one entry per partition
   * wrt valid data size. The node level stats format is as follows.
   * <pre>
   *             ACCOUNT_REPORT                   |             PARTITION_CLASS_REPORT
   * ---------------------------------------------------------------------------------------------------
   * {                                            |    {
   *   value: 1000,                               |      value: 1000,
   *   subMap: {                                  |      subMap: {
   *     Partition[1]:{                           |        PartitionClass_1: {
   *       value: 1000,                           |          value: 400,
   *       subMap: {                              |          subMap: {
   *         Account[1]:{                         |            Partition[1]: {
   *           value: 400,                        |              value: 400,
   *           subMap: {                          |              subMap: {
   *             Container[1]:{                   |                Account[1]_Container[1]: {
   *               value: 400,                    |                  value: 400,
   *               subMap: null                   |                  subMap: null
   *             }                                |                }
   *           }                                  |              }
   *         },                                   |            }
   *         Account[2]:{                         |          }
   *           value: 600,                        |        },
   *           subMap: {                          |        PartitionClass_2: {
   *             Container[2]:{                   |          value: 600,
   *               value: 600,                    |          subMap: {
   *               subMap: null                   |            Partition[2]: {
   *             }                                |              value: 600,
   *           }                                  |              subMap: {
   *         }                                    |                Account[2]_Container[2]: {
   *       }                                      |                  value: 600,
   *     }                                        |                  subMap: null
   *   }                                          |                }
   * }                                            |              }
   *                                              |            }
   *                                              |          }
   *                                              |        }
   *                                              |      }
   *                                              |    }
   * </pre>
   * @param statsReportType the {@link StatsReportType} to get from this node
   * @return a combined {@link StatsSnapshot} of this node
   */
  String getNodeStatsInJSON(StatsReportType statsReportType) {
    String statsWrapperJSON = "";
    logger.info("Aggregating node-level stats for Helix report");
    try {
      long totalFetchAndAggregateStartTimeMs = time.milliseconds();
      StatsSnapshot combinedSnapshot = new StatsSnapshot(0L, new HashMap<>());
      long totalValue = 0;
      List<PartitionId> unreachablePartitions = new ArrayList<>();
      Set<PartitionId> partitionsBeforeAggregation = new HashSet<>(partitionToReplicaMap.keySet());
      Iterator<PartitionId> iterator = partitionsBeforeAggregation.iterator();
      while (iterator.hasNext()) {
        PartitionId partitionId = iterator.next();
        long fetchSnapshotStartTimeMs = time.milliseconds();
        StatsSnapshot statsSnapshot = fetchSnapshot(partitionId, unreachablePartitions, statsReportType);
        if (statsSnapshot != null) {
          Map<String, StatsSnapshot> combinedSnapshotSubMap = combinedSnapshot.getSubMap();
          switch (statsReportType) {
            case ACCOUNT_REPORT:
              combinedSnapshotSubMap.put(partitionId.toString(), statsSnapshot);
              break;
            case PARTITION_CLASS_REPORT:
              StatsSnapshot partitionClassSnapshot =
                  combinedSnapshotSubMap.getOrDefault(partitionId.getPartitionClass(),
                      new StatsSnapshot(0L, new HashMap<>()));
              partitionClassSnapshot.setValue(partitionClassSnapshot.getValue() + statsSnapshot.getValue());
              partitionClassSnapshot.getSubMap().put(partitionId.toString(), statsSnapshot);
              combinedSnapshotSubMap.put(partitionId.getPartitionClass(), partitionClassSnapshot);
              break;
            default:
              throw new IllegalArgumentException("Unrecognized stats report type: " + statsReportType);
          }
          totalValue += statsSnapshot.getValue();
        }
        metrics.fetchAndAggregateTimePerStoreMs.update(time.milliseconds() - fetchSnapshotStartTimeMs);
      }
      combinedSnapshot.setValue(totalValue);
      List<String> examinedUnreachableStores = examineUnreachablePartitions(unreachablePartitions);
      metrics.totalFetchAndAggregateTimeMs.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
      StatsHeader statsHeader = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, time.milliseconds(),
          partitionsBeforeAggregation.size(), partitionsBeforeAggregation.size() - unreachablePartitions.size(),
          examinedUnreachableStores);
      statsWrapperJSON = mapper.writeValueAsString(new StatsWrapper(statsHeader, combinedSnapshot));
      logger.info("Node-level stats aggregated for Helix report");
    } catch (Exception | Error e) {
      metrics.statsAggregationFailureCount.inc();
      logger.error("Exception while aggregating stats for Helix report", e);
    }
    return statsWrapperJSON;
  }

  /**
   * Exposed for testing.
   * @return aggregated delete tombstone stats.
   */
  AggregatedDeleteTombstoneStats getAggregatedDeleteTombstoneStats() {
    return aggregatedDeleteTombstoneStats.get();
  }

  /**
   * Update delete tombstone related stats from given {@link StoreStats}
   * @param storeStats the {@link StoreStats} that contains delete tombstone stats of single store.
   */
  private void updateDeleteTombstoneStats(StoreStats storeStats) {
    Map<String, Pair<Long, Long>> storeDeleteStats = storeStats.getDeleteTombstoneStats();
    Pair<Long, Long> expiredDeleteTombstoneStats = storeDeleteStats.get(EXPIRED_DELETE_TOMBSTONE);
    Pair<Long, Long> permanentDeleteTombstoneStats = storeDeleteStats.get(PERMANENT_DELETE_TOMBSTONE);
    expiredDeleteTombstoneCount += expiredDeleteTombstoneStats.getFirst();
    expiredDeleteTombstoneTotalSize += expiredDeleteTombstoneStats.getSecond();
    permanentDeleteTombstoneCount += permanentDeleteTombstoneStats.getFirst();
    permanentDeleteTombstoneTotalSize += permanentDeleteTombstoneStats.getSecond();
  }

  /**
   * Update the aggregated delete tombstone stats by atomic switch.
   */
  void updateAggregatedDeleteTombstoneStats() {
    aggregatedDeleteTombstoneStats.set(
        new AggregatedDeleteTombstoneStats(expiredDeleteTombstoneCount, expiredDeleteTombstoneTotalSize,
            permanentDeleteTombstoneCount, permanentDeleteTombstoneTotalSize));
  }

  /**
   * Reset delete tombstone related stats.
   */
  private void resetDeleteTombstoneStats() {
    expiredDeleteTombstoneCount = 0;
    expiredDeleteTombstoneTotalSize = 0;
    permanentDeleteTombstoneCount = 0;
    permanentDeleteTombstoneTotalSize = 0;
  }

  /**
   * A class to hold aggregated delete tombstone stats result. This is used by {@link StatsManagerMetrics}.
   */
  static class AggregatedDeleteTombstoneStats {
    Pair<Long, Long> aggregatedExpiredDeleteTombstoneStats;
    Pair<Long, Long> aggregatedPermanentDeleteTombstoneStats;

    AggregatedDeleteTombstoneStats() {
      this(0, 0, 0, 0);
    }

    AggregatedDeleteTombstoneStats(long expiredDeleteCount, long expiredDeleteSize, long permanentDeleteCount,
        long permanentDeleteSize) {
      aggregatedExpiredDeleteTombstoneStats = new Pair<>(expiredDeleteCount, expiredDeleteSize);
      aggregatedPermanentDeleteTombstoneStats = new Pair<>(permanentDeleteCount, permanentDeleteSize);
    }

    long getExpiredDeleteTombstoneCount() {
      return aggregatedExpiredDeleteTombstoneStats.getFirst();
    }

    long getExpiredDeleteTombstoneSize() {
      return aggregatedExpiredDeleteTombstoneStats.getSecond();
    }

    long getPermanentDeleteTombstoneCount() {
      return aggregatedPermanentDeleteTombstoneStats.getFirst();
    }

    long getPermanentDeleteTombstoneSize() {
      return aggregatedPermanentDeleteTombstoneStats.getSecond();
    }
  }

  /**
   * Runnable class that collects, publish account stats to mysql database and local backup file.
   */
  class AccountStatsPublisher implements Runnable {
    private volatile boolean cancelled = false;
    private final AccountStatsMySqlStore accountStatsMySqlStore;

    AccountStatsPublisher(AccountStatsMySqlStore accountStatsMySqlStore) {
      this.accountStatsMySqlStore = accountStatsMySqlStore;
    }

    @Override
    public void run() {
      Object lock = accountStatsMySqlStore != null ? accountStatsMySqlStore : this;
      // Lock on the accountStatsMySqlStore object so prevent any other thread to use mysql connection from this
      // object. It's important to know that if the accountStatsMySqlStore is not null, we have to lock on it since
      // MySqlReportAggregatorTask might use this connection at the same time. Since accountStatsMysqlStore has only
      // one mysql connection, so we need to synchronize by locking this object.
      synchronized (lock) {
        logger.info("Aggregating account stats for local report");
        try {
          // Each time we collect account stats from blob stores, we will recalculate the delete tombstone related stats
          // as well. So before starting collecting account stats, let's reset the delete tombstone stats.
          resetDeleteTombstoneStats();
          long totalFetchAndAggregateStartTimeMs = time.milliseconds();
          StatsSnapshot aggregatedSnapshot = new StatsSnapshot(0L, new HashMap<>());
          List<PartitionId> unreachablePartitions = new ArrayList<>();

          // 1. First, collect account stats from each replicas and aggregate the result to aggregatedSnapshot.
          Iterator<PartitionId> iterator = partitionToReplicaMap.keySet().iterator();
          while (!cancelled && iterator.hasNext()) {
            PartitionId partitionId = iterator.next();
            logger.debug("Aggregating account stats for local report started for store {}", partitionId);
            collectAndAggregateAccountStats(aggregatedSnapshot, partitionId, unreachablePartitions);
          }
          aggregatedSnapshot.updateValue();

          // 2. Second, filter out the unreachable partitions that are not in the map.
          List<String> unreachableStores = examineUnreachablePartitions(unreachablePartitions);

          // 3. Update the delete tombstone related stats.
          updateAggregatedDeleteTombstoneStats();
          metrics.initDeleteStatsGaugesIfNeeded();
          if (!cancelled) {
            metrics.totalFetchAndAggregateTimeMs.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
            // 4. Construct a StatsWrapper.
            StatsHeader statsHeader =
                new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, time.milliseconds(),
                    partitionToReplicaMap.keySet().size(),
                    partitionToReplicaMap.keySet().size() - unreachableStores.size(), unreachableStores);
            StatsWrapper statsWrapper = new StatsWrapper(statsHeader, aggregatedSnapshot);
            // 5. Persist this statsWrapper to mysql database if connection exists.
            if (accountStatsMySqlStore != null) {
              accountStatsMySqlStore.storeAccountStats(statsWrapper);
            }
            // 6. Persist this statsWrapper to local backup file.
            publish(statsWrapper);
            logger.info("Local account stats snapshot published to {}", statsOutputFile.getAbsolutePath());
          }
        } catch (Exception e) {
          metrics.statsAggregationFailureCount.inc();
          logger.error("Exception while aggregating account stats for local report. Stats output file path - {}",
              statsOutputFile.getAbsolutePath(), e);
        } finally {
          // Finally, close the mysql database connection.
          if (accountStatsMySqlStore != null) {
            accountStatsMySqlStore.closeConnection();
          }
        }
      }
    }

    void cancel() {
      cancelled = true;
    }
  }

  /**
   * Runnable class that collects, publishes partition class stats to mysql database.
   */
  class PartitionClassStatsPublisher implements Runnable {
    private volatile boolean cancelled = false;
    private final AccountStatsMySqlStore accountStatsMySqlStore;

    PartitionClassStatsPublisher(AccountStatsMySqlStore accountStatsMySqlStore) {
      this.accountStatsMySqlStore = accountStatsMySqlStore;
    }

    @Override
    public void run() {
      // Locking accountStatsMysqlStore object so other thread won't be able to use the mysql connection.
      synchronized (accountStatsMySqlStore) {
        logger.info("Aggregating partition class stats for local report");
        try {
          long totalFetchAndAggregateStartTimeMs = time.milliseconds();
          StatsSnapshot aggregatedSnapshot = new StatsSnapshot(0L, new HashMap<>());
          List<PartitionId> unreachablePartitions = new ArrayList<>();

          // 1. First, collect partition class stats from each replicas and aggregate the result to aggregatedSnapshot.
          Iterator<PartitionId> iterator = partitionToReplicaMap.keySet().iterator();
          while (!cancelled && iterator.hasNext()) {
            PartitionId partitionId = iterator.next();
            logger.debug("Aggregating partition class stats for local report started for store {}", partitionId);
            collectAndAggregatePartitionClassStats(aggregatedSnapshot, partitionId, unreachablePartitions);
          }
          aggregatedSnapshot.updateValue();

          // 2. Second, filter out the unreachable partitions that are not in the map.
          List<String> unreachableStores = examineUnreachablePartitions(unreachablePartitions);
          if (!cancelled) {
            metrics.totalFetchAndAggregateTimeMs.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
            // 3. Construct a StatsWrapper.
            StatsHeader statsHeader =
                new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, time.milliseconds(),
                    partitionToReplicaMap.keySet().size(),
                    partitionToReplicaMap.keySet().size() - unreachableStores.size(), unreachableStores);
            StatsWrapper statsWrapper = new StatsWrapper(statsHeader, aggregatedSnapshot);
            // 4. Persist this statsWrapper to mysql database.
            accountStatsMySqlStore.storePartitionClassStats(statsWrapper);
            logger.info("Local partition class stats snapshot published to {}", statsOutputFile.getAbsolutePath());
          }
        } catch (Exception e) {
          metrics.statsAggregationFailureCount.inc();
          logger.error("Exception while aggregating partition class stats for mysql", e);
        } finally {
          // Finally, close the mysql database connection
          accountStatsMySqlStore.closeConnection();
        }
      }
    }

    void cancel() {
      cancelled = true;
    }
  }

  /**
   * Re-examine the unreachable partitions to preclude those already removed from StatsManager.
   * @param unreachablePartitions a list of unreachable partitions generated during stats aggregation.
   * @return a list of stores which are unreachable and still present in StatsManager.
   */
  List<String> examineUnreachablePartitions(List<PartitionId> unreachablePartitions) {
    List<String> unreachableStores = new ArrayList<>();
    for (PartitionId partition : unreachablePartitions) {
      if (partitionToReplicaMap.containsKey(partition)) {
        unreachableStores.add(partition.toPathString());
      } else {
        logger.info("Removing partition {} from unreachable list because it is no longer in StatsManager",
            partition.toPathString());
      }
    }
    return unreachableStores;
  }

  /**
   * {@link PartitionStateChangeListener} to capture changes in partition state.
   */
  private class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      // check if partition exists
      ReplicaId replica = storageManager.getReplica(partitionName);
      if (replica == null) {
        // no matter this is an existing replica or new added one, it should be present in storage manager because new
        // replica is added into storage manager first.
        throw new StateTransitionException("Replica " + partitionName + " is not found on current node",
            ReplicaNotFound);
      }
      if (!partitionToReplicaMap.containsKey(replica.getPartitionId())) {
        // if replica is not present in partitionToReplicaMap, it means this new replica was just added into storage
        // manager. Here we add it into stats manager accordingly.
        logger.info("Didn't find replica {} in stats manager, starting to add it.", partitionName);
        if (!addReplica(replica)) {
          throw new StateTransitionException("Failed to add new replica into stats manager", ReplicaOperationFailure);
        }
      }
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      logger.info("Partition state change notification from Bootstrap to Standby received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {
      logger.info("Partition state change notification from Standby to Leader received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {
      logger.info("Partition state change notification from Leader to Standby received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {
      logger.info("Partition state change notification from Standby to Inactive received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {
      logger.info("Partition state change notification from Inactive to Offline received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeDroppedFromOffline(String partitionName) {
      // check if partition exists
      ReplicaId replica = storageManager.getReplica(partitionName);
      if (replica == null) {
        throw new StateTransitionException("Replica " + partitionName + " is not found on current node",
            ReplicaNotFound);
      }
      // remove replica from in-mem data structure. If replica doesn't exist, log info but don't fail the transition
      removeReplica(replica);
    }
  }
}
