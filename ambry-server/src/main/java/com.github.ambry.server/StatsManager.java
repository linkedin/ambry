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
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * The stats manager is responsible for periodic aggregation of node level stats and expose/publish such stats to
 * potential consumers.
 */
class StatsManager {
  private static final Logger logger = LoggerFactory.getLogger(StatsManager.class);

  private final StorageManager storageManager;
  private final File statsOutputFile;
  private final long publishPeriodInSecs;
  private final int initialDelayInSecs;
  private final Map<PartitionId, ReplicaId> partitionToReplicaMap;
  private final StatsManagerMetrics metrics;
  private final Time time;
  private final ObjectMapper mapper = new ObjectMapper();

  private ScheduledExecutorService scheduler = null;
  private StatsAggregator statsAggregator = null;

  /**
   * Constructs a {@link StatsManager}.
   * @param storageManager the {@link StorageManager} to be used to fetch the {@link Store}s
   * @param replicaIds a {@link List} of {@link ReplicaId}s that are going to be fetched
   * @param registry the {@link MetricRegistry} to be used for {@link StatsManagerMetrics}
   * @param config the {@link StatsManagerConfig} to be used to configure the output file path and publish period
   * @param time the {@link Time} instance to be used for reporting
   * @throws IOException
   */
  StatsManager(StorageManager storageManager, List<? extends ReplicaId> replicaIds, MetricRegistry registry,
      StatsManagerConfig config, Time time) throws IOException {
    this.storageManager = storageManager;
    statsOutputFile = new File(config.outputFilePath);
    publishPeriodInSecs = config.publishPeriodInSecs;
    initialDelayInSecs = config.initialDelayUpperBoundInSecs;
    metrics = new StatsManagerMetrics(registry);
    partitionToReplicaMap =
        replicaIds.stream().collect(Collectors.toMap(ReplicaId::getPartitionId, Function.identity()));
    this.time = time;
  }

  /**
   * Start the stats manager by scheduling the periodic task that collect, aggregate and publish stats.
   */
  void start() {
    scheduler = Utils.newScheduler(1, false);
    statsAggregator = new StatsAggregator();
    int actualDelay = initialDelayInSecs > 0 ? ThreadLocalRandom.current().nextInt(initialDelayInSecs) : 0;
    logger.info("Scheduling stats aggregation job with an initial delay of {} secs", actualDelay);
    scheduler.scheduleAtFixedRate(statsAggregator, actualDelay, publishPeriodInSecs, TimeUnit.SECONDS);
  }

  /**
   * Stops the periodic task that is collecting, aggregating and publishing stats.
   */
  void shutdown() {
    if (statsAggregator != null) {
      statsAggregator.cancel();
    }
    if (scheduler != null) {
      shutDownExecutorService(scheduler, 30, TimeUnit.SECONDS);
    }
  }

  /**
   * Publishes stats to a local file in JSON format.
   * @param statsWrapper the {@link StatsWrapper} to be published
   * @throws IOException
   */
  void publishLocally(StatsWrapper statsWrapper) throws IOException {
    File tempFile = new File(statsOutputFile.getAbsolutePath() + ".tmp");
    if (tempFile.createNewFile()) {
      mapper.defaultPrettyPrintingWriter().writeValue(tempFile, statsWrapper);
      if (!tempFile.renameTo(statsOutputFile)) {
        throw new IOException(
            "Failed to rename " + tempFile.getAbsolutePath() + " to " + statsOutputFile.getAbsolutePath());
      }
    } else {
      throw new IOException("Temporary file creation failed when publishing stats " + tempFile.getAbsolutePath());
    }
  }

  /**
   * Fetch and aggregate stats from a given {@link Store}
   * @param aggregatedSnapshot the {@link StatsSnapshot} to hold the aggregated result
   * @param partitionId specifies the {@link Store} to be fetched from
   * @param unreachableStores a {@link List} containing partition Ids that were unable to successfully fetch from
   */
  void collectAndAggregateLocally(StatsSnapshot aggregatedSnapshot, PartitionId partitionId,
      List<String> unreachableStores) {
    Store store = storageManager.getStore(partitionId);
    if (store == null) {
      unreachableStores.add(partitionId.toString());
    } else {
      try {
        long fetchAndAggregatePerStoreStartTimeMs = time.milliseconds();
        StatsSnapshot statsSnapshot = store.getStoreStats().getStatsSnapshot(time.milliseconds());
        StatsSnapshot.aggregate(aggregatedSnapshot, statsSnapshot);
        metrics.fetchAndAggregateTimePerStoreMs.update(time.milliseconds() - fetchAndAggregatePerStoreStartTimeMs);
      } catch (StoreException e) {
        unreachableStores.add(partitionId.toString());
      }
    }
  }

  /**
   * Fetch all types of {@link StatsSnapshot}s for the given {@link PartitionId}.
   * @param partitionId the {@link PartitionId} to try to fetch the {@link StatsSnapshot} from
   * @param unreachableStores a list of partitionIds to keep track of the unreachable stores (partitions)
   * @return a map in which all types of {@link StatsSnapshot}s are associated with corresponding {@link StatsReportType}
   * for the given {@link PartitionId}.
   */
  private Map<StatsReportType, StatsSnapshot> fetchAllSnapshots(PartitionId partitionId, List<String> unreachableStores) {
    Map<StatsReportType, StatsSnapshot> allSnapshots = null;
    Store store = storageManager.getStore(partitionId);
    if (store == null) {
      unreachableStores.add(partitionId.toString());
    } else {
      try {
        allSnapshots = store.getStoreStats().getAllStatsSnapshots(time.milliseconds());
      } catch (StoreException e) {
        logger.error("StoreException on fetching stats snapshots for store {}", store, e);
        unreachableStores.add(partitionId.toString());
      }
    }
    return allSnapshots;
  }

  /**
   * Get all types of combined {@link StatsSnapshot} of all partitions in this node. This json will contain one entry per partition
   * wrt valid data size.
   * @return a combined {@link StatsSnapshot} of this node
   */
  Map<String, String> getNodeStatsInJSON() {
    Map<String, String> allStatsInJSON = new HashMap<>();
    try {
      long totalFetchAndAggregateStartTimeMs = time.milliseconds();
      StatsSnapshot combinedPartitionClassSnapshot = new StatsSnapshot(0L, new HashMap<>());
      StatsSnapshot combinedAccountSnapshot = new StatsSnapshot(0L, new HashMap<>());
      Map<String, StatsSnapshot> combinedSubMapForPartitionClass = new HashMap<>();
      Map<String, StatsSnapshot> combinedSubMapForAccount = new HashMap<>();
      long totalValue = 0;
      List<String> unreachableStores = new ArrayList<>();
      for (PartitionId partitionId : partitionToReplicaMap.keySet()) {
        long fetchSnapshotStartTimeMs = time.milliseconds();
        Map<StatsReportType, StatsSnapshot> allSnapshots = fetchAllSnapshots(partitionId, unreachableStores);
        if (allSnapshots != null) {
          StatsSnapshot containerSnapshot = allSnapshots.get(StatsReportType.PARTITION_CLASS_REPORT);
          StatsSnapshot accountSnapshot = allSnapshots.get(StatsReportType.ACCOUNT_REPORT);
          StatsSnapshot partitionClassSnapshot =
              combinedSubMapForPartitionClass.getOrDefault(partitionId.getPartitionClass(),
                  new StatsSnapshot(0L, new HashMap<>()));
          partitionClassSnapshot.setValue(partitionClassSnapshot.getValue() + containerSnapshot.getValue());
          partitionClassSnapshot.getSubMap().put(partitionId.toString(), containerSnapshot);
          combinedSubMapForPartitionClass.put(partitionId.getPartitionClass(), partitionClassSnapshot);
          combinedSubMapForAccount.put(partitionId.toString(), accountSnapshot);
          totalValue += containerSnapshot.getValue();
        }
        metrics.fetchAndAggregateTimePerStoreMs.update(time.milliseconds() - fetchSnapshotStartTimeMs);
      }
      combinedPartitionClassSnapshot.setValue(totalValue);
      combinedPartitionClassSnapshot.setSubMap(combinedSubMapForPartitionClass);
      combinedAccountSnapshot.setValue(totalValue);
      combinedAccountSnapshot.setSubMap(combinedSubMapForAccount);
      metrics.totalFetchAndAggregateTimeMs.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
      StatsHeader statsHeader = new StatsHeader(StatsHeader.StatsDescription.QUOTA, time.milliseconds(),
          partitionToReplicaMap.keySet().size(), partitionToReplicaMap.keySet().size() - unreachableStores.size(),
          unreachableStores);
      allStatsInJSON.put("PartitionClassStats",
          mapper.writeValueAsString(new StatsWrapper(statsHeader, combinedPartitionClassSnapshot)));
      allStatsInJSON.put("AccountStats",
          mapper.writeValueAsString(new StatsWrapper(statsHeader, combinedAccountSnapshot)));
    } catch (Exception | Error e) {
      metrics.statsAggregationFailureCount.inc();
      logger.error("Exception while aggregating stats.", e);
    }
    return allStatsInJSON;
  }

  /**
   * Runnable class that collects, aggregate and publish stats via methods in StatsManager.
   */
  private class StatsAggregator implements Runnable {
    private volatile boolean cancelled = false;

    @Override
    public void run() {
      logger.info("Aggregating stats");
      try {
        long totalFetchAndAggregateStartTimeMs = time.milliseconds();
        StatsSnapshot aggregatedSnapshot = new StatsSnapshot(0L, null);
        List<String> unreachableStores = new ArrayList<>();
        Iterator<PartitionId> iterator = partitionToReplicaMap.keySet().iterator();
        while (!cancelled && iterator.hasNext()) {
          PartitionId partitionId = iterator.next();
          logger.info("Aggregating stats started for store {}", partitionId);
          collectAndAggregateLocally(aggregatedSnapshot, partitionId, unreachableStores);
        }
        if (!cancelled) {
          metrics.totalFetchAndAggregateTimeMs.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
          StatsHeader statsHeader = new StatsHeader(StatsHeader.StatsDescription.QUOTA, time.milliseconds(),
              partitionToReplicaMap.keySet().size(), partitionToReplicaMap.keySet().size() - unreachableStores.size(),
              unreachableStores);
          publishLocally(new StatsWrapper(statsHeader, aggregatedSnapshot));
          logger.info("Stats snapshot published to {}", statsOutputFile.getAbsolutePath());
        }
      } catch (Exception | Error e) {
        metrics.statsAggregationFailureCount.inc();
        logger.error("Exception while aggregating stats. Stats output file path - {}",
            statsOutputFile.getAbsolutePath(), e);
      }
    }

    void cancel() {
      cancelled = true;
    }
  }
}
