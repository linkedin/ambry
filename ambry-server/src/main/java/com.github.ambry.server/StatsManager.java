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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.annotate.JsonAutoDetect;
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
  private final List<PartitionId> totalPartitionIds;
  private final StatsManagerMetrics metrics;
  private final Time time;
  private final ObjectMapper mapper = new ObjectMapper();
  private ScheduledExecutorService scheduler = null;
  private StatsAggregator statsAggregator = null;

  /**
   * Constructs a {@link StatsManager}.
   * @param storageManager the {@link StorageManager} to be used to fetch the {@link Store}s
   * @param partitionIds a {@link List} of {@link PartitionId}s that are going to be fetched
   * @param registry the {@link MetricRegistry} to be used for {@link StatsManagerMetrics}
   * @param config the {@link StatsManagerConfig} to be used to configure the output file path and publish period
   * @param time the {@link Time} instance to be used for reporting
   * @throws IOException
   */
  StatsManager(StorageManager storageManager, List<PartitionId> partitionIds, MetricRegistry registry,
      StatsManagerConfig config, Time time) throws IOException {
    this.storageManager = storageManager;
    totalPartitionIds = partitionIds;
    statsOutputFile = new File(config.outputFilePath);
    publishPeriodInSecs = config.publishPeriodInSecs;
    initialDelayInSecs = config.initialDelayUpperBoundInSecs;
    metrics = new StatsManagerMetrics(registry);
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
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
  void shutdown() throws InterruptedException {
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
  void publish(StatsWrapper statsWrapper) throws IOException {
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
  void collectAndAggregate(StatsSnapshot aggregatedSnapshot, PartitionId partitionId, List<String> unreachableStores) {
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
   * Fetch the {@link StatsSnapshot} for the given {@link PartitionId}.
   * @param partitionId the {@link PartitionId} to try to fetch the {@link StatsSnapshot} from
   * @param unreachableStores a list of partitionIds to keep track of the unreachable stores (partitions)
   * @return
   */
  StatsSnapshot fetchSnapshot(PartitionId partitionId, List<String> unreachableStores) {
    StatsSnapshot statsSnapshot = null;
    Store store = storageManager.getStore(partitionId);
    if (store == null) {
      unreachableStores.add(partitionId.toString());
    } else {
      try {
        statsSnapshot = store.getStoreStats().getStatsSnapshot(time.milliseconds());
      } catch (StoreException e) {
        logger.error("StoreException on fetching stats snapshot for store {}", store, e);
        unreachableStores.add(partitionId.toString());
      }
    }
    return statsSnapshot;
  }

  /**
   * Get the combined {@link StatsSnapshot} of all partitions in this node. This json will contain one entry per partition
   * wrt valid data size.
   * @return a combined {@link StatsSnapshot} of this node
   */
  String getNodeStatsInJSON() {
    String statsWrapperJSON = "";
    try {
      long totalFetchAndAggregateStartTimeMs = time.milliseconds();
      StatsSnapshot combinedSnapshot = new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>());
      long totalValue = 0;
      List<String> unreachableStores = new ArrayList<>();
      Iterator<PartitionId> iterator = totalPartitionIds.iterator();
      while (iterator.hasNext()) {
        PartitionId partitionId = iterator.next();
        long fetchSnapshotStartTimeMs = time.milliseconds();
        StatsSnapshot statsSnapshot = fetchSnapshot(partitionId, unreachableStores);
        if (statsSnapshot != null) {
          combinedSnapshot.getSubMap().put(partitionId.toString(), statsSnapshot);
          totalValue += statsSnapshot.getValue();
        }
        metrics.fetchAndAggregateTimePerStoreMs.update(time.milliseconds() - fetchSnapshotStartTimeMs);
      }
      combinedSnapshot.setValue(totalValue);
      metrics.totalFetchAndAggregateTimeMs.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
      StatsHeader statsHeader =
          new StatsHeader(StatsHeader.StatsDescription.QUOTA, time.milliseconds(), totalPartitionIds.size(),
              totalPartitionIds.size() - unreachableStores.size(), unreachableStores);
      statsWrapperJSON = mapper.writeValueAsString(new StatsWrapper(statsHeader, combinedSnapshot));
    } catch (Exception | Error e) {
      metrics.statsAggregationFailureCount.inc();
      logger.error("Exception while aggregating stats.", e);
    }
    return statsWrapperJSON;
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
        Iterator<PartitionId> iterator = totalPartitionIds.iterator();
        while (!cancelled && iterator.hasNext()) {
          PartitionId partitionId = iterator.next();
          logger.info("Aggregating stats started for store {}", partitionId);
          collectAndAggregate(aggregatedSnapshot, partitionId, unreachableStores);
        }
        if (!cancelled) {
          metrics.totalFetchAndAggregateTimeMs.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
          StatsHeader statsHeader =
              new StatsHeader(StatsHeader.StatsDescription.QUOTA, time.milliseconds(), totalPartitionIds.size(),
                  totalPartitionIds.size() - unreachableStores.size(), unreachableStores);
          publish(new StatsWrapper(statsHeader, aggregatedSnapshot));
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
