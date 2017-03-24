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

package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The stats manager is responsible for periodic aggregation of node level stats and expose/publish such stats to
 * potential consumers.
 */
public class StatsManager {
  private static final Logger logger = LoggerFactory.getLogger(StatsManager.class);

  private final StorageManager storageManager;
  private final File statsOutputFile;
  private final ScheduledExecutorService scheduler;
  private final long publishPeriodInSecs;
  private final List<PartitionId> initialPartitionIds;
  private final StatsManagerMetrics metrics;
  private final Time time;
  private StatsAggregator statsAggregator = null;

  /**
   * Constructs a {@link StatsManager}.
   * @param storageManager the {@link StorageManager} to be used to fetch the {@link Store}s.
   * @param config the {@link StatsManagerConfig} to be used to configure the output file path and publish period.
   * @throws IOException
   */
  public StatsManager(StorageManager storageManager, List<PartitionId> partitionIds, MetricRegistry registry,
      StatsManagerConfig config, Time time) throws IOException {
    this.storageManager = storageManager;
    initialPartitionIds = partitionIds;
    statsOutputFile = new File(config.outputFilePath);
    publishPeriodInSecs = config.publishPeriodInSecs;
    metrics = new StatsManagerMetrics(registry);
    this.time = time;
    scheduler = Utils.newScheduler(1, false);
  }

  /**
   * Start the stats manager by scheduling the periodic task that collect, aggregate and publish stats.
   */
  public void start() {
    // random initial delay between 1 to 10 minutes to offset nodes from collecting stats at the same time
    statsAggregator = new StatsAggregator();
    scheduler.scheduleAtFixedRate(statsAggregator, 0, publishPeriodInSecs, TimeUnit.SECONDS);
  }

  /**
   * Stops the periodic task that is collecting, aggregating and publishing stats.
   */
  public void shutdown() throws InterruptedException {
    if (statsAggregator != null) {
      statsAggregator.cancel();
    }
    scheduler.shutdown();
    if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
      logger.error("Could not terminate aggregator tasks after StatsManager shutdown");
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
      OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
      DatumWriter<StatsWrapper> statsWrapperDatumWriter = new SpecificDatumWriter<StatsWrapper>(StatsWrapper.class);
      JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(StatsWrapper.getClassSchema(), outputStream, true);
      try {
        statsWrapperDatumWriter.write(statsWrapper, jsonEncoder);
        jsonEncoder.flush();
        outputStream.flush();
      } finally {
        outputStream.close();
      }
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
   * @return true if stats were successfully fetched from the given {@link PartitionId}, false otherwise
   */
  boolean collectAndAggregate(StatsSnapshot aggregatedSnapshot, PartitionId partitionId) {
    long fetchAndAggregatePerStoreStartTimeMs = time.milliseconds();
    Store store = storageManager.getStore(partitionId);
    if (store == null) {
      return false;
    } else {
      StatsSnapshot statsSnapshot = null;
      try {
        statsSnapshot = store.getStoreStats().getStatsSnapshot();
      } catch (StoreException e) {
        logger.error("Store exception thrown when getting stats for partitionId: {}", partitionId, e);
        return false;
      }
      aggregate(aggregatedSnapshot, statsSnapshot);
      metrics.fetchAndAggregateTimePerStore.update(time.milliseconds() - fetchAndAggregatePerStoreStartTimeMs);
      return true;
    }
  }

  /**
   * Performs recursive aggregation of two {@link StatsSnapshot} and stores the result in the first one.
   * @param baseSnapshot one of the addends and where the result will be
   * @param newSnapshot the other addend to be added into the first {@link StatsSnapshot}
   */
  private void aggregate(StatsSnapshot baseSnapshot, StatsSnapshot newSnapshot) {
    baseSnapshot.setValue(baseSnapshot.getValue() + newSnapshot.getValue());
    if (baseSnapshot.getSubtree() == null) {
      baseSnapshot.setSubtree(newSnapshot.getSubtree());
    } else if (newSnapshot.getSubtree() != null) {
      for (Map.Entry<String, StatsSnapshot> entry : newSnapshot.getSubtree().entrySet()) {
        if (!baseSnapshot.getSubtree().containsKey(entry.getKey())) {
          baseSnapshot.getSubtree().put(entry.getKey(), new StatsSnapshot(0L, null));
        }
        aggregate(baseSnapshot.getSubtree().get(entry.getKey()), entry.getValue());
      }
    }
  }

  /**
   * Runnable class that collects, aggregate and publish stats via methods in StatsManager.
   */
  private class StatsAggregator implements Runnable {
    private boolean cancelled = false;

    @Override
    public void run() {
      long totalFetchAndAggregateStartTimeMs = time.milliseconds();
      StatsSnapshot aggregatedSnapshot = new StatsSnapshot(0L, null);
      List<String> unreachableStores = new ArrayList<>();
      Iterator<PartitionId> iterator = initialPartitionIds.iterator();
      while (!cancelled && iterator.hasNext()) {
        PartitionId partitionId = iterator.next();
        if (!collectAndAggregate(aggregatedSnapshot, partitionId)) {
          unreachableStores.add(partitionId.toString());
        }
      }
      if (!cancelled) {
        metrics.totalFetchAndAggregateTime.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
        StatsHeader statsHeader =
            new StatsHeader(Description.QUOTA, SystemTime.getInstance().milliseconds(), initialPartitionIds.size(),
                initialPartitionIds.size() - unreachableStores.size(), unreachableStores);
        try {
          publish(new StatsWrapper(statsHeader, aggregatedSnapshot));
          logger.info("Stats snapshot published to {}", statsOutputFile.getAbsolutePath());
        } catch (IOException e) {
          metrics.statsPublishFailureCount.inc();
          logger.error("IOException when publishing stats to {}", statsOutputFile.getAbsolutePath(), e);
        }
        logger.error("{}", metrics.totalFetchAndAggregateTime.getSnapshot().getMean());
      }
    }

    void cancel() {
      cancelled = true;
    }
  }
}
