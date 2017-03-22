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

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The stats manager is responsible for periodic node level aggregation of store stats and expose/publish such stats to
 * potential consumers.
 */
public class StatsManager {
  private static final Logger logger = LoggerFactory.getLogger(StatsManager.class);

  private final StorageManager storageManager;
  private final File statsOutputFile;
  private final ScheduledExecutorService scheduler;
  private final long publishPeriodInSecs;
  private ScheduledFuture<?> statsAggregator = null;

  /**
   * Constructs a {@link StatsManager}.
   * @param storageManager the {@link StorageManager} to be used to fetch the {@link Store}s.
   * @param config the {@link StatsManagerConfig} to be used to configure the output file path and publish period.
   * @throws IOException
   */
  public StatsManager(StorageManager storageManager, StatsManagerConfig config) throws IOException {
    this.storageManager = storageManager;
    statsOutputFile = new File(config.outputFilePath);
    publishPeriodInSecs = config.publishPeriodInSecs;
    scheduler = Utils.newScheduler(1, false);
  }

  /**
   * Start the stats manager by scheduling the periodic task that collect, aggregate and publish stats.
   */
  public void start() {
    // random initial delay between 1 to 10 minutes to offset nodes from collecting stats at the same time
    statsAggregator =
        scheduler.scheduleAtFixedRate(new StatsAggregator(), new Random().nextInt(540) + 60, publishPeriodInSecs,
            TimeUnit.SECONDS);
  }

  /**
   * Stops the periodic task that is collecting, aggregating and publishing stats.
   */
  public void shutdown() throws InterruptedException {
    if (statsAggregator != null) {
      statsAggregator.cancel(true);
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
      throw new IOException("Temporary file creation failed when publishing stats");
    }
  }

  /**
   * Collect and aggregate quota stats from all given {@link Store}s by fetching stats from them sequentially via the
   * {@link StorageManager}.
   * @param partitionIds a {@link Set} of {@link PartitionId}s representing a set of {@link Store}s to be fetched
   * @return a {@link Pair} where the first element is the result in the form of a {@link StatsSnapshot} and the
   * second element is the number of {@link Store}s that were skipped (either unreachable or an error has occurred).
   */
  Pair<StatsSnapshot, List<String>> collectAndAggregate(Set<PartitionId> partitionIds) {
    StatsSnapshot aggregatedStatsSnapshot = new StatsSnapshot(0L, null);
    List<String> unreachableStores = new ArrayList<>();
    for (PartitionId partitionId : partitionIds) {
      Store store = storageManager.getStore(partitionId);
      if (store != null) {
        try {
          StatsSnapshot statsSnapshot = store.getStoreStats().getStatsSnapshot();
          aggregate(aggregatedStatsSnapshot, statsSnapshot);
        } catch (StoreException e) {
          logger.error("Store exception thrown when getting stats for partitionId: " + partitionId.toString(), e);
          unreachableStores.add(partitionId.toString());
        }
      } else {
        unreachableStores.add(partitionId.toString());
      }
    }
    return new Pair<>(aggregatedStatsSnapshot, unreachableStores);
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

    @Override
    public void run() {
      Set<PartitionId> partitionIds = storageManager.getPartitionIds();
      Pair<StatsSnapshot, List<String>> result = collectAndAggregate(partitionIds);
      List<String> unreachableStores = result.getSecond();
      StatsHeader statsHeader =
          new StatsHeader(Description.QUOTA, SystemTime.getInstance().milliseconds(), partitionIds.size(),
              partitionIds.size() - unreachableStores.size(), unreachableStores);
      try {
        publish(new StatsWrapper(statsHeader, result.getFirst()));
        logger.info("Stats snapshot published to " + statsOutputFile.getAbsolutePath());
      } catch (IOException e) {
        logger.error("IOException when publishing stats to " + statsOutputFile.getAbsolutePath(), e);
      }
    }
  }
}
