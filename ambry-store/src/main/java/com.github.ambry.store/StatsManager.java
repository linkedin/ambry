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
import com.github.ambry.config.StatsConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatsManager {
  private static final Logger logger = LoggerFactory.getLogger(StatsManager.class);

  private final StorageManager storageManager;
  private final File statsOutputFile;
  private final ScheduledExecutorService scheduler;
  private final long publishPeriodInMs;

  /**
   * Constructs a {@link StatsManager}.
   * @param storageManager the {@link StorageManager} to be used to fetch the {@link Store}s.
   * @param config the {@link StatsConfig} to be used to configure the output file path and publish period.
   * @throws IOException
   */
  public StatsManager(StorageManager storageManager, StatsConfig config) throws IOException {
    this.storageManager = storageManager;
    statsOutputFile = new File(config.outputFilePath);
    this.publishPeriodInMs = config.publishPeriodInMs;
    scheduler = Utils.newScheduler(1, false);
  }

  /**
   * Start the stats manager by scheduling the periodic task that collect, aggregate and publish stats.
   */
  public void start() {
    scheduler.scheduleAtFixedRate(new StatsAggregator(), 0, publishPeriodInMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Stops the periodic task that is collecting, aggregating and publishing stats.
   */
  public void shutdown() {
    scheduler.shutdown();
  }

  /**
   * Publishes stats to a local file in JSON format.
   * @param statsWrapper the {@link StatsWrapper} to be published
   * @throws IOException
   */
  void publish(StatsWrapper statsWrapper) throws IOException {
    if (!statsOutputFile.exists()) {
      statsOutputFile.createNewFile();
    }
    OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(statsOutputFile));
    DatumWriter<StatsWrapper> statsWrapperDatumWriter = new SpecificDatumWriter<StatsWrapper>(StatsWrapper.class);
    JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(StatsWrapper.getClassSchema(), outputStream, true);
    try {
      statsWrapperDatumWriter.write(statsWrapper, jsonEncoder);
      jsonEncoder.flush();
      outputStream.flush();
    } finally {
      outputStream.close();
    }
  }

  /**
   * Collect and aggregate quota stats from all {@link Store}s in the node by fetching stats from them sequentially
   * via the {@link StorageManager}.
   * @param partitionIds a {@link Set} of {@link PartitionId}s representing a set of {@link Store}s to be fetched
   * @return a {@link Pair} where the first element is the result in the form of a {@link StatsDirectory} and the
   * second element is the number of {@link Store}s that were skipped (either unreachable or an error has occurred).
   */
  Pair<StatsDirectory, Integer> collectAndAggregate(Set<PartitionId> partitionIds) {
    StatsDirectory aggregatedStatsDirectory = new StatsDirectory(0L, null);
    int storeSkipped = 0;
    for (PartitionId partitionId : partitionIds) {
      Store store = storageManager.getStore(partitionId);
      if (store != null) {
        try {
          StatsDirectory statsDirectory = store.getStoreStats();
          aggregate(aggregatedStatsDirectory, statsDirectory);
        } catch (StoreException e) {
          logger.error("Store exception thrown when getting stats for partitionId: " + partitionId.toString(), e);
          storeSkipped++;
        }
      } else {
        storeSkipped++;
      }
    }
    return new Pair<>(aggregatedStatsDirectory, storeSkipped);
  }

  /**
   * Performs recursive aggregation of two {@link StatsDirectory} and stores the result in the first one.
   * @param baseDirectory one of the addends and where the result will be
   * @param newDirectory the other addend to be added into the first {@link StatsDirectory}
   */
  private void aggregate(StatsDirectory baseDirectory, StatsDirectory newDirectory) {
    baseDirectory.setValue(baseDirectory.getValue() + newDirectory.getValue());
    if (baseDirectory.getSubDirectory() == null) {
      baseDirectory.setSubDirectory(newDirectory.getSubDirectory());
    } else if (newDirectory.getSubDirectory() != null) {
      for (Map.Entry<String, StatsDirectory> entry : newDirectory.getSubDirectory().entrySet()) {
        if (!baseDirectory.getSubDirectory().containsKey(entry.getKey())) {
          baseDirectory.getSubDirectory().put(entry.getKey(), new StatsDirectory(0L, null));
        }
        aggregate(baseDirectory.getSubDirectory().get(entry.getKey()), entry.getValue());
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
      Pair<StatsDirectory, Integer> result = collectAndAggregate(partitionIds);
      StatsHeader statsHeader =
          new StatsHeader(Description.QUOTA, SystemTime.getInstance().milliseconds(), partitionIds.size(),
              partitionIds.size() - result.getSecond());
      try {
        publish(new StatsWrapper(statsHeader, result.getFirst()));
      } catch (IOException e) {
        logger.error("IOException when trying to publish stats to file", e);
      }
    }
  }
}
