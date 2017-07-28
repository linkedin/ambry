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

package com.github.ambry.clustermap;

import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Responsible for performing cluster wide stats aggregation.
 */
public class HelixClusterAggregator {
  private static final Logger logger = LoggerFactory.getLogger(HelixClusterAggregator.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private final long relevantTimePeriodInMs;

  HelixClusterAggregator(long relevantTimePeriodInMinutes) {
    relevantTimePeriodInMs = TimeUnit.MINUTES.toMillis(relevantTimePeriodInMinutes);
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
  }

  /**
   * Take a {@link Map} of instance name to JSON string representation of {@link StatsWrapper} objects and perform cluster wide
   * aggregation with them.
   * @param statsWrappersJSON a {@link Map} of instance name to JSON string representation of {@link StatsWrapper} objects from the
   *                          node level
   * @return a {@link Pair} of Strings whose values represents valid quota stats across all partitions.
   * First element is the raw (sum) aggregated stats and second element is average(aggregated) stats for all replicas
   * for each partition.
   * @throws IOException
   */
  Pair<String, String> doWork(Map<String, String> statsWrappersJSON) throws IOException {
    StatsSnapshot partitionSnapshot = new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>());
    Map<String, Long> partitionTimestampMap = new HashMap<>();
    StatsSnapshot rawPartitionSnapshot = new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>());
    for (Map.Entry<String, String> statsWrapperJSON : statsWrappersJSON.entrySet()) {
      if (statsWrapperJSON != null) {
        StatsWrapper snapshotWrapper = mapper.readValue(statsWrapperJSON.getValue(), StatsWrapper.class);
        StatsWrapper snapshotWrapperCopy = mapper.readValue(statsWrapperJSON.getValue(), StatsWrapper.class);
        combineRaw(rawPartitionSnapshot, snapshotWrapper);
        combine(partitionSnapshot, snapshotWrapperCopy, statsWrapperJSON.getKey(), partitionTimestampMap);
      }
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Combined raw snapshot {}", mapper.writeValueAsString(rawPartitionSnapshot));
      logger.trace("Combined snapshot {}", mapper.writeValueAsString(partitionSnapshot));
    }
    StatsSnapshot reducedRawSnapshot = reduce(rawPartitionSnapshot);
    StatsSnapshot reducedSnapshot = reduce(partitionSnapshot);
    if (logger.isTraceEnabled()) {
      logger.trace("Reduced raw snapshot {}", mapper.writeValueAsString(reducedRawSnapshot));
      logger.trace("Reduced snapshot {}", mapper.writeValueAsString(reducedSnapshot));
    }
    return new Pair<>(mapper.writeValueAsString(reducedRawSnapshot), mapper.writeValueAsString(reducedSnapshot));
  }

  /**
   * Aggregate the given {@link StatsSnapshot} by simply add it to the raw base {@link StatsSnapshot} by partition.
   * @param rawBaseSnapshot the raw base {@link StatsSnapshot} which will contain the aggregated result
   * @param snapshotWrapper the {@link StatsSnapshot} to be added to the raw base {@link StatsSnapshot}
   */
  private void combineRaw(StatsSnapshot rawBaseSnapshot, StatsWrapper snapshotWrapper) {
    long totalValue = rawBaseSnapshot.getValue();
    Map<String, StatsSnapshot> partitionSnapshotMap = snapshotWrapper.getSnapshot().getSubMap();
    Map<String, StatsSnapshot> basePartitionSnapshotMap = rawBaseSnapshot.getSubMap();
    for (Map.Entry<String, StatsSnapshot> partitionSnapshot : partitionSnapshotMap.entrySet()) {
      if (basePartitionSnapshotMap.containsKey(partitionSnapshot.getKey())) {
        StatsSnapshot.aggregate(basePartitionSnapshotMap.get(partitionSnapshot.getKey()), partitionSnapshot.getValue());
      } else {
        basePartitionSnapshotMap.put(partitionSnapshot.getKey(), partitionSnapshot.getValue());
      }
      totalValue += partitionSnapshot.getValue().getValue();
    }
    rawBaseSnapshot.setValue(totalValue);
  }

  /**
   * Aggregate the given {@link StatsSnapshot} with the base {@link StatsSnapshot} by partition with the following
   * rules:
   * 1. Append the partition entry if the base doesn't already contain an entry for a given partition.
   * 2. If the base already has an entry for a given partition, update/replace the entry only if:
   *    a) Timestamp difference of a given partition entry and base entry are within one aggregation period, and the
   *       given partition entry has a greater value.
   *    b) Timestamp of a given partition entry is one aggregation period newer (greater) than the timestamp of the
   *       base entry.
   * @param baseSnapshot the base {@link StatsSnapshot} which will contain the aggregated result
   * @param snapshotWrapper the {@link StatsSnapshot} to be aggregated to the base {@link StatsSnapshot}
   * @param instance new instance from which snapshot is being combined
   * @param partitionTimestampMap a {@link Map} of partition to timestamp to keep track the current timestamp of each
   *                              partition entry in the base {@link StatsSnapshot}
   */
  private void combine(StatsSnapshot baseSnapshot, StatsWrapper snapshotWrapper, String instance,
      Map<String, Long> partitionTimestampMap) throws IOException {
    long totalValue = baseSnapshot.getValue();
    long snapshotTimestamp = snapshotWrapper.getHeader().getTimestamp();
    Map<String, StatsSnapshot> partitionSnapshotMap = snapshotWrapper.getSnapshot().getSubMap();
    Map<String, StatsSnapshot> basePartitionSnapshotMap = baseSnapshot.getSubMap();
    for (Map.Entry<String, StatsSnapshot> partitionSnapshot : partitionSnapshotMap.entrySet()) {
      String partitionId = partitionSnapshot.getKey();
      if (basePartitionSnapshotMap.containsKey(partitionId)) {
        long deltaInValue =
            partitionSnapshot.getValue().getValue() - basePartitionSnapshotMap.get(partitionId).getValue();
        long deltaInTimeMs = snapshotTimestamp - partitionTimestampMap.get(partitionId);
        if (Math.abs(deltaInTimeMs) < relevantTimePeriodInMs && deltaInValue > 0) {
          logger.trace("Updating partition {} snapshot from instance {} as last updated time is within"
              + "relevant time period and delta value has increased ", partitionId, instance);
          basePartitionSnapshotMap.put(partitionId, partitionSnapshot.getValue());
          partitionTimestampMap.put(partitionId, snapshotTimestamp);
          totalValue += deltaInValue;
        } else if (deltaInTimeMs > relevantTimePeriodInMs) {
          logger.trace("Updating partition {} snapshot from instance {} as new value is updated "
              + "within relevant time period compared to already existing one", partitionId, instance);
          basePartitionSnapshotMap.put(partitionId, partitionSnapshot.getValue());
          partitionTimestampMap.put(partitionId, snapshotTimestamp);
          totalValue += deltaInValue;
        } else {
          logger.trace("Ignoring snapshot from {} for partition {}", instance, partitionId);
        }
      } else {
        logger.trace("First entry for partition {} is from {}", partitionId, instance);
        basePartitionSnapshotMap.put(partitionId, partitionSnapshot.getValue());
        partitionTimestampMap.put(partitionId, snapshotTimestamp);
        totalValue += partitionSnapshot.getValue().getValue();
      }
    }
    baseSnapshot.setValue(totalValue);
  }

  /**
   * Reduce the given {@link StatsSnapshot} whose first level mapped by partitions to a shallower {@link StatsSnapshot}
   * by adding entries belonging to the same partition together.
   * @param statsSnapshot the {@link StatsSnapshot} to be reduced
   * @return the reduced {@link StatsSnapshot}
   */
  private StatsSnapshot reduce(StatsSnapshot statsSnapshot) {
    StatsSnapshot reducedSnapshot = new StatsSnapshot(0L, null);
    if (statsSnapshot.getSubMap() != null) {
      for (StatsSnapshot snapshot : statsSnapshot.getSubMap().values()) {
        statsSnapshot.aggregate(reducedSnapshot, snapshot);
      }
    }
    return reducedSnapshot;
  }
}
