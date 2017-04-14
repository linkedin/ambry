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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Responsible for performing cluster wide stats aggregation.
 */
public class HelixClusterAggregator {
  private final ObjectMapper mapper = new ObjectMapper();
  private final long relevantTimePeriodInMs;

  HelixClusterAggregator(long relevantTimePeriodInMinutes) {
    relevantTimePeriodInMs = TimeUnit.MINUTES.toMillis(relevantTimePeriodInMinutes);
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
  }

  /**
   * Take a {@link List} of JSON string representation of {@link StatsWrapper} objects and perform cluster wide
   * aggregation with them.
   * @param statsWrappersJSON a {@link List} of JSON string representation of {@link StatsWrapper} objects from the
   *                          node level
   * @return a {@link Pair} of Strings whose first element is the raw (sum) aggregated stats and whose second element is
   * the aggregated stats based on timestamp and value from each partition.
   * @throws IOException
   */
  Pair<String, String> doWork(List<String> statsWrappersJSON) throws IOException {
    StatsSnapshot partitionSnapshot = new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>());
    Map<String, Long> partitionTimestampMap = new HashMap<>();
    StatsSnapshot rawPartitionSnapshot = new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>());
    for (String statsWrapperJSON : statsWrappersJSON) {
      if (statsWrapperJSON != null) {
        StatsWrapper snapshotWrapper = mapper.readValue(statsWrapperJSON, StatsWrapper.class);
        StatsWrapper snapshotWrapperCopy = mapper.readValue(statsWrapperJSON, StatsWrapper.class);
        combineRaw(rawPartitionSnapshot, snapshotWrapper);
        combine(partitionSnapshot, snapshotWrapperCopy, partitionTimestampMap);
      }
    }
    StatsSnapshot reducedRawSnapshot = reduce(rawPartitionSnapshot);
    StatsSnapshot reducedSnapshot = reduce(partitionSnapshot);
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
   * @param partitionTimestampMap a {@link Map} of partition to timestamp to keep track the current timestamp of each
   *                              partition entry in the base {@link StatsSnapshot}
   */
  private void combine(StatsSnapshot baseSnapshot, StatsWrapper snapshotWrapper,
      Map<String, Long> partitionTimestampMap) {
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
          basePartitionSnapshotMap.put(partitionId, partitionSnapshot.getValue());
          partitionTimestampMap.put(partitionId, snapshotTimestamp);
          totalValue += deltaInValue;
        } else if (deltaInTimeMs > relevantTimePeriodInMs) {
          basePartitionSnapshotMap.put(partitionId, partitionSnapshot.getValue());
          partitionTimestampMap.put(partitionId, snapshotTimestamp);
          totalValue += deltaInValue;
        }
      } else {
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
