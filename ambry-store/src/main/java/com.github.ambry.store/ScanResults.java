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

import com.github.ambry.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;


/**
 * Hold the data structures needed by {@link BlobStoreStats} to serve requests. The class also holds helper methods
 * used to modify and access the stored data structures.
 */
class ScanResults {
  final NavigableMap<Long, Map<String, Map<String, Long>>> containerBuckets = new TreeMap<>();
  final NavigableMap<Long, NavigableMap<String, Long>> logSegmentBuckets = new TreeMap<>();
  final long containerForecastStartTimeInMs;
  final long containerLastBucketEndTimeInMs;
  final long containerForecastEndTimeInMs;
  final long logSegmentForecastStartTimeInMs;
  final long logSegmentLastBucketEndTimeInMs;
  final long logSegmentForecastEndTimeInMs;
  Offset lastScannedOffset = null;

  /**
   * Create the bucket data structures in advance based on the given scanStartTime and segmentScanTimeOffset.
   */
  ScanResults(long startTimeInMs, long logSegmentForecastOffsetMs, int bucketCount, long bucketSpanInMs) {
    long containerBucketEndTime = startTimeInMs - startTimeInMs % bucketSpanInMs;
    long logSegmentStartTimeInMs = startTimeInMs - logSegmentForecastOffsetMs;
    long logSegmentBucketEndTime = logSegmentStartTimeInMs - logSegmentStartTimeInMs % bucketSpanInMs;
    for (int i = 0; i < bucketCount; i++) {
      containerBuckets.put(containerBucketEndTime, new HashMap<String, Map<String, Long>>());
      logSegmentBuckets.put(logSegmentBucketEndTime, new TreeMap<String, Long>());
      containerBucketEndTime += bucketSpanInMs;
      logSegmentBucketEndTime += bucketSpanInMs;
    }
    containerForecastStartTimeInMs = containerBuckets.firstKey();
    containerLastBucketEndTimeInMs = containerBuckets.lastKey();
    containerForecastEndTimeInMs = containerLastBucketEndTimeInMs + bucketSpanInMs;
    logSegmentForecastStartTimeInMs = logSegmentBuckets.firstKey();
    logSegmentLastBucketEndTimeInMs = logSegmentBuckets.lastKey();
    logSegmentForecastEndTimeInMs = logSegmentLastBucketEndTimeInMs + bucketSpanInMs;
  }

  Long getContainerBucketKey(long referenceTimeInMs) {
    return containerBuckets.higherKey(referenceTimeInMs);
  }

  Long getLogSegmentBucketKey(long referenceTimeInMs) {
    return logSegmentBuckets.higherKey(referenceTimeInMs);
  }

  void updateContainerBucket(Long bucketKey, String serviceId, String containerId, long value) {
    if (bucketKey != null && containerBuckets.containsKey(bucketKey)) {
      Map<String, Map<String, Long>> deltaInValidDataSizePerContainer = containerBuckets.get(bucketKey);
      updateNestedMapHelper(deltaInValidDataSizePerContainer, serviceId, containerId, value);
    }
  }

  void updateLogSegmentBucket(Long bucketKey, String logSegmentName, long value) {
    if (bucketKey != null && logSegmentBuckets.containsKey(bucketKey)) {
      Map<String, Long> deltaInValidSizePerLogSegment = logSegmentBuckets.get(bucketKey);
      updateMapHelper(deltaInValidSizePerLogSegment, logSegmentName, value);
    }
  }

  Pair<Long, NavigableMap<String, Long>> getValidSizePerLogSegment(Long referenceTimeInMS) {
    NavigableMap<String, Long> validSizePerLogSegment = new TreeMap<>(logSegmentBuckets.firstEntry().getValue());
    NavigableMap<Long, NavigableMap<String, Long>> subMap =
        logSegmentBuckets.subMap(logSegmentBuckets.firstKey(), false, referenceTimeInMS, true);
    for (Map.Entry<Long, NavigableMap<String, Long>> bucket : subMap.entrySet()) {
      for (Map.Entry<String, Long> bucketEntry : bucket.getValue().entrySet()) {
        updateMapHelper(validSizePerLogSegment, bucketEntry.getKey(), bucketEntry.getValue());
      }
    }
    Long lastReferenceBucketTimeInMs = subMap.isEmpty() ? logSegmentBuckets.firstKey() : subMap.lastKey();
    return new Pair<>(lastReferenceBucketTimeInMs, validSizePerLogSegment);
  }

  Map<String, Map<String, Long>> getValidSizePerContainer(Long referenceTimeInMs) {
    Map<String, Map<String, Long>> validSizePerContainer = new HashMap<>();
    for (Map.Entry<String, Map<String, Long>> accountEntry : containerBuckets.firstEntry().getValue().entrySet()) {
      validSizePerContainer.put(accountEntry.getKey(), new HashMap<>(accountEntry.getValue()));
    }
    NavigableMap<Long, Map<String, Map<String, Long>>> subMap =
        containerBuckets.subMap(containerBuckets.firstKey(), false, referenceTimeInMs, true);
    for (Map.Entry<Long, Map<String, Map<String, Long>>> bucket : subMap.entrySet()) {
      for (Map.Entry<String, Map<String, Long>> accountEntry : bucket.getValue().entrySet()) {
        for (Map.Entry<String, Long> containerEntry : accountEntry.getValue().entrySet()) {
          updateNestedMapHelper(validSizePerContainer, accountEntry.getKey(), containerEntry.getKey(),
              containerEntry.getValue());
        }
      }
    }
    return validSizePerContainer;
  }

  /**
   * Helper function to update nested map data structure.
   * @param nestedMap nested {@link Map} to be updated
   * @param firstKey of the nested map
   * @param secondKey of the nested map
   * @param value the value to be added at the corresponding entry
   */
  private void updateNestedMapHelper(Map<String, Map<String, Long>> nestedMap, String firstKey, String secondKey,
      Long value) {
    if (!nestedMap.containsKey(firstKey)) {
      nestedMap.put(firstKey, new HashMap<String, Long>());
    }
    updateMapHelper(nestedMap.get(firstKey), secondKey, value);
  }

  /**
   * Helper function to update map data structure.
   * @param map {@link Map} to be updated
   * @param key of the map
   * @param value the value to be added at the corresponding entry
   */
  private void updateMapHelper(Map<String, Long> map, String key, Long value) {
    Long newValue = map.containsKey(key) ? map.get(key) + value : value;
    map.put(key, newValue);
  }
}
