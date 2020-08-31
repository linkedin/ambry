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
 * Hold the data structures needed by {@link BlobStoreStats} to serve requests. The class also exposes helper methods
 * used to modify and access the stored data structures.
 */
class ScanResults {
  // A NavigableMap that stores buckets for container valid data size. The key of the map is the end time of each
  // bucket and the value is the corresponding valid data size map. For example, there are two buckets with end time
  // t1 and t2. Bucket with end time t2 includes all events whose operation time is greater than or equal to t1 but
  // strictly less than t2.
  // Each bucket except for the very first one contains the delta in valid data size that occurred prior to the bucket
  // end time. The very first bucket's end time is the forecast start time for containers and it contains the valid data
  // size map at the forecast start time. The very first bucket is used as a base value, requested valid data size is
  // computed by applying the deltas from appropriate buckets on the base value.
  private final NavigableMap<Long, Map<String, Map<String, Long>>> containerBuckets = new TreeMap<>();

  // A NavigableMap that stores buckets for log segment valid data size. The rest of the structure is similar
  // to containerBuckets.
  private final NavigableMap<Long, NavigableMap<String, Long>> logSegmentBuckets = new TreeMap<>();

  final long containerForecastStartTimeMs;
  final long containerLastBucketTimeMs;
  final long containerForecastEndTimeMs;
  final long logSegmentForecastStartTimeMs;
  final long logSegmentLastBucketTimeMs;
  final long logSegmentForecastEndTimeMs;
  Offset scannedEndOffset = null;

  /**
   * Create the bucket data structures in advance based on the given scanStartTime and segmentScanTimeOffset.
   */
  ScanResults(long startTimeInMs, long logSegmentForecastOffsetMs, int bucketCount, long bucketSpanInMs) {
    long containerBucketTimeMs = startTimeInMs;
    long logSegmentBucketTimeMs = startTimeInMs - logSegmentForecastOffsetMs;
    for (int i = 0; i < bucketCount; i++) {
      containerBuckets.put(containerBucketTimeMs, new HashMap<>());
      logSegmentBuckets.put(logSegmentBucketTimeMs, new TreeMap<>(LogSegmentNameHelper.COMPARATOR));
      containerBucketTimeMs += bucketSpanInMs;
      logSegmentBucketTimeMs += bucketSpanInMs;
    }
    containerForecastStartTimeMs = containerBuckets.firstKey();
    containerLastBucketTimeMs = containerBuckets.lastKey();
    containerForecastEndTimeMs = containerLastBucketTimeMs + bucketSpanInMs;
    logSegmentForecastStartTimeMs = logSegmentBuckets.firstKey();
    logSegmentLastBucketTimeMs = logSegmentBuckets.lastKey();
    logSegmentForecastEndTimeMs = logSegmentLastBucketTimeMs + bucketSpanInMs;
  }

  /**
   * Given a reference time, return the key of the appropriate container bucket whose end time is strictly greater than
   * the reference time.
   * @param referenceTimeInMs the reference time or operation time of an event.
   * @return the appropriate bucket key (bucket end time) to indicate which bucket will an event with
   * the given reference time as operation time belong to.
   */
  Long getContainerBucketKey(long referenceTimeInMs) {
    return containerBuckets.higherKey(referenceTimeInMs);
  }

  /**
   * Given a reference time, return the key of the appropriate log segment bucket whose end time is strictly greater
   * than the reference time.
   * @param referenceTimeInMs the reference time or operation time of an event.
   * @return the appropriate bucket key (bucket end time) to indicate which bucket will an event with
   * the given reference time as operation time belong to.
   */
  Long getLogSegmentBucketKey(long referenceTimeInMs) {
    return logSegmentBuckets.higherKey(referenceTimeInMs);
  }

  /**
   * Helper function to update the container base value bucket with the given value.
   * @param serviceId the serviceId of the map entry to be updated
   * @param containerId the containerId of the map entry to be updated
   * @param value the value to be added
   */
  void updateContainerBaseBucket(String serviceId, String containerId, long value) {
    updateContainerBucket(containerBuckets.firstKey(), serviceId, containerId, value);
  }

  /**
   * Helper function to update the log segment base value bucket with the given value.
   * @param logSegmentName the log segment name of the map entry to be updated
   * @param value the value to be added
   */
  void updateLogSegmentBaseBucket(String logSegmentName, long value) {
    updateLogSegmentBucket(logSegmentBuckets.firstKey(), logSegmentName, value);
  }

  /**
   * Helper function to update a container bucket with the given value.
   * @param bucketKey the bucket key to specify which bucket will be updated
   * @param serviceId the serviceId of the map entry to be updated
   * @param containerId the containerId of the map entry to be updated
   * @param value the value to be added
   */
  void updateContainerBucket(Long bucketKey, String serviceId, String containerId, long value) {
    if (bucketKey != null && containerBuckets.containsKey(bucketKey)) {
      Map<String, Map<String, Long>> existingBucketEntry = containerBuckets.get(bucketKey);
      updateNestedMapHelper(existingBucketEntry, serviceId, containerId, value);
    }
  }

  /**
   * Helper function to update a log segment bucket with a given value.
   * @param bucketKey the bucket key to specify which bucket will be updated
   * @param logSegmentName the log segment name of the map entry to be updated
   * @param value the value to be added
   */
  void updateLogSegmentBucket(Long bucketKey, String logSegmentName, long value) {
    if (bucketKey != null && logSegmentBuckets.containsKey(bucketKey)) {
      Map<String, Long> existingBucketEntry = logSegmentBuckets.get(bucketKey);
      updateMapHelper(existingBucketEntry, logSegmentName, value);
    }
  }

  /**
   * Given a reference time in milliseconds return the corresponding valid data size per log segment map by aggregating
   * all buckets whose end time is less than or equal to the reference time.
   * @param referenceTimeInMS the reference time in ms until which deletes and expiration are relevant
   * @return a {@link Pair} whose first element is the end time of the last bucket that was aggregated and whose second
   * element is the requested valid data size per log segment {@link NavigableMap}.
   */
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

  /**
   * Given a reference time in ms return the corresponding valid data size per container map by aggregating all buckets
   * whose end time is less than or equal to the reference time.
   * @param referenceTimeInMs the reference time in ms until which deletes and expiration are relevant.
   * @return a {@link Pair} whose first element is the end time of the last bucket that was aggregated and whose second
   * element is the requested valid data size per container {@link Map}.
   */
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
