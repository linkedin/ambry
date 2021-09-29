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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.ambry.store.StatsUtils.*;


/**
 * Hold the data structures needed by {@link BlobStoreStats} to serve requests. The class also exposes helper methods
 * used to modify and access the stored data structures.
 */
class ScanResults {
  // A bucket is a period of time, whose length is defined by bucketSpanInMs parameter in constructor. In this class,
  // several maps are created to represent different purposes of bucket.
  // An object is created to represent base bucket and another map, whose value is the same type as the base bucket,
  // is created to represent delta in each time bucket. This is an example to illustrate how base and delta bucket work.
  //
  // If we want to calculate valid data size for all account and container, we create at least two fields
  // <pre><code>
  //  private long validSize;
  //  private final NavigableMap<Long, Long> deltaBuckets = new TreeMap<>();
  // </code></pre>
  //
  // When starting calculating, we add valid data size to {@code validSize} and add delta to its corresponding bucket.
  // Assuming each bucket is one hour long and the start time is T0, then we have deltaBuckets as
  //    T0                    T0+1H                   T0+2H                 T0+3H
  // baseBucket   [              ]    [                  ]    [                 ]   .....
  //
  // T0+1H is the first key in the deltaBuckets. All the deltas that happen before T0+1H but after T0 will be added to
  // its value. T0+2H is the second key in the deltaBuckets. All the deltas that happen before T0+2H but after T0+1H will
  // be added to its value. But what are deltas? This is an example to show the answer.
  //
  // Now let's go through some IndexValues like below:
  // 1. PUT[ID1]: created at T0-1D, but will expire at T0+2.5H
  // 2. PUT[ID2]: created at T0-1D, permanent blob
  // The validSize at T0 should be PUT[ID1] + PUT[ID2] because PUT[ID1] haven't expired yet. But we know at T0+2.5H, the
  // validSize should be PUT[ID2] because PUT[ID1] is expired. Since we are not going to handle PUT[ID1] again, we need
  // to put a delta in T0+3H bucket. And it looks like this:
  //    T0                           T0+1H                   T0+2H                 T0+3H
  // PUT[ID1]+PUT[ID2]   [              ]    [                  ]    [ -PUT[ID1]     ]   .....
  // Please notice that the delta is negative. So when we pick any point of time and add the base validSize and all the
  // delta values in the buckets before this point of time, we will get the correct answer. For instance, validSize of
  // T0 is PUT[ID1]+PUT[ID2], validSize of T0+1H is PUT[ID1]+PUT[ID2]+delta of T0+1H. And validSize of T0+3H is PUT[ID1]
  // +PUT[ID2]-PUT[ID1] because the delta of T0+3H bucket is -PUT[ID1].
  //
  // As new IndexValue comes in, we have to deal with them and fill the delta for them as well.
  // 1. DELETE[ID1]: deleted at T0+0.5H
  // 2. PUT[ID3]: created at T0+1.2H, but will expire at T0+2.8H
  // 3. DELETE[ID2]: deleted at T0+1.5H
  // For DELETE[ID1], it adds -PUT[ID1] delta to T0+1H bucket since it makes PUT[ID1] invalid. However, PUT[ID1] will
  // expire at T0+2.5H and delta has already been added to T0+3H bucket. In order to avoid double adding, we have to add
  // PUT[ID1] back to T0+3H bucket. PUT[ID3] adds delta in T0+2H bucket but it will expire at T0+2.8H, so it has be to
  // subtracted in T0+3H bucket. DELETE[ID2] makes PUT[ID2] invalid, so a delta will be added to T0+2H bucket.
  //    T0                           T0+1H                   T0+2H                             T0+3H
  // PUT[ID1]+PUT[ID2]   [-PUT[ID1]      ]    [PUT[ID3]-PUT[ID2] ]    [-PUT[ID1]+PUT[ID1]-PUT[ID3] ]   .....
  //
  // This is how delta bucket works. We can pick any point of time and add base validSize and all the delta value in those
  // buckets prior to this point of time and get the answer we need.

  // Container buckets are very close to the example above, except that it doesn't aggregate all containers' valid data,
  // rather it keeps them in a map. The key of this map is the AccountId, and the value of this map is yet another map,
  // whose key is the ContainerId and the value is the valid size of this container.
  // So {@code containerBaseBucket} is base value for all containers and the {@code containerBuckets} is the delta values
  // on each time bucket.
  private final Map<Short, Map<Short, Long>> containerBaseBucket = new ConcurrentHashMap<>();
  private final NavigableMap<Long, Map<Short, Map<Short, Long>>> containerDeltaBuckets = new TreeMap<>();
  final long containerForecastStartTimeMs;
  final long containerLastBucketTimeMs;
  final long containerForecastEndTimeMs;

  // This is for container physical storage usage, it doesn't have to take delta into consideration, so it's more like a base
  // bucket for valid  storage usage.
  private final Map<Short, Map<Short, Long>> containerPhysicalStorageUsage = new ConcurrentHashMap<>();
  private final Map<Short, Map<Short, Long>> containerNumberOfStoreKeys = new ConcurrentHashMap<>();

  // LogSegment buckets keep track of valid IndexValue size in each log segments. So the base value of log segment bucket
  // is a map, whose key is the log segment name and the value is the sum of valid IndexValues' sizes. To test if an
  // IndexValue is valid or not, we follow the same rule in the {@link BlobStoreCompactor}.
  //
  // LogSegment buckets is very close to container buckets, except for several differences.
  // 1. LogSegment has two delta buckets. one for deleted blobs, the other for expired blobs.
  // 2. Since we have a retention period for deleted blob, deleted delta's first bucket would start earlier than expired
  //    delta.
  // The reason to have two delta buckets is because deleted blobs and expired blobs are invalidated at different time.
  // When a blob is deleted, we have to wait for a retention duration(7 days in prod) to invalidate it. But when a blob
  // is expired, it's invalidated right aways. So when calculating valid size for log segment, we have to pass two time
  // stamps, a delete reference time, usually is now - 7 days, and a expiry time, usually is now.
  private final NavigableMap<LogSegmentName, Long> logSegmentBaseBucket = new TreeMap<>();
  private final NavigableMap<Long, NavigableMap<LogSegmentName, Long>> logSegmentDeltaBucketsForDeleted =
      new TreeMap<>();
  private final NavigableMap<Long, NavigableMap<LogSegmentName, Long>> logSegmentDeltaBucketsForExpired =
      new TreeMap<>();

  final long logSegmentForecastStartTimeMsForDeleted;
  final long logSegmentLastBucketTimeMsForDeleted;
  final long logSegmentForecastEndTimeMsForDeleted;
  final long logSegmentForecastStartTimeMsForExpired;
  final long logSegmentLastBucketTimeMsForExpired;
  final long logSegmentForecastEndTimeMsForExpired;
  Offset scannedEndOffset = null;

  /**
   * Create the bucket data structures in advance based on the given scanStartTime and segmentScanTimeOffset.
   */
  ScanResults(long startTimeInMs, long logSegmentForecastOffsetMs, int bucketCount, long bucketSpanInMs) {
    // Set up container buckets
    containerForecastStartTimeMs = startTimeInMs;
    long containerBucketTimeMs = containerForecastStartTimeMs + bucketSpanInMs;
    for (int i = 1; i < bucketCount; i++) {
      containerDeltaBuckets.put(containerBucketTimeMs, new HashMap<>());
      containerBucketTimeMs += bucketSpanInMs;
    }
    containerLastBucketTimeMs =
        containerDeltaBuckets.isEmpty() ? containerForecastStartTimeMs : containerDeltaBuckets.lastKey();
    containerForecastEndTimeMs = containerLastBucketTimeMs + bucketSpanInMs;

    // Set up log segment buckets
    logSegmentForecastStartTimeMsForExpired = startTimeInMs;
    long bucketTimeMs = logSegmentForecastStartTimeMsForExpired + bucketSpanInMs;
    for (int i = 1; i < bucketCount; i++) {
      logSegmentDeltaBucketsForExpired.put(bucketTimeMs, new TreeMap<>());
      bucketTimeMs += bucketSpanInMs;
    }
    logSegmentLastBucketTimeMsForExpired =
        logSegmentDeltaBucketsForExpired.isEmpty() ? logSegmentForecastStartTimeMsForExpired
            : logSegmentDeltaBucketsForExpired.lastKey();
    logSegmentForecastEndTimeMsForExpired = logSegmentLastBucketTimeMsForExpired + bucketSpanInMs;

    logSegmentForecastStartTimeMsForDeleted = startTimeInMs - logSegmentForecastOffsetMs;
    bucketTimeMs = logSegmentForecastStartTimeMsForDeleted + bucketSpanInMs;
    for (int i = 1; i < bucketCount; i++) {
      logSegmentDeltaBucketsForDeleted.put(bucketTimeMs, new TreeMap<>());
      bucketTimeMs += bucketSpanInMs;
    }
    logSegmentLastBucketTimeMsForDeleted =
        logSegmentDeltaBucketsForDeleted.isEmpty() ? logSegmentForecastStartTimeMsForDeleted
            : logSegmentDeltaBucketsForDeleted.lastKey();
    logSegmentForecastEndTimeMsForDeleted = logSegmentLastBucketTimeMsForDeleted + bucketSpanInMs;
  }

  /**
   * Given a reference time, return the key of the appropriate container bucket whose end time is strictly greater than
   * the reference time.
   * @param referenceTimeInMs the reference time or operation time of an event.
   * @return the appropriate bucket key (bucket end time) to indicate which bucket will an event with
   * the given reference time as operation time belong to.
   */
  Long getContainerBucketKey(long referenceTimeInMs) {
    return containerDeltaBuckets.higherKey(referenceTimeInMs);
  }

  /**
   * Given a reference time, return the key of the appropriate log segment deleted bucket whose end time is strictly
   * greater than the reference time.
   * @param referenceTimeInMs the reference time or operation time of an event.
   * @return the appropriate bucket key (bucket end time) to indicate which bucket will an event with
   * the given reference time as operation time belong to.
   */
  Long getLogSegmentDeletedBucketKey(long referenceTimeInMs) {
    return logSegmentDeltaBucketsForDeleted.higherKey(referenceTimeInMs);
  }

  /**
   * Given a reference time, return the key of the appropriate log segment expired bucket whose end time is strictly
   * greater than the reference time.
   * @param referenceTimeInMs the reference time or operation time of an event.
   * @return the appropriate bucket key (bucket end time) to indicate which bucket will an event with
   * the given reference time as operation time belong to.
   */
  Long getLogSegmentExpiredBucketKey(long referenceTimeInMs) {
    return logSegmentDeltaBucketsForExpired.higherKey(referenceTimeInMs);
  }

  /**
   * Update the container base value bucket with the given value.
   * @param accountId the accountId of the map entry to be updated
   * @param containerId the containerId of the map entry to be updated
   * @param value the value to be added
   */
  void updateContainerBaseBucket(short accountId, short containerId, long value) {
    updateNestedMapHelper(containerBaseBucket, accountId, containerId, value);
  }

  /**
   * Update the physical storage usage and store key for the givne account and container.
   * @param accountId The account id
   * @param containerId The container id
   * @param usage The new physical storage usage
   * @param newKey The number of new keys to add
   */
  void updateContainerPhysicalStorageUsageAndStoreKey(short accountId, short containerId, long usage, long newKey) {
    updateNestedMapHelper(containerPhysicalStorageUsage, accountId, containerId, usage);
    containerNumberOfStoreKeys.computeIfAbsent(accountId, k -> new HashMap<>())
        .compute(containerId, (k, v) -> v == null ? newKey : v.longValue() + newKey);
  }

  /**
   * Update the log segment base value bucket with the given value.
   * @param logSegmentName the log segment name of the map entry to be updated
   * @param value the value to be added
   */
  void updateLogSegmentBaseBucket(LogSegmentName logSegmentName, long value) {
    updateMapHelper(logSegmentBaseBucket, logSegmentName, value);
  }

  /**
   * Helper function to update a container bucket with the given value.
   * @param bucketKey the bucket key to specify which bucket will be updated
   * @param accountId the accountId of the map entry to be updated
   * @param containerId the containerId of the map entry to be updated
   * @param value the value to be added
   */
  void updateContainerBucket(Long bucketKey, short accountId, short containerId, long value) {
    if (bucketKey != null && containerDeltaBuckets.containsKey(bucketKey)) {
      Map<Short, Map<Short, Long>> existingBucketEntry = containerDeltaBuckets.get(bucketKey);
      updateNestedMapHelper(existingBucketEntry, accountId, containerId, value);
    }
  }

  /**
   * Helper function to update a log segment deleted bucket with a given value.
   * @param bucketKey the bucket key to specify which bucket will be updated
   * @param logSegmentName the log segment name of the map entry to be updated
   * @param value the value to be added
   */
  void updateLogSegmentDeletedBucket(Long bucketKey, LogSegmentName logSegmentName, long value) {
    if (bucketKey != null && logSegmentDeltaBucketsForDeleted.containsKey(bucketKey)) {
      Map<LogSegmentName, Long> existingBucketEntry = logSegmentDeltaBucketsForDeleted.get(bucketKey);
      updateMapHelper(existingBucketEntry, logSegmentName, value);
    }
  }

  /**
   * Helper function to update a log segment expired bucket with a given value.
   * @param bucketKey the bucket key to specify which bucket will be updated
   * @param logSegmentName the log segment name of the map entry to be updated
   * @param value the value to be added
   */
  void updateLogSegmentExpiredBucket(Long bucketKey, LogSegmentName logSegmentName, long value) {
    if (bucketKey != null && logSegmentDeltaBucketsForExpired.containsKey(bucketKey)) {
      Map<LogSegmentName, Long> existingBucketEntry = logSegmentDeltaBucketsForExpired.get(bucketKey);
      updateMapHelper(existingBucketEntry, logSegmentName, value);
    }
  }

  /**
   * Given a deleted and expired reference time in milliseconds return the corresponding valid data size per log segment
   * map by aggregating all buckets whose end time is less than or equal to the reference time.
   * @param deleteReferenceTimeInMS the reference time in ms until which deletes are relevant
   * @param expiryReferenceTimeInMs the reference time in ms until which expiration is relevant
   * @return a {@link Pair} whose first element is the end time of the last deleted bucket that was aggregated and whose
   * second element is the requested valid data size per log segment {@link NavigableMap}.
   */
  Pair<Long, NavigableMap<LogSegmentName, Long>> getValidSizePerLogSegment(Long deleteReferenceTimeInMS,
      long expiryReferenceTimeInMs) {
    NavigableMap<LogSegmentName, Long> validSizePerLogSegment = new TreeMap<>(logSegmentBaseBucket);
    NavigableMap<Long, NavigableMap<LogSegmentName, Long>> subMap =
        logSegmentDeltaBucketsForDeleted.headMap(deleteReferenceTimeInMS, true);
    for (Map.Entry<Long, NavigableMap<LogSegmentName, Long>> bucket : subMap.entrySet()) {
      for (Map.Entry<LogSegmentName, Long> bucketEntry : bucket.getValue().entrySet()) {
        updateMapHelper(validSizePerLogSegment, bucketEntry.getKey(), bucketEntry.getValue());
      }
    }
    Long lastDeleteReferenceBucketTimeInMs =
        subMap.isEmpty() ? logSegmentForecastStartTimeMsForDeleted : subMap.lastKey();

    subMap = logSegmentDeltaBucketsForExpired.headMap(expiryReferenceTimeInMs, true);
    for (Map.Entry<Long, NavigableMap<LogSegmentName, Long>> bucket : subMap.entrySet()) {
      for (Map.Entry<LogSegmentName, Long> bucketEntry : bucket.getValue().entrySet()) {
        updateMapHelper(validSizePerLogSegment, bucketEntry.getKey(), bucketEntry.getValue());
      }
    }
    return new Pair<>(lastDeleteReferenceBucketTimeInMs, validSizePerLogSegment);
  }

  /**
   * Given a reference time in ms return the corresponding valid data size per container map by aggregating all buckets
   * whose end time is less than or equal to the reference time.
   * @param referenceTimeInMs the reference time in ms until which deletes and expiration are relevant.
   * @return a {@link Pair} whose first element is the end time of the last bucket that was aggregated and whose second
   * element is the requested valid data size per container {@link Map}.
   */
  Map<Short, Map<Short, Long>> getValidSizePerContainer(Long referenceTimeInMs) {
    Map<Short, Map<Short, Long>> validSizePerContainer = new HashMap<>();
    for (Map.Entry<Short, Map<Short, Long>> accountEntry : containerBaseBucket.entrySet()) {
      validSizePerContainer.put(accountEntry.getKey(), new HashMap<>(accountEntry.getValue()));
    }
    NavigableMap<Long, Map<Short, Map<Short, Long>>> subMap = containerDeltaBuckets.headMap(referenceTimeInMs, true);
    for (Map.Entry<Long, Map<Short, Map<Short, Long>>> bucket : subMap.entrySet()) {
      for (Map.Entry<Short, Map<Short, Long>> accountEntry : bucket.getValue().entrySet()) {
        for (Map.Entry<Short, Long> containerEntry : accountEntry.getValue().entrySet()) {
          updateNestedMapHelper(validSizePerContainer, accountEntry.getKey(), containerEntry.getKey(),
              containerEntry.getValue());
        }
      }
    }
    return validSizePerContainer;
  }

  Map<Short, Map<Short, Long>> getContainerPhysicalStorageUsage() {
    return Collections.unmodifiableMap(containerPhysicalStorageUsage);
  }

  Map<Short, Map<Short, Long>> getContainerNumberOfStoreKeys() {
    return Collections.unmodifiableMap(containerNumberOfStoreKeys);
  }
}
