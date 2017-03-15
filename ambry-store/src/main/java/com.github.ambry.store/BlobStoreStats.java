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
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Exposes stats related to a {@link BlobStore} that is useful to different components.
 *
 * Note: This is the v0 implementation of BlobStoreStats. The v0 implementation walks through the entire index
 * and collect data needed to serve stats related requests.
 */
class BlobStoreStats implements StoreStats {
  static final String IO_SCHEDULER_JOB_TYPE = "BlobStoreStats";
  static final String IO_SCHEDULER_JOB_ID = "indexSegment_read";

  private final PersistentIndex index;
  private final Time time;
  private final DiskIOScheduler diskIOScheduler;

  BlobStoreStats(PersistentIndex index, Time time, DiskIOScheduler diskIOScheduler) {
    this.index = index;
    this.time = time;
    this.diskIOScheduler = diskIOScheduler;
  }

  @Override
  public Pair<Long, Long> getValidSize(TimeRange timeRange) throws StoreException {
    Pair<Long, NavigableMap<String, Long>> logSegmentValidSizeResult = getValidDataSizeByLogSegment(timeRange);
    Long totalValidSize = 0L;
    for (Long value : logSegmentValidSizeResult.getSecond().values()) {
      totalValidSize += value;
    }
    return new Pair<>(logSegmentValidSizeResult.getFirst(), totalValidSize);
  }

  /**
   * Gets the size of valid data at a particular point in time for all log segments. The caller specifies a reference
   * time and acceptable resolution for the stats in the form of a {@link TimeRange}. The store will return data
   * for a point in time within the specified range.
   * The following data are considered as valid data for this API:
   * 1. PUT with no expiry and no corresponding DELETE
   * 2. PUT expiring at t_exp but t_exp_ref < t_exp
   * 3. PUT with corresponding DELETE at time t_delete but t_del_ref < t_delete
   * 4. DELETE record
   * For this API, t_del_ref is based on the given {@link TimeRange} and t_exp_ref is the time when the API is called.
   * @param timeRange the reference {@link TimeRange} at which the data is requested. Defines both the reference time
   *                  and the acceptable resolution.
   * @return a {@link Pair} whose first element is the time at which stats was collected (in ms) and whose second
   * element is the valid data size for each segment in the form of a {@link NavigableMap} of segment names to
   * valid data sizes.
   */
  Pair<Long, NavigableMap<String, Long>> getValidDataSizeByLogSegment(TimeRange timeRange) throws StoreException {
    long deleteReferenceTimeInMs = timeRange.getEndTimeInMs();
    long expirationReferenceTimeInMs = time.milliseconds();
    NavigableMap<String, Long> validSizePerLogSegment =
        collectValidDataSizeByLogSegment(deleteReferenceTimeInMs, expirationReferenceTimeInMs);
    return new Pair<>(deleteReferenceTimeInMs, validSizePerLogSegment);
  }

  /**
   * Gets the size of valid data for all serviceIds and their containerIds as of now (the time when the API is called).
   * The following data are considered as valid data for this API:
   * 1. PUT with no expiry and no corresponding DELETE
   * 2. PUT expiring at t_exp but t_exp_ref < t_exp
   * 3. PUT with corresponding DELETE at time t_delete but t_del_ref < t_delete
   * For this API, t_del_ref and t_exp_ref are the same and its value is when the API is called.
   * @return the valid data size for each container in the form of a nested {@link Map} of serviceIds to another map of
   * containerIds to valid data size.
   */
  Map<String, Map<String, Long>> getValidDataSizeByContainer() throws StoreException {
    long deleteAndExpirationRefTimeInMs = time.milliseconds();
    return collectValidDataSizeByContainer(deleteAndExpirationRefTimeInMs);
  }

  /**
   * Walk through the entire index and collect valid data size information per container (delete records not included).
   * @param deleteAndExpirationRefTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return a nested {@link Map} of serviceId to containerId to valid data size
   */
  private Map<String, Map<String, Long>> collectValidDataSizeByContainer(long deleteAndExpirationRefTimeInMs)
      throws StoreException {
    Set<StoreKey> deletedKeys = new HashSet<>();
    Map<String, Map<String, Long>> validDataSizePerContainer = new HashMap<>();
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      List<IndexValue> validIndexValues =
          getValidIndexValues(indexSegment, deleteAndExpirationRefTimeInMs, deleteAndExpirationRefTimeInMs,
              deletedKeys);
      for (IndexValue value : validIndexValues) {
        if (!value.isFlagSet(IndexValue.Flags.Delete_Index)) {
          // delete records does not count towards valid data size for quota (containers)
          updateNestedMapHelper(validDataSizePerContainer, String.valueOf(value.getServiceId()),
              String.valueOf(value.getContainerId()), value.getSize());
        }
      }
    }
    return validDataSizePerContainer;
  }

  /**
   * Walk through the entire index and collect valid size information per log segment (size of delete records included).
   * @param deleteReferenceTimeInMs the reference time in ms until which deletes are relevant
   * @param expirationReferenceTimeInMs the reference time in ms until which expiration are relevant
   * @return a {@link NavigableMap} of log segment name to valid data size
   */
  private NavigableMap<String, Long> collectValidDataSizeByLogSegment(long deleteReferenceTimeInMs,
      long expirationReferenceTimeInMs) throws StoreException {
    Set<StoreKey> deletedKeys = new HashSet<>();
    NavigableMap<String, Long> validSizePerLogSegment = new TreeMap<>();
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      List<IndexValue> validIndexValues =
          getValidIndexValues(indexSegment, deleteReferenceTimeInMs, expirationReferenceTimeInMs, deletedKeys);
      for (IndexValue value : validIndexValues) {
        updateMapHelper(validSizePerLogSegment, indexSegment.getLogSegmentName(), value.getSize());
      }
    }
    return validSizePerLogSegment;
  }

  /**
   * Get a {@link List} of valid {@link IndexValue} in a given {@link IndexSegment}.
   * @param indexSegment the indexSegment to be read from
   * @param deleteReferenceTimeInMs the reference time in ms until which deletes are relevant
   * @param expirationReferenceTimeInMs the reference time in ms until which expiration are relevant
   * @param deletedKeys a {@link Set} of deleted keys to help determine whether a put is deleted
   * @return a {@link List} of valid {@link IndexValue}
   * @throws StoreException
   */
  private List<IndexValue> getValidIndexValues(IndexSegment indexSegment, long deleteReferenceTimeInMs,
      long expirationReferenceTimeInMs, Set<StoreKey> deletedKeys) throws StoreException {
    diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID, 1);
    List<IndexEntry> indexEntries = new ArrayList<>();
    List<IndexValue> validIndexValues = new ArrayList<>();
    try {
      indexSegment.getIndexEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), indexEntries,
          new AtomicLong(0));
    } catch (IOException e) {
      throw new StoreException("I/O error while getting entries from index segment", e, StoreErrorCodes.IOError);
    }
    for (IndexEntry indexEntry : indexEntries) {
      IndexValue indexValue = indexEntry.getValue();
      if (indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
        // delete record
        validIndexValues.add(indexValue);
        long operationTimeInMs =
            indexValue.getOperationTimeInMs() == Utils.Infinite_Time ? indexSegment.getLastModifiedTimeMs()
                : indexValue.getOperationTimeInMs();
        if (operationTimeInMs < deleteReferenceTimeInMs) {
          // delete is relevant
          deletedKeys.add(indexEntry.getKey());
        } else if (!isExpired(indexValue.getExpiresAtMs(), expirationReferenceTimeInMs)
            && indexValue.getOriginalMessageOffset() != -1
            && indexValue.getOriginalMessageOffset() >= indexSegment.getStartOffset().getOffset()) {
          // delete is irrelevant but it's in the same index segment as the put and the put is still valid
          BlobReadOptions originalPut =
              index.getBlobReadInfo(indexEntry.getKey(), EnumSet.of(StoreGetOptions.Store_Include_Deleted));
          Offset originalPutOffset = new Offset(originalPut.getLogSegmentName(), originalPut.getOffset());
          validIndexValues.add(new IndexValue(originalPut.getSize(), originalPutOffset, originalPut.getExpiresAtMs(),
              Utils.Infinite_Time, indexValue.getServiceId(), indexValue.getContainerId()));
        }
      } else if (!isExpired(indexValue.getExpiresAtMs(), expirationReferenceTimeInMs) && !deletedKeys.contains(
          indexEntry.getKey())) {
        // put record that is not deleted and not expired according to deleteReferenceTimeInMs and expirationReferenceTimeInMs
        validIndexValues.add(indexValue);
      }
    }
    return validIndexValues;
  }

  /**
   * Determine whether a blob is expired or not given the expiration time and a reference time.
   * @param expirationTimeInMs the expiration time of the blob in ms
   * @param referenceTimeInMs the reference time in ms until which expiration are relevant
   * @return whether the blob is expired or not
   */
  private boolean isExpired(long expirationTimeInMs, long referenceTimeInMs) {
    return expirationTimeInMs != Utils.Infinite_Time && expirationTimeInMs < referenceTimeInMs;
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
