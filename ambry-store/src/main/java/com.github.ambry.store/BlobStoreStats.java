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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  // TODO remove this once #541 is merged
  static String getServiceId(StoreKey key) {
    return key.getID().substring(0, 1);
  }

  // TODO remove this once #541 is merged
  static String getContainerId(StoreKey key) {
    return key.getID().substring(key.getID().length() - 1);
  }

  @Override
  public Pair<Long, Long> getValidSize(TimeRange timeRange) throws StoreException {
    Pair<Long, Map<String, Long>> logSegmentValidSizeResult = getValidSizeByLogSegment(timeRange);
    Long totalValidSize = 0L;
    for (Long value : logSegmentValidSizeResult.getSecond().values()) {
      totalValidSize += value;
    }
    return new Pair<>(logSegmentValidSizeResult.getFirst(), totalValidSize);
  }

  /**
   * Gets the size of valid data at a particular point in time for all log segments. The caller specifies a reference time and
   * acceptable resolution for the stats in the form of a {@link TimeRange}. The store will return data for a point in time within
   * the specified range.
   * @param timeRange the reference {@link TimeRange} at which the data is requested. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the time at which stats was collected (in ms) and whose second element is the valid data
   * size for each segment in the form of a {@link Map} of segment names to valid data sizes.
   */
  Pair<Long, Map<String, Long>> getValidSizeByLogSegment(TimeRange timeRange) throws StoreException {
    long deleteReferenceTimeInMs = timeRange.getEndTimeInMs();
    long expirationReferenceTimeInMs = time.milliseconds();
    Map<String, Long> validSizePerLogSegment =
        collectValidSizeByLogSegment(deleteReferenceTimeInMs, expirationReferenceTimeInMs);
    return new Pair<>(deleteReferenceTimeInMs, validSizePerLogSegment);
  }

  /**
   * Gets the size of valid data for all serviceIds and their respective containerIds in the store as of now (the time when the API is called).
   * @return the valid data size for each container in the form of a nested {@link Map} of serviceIds to another map of
   * containerIds to valid data size.
   */
  Map<String, Map<String, Long>> getValidDataSizeByContainers() throws StoreException {
    long referenceTimeInMs = time.milliseconds();
    return collectValidDataSizeByContainer(referenceTimeInMs);
  }

  /**
   * Walk through the entire index and collect valid data size information per container.
   * @param expirationReferenceTimeInMs the reference time to be used to decide whether or not a blob is valid
   * @return a nested {@link Map} of serviceId to containerId to valid data size
   */
  private Map<String, Map<String, Long>> collectValidDataSizeByContainer(long expirationReferenceTimeInMs)
      throws StoreException {
    Set<String> deleteKeys = new HashSet<>();
    Map<String, Map<String, Long>> validDataSizePerContainer = new HashMap<>();
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      List<MessageInfo> messageInfos = getEntries(indexSegment);
      populateContainerMap(validDataSizePerContainer, expirationReferenceTimeInMs, messageInfos, deleteKeys);
    }
    return validDataSizePerContainer;
  }

  /**
   * Walk through the entire index and collect valid size information per log segment.
   * @param deleteReferenceTimeInMs the reference time to be used to decide whether or not a blob is deleted
   * @param expirationReferenceTimeInMs the reference time to be used to decide whether or not a blob is expired
   * @return a {@link Map} of log segment name to valid size
   */
  private Map<String, Long> collectValidSizeByLogSegment(long deleteReferenceTimeInMs, long expirationReferenceTimeInMs)
      throws StoreException {
    Set<String> deleteKeys = new HashSet<>();
    Map<String, Long> validSizePerLogSegment = new HashMap<>();
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      List<MessageInfo> messageInfos = getEntries(indexSegment);
      populateLogSegmentMap(validSizePerLogSegment, deleteReferenceTimeInMs, expirationReferenceTimeInMs, messageInfos,
          indexSegment.getLastModifiedTime() * Time.MsPerSec, indexSegment.getLogSegmentName(), deleteKeys);
    }
    return validSizePerLogSegment;
  }

  /**
   * Process a record in the index and populate the valid data size per container map.
   * @param validDataSizePerContainer the valid data size per container map
   * @param expirationReferenceTimeInMs the reference time used to decide whether or not a record is valid
   * @param messageInfos a list of {@link MessageInfo} to be processed
   * @param deleteKeys a {@link Set} of collected delete keys
   * @throws StoreException
   */
  private void populateContainerMap(Map<String, Map<String, Long>> validDataSizePerContainer,
      long expirationReferenceTimeInMs, List<MessageInfo> messageInfos, Set<String> deleteKeys) throws StoreException {
    for (MessageInfo messageInfo : messageInfos) {
      if (messageInfo.isDeleted()) {
        // delete record
        deleteKeys.add(messageInfo.getStoreKey().getID());
      } else if (!isExpired(messageInfo.getExpirationTimeInMs(), expirationReferenceTimeInMs) && !deleteKeys.contains(
          messageInfo.getStoreKey().getID())) {
        // put record that is not expired and not deleted
        updateNestedMapHelper(validDataSizePerContainer, BlobStoreStats.getServiceId(messageInfo.getStoreKey()),
            BlobStoreStats.getContainerId(messageInfo.getStoreKey()), messageInfo.getSize());
      }
    }
  }

  /**
   * Process a record in the index and populate the valid size per log segment map.
   * @param validSizePerLogSegment the valid size per log segment map
   * @param deleteReferenceTimeInMs the reference time to be used to decide whether or not a blob is deleted
   * @param expirationReferenceTimeInMs the reference time to be used to decide whether or not a blob is expired
   * @param messageInfos a list {@link MessageInfo} to be processed
   * @param lastModifiedTimeInMs the time stamp indicating when the record that is being processed was added
   * @param logSegmentName the log segment name of the record that is being processed
   * @param deleteKeys a {@link Set} of collected delete keys
   * @throws StoreException
   */
  private void populateLogSegmentMap(Map<String, Long> validSizePerLogSegment, long deleteReferenceTimeInMs,
      long expirationReferenceTimeInMs, List<MessageInfo> messageInfos, long lastModifiedTimeInMs,
      String logSegmentName, Set<String> deleteKeys) throws StoreException {
    for (MessageInfo messageInfo : messageInfos) {
      if (messageInfo.isDeleted()) {
        // delete record, always counted towards valid size
        updateMapHelper(validSizePerLogSegment, logSegmentName, messageInfo.getSize());
        if (lastModifiedTimeInMs < deleteReferenceTimeInMs) {
          // delete that needs to be accounted for
          deleteKeys.add(messageInfo.getStoreKey().getID());
        }
      } else if (!isExpired(messageInfo.getExpirationTimeInMs(), expirationReferenceTimeInMs) && !deleteKeys.contains(
          messageInfo.getStoreKey().getID())) {
        // put record that is not expired and not deleted according to the deleteReferenceTime
        updateMapHelper(validSizePerLogSegment, logSegmentName, messageInfo.getSize());
      }
    }
  }

  /**
   * Get all the {@link MessageInfo} in a given {@link IndexSegment}.
   * @param indexSegment the {@link IndexSegment} to be read from
   * @return a {@link List} of all {@link MessageInfo} in the given {@link IndexSegment}
   * @throws StoreException
   */
  private List<MessageInfo> getEntries(IndexSegment indexSegment) throws StoreException {
    diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID, 1);
    List<MessageInfo> messageInfos = new ArrayList<>();
    try {
      indexSegment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), messageInfos, new AtomicLong(0));
    } catch (IOException e) {
      throw new StoreException("I/O error while getting entries from index segment", e, StoreErrorCodes.IOError);
    }
    return messageInfos;
  }

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
    Long existingValue = map.containsKey(key) ? map.get(key) + value : value;
    map.put(key, existingValue);
  }
}
