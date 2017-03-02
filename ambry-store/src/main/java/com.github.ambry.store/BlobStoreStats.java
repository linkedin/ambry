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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exposes stats related to a {@link BlobStore} that is useful to different components.
 *
 * Note: This is the v0 implementation of BlobStoreStats. The v0 implementation walks through the entire index
 * and collect data needed to serve stats related requests.
 */
public class BlobStoreStats implements StoreStats {
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
  public static String getServiceId(StoreKey key) {
    return key.getID().substring(0, 1);
  }

  // TODO remove this once #541 is merged
  public static String getContainerId(StoreKey key) {
    return key.getID().substring(key.getID().length() - 1);
  }

  @Override
  public Pair<Long, Long> getValidDataSize(TimeRange timeRange) throws StoreException {
    Pair<Long, Map<String, Long>> logSegmentValidSizeResult = getValidDataSizeByLogSegment(timeRange);
    Long allLogSegmentValidDataSize = 0L;
    for (Long value : logSegmentValidSizeResult.getSecond().values()) {
      allLogSegmentValidDataSize += value;
    }
    return new Pair<>(logSegmentValidSizeResult.getFirst(), allLogSegmentValidDataSize);
  }

  @Override
  public Pair<Long, Map<String, Long>> getValidDataSizeByLogSegment(TimeRange timeRange) throws StoreException {
    long referenceTimeInSecs = timeRange.getEndTimeInSecs();
    Map<String, Long> logSegmentValidSizeMap = collectValidDataSizeByLogSegment(referenceTimeInSecs * Time.MsPerSec);
    return new Pair<>(referenceTimeInSecs, logSegmentValidSizeMap);
  }

  @Override
  public Map<String, Map<String, Long>> getValidDataSize() throws StoreException {
    long referenceTimeInMs = time.milliseconds();
    return collectValidDataSizeByContainer(referenceTimeInMs);
  }

  /**
   * Walk through the entire index and collect valid data size information per container.
   * @param referenceTimeInMs the reference time to be used to decide whether or not a blob is valid
   * @return a nested {@link Map} of serviceId to containerId to valid data size
   */
  private Map<String, Map<String, Long>> collectValidDataSizeByContainer(long referenceTimeInMs)
      throws StoreException {
    Offset previousSegmentEndOffset = null;
    Offset indexStartOffset = index.getStartOffset();
    Map<String, Map<String, Long>> containerValidSizeMap = new HashMap<>();
    for (IndexSegment indexSegment : index.getIndexSegments().values()) {
      diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID, 1);
      List<MessageInfo> messageInfos = new ArrayList<>();
      try {
        indexSegment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), messageInfos, new AtomicLong(0));
      } catch (IOException e) {
        throw new StoreException("I/O error while collecting valid data size information", e, StoreErrorCodes.IOError);
      }
      Offset backwardSearchEndOffset =
          previousSegmentEndOffset == null ? indexSegment.getStartOffset() : previousSegmentEndOffset;
      FileSpan backwardSearchSpan = new FileSpan(indexStartOffset, backwardSearchEndOffset);
      for (MessageInfo messageInfo : messageInfos) {
        populateContainerMap(containerValidSizeMap, referenceTimeInMs, messageInfo, backwardSearchSpan);
      }
      previousSegmentEndOffset = indexSegment.getStartOffset();
    }

    return containerValidSizeMap;
  }

  /**
   * Walk through the entire index and collect valid data size information per log segment.
   * @param referenceTimeInMs the reference time to be used to decide whether or not a blob is valid
   * @return a {@link Map} of log segment name to valid data size
   */
  private Map<String, Long> collectValidDataSizeByLogSegment(long referenceTimeInMs)
      throws StoreException {
    Offset previousSegmentEndOffset = null;
    Offset indexStartOffset = index.getStartOffset();
    Map<String, Long> logSegmentValidSizeMap = new HashMap<>();
    for (IndexSegment indexSegment : index.getIndexSegments().values()) {
      diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID, 1);
      List<MessageInfo> messageInfos = new ArrayList<>();
      try {
        indexSegment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), messageInfos, new AtomicLong(0));
      } catch (IOException e) {
        throw new StoreException("I/O error while collecting valid data size information", e, StoreErrorCodes.IOError);
      }
      Offset backwardSearchEndOffset =
          previousSegmentEndOffset == null ? indexSegment.getStartOffset() : previousSegmentEndOffset;
      FileSpan backwardSearchSpan = new FileSpan(indexStartOffset, backwardSearchEndOffset);
      for (MessageInfo messageInfo : messageInfos) {
        populateLogSegmentMap(logSegmentValidSizeMap, referenceTimeInMs, messageInfo,
            indexSegment.getLastModifiedTime() * Time.MsPerSec, indexSegment.getLogSegmentName(), backwardSearchSpan);
      }
      previousSegmentEndOffset = indexSegment.getStartOffset();
    }
    return logSegmentValidSizeMap;
  }

  /**
   * Process a record in the index and populate the valid data size per container map.
   * @param containerValidSizeMap the valid data size per container map
   * @param referenceTimeInMs the reference time used to decide whether or not a record is valid
   * @param messageInfo the {@link MessageInfo} to be processed
   * @param backwardSearchSpan a {@link FileSpan} that spans from the beginning of the index to the beginning of the
   *                           previous index segment and is used for looking up corresponding put record when applicable
   * @throws StoreException
   */
  private void populateContainerMap(Map<String, Map<String, Long>> containerValidSizeMap, long referenceTimeInMs,
      MessageInfo messageInfo, FileSpan backwardSearchSpan) throws StoreException {

    if (messageInfo.isDeleted()) {
      // delete record
      IndexValue putIndexValue = index.findKey(messageInfo.getStoreKey(), backwardSearchSpan);
      if (putIndexValue != null && !putIndexValue.isFlagSet(IndexValue.Flags.Delete_Index) && !isExpired(
          putIndexValue.getExpiresAtMs(), referenceTimeInMs)) {
        // correct previously counted value only if the following conditions are true:
        // 1. the put and delete record are not in the same index segment (putIndexValue != null)
        // 2. the found record is actually a put record (!putIndexValue.isFlagSet)
        // 3. the found put record is not expired
        updateNestedMapHelper(containerValidSizeMap, BlobStoreStats.getServiceId(messageInfo.getStoreKey()),
            BlobStoreStats.getContainerId(messageInfo.getStoreKey()), putIndexValue.getSize() * -1);
      }
    } else if (!isExpired(messageInfo.getExpirationTimeInMs(), referenceTimeInMs)) {
      // put record that is not expired
      updateNestedMapHelper(containerValidSizeMap, BlobStoreStats.getServiceId(messageInfo.getStoreKey()),
          BlobStoreStats.getContainerId(messageInfo.getStoreKey()), messageInfo.getSize());
    }
  }

  /**
   * Process a record in the index and populate the valid data size per log segment map.
   * @param logSegmentValidSizeMap the valid data size per log segment map
   * @param referenceTimeInMs the reference time used to decide whether or not a record is valid
   * @param messageInfo the {@link MessageInfo} to be processed
   * @param lastModifiedTimeInMs the time stamp indicating when the record that is being processed was added
   * @param logSegmentName the log segment name of the record that is being processed
   * @param backwardSearchSpan a {@link FileSpan} that spans from the beginning of the index to the beginning of the
   *                           previous index segment and is used for looking up corresponding put record when applicable
   * @throws StoreException
   */
  private void populateLogSegmentMap(Map<String, Long> logSegmentValidSizeMap, long referenceTimeInMs,
      MessageInfo messageInfo, long lastModifiedTimeInMs, String logSegmentName, FileSpan backwardSearchSpan)
      throws StoreException {

    if (messageInfo.isDeleted()) {
      // delete record
      updateMapHelper(logSegmentValidSizeMap, logSegmentName, messageInfo.getSize());
      if (lastModifiedTimeInMs < referenceTimeInMs) {
        // delete that needs to be accounted for
        IndexValue putIndexValue = index.findKey(messageInfo.getStoreKey(), backwardSearchSpan);
        if (putIndexValue != null && !putIndexValue.isFlagSet(IndexValue.Flags.Delete_Index) && !isExpired(
            putIndexValue.getExpiresAtMs(), referenceTimeInMs)) {
          // correct previously counted value only if the following conditions are true:
          // 1. the put and delete record are not in the same index segment (putIndexValue != null)
          // 2. the found record is actually a put record (!putIndexValue.isFlagSet)
          // 3. the found put record is not expired
          updateMapHelper(logSegmentValidSizeMap, putIndexValue.getOffset().getName(), putIndexValue.getSize() * -1);
        }
      }
    } else if (!isExpired(messageInfo.getExpirationTimeInMs(), referenceTimeInMs)) {
      // put record that is not expired
      updateMapHelper(logSegmentValidSizeMap, logSegmentName, messageInfo.getSize());
    }
  }

  private boolean isExpired(long expirationTimeInMs, long referenceTimeInMs) {
    return expirationTimeInMs < referenceTimeInMs && expirationTimeInMs != Utils.Infinite_Time;
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
    Long existingValue = map.get(key);
    if (existingValue == null) {
      existingValue = value;
    } else {
      existingValue += value;
    }
    map.put(key, existingValue);
  }
}
