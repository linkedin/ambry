/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Exposes stats related to {@link BlobStore} that is useful to different components.
 *
 * Note: The initial implementation of this class will scan the indexes completely when any of the API calls are made.
 * It may not persist any data. Going forward, this will change. We may persist the data, collect stats more
 * "intelligently", actively push data from the {@link BlobStore} or any other multitude of things.
 */
class BlobStoreStats implements StoreStats {
  private final Log log;
  private final PersistentIndex index;
  private final long capacityInBytes;
  private final Time time;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  BlobStoreStats(Log log, PersistentIndex index, long capacityInBytes, Time time) {
    this.log = log;
    this.index = index;
    this.capacityInBytes = capacityInBytes;
    this.time = time;
  }

  /**
   * Gets the total size of valid data of the store
   * @return the valid data size
   */
  public Pair<Long, Long> getValidDataSize() {
    try {
      return getValidDataSize(null);
    } catch (IOException e) {
      logger.error("IOException thrown while calculating valid data size ", e);
    } catch (StoreException e) {
      logger.error("StoreException thrown while calculating valid data size ", e);
    }
    return null;
  }

  /**
   * Gets the current used capacity of the {@link Store}.
   * @return the used capacity of the {@link Store}
   */
  public long getUsedCapacity() {
    return log.getUsedCapacity();
  }

  /**
   * Gets the total capacity of the {@link Store}.
   * @return the total capacity of the {@link Store}.
   */
  public long getTotalCapacity() {
    return capacityInBytes;
  }

  /**
   * Gets the size of valid data of all log segments
   @return a {@link Pair} of reference time at which stats was collected to a {@link SortedMap} of log segment name
   to the respective valid data size
   */
  Pair<Long, SortedMap<String, Long>> getValidDataSizeBySegment() throws IOException, StoreException {
    SortedMap<String, Long> validDataSize = new TreeMap<>();
    Pair<Long, Long> validDataSizeBySegment = getValidDataSize(validDataSize);
    return new Pair<>(validDataSizeBySegment.getFirst(), validDataSize);
  }

  /**
   * Gets the used capacity of each log segment.
   * @return a {@link Pair} of reference time at which stats was collected to a {@link SortedMap} of log segment name
  to the respective used capacity
   */
  Pair<Long, SortedMap<String, Long>> getUsedCapacityBySegment() {
    long referenceTime = time.milliseconds();
    SortedMap<String, Long> usedCapacityBySegments = new TreeMap<>();
    Iterator<IndexSegment> indexSegmentIterator = index.indexes.values().iterator();
    IndexSegment indexSegment = null;
    if (indexSegmentIterator.hasNext()) {
      indexSegment = indexSegmentIterator.next();
    }
    while (indexSegmentIterator.hasNext()) {
      IndexSegment nextIndexSegment = indexSegmentIterator.next();
      if (!nextIndexSegment.getLogSegmentName().equals(indexSegment.getLogSegmentName())) {
        // index segment refers to a new log segment compared to previous index segment
        String logSegmentName = indexSegment.getLogSegmentName();
        usedCapacityBySegments.put(logSegmentName, log.getSegment(logSegmentName).getEndOffset());
        indexSegment = nextIndexSegment;
      }
    }
    if (index.indexes.size() > 1) {
      String logSegmentName = indexSegment.getLogSegmentName();
      usedCapacityBySegments.put(logSegmentName, log.getSegment(logSegmentName).getEndOffset());
    }
    return new Pair<>(referenceTime, usedCapacityBySegments);
  }

  /**
   * Derives valid data size for the store
   * @param validDataSizeBySegments the {@link SortedMap} that needs to be updated with valid data size value
   *                                for every segment. Could be {@code null} if interested only in total valid data size
   * @return a {@link Pair} of reference time and total valid data size
   * @throws IOException
   * @throws StoreException
   */
  private Pair<Long, Long> getValidDataSize(SortedMap<String, Long> validDataSizeBySegments)
      throws IOException, StoreException {
    long validDataSize = 0;
    long totalValidDataSize = 0;
    long referenceTime = time.milliseconds();
    Iterator<IndexSegment> indexSegmentIterator = index.indexes.values().iterator();
    IndexSegment indexSegment = null;
    if (indexSegmentIterator.hasNext()) {
      indexSegment = indexSegmentIterator.next();
      validDataSize += getValidDataSizePerIndexSegment(indexSegment, referenceTime);
    }
    while (indexSegmentIterator.hasNext()) {
      IndexSegment nextIndexSegment = indexSegmentIterator.next();
      if (nextIndexSegment.getLogSegmentName().equals(indexSegment.getLogSegmentName())) {
        // index segment refers to same log segment as previous index segment
        validDataSize += getValidDataSizePerIndexSegment(nextIndexSegment, referenceTime);
      } else {
        // index segment refers to a new log segment compared to previous index segment
        String logSegmentName = indexSegment.getLogSegmentName();
        totalValidDataSize += validDataSize;
        if (validDataSizeBySegments != null) {
          validDataSizeBySegments.put(logSegmentName, validDataSize);
        }
        indexSegment = nextIndexSegment;
        validDataSize = getValidDataSizePerIndexSegment(nextIndexSegment, referenceTime);
      }
    }
    if (index.indexes.size() > 1) {
      String logSegmentName = indexSegment.getLogSegmentName();
      totalValidDataSize += validDataSize;
      if (validDataSizeBySegments != null) {
        validDataSizeBySegments.put(logSegmentName, validDataSize);
      }
    }
    return new Pair<>(referenceTime, totalValidDataSize);
  }

  /**
   * Get valid data size of an index segment
   * @param indexSegment the {@link IndexSegment} for which valid data size has to be determined
   * @param referenceTimeInMs time in ms used as reference to check for expiration
   * @return the valid data size of the given index segment
   * @throws IOException
   * @throws StoreException
   */
  private long getValidDataSizePerIndexSegment(IndexSegment indexSegment, long referenceTimeInMs)
      throws IOException, StoreException {
    List<MessageInfo> messageInfos = new ArrayList<>();
    indexSegment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), messageInfos, new AtomicLong(0));
    long validSize = 0;
    for (MessageInfo messageInfo : messageInfos) {
      IndexValue value = index.findKey(messageInfo.getStoreKey());
      if (!PersistentIndex.isDeleted(value) && !isExpired(messageInfo.getExpirationTimeInMs(), referenceTimeInMs)) {
        validSize += messageInfo.getSize();
      }
    }
    return validSize;
  }

  /**
   * Check if {@code expirationTimeInMs} has passed {@code timeToCompareInMs} if not set to {@link Utils#Infinite_Time}
   * @param expirationTimeInMs time in ms to check if {@code timeToCompareInMs} has passed the value or not
   * @param timeToCompareInMs time in ms to be checked if {@code timeToCompareInMs} has passed {@code expirationTimeInMs}
   * @return {@code true} if {@code timeToCompareInMs} has passed {@code expirationTimeInMs}. {@code false} otherwise or
   * if {@code expirationTimeInMs} is set to {@link Utils#Infinite_Time}
   */
  private boolean isExpired(long expirationTimeInMs, long timeToCompareInMs) {
    return expirationTimeInMs != Utils.Infinite_Time && timeToCompareInMs > expirationTimeInMs;
  }
}
