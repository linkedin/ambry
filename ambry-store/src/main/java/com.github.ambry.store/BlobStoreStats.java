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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
 * This is not thread safe for now as the {@link PersistentIndex} as such is not threadsafe.
 */
class BlobStoreStats implements StoreStats {
  private final Log log;
  private final PersistentIndex index;
  private final long capacityInBytes;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  BlobStoreStats(Log log, PersistentIndex index, long capacityInBytes, Time time) {
    this.log = log;
    this.index = index;
    this.capacityInBytes = capacityInBytes;
  }

  /**
   * Gets the size of valid data at at a particular point in time. The caller specifies a reference time and acceptable resolution
   * for the stats in the form of a {@code timeRange}. The store will return data for a point in time within the specified range.
   * @param timeRange the time range for the expected output. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the exact time at which the stats are valid and whose second element is the valid data
   * size
   */
  @Override
  public Pair<Long, Long> getValidDataSize(TimeRange timeRange) {
    try {
      return getValidDataSizeOfAllSegments(timeRange.getStart(), null);
    } catch (IOException e) {
      logger.error("IOException thrown while calculating valid data size ", e);
    } catch (StoreException e) {
      logger.error("StoreException thrown while calculating valid data size ", e);
    }
    return null;
  }

  /**
   * Gets the current used capacity of the {@link Store}.  Total bytes that are not available for new content are
   * considered to be used.
   * @return the used capacity of the {@link Store}
   */
  @Override
  public long getUsedCapacity() {
    return log.getUsedCapacity();
  }

  /**
   * Gets the total capacity of the {@link Store}.
   * @return the total capacity of the {@link Store}.
   */
  @Override
  public long getTotalCapacity() {
    return capacityInBytes;
  }

  /**
   * Gets the size of valid data at at a particular point in time for all log segments. The caller specifies a reference time and
   * acceptable resolution for the stats in the form of a {@code timeRange}. The store will return data for a point in time within
   * the specified range.
   * @param timeRange the time range for the expected output. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the exact time at which the stats are valid and whose second element is the valid data
   * size for each segment in the form of a {@link SortedMap} of segment names to valid data sizes.
   */
  Pair<Long, SortedMap<String, Long>> getValidDataSizeBySegment(TimeRange timeRange) throws IOException, StoreException {
    SortedMap<String, Long> validDataSize = new TreeMap<>();
    Pair<Long, Long> validDataSizeBySegment = getValidDataSizeOfAllSegments(timeRange.getStart(), validDataSize);
    return new Pair<>(validDataSizeBySegment.getFirst(), validDataSize);
  }

  /**
   * Gets the used capacity of each log segment. Total bytes that are not available for new content are considered
   * to be used.
   * @return a {@link SortedMap} of log segment name to the respective used capacity
   */
  SortedMap<String, Long> getUsedCapacityBySegment() {
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
    return usedCapacityBySegments;
  }

  /**
   * Derives valid data size for the store
   * @param timeRefInMs the time reference in Ms that is of interest at which valid data size is required
   * @param validDataSizeBySegments the {@link SortedMap} that needs to be updated with valid data size value
   *                                for every segment. Could be {@code null} if interested only in total valid data size
   * @return a {@link Pair} of reference time and total valid data size
   * @throws IOException
   * @throws StoreException
   */
  private Pair<Long, Long> getValidDataSizeOfAllSegments(long timeRefInMs, SortedMap<String, Long> validDataSizeBySegments)
      throws IOException, StoreException {
    long validDataSize = 0;
    long totalValidDataSize = 0;
    Offset lastEligibleStartOffset = getLastEligibleStartOffsetForDelete(timeRefInMs);
    IndexSegment indexSegment = null;
    for(Map.Entry<Offset,IndexSegment> indexSegmentEntry: index.indexes.entrySet()){
      if(indexSegmentEntry.getKey().compareTo(lastEligibleStartOffset) == -1){
        // index segment is considered valid
        IndexSegment nextIndexSegment = indexSegmentEntry.getValue();
        if (indexSegment == null || nextIndexSegment.getLogSegmentName().equals(indexSegment.getLogSegmentName())) {
          // index segment refers to same log segment as previous index segment
          validDataSize += getValidDataSizePerIndexSegment(nextIndexSegment,
              new FileSpan(nextIndexSegment.getStartOffset(), lastEligibleStartOffset), timeRefInMs);
        } else {
          // index segment refers to a new log segment compared to previous index segment
          String logSegmentName = indexSegment.getLogSegmentName();
          totalValidDataSize += validDataSize;
          if (validDataSizeBySegments != null) {
            validDataSizeBySegments.put(logSegmentName, validDataSize);
          }
          validDataSize = getValidDataSizePerIndexSegment(nextIndexSegment,
              new FileSpan(nextIndexSegment.getStartOffset(), lastEligibleStartOffset),  timeRefInMs);
        }
        indexSegment = nextIndexSegment;
      }
    }
    if (indexSegment != null) {
      String logSegmentName = indexSegment.getLogSegmentName();
      totalValidDataSize += validDataSize;
      if (validDataSizeBySegments != null) {
        validDataSizeBySegments.put(logSegmentName, validDataSize);
      }
    }
    return new Pair<>(timeRefInMs, totalValidDataSize);
  }


  private Offset getLastEligibleStartOffsetForDelete(long referenceTimeInMs) {
    Offset latestEligibleOffset = index.indexes.lastKey();
    for (IndexSegment indexSegment : index.indexes.values()) {
      if (indexSegment.getLastModifiedTime() > referenceTimeInMs) {
        latestEligibleOffset = indexSegment.getStartOffset();
        break;
      }
    }
    return latestEligibleOffset;
  }


  /**
   * Get valid data size of an index segment
   * @param indexSegment the {@link IndexSegment} for which valid data size has to be determined
   * @param forwardSearchSpan the {@link FileSpan} within which search has to be done to determine validity of data
   * @param referenceTime time in ms used as reference to check for expiration
   * @return the valid data size of the given index segment
   * @throws IOException
   * @throws StoreException
   */
  private long getValidDataSizePerIndexSegment(IndexSegment indexSegment, FileSpan forwardSearchSpan, long referenceTime)
      throws IOException, StoreException {
    long validSize = 0;
    List<MessageInfo> messageInfos = new ArrayList<>();
    indexSegment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), messageInfos, new AtomicLong(0));
    for(MessageInfo messageInfo: messageInfos) {
      IndexValue value = index.findKey(messageInfo.getStoreKey(), forwardSearchSpan);
      if (messageInfo.getExpirationTimeInMs() == Utils.Infinite_Time && !value.isFlagSet(
          IndexValue.Flags.Delete_Index)) {
        // put record w/o any expiration and not deleted
          validSize += messageInfo.getSize();
      } else if(!messageInfo.isDeleted() && (isExpired(messageInfo.getExpirationTimeInMs(), referenceTime) || value.isFlagSet(
          IndexValue.Flags.Delete_Index))){
        // a put record either expired or deleted(in future index segment) wrt reference time
         validSize += messageInfo.getSize();
      } else if (messageInfo.isDeleted()) {
        // delete record
        validSize += messageInfo.getSize();
      }
    }
    return validSize;
  }

  /**
   * Check if {@code expirationTimeInMs} has expired compared to {@code referenceTimeInMs}
   * @param expirationTimeInMs time in ms to be checked for expiration
   * @param referenceTimeInMs the epoch time to use to check for expiration
   * @return {@code true} if {@code expirationTimeInMs} expired wrt {@code referenceTimeInMs}, {@code false} otherwise
   */
  static boolean isExpired(long expirationTimeInMs, long referenceTimeInMs) {
    return expirationTimeInMs != Utils.Infinite_Time && referenceTimeInMs > expirationTimeInMs;
  }
}
