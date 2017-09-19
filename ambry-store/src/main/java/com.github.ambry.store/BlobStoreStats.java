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

import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Exposes stats related to a {@link BlobStore} that is useful to different components.
 *
 * The {@link IndexScanner} implementation periodically scans the index and create buckets to help serve requests within
 * a forecast boundary. Stats related requests are either served via these buckets or a separate scan that will walk
 * through the entire index if the request is outside of the forecast boundary.
 */
class BlobStoreStats implements StoreStats, Closeable {
  static final String IO_SCHEDULER_JOB_TYPE = "BlobStoreStats";
  static final String IO_SCHEDULER_JOB_ID = "indexSegment_read";
  // Max blob size that's encountered while generating stats
  // TODO: make this dynamically generated
  private static final long MAX_BLOB_SIZE = 4 * 1024 * 1024;
  private static final int ADD = 1;
  private static final int SUBTRACT = -1;
  private static final long REF_TIME_OUT_OF_BOUNDS = -1;
  private static final Logger logger = LoggerFactory.getLogger(BlobStoreStats.class);

  private final String storeId;
  private final PersistentIndex index;
  private final Time time;
  private final DiskIOScheduler diskIOScheduler;
  private final boolean bucketingEnabled;
  private final int bucketCount;
  private final long bucketSpanTimeInMs;
  private final long logSegmentForecastOffsetMs;
  private final long waitTimeoutInSecs;
  private final StoreMetrics metrics;
  private final ReentrantLock scanLock = new ReentrantLock();
  private final Condition waitCondition = scanLock.newCondition();
  private final Queue<Pair<IndexValue, IndexValue>> recentEntryQueue = new LinkedBlockingQueue<>();
  private final AtomicInteger queueEntryCount = new AtomicInteger(0);
  private final AtomicBoolean enabled = new AtomicBoolean(true);

  private volatile boolean isScanning = false;
  private volatile boolean recentEntryQueueEnabled = false;
  private final AtomicReference<ScanResults> scanResults = new AtomicReference<>();
  private IndexScanner indexScanner;
  private QueueProcessor queueProcessor;

  /**
   * Convert a given nested {@link Map} of accountId to containerId to valid size to its corresponding
   * {@link StatsSnapshot} object.
   * @param quotaMap the nested {@link Map} to be converted
   * @return the corresponding {@link StatsSnapshot} object.
   */
  static StatsSnapshot convertQuotaToStatsSnapshot(Map<String, Map<String, Long>> quotaMap) {
    Map<String, StatsSnapshot> accountValidSizeMap = new HashMap<>();
    long totalSize = 0;
    for (Map.Entry<String, Map<String, Long>> accountEntry : quotaMap.entrySet()) {
      long subTotalSize = 0;
      Map<String, StatsSnapshot> containerValidSizeMap = new HashMap<>();
      for (Map.Entry<String, Long> containerEntry : accountEntry.getValue().entrySet()) {
        subTotalSize += containerEntry.getValue();
        containerValidSizeMap.put(containerEntry.getKey(), new StatsSnapshot(containerEntry.getValue(), null));
      }
      totalSize += subTotalSize;
      accountValidSizeMap.put(accountEntry.getKey(), new StatsSnapshot(subTotalSize, containerValidSizeMap));
    }
    return new StatsSnapshot(totalSize, accountValidSizeMap);
  }

  BlobStoreStats(String storeId, PersistentIndex index, int bucketCount, long bucketSpanTimeInMs,
      long logSegmentForecastOffsetMs, long queueProcessingPeriodInMs, long waitTimeoutInSecs, Time time,
      ScheduledExecutorService longLiveTaskScheduler, ScheduledExecutorService shortLiveTaskScheduler,
      DiskIOScheduler diskIOScheduler, StoreMetrics metrics) {
    this.storeId = storeId;
    this.index = index;
    this.time = time;
    this.diskIOScheduler = diskIOScheduler;
    this.bucketCount = bucketCount;
    this.bucketSpanTimeInMs = bucketSpanTimeInMs;
    this.logSegmentForecastOffsetMs = logSegmentForecastOffsetMs;
    this.waitTimeoutInSecs = waitTimeoutInSecs;
    this.metrics = metrics;
    bucketingEnabled = bucketCount > 0;

    if (bucketingEnabled) {
      indexScanner = new IndexScanner();
      longLiveTaskScheduler.scheduleAtFixedRate(indexScanner, 0,
          TimeUnit.MILLISECONDS.toSeconds(bucketCount * bucketSpanTimeInMs), TimeUnit.SECONDS);
      queueProcessor = new QueueProcessor();
      shortLiveTaskScheduler.scheduleAtFixedRate(queueProcessor, 0, queueProcessingPeriodInMs, TimeUnit.MILLISECONDS);
    }
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
   * {@inheritDoc}
   * Implementation in {@link BlobStoreStats} which returns the quota stats of a {@link BlobStore}. Size of delete
   * records are not accounted as valid data size here.
   */
  @Override
  public StatsSnapshot getStatsSnapshot(long referenceTimeInMs) throws StoreException {
    return convertQuotaToStatsSnapshot(getValidDataSizeByContainer(referenceTimeInMs));
  }

  /**
   * Returns the max blob size that is encountered while generating stats
   * @return the max blob size that is encountered while generating stats
   */
  long getMaxBlobSize() {
    return MAX_BLOB_SIZE;
  }

  /**
   * Gets the size of valid data at a particular point in time for all log segments. The caller specifies a reference
   * time and acceptable resolution for the stats in the form of a {@link TimeRange}. The store will return data
   * for a point in time within the specified range.
   * The following data are considered as valid data for this API:
   * 1. PUT with no expiry and no corresponding DELETE
   * 2. PUT expiring at t_exp but t_ref < t_exp
   * 3. PUT with corresponding DELETE at time t_delete but t_ref < t_delete
   * 4. DELETE record
   * For this API, t_ref is specified by the given {@link TimeRange}.
   * @param timeRange the reference {@link TimeRange} at which the data is requested. Defines both the reference time
   *                  and the acceptable resolution.
   * @return a {@link Pair} whose first element is the time at which stats was collected (in ms) and whose second
   * element is the valid data size for each segment in the form of a {@link NavigableMap} of segment names to
   * valid data sizes.
   * @throws StoreException
   */
  Pair<Long, NavigableMap<String, Long>> getValidDataSizeByLogSegment(TimeRange timeRange) throws StoreException {
    if (!enabled.get()) {
      throw new StoreException(String.format("BlobStoreStats is not enabled or closing for store %s", storeId),
          StoreErrorCodes.Store_Shutting_Down);
    }
    ScanResults currentScanResults = scanResults.get();
    Pair<Long, NavigableMap<String, Long>> retValue = null;
    long referenceTimeInMs = getLogSegmentRefTimeMs(currentScanResults, timeRange);
    if (referenceTimeInMs != REF_TIME_OUT_OF_BOUNDS) {
      retValue = currentScanResults.getValidSizePerLogSegment(referenceTimeInMs);
    } else {
      if (isScanning && getLogSegmentRefTimeMs(indexScanner.newScanResults, timeRange) != REF_TIME_OUT_OF_BOUNDS) {
        scanLock.lock();
        try {
          if (isScanning) {
            if (waitCondition.await(waitTimeoutInSecs, TimeUnit.SECONDS)) {
              currentScanResults = scanResults.get();
              referenceTimeInMs = getLogSegmentRefTimeMs(currentScanResults, timeRange);
              if (referenceTimeInMs != REF_TIME_OUT_OF_BOUNDS) {
                retValue = currentScanResults.getValidSizePerLogSegment(referenceTimeInMs);
              }
            } else {
              metrics.blobStoreStatsIndexScannerErrorCount.inc();
              logger.error("Timed out while waiting for BlobStoreStats index scan to complete for store {}", storeId);
            }
          } else {
            currentScanResults = scanResults.get();
            referenceTimeInMs = getLogSegmentRefTimeMs(currentScanResults, timeRange);
            if (referenceTimeInMs != REF_TIME_OUT_OF_BOUNDS) {
              retValue = currentScanResults.getValidSizePerLogSegment(referenceTimeInMs);
            }
          }
        } catch (InterruptedException e) {
          metrics.blobStoreStatsIndexScannerErrorCount.inc();
          throw new IllegalStateException(
              String.format("Illegal state, wait for scan to complete is interrupted for store %s", storeId), e);
        } finally {
          scanLock.unlock();
        }
      }
      if (retValue == null) {
        // retValue could be null in three scenarios:
        // 1. timeRange is outside of current forecast coverage and there is no ongoing scan.
        // 2. timed out while waiting for an ongoing scan.
        // 3. rare edge case where currentScanResults updated twice since the start of the wait.
        referenceTimeInMs = timeRange.getEndTimeInMs();
        retValue = new Pair<>(referenceTimeInMs, collectValidDataSizeByLogSegment(referenceTimeInMs));
      }
    }
    return retValue;
  }

  /**
   * Gets the size of valid data for all serviceIds and their containerIds as of now (the time when the API is called).
   * The following data are considered as valid data for this API:
   * 1. PUT with no expiry and no corresponding DELETE
   * 2. PUT expiring at t_exp but t_exp_ref < t_exp
   * 3. PUT with corresponding DELETE at time t_delete but t_del_ref < t_delete
   * For this API, t_ref is specified by the given reference time.
   * @param referenceTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return the valid data size for each container in the form of a nested {@link Map} of serviceIds to another map of
   * containerIds to valid data size.
   */
  Map<String, Map<String, Long>> getValidDataSizeByContainer(long referenceTimeInMs) throws StoreException {
    if (!enabled.get()) {
      throw new StoreException(String.format("BlobStoreStats is not enabled or closing for store %s", storeId),
          StoreErrorCodes.Store_Shutting_Down);
    }
    Map<String, Map<String, Long>> retValue = null;
    ScanResults currentScanResults = scanResults.get();
    if (currentScanResults != null && isWithinRange(currentScanResults.containerForecastStartTimeMs,
        currentScanResults.containerForecastEndTimeMs, referenceTimeInMs)) {
      retValue = currentScanResults.getValidSizePerContainer(referenceTimeInMs);
    } else {
      if (isScanning && isWithinRange(indexScanner.newScanResults.containerForecastStartTimeMs,
          indexScanner.newScanResults.containerForecastEndTimeMs, referenceTimeInMs)) {
        scanLock.lock();
        try {
          if (isScanning) {
            if (waitCondition.await(waitTimeoutInSecs, TimeUnit.SECONDS)) {
              currentScanResults = scanResults.get();
              if (isWithinRange(currentScanResults.containerForecastStartTimeMs,
                  currentScanResults.containerForecastEndTimeMs, referenceTimeInMs)) {
                retValue = currentScanResults.getValidSizePerContainer(referenceTimeInMs);
              }
            } else {
              metrics.blobStoreStatsIndexScannerErrorCount.inc();
              logger.error("Timed out while waiting for BlobStoreStats index scan to complete for store {}", storeId);
            }
          } else {
            currentScanResults = scanResults.get();
            if (isWithinRange(currentScanResults.containerForecastStartTimeMs,
                currentScanResults.containerForecastEndTimeMs, referenceTimeInMs)) {
              retValue = currentScanResults.getValidSizePerContainer(referenceTimeInMs);
            }
          }
        } catch (InterruptedException e) {
          metrics.blobStoreStatsIndexScannerErrorCount.inc();
          throw new IllegalStateException(
              String.format("Illegal state, wait for scan to complete is interrupted for store %s", storeId), e);
        } finally {
          scanLock.unlock();
        }
      }
      if (retValue == null) {
        // retValue could be null in three scenarios:
        // 1. referenceTimeInMs is outside of current forecast coverage and there is no ongoing scan.
        // 2. timed out while waiting for an ongoing scan.
        // 3. rare edge case where currentScanResults updated twice since the start of the wait.
        retValue = collectValidDataSizeByContainer(referenceTimeInMs);
      }
    }
    return retValue;
  }

  /**
   * Function that handles new PUT after a scan to keep the current {@link ScanResults} relevant.
   * @param putValue the {@link IndexValue} of the new PUT
   */
  void handleNewPutEntry(IndexValue putValue) {
    if (recentEntryQueueEnabled) {
      recentEntryQueue.offer(new Pair<IndexValue, IndexValue>(putValue, null));
      metrics.statsRecentEntryQueueSize.update(queueEntryCount.incrementAndGet());
    }
  }

  /**
   * Function that handles new DELETE after a scan to keep the current {@link ScanResults} relevant.
   * @param deleteValue the {@link IndexValue} of the new DELETE
   * @param originalPutValue the {@link IndexValue} of the original PUT that is getting deleted
   */
  void handleNewDeleteEntry(IndexValue deleteValue, IndexValue originalPutValue) {
    if (recentEntryQueueEnabled) {
      recentEntryQueue.offer(new Pair<>(deleteValue, originalPutValue));
      metrics.statsRecentEntryQueueSize.update(queueEntryCount.incrementAndGet());
    }
  }

  /**
   * Disable this {@link BlobStoreStats} and cancel any ongoing and scheduled index scan.
   * @throws InterruptedException
   */
  @Override
  public void close() {
    if (enabled.compareAndSet(true, false)) {
      if (indexScanner != null) {
        indexScanner.cancel();
      }
      if (queueProcessor != null) {
        queueProcessor.cancel();
      }
    }
  }

  /**
   * Walk through the entire index and collect valid data size information per container (delete records not included).
   * @param referenceTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return a nested {@link Map} of serviceId to containerId to valid data size
   */
  private Map<String, Map<String, Long>> collectValidDataSizeByContainer(long referenceTimeInMs) throws StoreException {
    logger.trace("On demand index scanning to collect container valid data sizes for store {} wrt ref time {}", storeId,
        referenceTimeInMs);
    long startTimeMs = time.milliseconds();
    Map<StoreKey, Long> deletedKeys = new HashMap<>();
    Map<String, Map<String, Long>> validDataSizePerContainer = new HashMap<>();
    int indexSegmentCount = 0;
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      if (!enabled.get()) {
        throw new StoreException(String.format("BlobStoreStats is not enabled or closing for store %s", storeId),
            StoreErrorCodes.Store_Shutting_Down);
      }
      long indexSegmentStartProcessTimeMs = time.milliseconds();
      List<IndexEntry> validIndexEntries =
          getValidIndexEntries(indexSegment, getIndexEntries(indexSegment), referenceTimeInMs, deletedKeys);
      for (IndexEntry indexEntry : validIndexEntries) {
        IndexValue indexValue = indexEntry.getValue();
        if (!indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
          // delete records does not count towards valid data size for quota (containers)
          updateNestedMapHelper(validDataSizePerContainer, String.valueOf(indexValue.getAccountId()),
              String.valueOf(indexValue.getContainerId()), indexValue.getSize());
        }
      }
      metrics.statsOnDemandScanTimePerIndexSegmentMs.update(time.milliseconds() - indexSegmentStartProcessTimeMs,
          TimeUnit.MILLISECONDS);
      indexSegmentCount++;
      if (indexSegmentCount == 1 || indexSegmentCount % 10 == 0) {
        logger.info("Container Stats: Index segment {} processing complete (on-demand scanning) for store {}",
            indexSegment.getFile().getName(), storeId);
      }
    }
    metrics.statsOnDemandScanTotalTimeMs.update(time.milliseconds() - startTimeMs, TimeUnit.MILLISECONDS);
    return validDataSizePerContainer;
  }

  /**
   * Walk through the entire index and collect valid size information per log segment (size of delete records included).
   * @param referenceTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return a {@link NavigableMap} of log segment name to valid data size
   */
  private NavigableMap<String, Long> collectValidDataSizeByLogSegment(long referenceTimeInMs) throws StoreException {
    logger.trace("On demand index scanning to collect compaction data stats for store {} wrt ref time {}", storeId,
        referenceTimeInMs);
    long startTimeMs = time.milliseconds();
    Map<StoreKey, Long> deletedKeys = new HashMap<>();
    NavigableMap<String, Long> validSizePerLogSegment = new TreeMap<>(LogSegmentNameHelper.COMPARATOR);
    int indexSegmentCount = 0;
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      if (!enabled.get()) {
        throw new StoreException(String.format("BlobStoreStats is not enabled or closing for store %s", storeId),
            StoreErrorCodes.Store_Shutting_Down);
      }
      long indexSegmentStartProcessTimeMs = time.milliseconds();
      List<IndexEntry> validIndexEntries =
          getValidIndexEntries(indexSegment, getIndexEntries(indexSegment), referenceTimeInMs, deletedKeys);
      for (IndexEntry indexEntry : validIndexEntries) {
        updateMapHelper(validSizePerLogSegment, indexSegment.getLogSegmentName(), indexEntry.getValue().getSize());
      }
      metrics.statsOnDemandScanTimePerIndexSegmentMs.update(time.milliseconds() - indexSegmentStartProcessTimeMs,
          TimeUnit.MILLISECONDS);
      indexSegmentCount++;
      if (indexSegmentCount == 1 || indexSegmentCount % 10 == 0) {
        logger.info("Compaction Stats: Index segment {} processing complete (on-demand scanning) for store {}",
            indexSegment.getFile().getName(), storeId);
      }
    }
    metrics.statsOnDemandScanTotalTimeMs.update(time.milliseconds() - startTimeMs, TimeUnit.MILLISECONDS);
    if (validSizePerLogSegment.isEmpty()) {
      validSizePerLogSegment.put(index.getStartOffset().getName(), 0L);
    }
    return validSizePerLogSegment;
  }

  /**
   * Get all {@link IndexEntry} in a given {@link IndexSegment}.
   * @param indexSegment the {@link IndexSegment} to fetch the index entries from
   * @return a {@link List} of all {@link IndexEntry} in the given {@link IndexSegment}
   * @throws StoreException
   */
  private List<IndexEntry> getIndexEntries(IndexSegment indexSegment) throws StoreException {
    List<IndexEntry> indexEntries = new ArrayList<>();
    try {
      indexSegment.getIndexEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), indexEntries,
          new AtomicLong(0));
      diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID,
          indexEntries.size());
    } catch (IOException e) {
      throw new StoreException(
          String.format("I/O exception while getting entries from index segment for store %s", storeId), e,
          StoreErrorCodes.IOError);
    }
    return indexEntries;
  }

  /**
   * Get a {@link List} of valid {@link IndexValue}s from a given {@link List} of {@link IndexEntry} that belong to the
   * same {@link IndexSegment}.
   * @param indexSegment the {@link IndexSegment} where the entries came from
   * @param indexEntries a {@link List} of unfiltered {@link IndexEntry} whose elements could be valid or invalid
   * @param referenceTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @param deletedKeys a {@link Map} of deleted keys to operation time. Used to determine whether a PUT is deleted
   * @return a {@link List} of valid {@link IndexValue}
   * @throws StoreException
   */
  private List<IndexEntry> getValidIndexEntries(IndexSegment indexSegment, List<IndexEntry> indexEntries,
      long referenceTimeInMs, Map<StoreKey, Long> deletedKeys) throws StoreException {
    List<IndexEntry> validIndexEntries = new ArrayList<>();
    for (IndexEntry indexEntry : indexEntries) {
      IndexValue indexValue = indexEntry.getValue();
      if (indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
        // delete record is always valid
        validIndexEntries.add(indexEntry);
        if (!isExpired(indexValue.getExpiresAtMs(), referenceTimeInMs)) {
          long operationTimeInMs =
              indexValue.getOperationTimeInMs() == Utils.Infinite_Time ? indexSegment.getLastModifiedTimeMs()
                  : indexValue.getOperationTimeInMs();
          deletedKeys.put(indexEntry.getKey(), operationTimeInMs);
          if (indexValue.getOriginalMessageOffset() != IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET
              && indexValue.getOriginalMessageOffset() != indexValue.getOffset().getOffset()
              && indexValue.getOriginalMessageOffset() >= indexSegment.getStartOffset().getOffset()
              && operationTimeInMs >= referenceTimeInMs) {
            // delete is not relevant yet and it's in the same index segment as the original put. We need to find the
            // original put since it's still valid
            IndexValue originalPutValue = getPutRecordForDeletedKey(indexEntry.getKey(), indexValue);
            validIndexEntries.add(new IndexEntry(indexEntry.getKey(), originalPutValue));
          }
        }
      } else if (!isExpired(indexValue.getExpiresAtMs(), referenceTimeInMs) && (
          !deletedKeys.containsKey(indexEntry.getKey()) || deletedKeys.get(indexEntry.getKey()) >= referenceTimeInMs)) {
        // put record that is not deleted and not expired according to given reference times
        validIndexEntries.add(indexEntry);
      }
    }
    return validIndexEntries;
  }

  /**
   * Given {@link ScanResults} and a {@link TimeRange}, try to find the latest point in time that is within the
   * {@link TimeRange} and the log segment forecast range.
   * @param results the {@link ScanResults} with the log segment forecast range information
   * @param timeRange the {@link TimeRange} used to find the latest shared point in time with the log segment forecast
   *                  range
   * @return the latest shared point in time in milliseconds if there is one, otherwise REF_TIME_OUT_OF_BOUNDS (-1)
   * is returned
   */
  private long getLogSegmentRefTimeMs(ScanResults results, TimeRange timeRange) {
    long refTimeInMs = results == null || timeRange.getStartTimeInMs() >= results.logSegmentForecastEndTimeMs
        || timeRange.getEndTimeInMs() < results.logSegmentForecastStartTimeMs ? REF_TIME_OUT_OF_BOUNDS
        : timeRange.getEndTimeInMs();
    if (refTimeInMs != REF_TIME_OUT_OF_BOUNDS && refTimeInMs >= results.logSegmentForecastEndTimeMs) {
      refTimeInMs = results.logSegmentLastBucketTimeMs;
    }
    return refTimeInMs;
  }

  /**
   * Find the original PUT that was deleted within the same index segment.
   * @param key the {@link StoreKey} for the entry
   * @param deleteIndexValue the {@link IndexValue} of the delete
   * @return a copy of the original put {@link IndexValue}
   * @throws StoreException
   */
  private IndexValue getPutRecordForDeletedKey(StoreKey key, IndexValue deleteIndexValue) throws StoreException {
    BlobReadOptions originalPut = index.getBlobReadInfo(key, EnumSet.allOf(StoreGetOptions.class));
    Offset originalPutOffset = new Offset(originalPut.getLogSegmentName(), originalPut.getOffset());
    return new IndexValue(originalPut.getMessageInfo().getSize(), originalPutOffset,
        originalPut.getMessageInfo().getExpirationTimeInMs(), deleteIndexValue.getOperationTimeInMs(),
        deleteIndexValue.getAccountId(), deleteIndexValue.getContainerId());
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

  /**
   * Helper function for container buckets for blob expiration/deletion related updates.
   * @param results the {@link ScanResults} to be updated
   * @param indexValue the PUT {@link IndexValue} of the expiring/deleting blob
   * @param expOrDelTimeInMs either the expiration or deletion time of the blob in ms
   * @param operator indicating the operator of the update, 1 for add and -1 for subtract
   */
  private void handleContainerBucketUpdate(ScanResults results, IndexValue indexValue, long expOrDelTimeInMs,
      int operator) {
    if (isWithinRange(results.containerForecastStartTimeMs, results.containerLastBucketTimeMs, expOrDelTimeInMs)) {
      results.updateContainerBucket(results.getContainerBucketKey(expOrDelTimeInMs),
          String.valueOf(indexValue.getAccountId()), String.valueOf(indexValue.getContainerId()),
          indexValue.getSize() * operator);
    }
  }

  /**
   * Helper function for log segment buckets for blob expiration/deletion related updates.
   * @param results the {@link ScanResults} to be updated
   * @param indexValue the PUT {@link IndexValue} of the expiring/deleting blob
   * @param expOrDelTimeInMs either the expiration or deletion time of the blob in ms
   * @param operator indicating the operator of the update, 1 for add and -1 for subtract
   */
  private void handleLogSegmentBucketUpdate(ScanResults results, IndexValue indexValue, long expOrDelTimeInMs,
      int operator) {
    if (isWithinRange(results.logSegmentForecastStartTimeMs, results.logSegmentLastBucketTimeMs, expOrDelTimeInMs)) {
      results.updateLogSegmentBucket(results.getLogSegmentBucketKey(expOrDelTimeInMs), indexValue.getOffset().getName(),
          indexValue.getSize() * operator);
    }
  }

  /**
   * Helper function to process new PUT entries and make appropriate updates to the given {@link ScanResults}.
   * @param results the {@link ScanResults} to apply the updates to
   * @param putValue the {@link IndexValue} of the new PUT
   */
  private void processNewPut(ScanResults results, IndexValue putValue) {
    long expiresAtMs = putValue.getExpiresAtMs();
    if (!isExpired(expiresAtMs, results.containerForecastStartTimeMs)) {
      results.updateContainerBaseBucket(String.valueOf(putValue.getAccountId()),
          String.valueOf(putValue.getContainerId()), putValue.getSize());
      if (expiresAtMs != Utils.Infinite_Time) {
        handleContainerBucketUpdate(results, putValue, expiresAtMs, SUBTRACT);
      }
    }
    if (!isExpired(expiresAtMs, results.logSegmentForecastStartTimeMs)) {
      results.updateLogSegmentBaseBucket(putValue.getOffset().getName(), putValue.getSize());
      if (expiresAtMs != Utils.Infinite_Time) {
        handleLogSegmentBucketUpdate(results, putValue, expiresAtMs, SUBTRACT);
      }
    }
  }

  /**
   * Helper function to process new DELETE entries and make appropriate updates to the given {@link ScanResults}.
   * @param results the {@link ScanResults} to apply the updates to
   * @param deleteValue the {@link IndexValue} of the new DELETE
   * @param originalPutValue the {@link IndexValue} of the original PUT that is getting deleted
   */
  private void processNewDelete(ScanResults results, IndexValue deleteValue, IndexValue originalPutValue) {
    long operationTimeInMs = deleteValue.getOperationTimeInMs();
    if (operationTimeInMs == Utils.Infinite_Time) {
      operationTimeInMs =
          index.getIndexSegments().floorEntry(deleteValue.getOffset()).getValue().getLastModifiedTimeMs();
    }
    results.updateLogSegmentBaseBucket(deleteValue.getOffset().getName(), deleteValue.getSize());
    if (!isExpired(originalPutValue.getExpiresAtMs(), operationTimeInMs)) {
      handleContainerBucketUpdate(results, originalPutValue, operationTimeInMs, SUBTRACT);
      handleLogSegmentBucketUpdate(results, originalPutValue, operationTimeInMs, SUBTRACT);
      if (originalPutValue.getExpiresAtMs() != Utils.Infinite_Time) {
        // make appropriate updates to avoid double counting
        handleContainerBucketUpdate(results, originalPutValue, originalPutValue.getExpiresAtMs(), ADD);
        handleLogSegmentBucketUpdate(results, originalPutValue, originalPutValue.getExpiresAtMs(), ADD);
      }
    }
  }

  /**
   * Process a {@link List} of valid {@link IndexEntry} based on forecast boundaries and populate corresponding
   * container buckets in the given {@link ScanResults}.
   * @param results the {@link ScanResults} to be populated
   * @param indexEntries a {@link List} of valid {@link IndexEntry} to be processed
   * @param deletedKeys a {@link Map} of deleted keys to their corresponding deletion time in ms
   */
  private void processEntriesForContainerBucket(ScanResults results, List<IndexEntry> indexEntries,
      Map<StoreKey, Long> deletedKeys) {
    for (IndexEntry indexEntry : indexEntries) {
      IndexValue indexValue = indexEntry.getValue();
      if (!indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
        // delete records does not count towards valid data size for quota (containers)
        results.updateContainerBaseBucket(String.valueOf(indexValue.getAccountId()),
            String.valueOf(indexValue.getContainerId()), indexValue.getSize());
        long expOrDelTimeInMs = indexValue.getExpiresAtMs();
        if (deletedKeys.containsKey(indexEntry.getKey())) {
          long deleteTimeInMs = deletedKeys.get(indexEntry.getKey());
          expOrDelTimeInMs =
              expOrDelTimeInMs != Utils.Infinite_Time && expOrDelTimeInMs < deleteTimeInMs ? expOrDelTimeInMs
                  : deleteTimeInMs;
        }
        if (expOrDelTimeInMs != Utils.Infinite_Time) {
          handleContainerBucketUpdate(results, indexValue, expOrDelTimeInMs, SUBTRACT);
        }
      }
    }
  }

  /**
   * Process a {@link List} of valid {@link IndexEntry} based on forecast boundaries and populate corresponding
   * log segment buckets in the given {@link ScanResults}.
   * @param results the {@link ScanResults} to be populated
   * @param indexEntries a {@link List} of valid {@link IndexEntry} to be processed
   * @param deletedKeys a {@link Map} of deleted keys to their corresponding deletion time in ms
   */
  private void processEntriesForLogSegmentBucket(ScanResults results, List<IndexEntry> indexEntries,
      Map<StoreKey, Long> deletedKeys) {
    for (IndexEntry indexEntry : indexEntries) {
      IndexValue indexValue = indexEntry.getValue();
      results.updateLogSegmentBaseBucket(indexValue.getOffset().getName(), indexValue.getSize());
      if (!indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
        // put records
        long expOrDelTimeInMs = indexValue.getExpiresAtMs();
        if (deletedKeys.containsKey(indexEntry.getKey())) {
          long deleteTimeInMs = deletedKeys.get(indexEntry.getKey());
          expOrDelTimeInMs =
              expOrDelTimeInMs != Utils.Infinite_Time && expOrDelTimeInMs < deleteTimeInMs ? expOrDelTimeInMs
                  : deleteTimeInMs;
        }
        handleLogSegmentBucketUpdate(results, indexValue, expOrDelTimeInMs, SUBTRACT);
      }
    }
  }

  /**
   * Determine if a given reference is within the boundary defined by the given start (inclusive) and end (exclusive)
   * @param start the start of the bound (inclusive)
   * @param end the end of the bound (exclusive)
   * @param reference the reference to check against the boundary defined by start and end
   * @return true if the reference is within the boundary, false otherwise
   */
  private boolean isWithinRange(long start, long end, long reference) {
    return start <= reference && reference < end;
  }

  /**
   * Runner that processes new entries buffered in the recentEntryQueue and make appropriate updates to keep the current
   * {@link ScanResults} relevant.
   */
  private class QueueProcessor implements Runnable {
    private volatile boolean cancelled = false;

    @Override
    public void run() {
      try {
        int entryCount;
        ScanResults currentScanResults = scanResults.get();
        // prevent new index entries getting dropped when IndexScanner is started before the QueueProcessor while
        // new index entries are being added. e.g. IndexScanner started and recorded the end offset as 100, meanwhile
        // new entries are added and IndexScanner is not finished yet. Before IndexScanner can finish, QueueProcessor
        // is started and it's going to process new entries with offset 90 to 120. Since IndexScanner is still in
        // progress the QueueProcessor will operate on the old ScanResults and index entries between 100 and
        // 120 will be missing for the next forecast period.
        if (isScanning || currentScanResults == null) {
          return;
        } else {
          entryCount = queueEntryCount.get();
        }
        long processStartTimeMs = time.milliseconds();
        for (int i = 0; i < entryCount && !cancelled; i++) {
          Pair<IndexValue, IndexValue> entry = recentEntryQueue.poll();
          if (entry == null) {
            throw new IllegalStateException(
                String.format("Invalid queue state in store %s. Expected entryCount %d, current index: %d", storeId,
                    entryCount, i));
          }
          metrics.statsRecentEntryQueueSize.update(queueEntryCount.decrementAndGet());
          IndexValue newValue = entry.getFirst();
          // prevent double counting new entries that were added after enabling the queue and just before the second
          // checkpoint is taken
          if (newValue.getOffset().compareTo(currentScanResults.scannedEndOffset) >= 0) {
            if (newValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
              // new delete
              processNewDelete(currentScanResults, newValue, entry.getSecond());
            } else {
              // new put
              processNewPut(currentScanResults, newValue);
            }
          }
        }
        metrics.statsRecentEntryQueueProcessTimeMs.update(time.milliseconds() - processStartTimeMs,
            TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        logger.error("Unexpected exception while running QueueProcessor in store {}", storeId, e);
        metrics.blobStoreStatsQueueProcessorErrorCount.inc();
      }
    }

    void cancel() {
      cancelled = true;
    }
  }

  /**
   * Runner that scans the entire index and populate a new {@link ScanResults} to start/extend the forecast coverage so
   * new requests can be answered using the created {@link ScanResults} instead of another scan of the index.
   */
  private class IndexScanner implements Runnable {
    private volatile boolean cancelled = false;
    volatile ScanResults newScanResults;
    long startTimeInMs;

    @Override
    public void run() {
      try {
        if (cancelled) {
          return;
        }
        logger.trace("IndexScanner triggered for store {}", storeId);
        recentEntryQueueEnabled = false;
        startTimeInMs = time.milliseconds();
        newScanResults = new ScanResults(startTimeInMs, logSegmentForecastOffsetMs, bucketCount, bucketSpanTimeInMs);
        scanLock.lock();
        try {
          isScanning = true;
        } finally {
          scanLock.unlock();
        }
        Offset firstCheckpoint = index.getCurrentEndOffset();
        logger.trace("First checkpoint by IndexScanner {} for store {}", firstCheckpoint, storeId);
        ConcurrentNavigableMap<Offset, IndexSegment> indexSegments = index.getIndexSegments();
        Map<StoreKey, Long> deletedKeys = new HashMap<>();
        // process the active index segment based on the firstCheckpoint in case index segment rolled over after the
        // checkpoint is taken
        if (!cancelled && indexSegments.size() > 0) {
          Map.Entry<Offset, IndexSegment> activeIndexSegmentEntry = indexSegments.floorEntry(firstCheckpoint);
          long indexSegmentStartProcessTime = time.milliseconds();
          logger.trace("Processing index entries in active segment {} before first checkpoint for store {}",
              activeIndexSegmentEntry.getValue().getFile().getName(), storeId);
          List<IndexEntry> activeIndexEntries =
              getIndexEntriesBeforeOffset(activeIndexSegmentEntry.getValue(), firstCheckpoint);
          processIndexSegmentEntriesBackward(activeIndexSegmentEntry.getValue(), activeIndexEntries, deletedKeys);
          metrics.statsBucketingScanTimePerIndexSegmentMs.update(time.milliseconds() - indexSegmentStartProcessTime,
              TimeUnit.MILLISECONDS);
          if (!cancelled && indexSegments.size() > 1) {
            ConcurrentNavigableMap<Offset, IndexSegment> sealedIndexSegments =
                indexSegments.subMap(index.getStartOffset(), activeIndexSegmentEntry.getKey());
            logger.trace("Sealed index segments count {} for store {}", sealedIndexSegments.size(), storeId);
            int segmentCount = 0;
            for (IndexSegment indexSegment : sealedIndexSegments.descendingMap().values()) {
              if (cancelled) {
                return;
              }
              indexSegmentStartProcessTime = time.milliseconds();
              List<IndexEntry> indexEntries = getIndexEntries(indexSegment);
              processIndexSegmentEntriesBackward(indexSegment, indexEntries, deletedKeys);
              metrics.statsBucketingScanTimePerIndexSegmentMs.update(time.milliseconds() - indexSegmentStartProcessTime,
                  TimeUnit.MILLISECONDS);
              segmentCount++;
              if (segmentCount == 1 || segmentCount % 10 == 0) {
                logger.info("IndexScanner: Completed scanning of sealed segment {} for store {}",
                    indexSegment.getFile().getName(), storeId);
              }
            }
          }
        } else {
          newScanResults.updateLogSegmentBaseBucket(index.getStartOffset().getName(), 0L);
        }
        recentEntryQueueEnabled = true;
        Offset secondCheckpoint = index.getCurrentEndOffset();
        logger.trace("Second checkpoint by IndexScanner {} for store {}", secondCheckpoint, storeId);
        if (secondCheckpoint.compareTo(firstCheckpoint) > 0) {
          forwardScan(firstCheckpoint, secondCheckpoint);
        }
        newScanResults.scannedEndOffset = secondCheckpoint;
        scanResults.set(newScanResults);
      } catch (Exception e) {
        logger.error("Exception thrown while scanning index for bucketing stats in store {}", storeId, e);
        metrics.blobStoreStatsIndexScannerErrorCount.inc();
      } finally {
        scanLock.lock();
        try {
          isScanning = false;
          waitCondition.signalAll();
        } finally {
          scanLock.unlock();
        }
        metrics.statsBucketingScanTotalTimeMs.update(time.milliseconds() - startTimeInMs, TimeUnit.MILLISECONDS);
      }
    }

    /**
     * Perform a forward scan (older to newest entries) from the given start offset to the given end offset. This
     * forward scan is used to process any new entries that were added to the index while scanning to the first
     * checkpoint.
     * @param startOffset the start offset defining the start of the scan range, inclusive (greater than or equal to)
     * @param endOffset the end offset defining the end of the scan range, exclusive (strictly less than)
     * @throws StoreException
     */
    private void forwardScan(Offset startOffset, Offset endOffset) throws StoreException {
      logger.trace("Forward scanning from {} to {} by IndexScanner for store {}", startOffset, endOffset, storeId);
      SortedMap<Offset, IndexSegment> tailIndexSegments =
          index.getIndexSegments().subMap(index.getIndexSegments().floorKey(startOffset), endOffset);
      int forwardScanEntryCount = 0;
      for (IndexSegment indexSegment : tailIndexSegments.values()) {
        if (cancelled) {
          return;
        }
        List<IndexEntry> indexEntries = getIndexEntries(indexSegment);
        forwardScanEntryCount += indexEntries.size();
        for (IndexEntry entry : indexEntries) {
          IndexValue indexValue = entry.getValue();
          if (indexValue.getOffset().compareTo(startOffset) >= 0 && indexValue.getOffset().compareTo(endOffset) < 0) {
            // index value is not yet processed and should be processed
            if (indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
              // delete record
              IndexValue originalPut;
              if (indexValue.getOriginalMessageOffset() == indexValue.getOffset().getOffset()) {
                // delete record with no put record due to compaction and legacy bugs
                originalPut = null;
              } else if (indexValue.getOriginalMessageOffset() != IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET
                  && indexValue.getOriginalMessageOffset() >= indexSegment.getStartOffset().getOffset()) {
                // delete and put are in the same index segment
                originalPut = getPutRecordForDeletedKey(entry.getKey(), indexValue);
                if (originalPut.getOffset().compareTo(startOffset) >= 0) {
                  processNewPut(newScanResults, originalPut);
                }
              } else {
                // ordinary delete record
                Offset previousIndexSegmentOffset = index.getIndexSegments().size() > 1 ? index.getIndexSegments()
                    .lowerKey(indexSegment.getStartOffset()) : index.getStartOffset();
                FileSpan searchSpan = new FileSpan(index.getStartOffset(), previousIndexSegmentOffset);
                originalPut = index.findKey(entry.getKey(), searchSpan);
              }
              if (originalPut == null) {
                newScanResults.updateLogSegmentBaseBucket(indexValue.getOffset().getName(), indexValue.getSize());
              } else if (!originalPut.isFlagSet(IndexValue.Flags.Delete_Index)) {
                // put exists and there are no multiple deletes
                processNewDelete(newScanResults, indexValue, originalPut);
              }
            } else {
              // put record
              processNewPut(newScanResults, indexValue);
            }
          }
        }
      }
      metrics.statsForwardScanEntryCount.update(forwardScanEntryCount);
    }

    /**
     * Process a {@link List} of {@link IndexEntry} belonging to the same {@link IndexSegment} to populate container and
     * log segment buckets. The function is called in reverse chronological order. That is, newest {@link IndexSegment}
     * to older ones.
     * @param indexSegment the {@link IndexSegment} where the index entries belong to
     * @param indexEntries a {@link List} of {@link IndexEntry} to be processed
     * @param deletedKeys a {@link Map} of processed deleted keys to their corresponding deletion time in ms
     * @throws StoreException
     */
    private void processIndexSegmentEntriesBackward(IndexSegment indexSegment, List<IndexEntry> indexEntries,
        Map<StoreKey, Long> deletedKeys) throws StoreException {
      logger.trace("Processing index entries backward by IndexScanner for segment {} for store {}",
          indexSegment.getFile().getName(), storeId);
      // valid index entries wrt container reference time
      List<IndexEntry> validIndexEntries =
          getValidIndexEntries(indexSegment, indexEntries, newScanResults.containerForecastStartTimeMs, deletedKeys);
      processEntriesForContainerBucket(newScanResults, validIndexEntries, deletedKeys);
      // valid index entries wrt log segment reference time
      validIndexEntries =
          getValidIndexEntries(indexSegment, indexEntries, newScanResults.logSegmentForecastStartTimeMs, deletedKeys);
      processEntriesForLogSegmentBucket(newScanResults, validIndexEntries, deletedKeys);
    }

    /**
     * Get a {@link List} of filtered {@link IndexEntry} from a given {@link IndexSegment} whose elements all have a
     * {@link Offset} that is strictly less than the given endOffset.
     * @param indexSegment the {@link IndexSegment} to get the index entries from
     * @param endOffset the {@link Offset} that defines the filter boundary
     * @return a {@link List} of {@link IndexEntry} whose elements all have a {@link Offset} that is strictly less than
     * the given endOffset
     * @throws StoreException
     */
    private List<IndexEntry> getIndexEntriesBeforeOffset(IndexSegment indexSegment, Offset endOffset)
        throws StoreException {
      List<IndexEntry> filteredIndexEntries = new ArrayList<>();
      List<IndexEntry> indexEntries = getIndexEntries(indexSegment);
      for (IndexEntry indexEntry : indexEntries) {
        if (indexEntry.getValue().getOffset().compareTo(endOffset) < 0) {
          filteredIndexEntries.add(indexEntry);
        }
      }
      return filteredIndexEntries;
    }

    void cancel() {
      cancelled = true;
    }
  }
}
