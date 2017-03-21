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
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Exposes stats related to a {@link BlobStore} that is useful to different components.
 *
 * Note: This is the v1 implementation of BlobStoreStats. The v1 implementation periodically scans the index and create
 * buckets to help serve requests within a forecast boundary. Stats related requests are either served via these buckets
 * or a separate scan will be triggered to walk through the entire index and collect the requested data if the request
 * is outside of the forecast boundary.
 */
class BlobStoreStats implements StoreStats {
  static final String IO_SCHEDULER_JOB_TYPE = "BlobStoreStats";
  static final String IO_SCHEDULER_JOB_ID = "indexSegment_read";
  // Max blob size thats encountered while generating stats
  // TODO: make this dynamically generated
  private static final long MAX_BLOB_SIZE = 4 * 1024 * 1024;
  private static final int ADD = 1;
  private static final int SUBTRACT = -1;
  private static final Logger logger = LoggerFactory.getLogger(BlobStoreStats.class);

  private final PersistentIndex index;
  private final Time time;
  private final DiskIOScheduler diskIOScheduler;
  private final boolean isBucketingEnabled;
  private final int bucketCount;
  private final long bucketSpanInMs;
  private final long logSegmentForecastOffsetMs;
  private final long queueProcessorPeriodInSecs;
  private final ScheduledExecutorService indexScannerAndQueueProcessorScheduler;
  private final StoreMetrics metrics;
  private final Object scanningLock = new Object();
  private final Queue<Pair<IndexValue, IndexValue>> newEntryQueue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger queueEntryCount = new AtomicInteger(0);

  private volatile boolean isScanning = false;
  private AtomicReference<ScanResults> scanResults = new AtomicReference<>();
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

  BlobStoreStats(PersistentIndex index, int bucketCount, long bucketSpanInMs, long logSegmentForecastOffsetSecs,
      long queueProcessorPeriodInSecs, Time time, ScheduledExecutorService scheduler, DiskIOScheduler diskIOScheduler,
      StoreMetrics metrics) {
    this.index = index;
    this.time = time;
    this.diskIOScheduler = diskIOScheduler;
    this.bucketCount = bucketCount;
    this.bucketSpanInMs = bucketSpanInMs;
    this.logSegmentForecastOffsetMs = logSegmentForecastOffsetSecs * Time.MsPerSec;
    this.queueProcessorPeriodInSecs = queueProcessorPeriodInSecs;
    this.metrics = metrics;
    isBucketingEnabled = bucketCount > 0;
    indexScannerAndQueueProcessorScheduler = scheduler;
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
   * The implementation in {@link BlobStoreStats} returns quota related stats of a {@link BlobStore}.
   */
  @Override
  public StatsSnapshot getStatsSnapshot(long deleteAndExpirationRefTimeInMs) throws StoreException {
    return convertQuotaToStatsSnapshot(getValidDataSizeByContainer(deleteAndExpirationRefTimeInMs));
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
   * 2. PUT expiring at t_exp but t_exp_ref < t_exp
   * 3. PUT with corresponding DELETE at time t_delete but t_del_ref < t_delete
   * 4. DELETE record
   * For this API, t_del_ref and t_exp_ref are the same and its value is based on the given {@link TimeRange}.
   * @param timeRange the reference {@link TimeRange} at which the data is requested. Defines both the reference time
   *                  and the acceptable resolution.
   * @return a {@link Pair} whose first element is the time at which stats was collected (in ms) and whose second
   * element is the valid data size for each segment in the form of a {@link NavigableMap} of segment names to
   * valid data sizes.
   */
  Pair<Long, NavigableMap<String, Long>> getValidDataSizeByLogSegment(TimeRange timeRange) throws StoreException {
    ScanResults currentScanResults = scanResults.get();
    Pair<Long, NavigableMap<String, Long>> retValue = null;
    long deleteAndExpirationRefTimeInMs = getLogSegmentRefTimeMs(currentScanResults, timeRange);
    if (deleteAndExpirationRefTimeInMs != -1) {
      retValue = currentScanResults.getValidSizePerLogSegment(deleteAndExpirationRefTimeInMs);
    }
    if (retValue == null) {
      if (isScanning) {
        deleteAndExpirationRefTimeInMs = getLogSegmentRefTimeMs(indexScanner.newScanResults, timeRange);
        if (deleteAndExpirationRefTimeInMs != -1) {
          synchronized (scanningLock) {
            while (isScanning) {
              try {
                scanningLock.wait();
              } catch (InterruptedException e) {
                // no-op
              }
            }
          }
          retValue = getValidDataSizeByLogSegment(timeRange);
        }
      }
      if (retValue == null) {
        deleteAndExpirationRefTimeInMs = timeRange.getEndTimeInMs();
        retValue = new Pair<>(deleteAndExpirationRefTimeInMs,
            collectValidDataSizeByLogSegment(deleteAndExpirationRefTimeInMs));
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
   * For this API, t_del_ref and t_exp_ref are the same and its value is when the API is called.
   * @param deleteAndExpirationRefTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return the valid data size for each container in the form of a nested {@link Map} of serviceIds to another map of
   * containerIds to valid data size.
   */
  Map<String, Map<String, Long>> getValidDataSizeByContainer(long deleteAndExpirationRefTimeInMs)
      throws StoreException {
    Map<String, Map<String, Long>> retValue = null;
    ScanResults currentScanResults = scanResults.get();
    if (currentScanResults != null && isWithinRange(currentScanResults.containerForecastStartTimeInMs,
        currentScanResults.containerForecastEndTimeInMs, deleteAndExpirationRefTimeInMs)) {
      retValue = currentScanResults.getValidSizePerContainer(deleteAndExpirationRefTimeInMs);
    }
    if (retValue == null) {
      if (isScanning && isWithinRange(indexScanner.newScanResults.containerForecastStartTimeInMs,
          indexScanner.newScanResults.containerForecastEndTimeInMs, deleteAndExpirationRefTimeInMs)) {
        synchronized (scanningLock) {
          while (isScanning) {
            try {
              scanningLock.wait();
            } catch (InterruptedException e) {
              // no-op
            }
          }
        }
        retValue = getValidDataSizeByContainer(deleteAndExpirationRefTimeInMs);
      } else {
        retValue = collectValidDataSizeByContainer(deleteAndExpirationRefTimeInMs);
      }
    }
    return retValue;
  }

  /**
   * Function that handles new PUT after a scan to keep the current {@link ScanResults} relevant.
   * @param putValue the {@link IndexValue} of the new PUT
   */
  void handleNewPut(IndexValue putValue) {
    if (isBucketingEnabled) {
      newEntryQueue.offer(new Pair<IndexValue, IndexValue>(putValue, null));
      queueEntryCount.incrementAndGet();
    }
  }

  /**
   * Function that handles new DELETE after a scan to keep the current {@link ScanResults} relevant.
   * @param deleteValue the {@link IndexValue} of the new DELETE
   * @param originalPutValue the {@link IndexValue} of the original PUT that is getting deleted
   */
  void handleNewDelete(IndexValue deleteValue, IndexValue originalPutValue) {
    if (isBucketingEnabled) {
      newEntryQueue.offer(new Pair<IndexValue, IndexValue>(deleteValue, originalPutValue));
      queueEntryCount.incrementAndGet();
    }
  }

  void start() {
    if (isBucketingEnabled) {
      indexScanner = new IndexScanner();
      int futureBucketCount = bucketCount > 1 ? bucketCount - 1 : 1;
      indexScannerAndQueueProcessorScheduler.scheduleAtFixedRate(indexScanner, 0,
          futureBucketCount * (bucketSpanInMs / Time.MsPerSec), TimeUnit.SECONDS);
      queueProcessor = new QueueProcessor();
      indexScannerAndQueueProcessorScheduler.scheduleAtFixedRate(queueProcessor, 0, queueProcessorPeriodInSecs,
          TimeUnit.SECONDS);
    }
  }

  void shutdown() throws InterruptedException {
    if (indexScanner != null) {
      indexScanner.cancel();
    }
    if (queueProcessor != null) {
      queueProcessor.cancel();
    }
    if (indexScannerAndQueueProcessorScheduler != null) {
      indexScannerAndQueueProcessorScheduler.shutdown();
      if (!indexScannerAndQueueProcessorScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.error("Could not terminate index scanner task after BlobStoreStats shutdown");
        metrics.blobStoreStatsErrorCount.inc();
      }
    }
  }

  /**
   * Walk through the entire index and collect valid data size information per container (delete records not included).
   * @param deleteAndExpirationRefTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return a nested {@link Map} of serviceId to containerId to valid data size
   */
  private Map<String, Map<String, Long>> collectValidDataSizeByContainer(long deleteAndExpirationRefTimeInMs)
      throws StoreException {
    Map<StoreKey, Long> deletedKeys = new HashMap<>();
    Map<String, Map<String, Long>> validDataSizePerContainer = new HashMap<>();
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      long indexSegmentStartProcessTimeMs = time.milliseconds();
      List<IndexEntry> validIndexEntries =
          getValidIndexEntries(indexSegment, getIndexEntries(indexSegment), deleteAndExpirationRefTimeInMs,
              deletedKeys);
      for (IndexEntry indexEntry : validIndexEntries) {
        IndexValue indexValue = indexEntry.getValue();
        if (!indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
          // delete records does not count towards valid data size for quota (containers)
          updateNestedMapHelper(validDataSizePerContainer, String.valueOf(indexValue.getServiceId()),
              String.valueOf(indexValue.getContainerId()), indexValue.getSize());
        }
      }
      metrics.statsOnDemandScanTimePerIndexSegmentMs.update(time.milliseconds() - indexSegmentStartProcessTimeMs);
    }
    metrics.statsOnDemandScanCount.inc();
    return validDataSizePerContainer;
  }

  /**
   * Walk through the entire index and collect valid size information per log segment (size of delete records included).
   * @param deleteAndExpirationRefTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return a {@link NavigableMap} of log segment name to valid data size
   */
  private NavigableMap<String, Long> collectValidDataSizeByLogSegment(long deleteAndExpirationRefTimeInMs)
      throws StoreException {
    Map<StoreKey, Long> deletedKeys = new HashMap<>();
    NavigableMap<String, Long> validSizePerLogSegment = new TreeMap<>();
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      long indexSegmentStartProcessTimeMs = time.milliseconds();
      List<IndexEntry> validIndexEntries =
          getValidIndexEntries(indexSegment, getIndexEntries(indexSegment), deleteAndExpirationRefTimeInMs,
              deletedKeys);
      for (IndexEntry indexEntry : validIndexEntries) {
        updateMapHelper(validSizePerLogSegment, indexSegment.getLogSegmentName(), indexEntry.getValue().getSize());
      }
      metrics.statsOnDemandScanTimePerIndexSegmentMs.update(time.milliseconds() - indexSegmentStartProcessTimeMs);
    }
    metrics.statsOnDemandScanCount.inc();
    return validSizePerLogSegment;
  }

  /**
   * Get all {@link IndexEntry} in a given {@link IndexSegment}.
   * @param indexSegment the {@link IndexSegment} to fetch the index entries from
   * @return a {@link List} of all {@link IndexEntry} in the given {@link IndexSegment}
   * @throws StoreException
   */
  private List<IndexEntry> getIndexEntries(IndexSegment indexSegment) throws StoreException {
    diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID, 1);
    List<IndexEntry> indexEntries = new ArrayList<>();
    try {
      indexSegment.getIndexEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), indexEntries,
          new AtomicLong(0));
    } catch (IOException e) {
      throw new StoreException("I/O exception while getting entries from index segment", e, StoreErrorCodes.IOError);
    }
    return indexEntries;
  }

  /**
   * Get a {@link List} of valid {@link IndexValue}s from a given {@link List} of {@link IndexEntry} that belong to the
   * same {@link IndexSegment}.
   * @param indexSegment the {@link IndexSegment} where the entries came from
   * @param indexEntries a {@link List} of unfiltered {@link IndexEntry} whose elements could be valid or invalid
   * @param deleteAndExpirationRefTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @param deletedKeys a {@link Map} of deleted keys to operation time. Used to determine whether a PUT is deleted
   * @return a {@link List} of valid {@link IndexValue}
   * @throws StoreException
   */
  private List<IndexEntry> getValidIndexEntries(IndexSegment indexSegment, List<IndexEntry> indexEntries,
      long deleteAndExpirationRefTimeInMs, Map<StoreKey, Long> deletedKeys) throws StoreException {
    List<IndexEntry> validIndexEntries = new ArrayList<>();
    for (IndexEntry indexEntry : indexEntries) {
      IndexValue indexValue = indexEntry.getValue();
      if (indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
        // delete record is always valid
        validIndexEntries.add(indexEntry);
        if (isExpired(indexValue.getExpiresAtMs(), deleteAndExpirationRefTimeInMs)) {
          continue;
        }
        long operationTimeInMs =
            indexValue.getOperationTimeInMs() == Utils.Infinite_Time ? indexSegment.getLastModifiedTimeMs()
                : indexValue.getOperationTimeInMs();
        deletedKeys.put(indexEntry.getKey(), operationTimeInMs);
        if (indexValue.getOriginalMessageOffset() != -1
            && indexValue.getOriginalMessageOffset() >= indexSegment.getStartOffset().getOffset()
            && operationTimeInMs >= deleteAndExpirationRefTimeInMs) {
          // delete is irrelevant but it's in the same index segment as the put and the put is still valid
          IndexValue originalPutValue = getDeletedPut(indexEntry.getKey(), indexValue);
          validIndexEntries.add(new IndexEntry(indexEntry.getKey(), originalPutValue));
        }
      } else if (!isExpired(indexValue.getExpiresAtMs(), deleteAndExpirationRefTimeInMs) && (
          !deletedKeys.containsKey(indexEntry.getKey())
              || deletedKeys.get(indexEntry.getKey()) >= deleteAndExpirationRefTimeInMs)) {
        // put record that is not deleted and not expired according to given reference times
        validIndexEntries.add(indexEntry);
      }
    }
    return validIndexEntries;
  }

  /**
   * Given {@link ScanResults} and a {@link TimeRange}, try to find the largest point in time that is within the
   * {@link TimeRange} and the log segment forecast range.
   * @param results the {@link ScanResults} with the log segment forecast range information
   * @param timeRange the {@link TimeRange} used to find the largest shared point in time with the log segment forecast
   *                  range
   * @return the largest shared point in time in milliseconds if there is one, otherwise -1 is returned
   */
  private long getLogSegmentRefTimeMs(ScanResults results, TimeRange timeRange) {
    long refTimeInMs = results == null || timeRange.getStartTimeInMs() >= results.logSegmentForecastEndTimeInMs
        || timeRange.getEndTimeInMs() < results.logSegmentForecastStartTimeInMs ? -1 : timeRange.getEndTimeInMs();
    if (refTimeInMs != -1 && refTimeInMs >= results.logSegmentForecastEndTimeInMs) {
      refTimeInMs = results.logSegmentLastBucketEndTimeInMs;
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
  private IndexValue getDeletedPut(StoreKey key, IndexValue deleteIndexValue) throws StoreException {
    BlobReadOptions originalPut = index.getBlobReadInfo(key, EnumSet.of(StoreGetOptions.Store_Include_Deleted));
    Offset originalPutOffset = new Offset(originalPut.getLogSegmentName(), originalPut.getOffset());
    return new IndexValue(originalPut.getSize(), originalPutOffset, originalPut.getExpiresAtMs(), Utils.Infinite_Time,
        deleteIndexValue.getServiceId(), deleteIndexValue.getContainerId());
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
    if (isWithinRange(results.containerForecastStartTimeInMs, results.containerLastBucketEndTimeInMs,
        expOrDelTimeInMs)) {
      results.updateContainerBucket(results.getContainerBucketKey(expOrDelTimeInMs),
          String.valueOf(indexValue.getServiceId()), String.valueOf(indexValue.getContainerId()),
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
    if (isWithinRange(results.logSegmentForecastStartTimeInMs, results.logSegmentLastBucketEndTimeInMs,
        expOrDelTimeInMs)) {
      results.updateLogSegmentBucket(results.getLogSegmentBucketKey(expOrDelTimeInMs), indexValue.getOffset().getName(),
          indexValue.getSize() * operator);
    }
  }

  /**
   * Helper function to process new PUT entries and make appropriate updates to current {@link ScanResults}.
   * @param results the {@link ScanResults} to apply the updates to
   * @param putValue the {@link IndexValue} of the new PUT
   */
  private void processNewPut(ScanResults results, IndexValue putValue) {
    results.updateContainerBucket(results.containerForecastStartTimeInMs, String.valueOf(putValue.getServiceId()),
        String.valueOf(putValue.getContainerId()), putValue.getSize());
    results.updateLogSegmentBucket(results.logSegmentForecastStartTimeInMs, putValue.getOffset().getName(),
        putValue.getSize());
    handleContainerBucketUpdate(results, putValue, putValue.getExpiresAtMs(), SUBTRACT);
    handleLogSegmentBucketUpdate(results, putValue, putValue.getExpiresAtMs(), SUBTRACT);
  }

  /**
   * Helper function to process new DELETE entries and make appropriate updates to current {@link ScanResults}.
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
    results.updateLogSegmentBucket(results.logSegmentForecastStartTimeInMs, deleteValue.getOffset().getName(),
        deleteValue.getSize());
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
        // put records, delete records does not count towards valid data size for quota (containers)
        results.updateContainerBucket(results.containerForecastStartTimeInMs, String.valueOf(indexValue.getServiceId()),
            String.valueOf(indexValue.getContainerId()), indexValue.getSize());
        long expOrDelTimeInMs = indexValue.getExpiresAtMs();
        if (deletedKeys.containsKey(indexEntry.getKey())) {
          long deleteTimeInMs = deletedKeys.get(indexEntry.getKey());
          expOrDelTimeInMs =
              expOrDelTimeInMs != Utils.Infinite_Time && expOrDelTimeInMs < deleteTimeInMs ? expOrDelTimeInMs
                  : deleteTimeInMs;
        }
        handleContainerBucketUpdate(results, indexValue, expOrDelTimeInMs, SUBTRACT);
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
      results.updateLogSegmentBucket(results.logSegmentForecastStartTimeInMs, indexValue.getOffset().getName(),
          indexValue.getSize());
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
   * Runner that processes new entries buffered in the newEntryQueue and make appropriate updates to keep the current
   * {@link ScanResults} relevant.
   */
  private class QueueProcessor implements Runnable {
    private volatile boolean cancelled = false;

    @Override
    public void run() {
      try {
        int entryCount;
        ScanResults currentScanResults = scanResults.get();
        synchronized (scanningLock) {
          // prevent new index entries getting dropped when IndexScanner is started right before the QueueProcessor while
          // new index entries are being added. e.g. IndexScanner started and recorded the end offset as 100, meanwhile
          // new entries are being added and IndexScanner is taking a while to finish. Before IndexScanner can finish,
          // QueueProcessor is started and it's going to process new entries with offset 90 to 120. Since IndexScanner is
          // still in progress the QueueProcessor will operate on the old ScanResults and index entries between 100 and
          // 120 will be missing for the next forecast period.
          if (isScanning || currentScanResults == null) {
            return;
          } else {
            entryCount = queueEntryCount.get();
          }
        }
        for (int i = 0; i < entryCount; i++) {
          if (cancelled) {
            return;
          }
          Pair<IndexValue, IndexValue> entry = newEntryQueue.poll();
          if (entry == null) {
            throw new IllegalStateException(
                "Invalid queue state. Expected entryCount " + entryCount + " current index: " + i);
          }
          queueEntryCount.decrementAndGet();
          IndexValue newValue = entry.getFirst();
          // prevent double counting new entries that were added just before a scan and after a queue processor cycle
          if (newValue.getOffset().compareTo(currentScanResults.lastScannedOffset) >= 0) {
            if (newValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
              // new delete
              processNewDelete(currentScanResults, newValue, entry.getSecond());
            } else {
              // new put
              processNewPut(currentScanResults, newValue);
            }
          }
        }
      } catch (Exception e) {
        logger.error("Unexpected exception while running QueueProcessor", e);
        metrics.blobStoreStatsErrorCount.inc();
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
        startTimeInMs = time.milliseconds();
        newScanResults = new ScanResults(startTimeInMs, logSegmentForecastOffsetMs, bucketCount, bucketSpanInMs);
        synchronized (scanningLock) {
          isScanning = true;
        }
        Offset checkpoint = index.getCurrentEndOffset();
        ConcurrentNavigableMap<Offset, IndexSegment> indexSegments = index.getIndexSegments();
        Map<StoreKey, Long> deletedKeys = new HashMap<>();
        if (indexSegments.size() > 0) {
          // taken this way to prevent double counting when index segment rolled over after taking the checkpoint
          IndexSegment activeIndexSegment = indexSegments.get(indexSegments.lowerKey(checkpoint));
          long indexSegmentStartProcessTime = time.milliseconds();
          List<IndexEntry> activeIndexSegmentEntries = getFilteredIndexEntries(activeIndexSegment, checkpoint);
          processIndexSegmentEntries(activeIndexSegment, activeIndexSegmentEntries, deletedKeys);
          metrics.statsBucketingScanTimePerIndexSegmentMs.update(time.milliseconds() - indexSegmentStartProcessTime);
          ConcurrentNavigableMap<Offset, IndexSegment> sealedIndexSegments =
              indexSegments.size() > 1 ? indexSegments.subMap(index.getStartOffset(),
                  activeIndexSegment.getStartOffset()) : new ConcurrentSkipListMap<Offset, IndexSegment>();
          for (IndexSegment indexSegment : sealedIndexSegments.descendingMap().values()) {
            if (cancelled) {
              return;
            }
            indexSegmentStartProcessTime = time.milliseconds();
            List<IndexEntry> indexEntries = getIndexEntries(indexSegment);
            processIndexSegmentEntries(indexSegment, indexEntries, deletedKeys);
            metrics.statsBucketingScanTimePerIndexSegmentMs.update(time.milliseconds() - indexSegmentStartProcessTime);
          }
        }
        newScanResults.lastScannedOffset = checkpoint;
        scanResults.set(newScanResults);
      } catch (Exception e) {
        logger.error("Exception while scanning index for stats bucketing", e);
        metrics.blobStoreStatsErrorCount.inc();
      } finally {
        synchronized (scanningLock) {
          isScanning = false;
          scanningLock.notifyAll();
        }
        metrics.statsBucketingScanTotalTimeMs.update(time.milliseconds() - startTimeInMs);
        metrics.statsBucketingScanCount.inc();
      }
    }

    /**
     * Process a {@link List} of {@link IndexEntry} belonging to the same {@link IndexSegment} to populate container and
     * log segment buckets.
     * @param indexSegment the {@link IndexSegment} where the index entries belong to
     * @param indexEntries a {@link List} of {@link IndexEntry} to be processed
     * @param deletedKeys a {@link Map} of deleted keys to their corresponding deletion time in ms
     * @throws StoreException
     */
    private void processIndexSegmentEntries(IndexSegment indexSegment, List<IndexEntry> indexEntries,
        Map<StoreKey, Long> deletedKeys) throws StoreException {
      // valid index entries wrt container reference time
      List<IndexEntry> validIndexEntries =
          getValidIndexEntries(indexSegment, indexEntries, newScanResults.containerForecastStartTimeInMs, deletedKeys);
      processEntriesForContainerBucket(newScanResults, validIndexEntries, deletedKeys);
      // valid index entries wrt log segment reference time
      validIndexEntries =
          getValidIndexEntries(indexSegment, indexEntries, newScanResults.logSegmentForecastStartTimeInMs, deletedKeys);
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
    private List<IndexEntry> getFilteredIndexEntries(IndexSegment indexSegment, Offset endOffset)
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
