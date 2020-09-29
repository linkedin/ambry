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

import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
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
  private final int bucketCount;
  private final long bucketSpanTimeInMs;
  private final long logSegmentForecastOffsetMs;
  private final long waitTimeoutInSecs;
  private final boolean enableBucketForLogSegmentReports;
  private final StoreMetrics metrics;
  private final ReentrantLock scanLock = new ReentrantLock();
  private final Condition waitCondition = scanLock.newCondition();
  private final Queue<EntryContext> recentEntryQueue = new LinkedBlockingQueue<>();
  private final AtomicInteger queueEntryCount = new AtomicInteger(0);
  private final AtomicBoolean enabled = new AtomicBoolean(true);

  private volatile boolean isScanning = false;
  private volatile boolean recentEntryQueueEnabled = false;
  private final AtomicReference<ScanResults> scanResults = new AtomicReference<>();
  private IndexScanner indexScanner;
  private QueueProcessor queueProcessor;

  /**
   * Convert a given nested {@link Map} of accountId to containerId to valid size to its corresponding
   * account {@link StatsSnapshot} object.
   * @param utilizationMap the nested {@link Map} to be converted
   * @return the corresponding {@link StatsSnapshot} object.
   */
  static StatsSnapshot convertStoreUsageToAccountStatsSnapshot(Map<String, Map<String, Long>> utilizationMap) {
    Map<String, StatsSnapshot> accountValidSizeMap = new HashMap<>();
    long totalSize = 0;
    for (Map.Entry<String, Map<String, Long>> accountEntry : utilizationMap.entrySet()) {
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

  /**
   * Convert a given nested {@link Map} of accountId to containerId to valid size to its corresponding
   * container {@link StatsSnapshot} object. The container snapshot does not have account level, each container is
   * uniquely identified by "Account[]_Container[]" pair.
   * @param utilizationMap the nested {@link Map} to be converted
   * @return the corresponding {@link StatsSnapshot} object.
   */
  static StatsSnapshot convertStoreUsageToContainerStatsSnapshot(Map<String, Map<String, Long>> utilizationMap) {
    Map<String, StatsSnapshot> containerValidSizeMap = new HashMap<>();
    long totalSize = 0;
    for (Map.Entry<String, Map<String, Long>> accountEntry : utilizationMap.entrySet()) {
      for (Map.Entry<String, Long> containerEntry : accountEntry.getValue().entrySet()) {
        totalSize += containerEntry.getValue();
        containerValidSizeMap.put(accountEntry.getKey() + Utils.ACCOUNT_CONTAINER_SEPARATOR + containerEntry.getKey(),
            new StatsSnapshot(containerEntry.getValue(), null));
      }
    }
    return new StatsSnapshot(totalSize, containerValidSizeMap);
  }

  BlobStoreStats(String storeId, PersistentIndex index, int bucketCount, long bucketSpanTimeInMs,
      long logSegmentForecastOffsetMs, long queueProcessingPeriodInMs, long waitTimeoutInSecs,
      boolean enableBucketForLogSegmentReports, Time time, ScheduledExecutorService longLiveTaskScheduler,
      ScheduledExecutorService shortLiveTaskScheduler, DiskIOScheduler diskIOScheduler, StoreMetrics metrics) {
    this.storeId = storeId;
    this.index = index;
    this.time = time;
    this.diskIOScheduler = diskIOScheduler;
    this.bucketCount = bucketCount;
    this.bucketSpanTimeInMs = bucketSpanTimeInMs;
    this.logSegmentForecastOffsetMs = logSegmentForecastOffsetMs;
    this.waitTimeoutInSecs = waitTimeoutInSecs;
    this.metrics = metrics;
    this.enableBucketForLogSegmentReports = enableBucketForLogSegmentReports;

    if (bucketCount > 0) {
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
   * Implementation in {@link BlobStoreStats} which returns the all types of snapshots for a {@link BlobStore}. Size of
   * delete records are not accounted as valid data size here. The formats of all snapshots are presented as follows.
   * <pre>
   *  AccountSnapshot (used for ACCOUNT_REPORT)   | ContainerSnapshot (used for PARTITION_CLASS_REPORT)
   * ---------------------------------------------------------------------------------------------------
   * {                                            |    {
   *   value: 1000,                               |      value: 1000,
   *   subMap: {                                  |      subMap: {
   *     Account[1]:{                             |         Account[1]_Container[1]: {
   *       value: 200,                            |             value: 200,
   *       subMap: {                              |             subMap: null
   *         Container[1]:{                       |         },
   *           value: 200,                        |         Account[2]_Container[3]: {
   *           subMap: null                       |             value: 800,
   *         }                                    |             subMap: null
   *       }                                      |         }
   *     },                                       |      }
   *     Account[2]:{                             |    }
   *       value: 800,                            |
   *       subMap:{                               |
   *         Container[3]:{                       |
   *           value: 800,                        |
   *           subMap: null                       |
   *         }                                    |
   *       }                                      |
   *     }                                        |
   *   }                                          |
   * }                                            |
   * </pre>
   */
  @Override
  public Map<StatsReportType, StatsSnapshot> getStatsSnapshots(Set<StatsReportType> statsReportTypes,
      long referenceTimeInMs) throws StoreException {
    Map<StatsReportType, StatsSnapshot> statsSnapshotsByType = new HashMap<>();
    Map<String, Map<String, Long>> utilizationMap = getValidDataSizeByContainer(referenceTimeInMs);
    for (StatsReportType reportType : statsReportTypes) {
      switch (reportType) {
        case ACCOUNT_REPORT:
          statsSnapshotsByType.put(StatsReportType.ACCOUNT_REPORT,
              convertStoreUsageToAccountStatsSnapshot(utilizationMap));
          break;
        case PARTITION_CLASS_REPORT:
          statsSnapshotsByType.put(StatsReportType.PARTITION_CLASS_REPORT,
              convertStoreUsageToContainerStatsSnapshot(utilizationMap));
          break;
        default:
          logger.error("Unrecognized stats report type: {}", reportType);
      }
    }
    return statsSnapshotsByType;
  }

  /**
   * Returns the max blob size that is encountered while generating stats
   * @return the max blob size that is encountered while generating stats
   */
  long getMaxBlobSize() {
    return MAX_BLOB_SIZE;
  }

  /**
   * Gets the size of valid data at a particular point in time for all log segments. The caller specifies a delete reference
   * time and acceptable resolution for the stats in the form of a {@link TimeRange}. The store will return data
   * for a point in time within the specified range.
   * The following data are considered as valid data for this API:
   * 1. PUT with no expiry and no corresponding DELETE
   * 2. PUT expiring at t_exp but t_ref < t_exp
   * 3. PUT with corresponding DELETE at time t_delete but t_ref < t_delete
   * 4. DELETE record
   * 5. TTL update record whose PUT record is still valid or is in a different log segment to the TTL update record
   * For this API, t_ref is specified by the given {@link TimeRange}.
   * @param timeRange the reference {@link TimeRange} at which the data is requested. Defines both the reference time
   *                  and the acceptable resolution.
   * @return a {@link Pair} whose first element is the time at which stats was collected (in ms) and whose second
   * element is the valid data size for each segment in the form of a {@link NavigableMap} of segment names to
   * valid data sizes.
   * @throws StoreException if BlobStoreStats is not enabled or closed
   */
  Pair<Long, NavigableMap<String, Long>> getValidDataSizeByLogSegment(TimeRange timeRange) throws StoreException {
    return getValidDataSizeByLogSegment(timeRange, time.milliseconds());
  }

  /**
   * Same as {@link #getValidDataSizeByLogSegment(TimeRange)}, but provides a expiry reference time. Blobs with expiration
   * time less than the given expiry reference time, are considered as expired.
   * @param timeRange the delete reference {@link TimeRange} at which the data is requested. Defines both the reference time
   *                  and the acceptable resolution.
   * @param expiryReferenceTime the reference time for expired blobs. Blobs with expiration time less than it would be
   *                            considered as expired. Usually it's now.
   * @return a {@link Pair} whose first element is the time at which stats was collected (in ms) and whose second
   * element is the valid data size for each segment in the form of a {@link NavigableMap} of segment names to
   * valid data sizes.
   * @throws StoreException
   */
  Pair<Long, NavigableMap<String, Long>> getValidDataSizeByLogSegment(TimeRange timeRange, long expiryReferenceTime)
      throws StoreException {
    if (!enabled.get()) {
      throw new StoreException(String.format("BlobStoreStats is not enabled or closing for store %s", storeId),
          StoreErrorCodes.Store_Shutting_Down);
    }
    Pair<Long, NavigableMap<String, Long>> retValue = null;
    ScanResults currentScanResults = scanResults.get();
    long referenceTimeInMs = getLogSegmentDeleteRefTimeMs(currentScanResults, timeRange);
    if (enableBucketForLogSegmentReports) {
      if (referenceTimeInMs != REF_TIME_OUT_OF_BOUNDS) {
        retValue = currentScanResults.getValidSizePerLogSegment(referenceTimeInMs, expiryReferenceTime);
      } else {
        if (isScanning
            && getLogSegmentDeleteRefTimeMs(indexScanner.newScanResults, timeRange) != REF_TIME_OUT_OF_BOUNDS) {
          scanLock.lock();
          try {
            if (isScanning) {
              if (waitCondition.await(waitTimeoutInSecs, TimeUnit.SECONDS)) {
                currentScanResults = scanResults.get();
                referenceTimeInMs = getLogSegmentDeleteRefTimeMs(currentScanResults, timeRange);
                if (referenceTimeInMs != REF_TIME_OUT_OF_BOUNDS) {
                  retValue = currentScanResults.getValidSizePerLogSegment(referenceTimeInMs, expiryReferenceTime);
                }
              } else {
                metrics.blobStoreStatsIndexScannerErrorCount.inc();
                logger.error("Timed out while waiting for BlobStoreStats index scan to complete for store {}", storeId);
              }
            } else {
              currentScanResults = scanResults.get();
              referenceTimeInMs = getLogSegmentDeleteRefTimeMs(currentScanResults, timeRange);
              if (referenceTimeInMs != REF_TIME_OUT_OF_BOUNDS) {
                retValue = currentScanResults.getValidSizePerLogSegment(referenceTimeInMs, expiryReferenceTime);
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
      }
    }

    if (retValue == null) {
      // retValue could be null in three scenarios:
      // 1. timeRange is outside of current forecast coverage and there is no ongoing scan.
      // 2. timed out while waiting for an ongoing scan.
      // 3. rare edge case where currentScanResults updated twice since the start of the wait.
      referenceTimeInMs = timeRange.getEndTimeInMs();
      retValue =
          new Pair<>(referenceTimeInMs, collectValidDataSizeByLogSegment(referenceTimeInMs, expiryReferenceTime));
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

  boolean isRecentEntryQueueEnabled() {
    return recentEntryQueueEnabled;
  }

  /**
   * Function that handles new PUT after a scan to keep the current {@link ScanResults} relevant.
   * @param putValue the {@link IndexValue} of the new PUT
   */
  void handleNewPutEntry(IndexValue putValue) {
    enqueueNewValue(null, putValue, null, null);
  }

  /**
   * Function that handles new DELETE after a scan to keep the current {@link ScanResults} relevant.
   * @param key the {@link StoreKey} of the new DELETE
   * @param deleteValue the {@link IndexValue} of the new DELETE
   * @param originalPutValue the {@link IndexValue} of the original PUT that is getting deleted
   * @param previousValue the {@link IndexValue} prior to the new DELETE.
   */
  void handleNewDeleteEntry(StoreKey key, IndexValue deleteValue, IndexValue originalPutValue,
      IndexValue previousValue) {
    enqueueNewValue(key, deleteValue, originalPutValue, previousValue);
  }

  /**
   * Function that handles new TTL updates after a scan to keep the current {@link ScanResults} relevant.
   * @param ttlUpdateValue the {@link IndexValue} of the new TTL update
   * @param originalPutValue the {@link IndexValue} of the original PUT that is getting updated
   */
  void handleNewTtlUpdateEntry(IndexValue ttlUpdateValue, IndexValue originalPutValue) {
    enqueueNewValue(null, ttlUpdateValue, originalPutValue, null);
  }

  /**
   * Function that handles new UNDELETE after a scan to keep the current {@link ScanResults} relevant.
   * @param key the {@link StoreKey} of the new DELETE
   * @param undeleteValue the {@link IndexValue} of the new UNDELETE
   * @param originalPutValue the {@link IndexValue} of the original PUT that is getting deleted
   * @param previousValue the {@link IndexValue} prior to the new UNDELETE.
   */
  void handleNewUndeleteEntry(StoreKey key, IndexValue undeleteValue, IndexValue originalPutValue,
      IndexValue previousValue) {
    enqueueNewValue(key, undeleteValue, originalPutValue, previousValue);
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

  private void enqueueNewValue(StoreKey key, IndexValue newValue, IndexValue originalPutValue,
      IndexValue previousValue) {
    if (recentEntryQueueEnabled) {
      recentEntryQueue.offer(new EntryContext(key, newValue, originalPutValue, previousValue));
      metrics.statsRecentEntryQueueSize.update(queueEntryCount.incrementAndGet());
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
    Map<StoreKey, IndexFinalState> keyFinalStates = new HashMap<>();
    Map<String, Map<String, Long>> validDataSizePerContainer = new HashMap<>();
    int indexSegmentCount = 0;
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      if (!enabled.get()) {
        throw new StoreException(String.format("BlobStoreStats is not enabled or closing for store %s", storeId),
            StoreErrorCodes.Store_Shutting_Down);
      }
      long indexSegmentStartProcessTimeMs = time.milliseconds();
      diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID,
          indexSegment.size());
      forEachValidIndexEntry(indexSegment, referenceTimeInMs, time.milliseconds(), keyFinalStates, true, entry -> {
        IndexValue indexValue = entry.getValue();
        if (indexValue.isPut()) {
          // delete and TTL update records does not count towards valid data size for usage (containers)
          updateNestedMapHelper(validDataSizePerContainer, "A[" + indexValue.getAccountId() + "]",
              "C[" + indexValue.getContainerId() + "]", indexValue.getSize());
        }
      });
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
   * @param deleteReferenceTimeInMs the reference time in ms until which deletes are relevant
   * @param expiryReferenceTimeInMs the reference tie in ms until which expiration is relevant
   * @return a {@link NavigableMap} of log segment name to valid data size
   */
  private NavigableMap<String, Long> collectValidDataSizeByLogSegment(long deleteReferenceTimeInMs,
      long expiryReferenceTimeInMs) throws StoreException {
    logger.trace("On demand index scanning to collect compaction data stats for store {} wrt ref time {}", storeId,
        deleteReferenceTimeInMs);
    long startTimeMs = time.milliseconds();
    Map<StoreKey, IndexFinalState> keyFinalStates = new HashMap<>();
    NavigableMap<String, Long> validSizePerLogSegment = new TreeMap<>(LogSegmentNameHelper.COMPARATOR);
    int indexSegmentCount = 0;
    for (IndexSegment indexSegment : index.getIndexSegments().descendingMap().values()) {
      if (!enabled.get()) {
        throw new StoreException(String.format("BlobStoreStats is not enabled or closing for store %s", storeId),
            StoreErrorCodes.Store_Shutting_Down);
      }
      long indexSegmentStartProcessTimeMs = time.milliseconds();
      String logSegmentName = indexSegment.getLogSegmentName();
      diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID,
          indexSegment.size());
      forEachValidIndexEntry(indexSegment, deleteReferenceTimeInMs, expiryReferenceTimeInMs, keyFinalStates, true,
          entry -> updateMapHelper(validSizePerLogSegment, logSegmentName, entry.getValue().getSize()));
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
   * Perform an action each valid {@link IndexEntry} from a given {@link List} of {@link IndexEntry}s that belong to the
   * same {@link IndexSegment}.
   * @param indexSegment the {@link IndexSegment} where the entries came from
   * @param deleteReferenceTimeInMs the reference time in ms until which deletes are relevant
   * @param expiryReferenceTimeInMs the reference time in ms until which expiration are relevant
   * @param keyFinalStates a {@link Map} of key to {@link IndexFinalState}.
   * @param removeFinalStateOnPut if {@code True}, then remove the {@link IndexFinalState} from the given map {@code keyFinalStates}
   *                         when encountering PUT IndexValue. This method iterates through IndexValues from most recent one to
   *                         earliest one, so PUT IndexValue is the last IndexValue for the same key.
   * @param validIndexEntryAction the action to take on each valid {@link IndexEntry} found.
   * @throws StoreException if there are problems reading the index.
   */
  private void forEachValidIndexEntry(IndexSegment indexSegment, long deleteReferenceTimeInMs,
      long expiryReferenceTimeInMs, Map<StoreKey, IndexFinalState> keyFinalStates, boolean removeFinalStateOnPut,
      IndexEntryAction validIndexEntryAction) throws StoreException {
    ListIterator<IndexEntry> it = indexSegment.listIterator(indexSegment.size());
    while (it.hasPrevious()) {
      IndexEntry indexEntry = it.previous();
      IndexValue indexValue = indexEntry.getValue();
      StoreKey key = indexEntry.getKey();
      if (indexValue.isDelete()) {
        if (keyFinalStates.containsKey(key)) {
          IndexFinalState state = keyFinalStates.get(key);
          if (state.isUndelete() || (state.isDelete() && state.getLifeVersion() != indexValue.getLifeVersion()) || state
              .isTtlUpdate()) {
            // This DELETE is not valid, when the final state of this storeKey is
            // 1. UNDELETE, or
            // 2. DELETE, but the current lifeVersion is not the same, or
            // 3. TTL_UPDATE
            continue;
          }
        } else {
          long operationTimeInMs =
              indexValue.getOperationTimeInMs() == Utils.Infinite_Time ? indexSegment.getLastModifiedTimeMs()
                  : indexValue.getOperationTimeInMs();
          keyFinalStates.put(indexEntry.getKey(),
              new IndexFinalState(indexValue.getFlags(), operationTimeInMs, indexValue.getLifeVersion()));
        }
        validIndexEntryAction.accept(indexEntry);
      } else if (indexValue.isUndelete()) {
        if (keyFinalStates.containsKey(key)) {
          IndexFinalState state = keyFinalStates.get(key);
          if (state.isDelete() || (state.getLifeVersion() != indexValue.getLifeVersion())) {
            // This UNDELETE is not valid, when the final state of this storeKey is
            // 1. DELETE, or
            // 2. the current lifeVersion is not the same
            continue;
          }
          if (state.isTtlUpdate()) {
            indexValue.setExpiresAtMs(Utils.Infinite_Time);
          }
        } else {
          long operationTimeInMs =
              indexValue.getOperationTimeInMs() == Utils.Infinite_Time ? indexSegment.getLastModifiedTimeMs()
                  : indexValue.getOperationTimeInMs();
          keyFinalStates.put(indexEntry.getKey(),
              new IndexFinalState(indexValue.getFlags(), operationTimeInMs, indexValue.getLifeVersion()));
        }
        if (!isExpired(indexValue.getExpiresAtMs(), expiryReferenceTimeInMs)) {
          validIndexEntryAction.accept(indexEntry);
        }
      } else if (indexValue.isTtlUpdate()) {
        if (isTtlUpdateEntryValid(key, indexValue, deleteReferenceTimeInMs, keyFinalStates)) {
          validIndexEntryAction.accept(indexEntry);
        }
      } else {
        IndexFinalState finalState = keyFinalStates.get(key);
        if (finalState != null && finalState.isDelete() && finalState.getOperationTime() < deleteReferenceTimeInMs) {
          // Put is deleted before reference time, it's not valid.
          if (removeFinalStateOnPut) {
            keyFinalStates.remove(key);
          }
          continue;
        }
        if (finalState != null && finalState.isTtlUpdate()) {
          // indexValue is a clone of the existing IndexValue, changing IndexValue will not change existing one.
          indexValue.setExpiresAtMs(Utils.Infinite_Time);
        }
        if (!isExpired(indexValue.getExpiresAtMs(), expiryReferenceTimeInMs)) {
          validIndexEntryAction.accept(indexEntry);
        }
        if (removeFinalStateOnPut) {
          keyFinalStates.remove(key);
        }
      }
    }
  }

  /**
   * @param key the {@link StoreKey} to check
   * @param ttlUpdateValue the {@link IndexValue} of the TTL update entry of {@code key}
   * @param referenceTimeInMs the reference time in ms until which deletes are relevant
   * @param keyFinalStates a {@link Map} of key to {@link IndexFinalState}.
   * @return {@code true} if the ttl update entry is valid
   * @throws StoreException if there are problems accessing the index
   */
  private boolean isTtlUpdateEntryValid(StoreKey key, IndexValue ttlUpdateValue, long referenceTimeInMs,
      Map<StoreKey, IndexFinalState> keyFinalStates) throws StoreException {
    // Offset sanity check
    if (ttlUpdateValue.getOffset().compareTo(index.getStartOffset()) < 0) {
      return false;
    }
    FileSpan searchSpan = new FileSpan(index.getStartOffset(), ttlUpdateValue.getOffset());
    IndexValue putValue = index.findKey(key, searchSpan, EnumSet.of(PersistentIndex.IndexEntryType.PUT));
    boolean valid = true;
    if (putValue == null) {
      // no put value so not valid
      valid = false;
    } else if (putValue.getOffset().getName().equals(ttlUpdateValue.getOffset().getName())) {
      // can be invalid if it is in the same log segment and the put is invalid
      IndexFinalState finalState = keyFinalStates.get(key);
      if (finalState != null && finalState.isDelete() && finalState.getOperationTime() < referenceTimeInMs) {
        valid = false;
      }
    }
    if (valid && !keyFinalStates.containsKey(key)) {
      keyFinalStates.put(key, new IndexFinalState(ttlUpdateValue.getFlags(), ttlUpdateValue.
          getOperationTimeInMs(), ttlUpdateValue.getLifeVersion()));
    }
    // else, valid = true because a ttl update entry with a put in another log segment is considered valid as long as
    // the put still exists (regardless of its validity)
    return valid;
  }

  /**
   * Given {@link ScanResults} and a {@link TimeRange}, try to find the latest point in time that is within the
   * {@link TimeRange} and the log segment forecast range for deleted.
   * @param results the {@link ScanResults} with the log segment forecast range information
   * @param timeRange the {@link TimeRange} used to find the latest shared point in time with the log segment forecast
   *                  range
   * @return the latest shared point in time in milliseconds if there is one, otherwise REF_TIME_OUT_OF_BOUNDS (-1)
   * is returned
   */
  private long getLogSegmentDeleteRefTimeMs(ScanResults results, TimeRange timeRange) {
    long refTimeInMs = results == null || timeRange.getStartTimeInMs() >= results.logSegmentForecastEndTimeMsForDeleted
        || timeRange.getEndTimeInMs() < results.logSegmentForecastStartTimeMsForDeleted ? REF_TIME_OUT_OF_BOUNDS
        : timeRange.getEndTimeInMs();
    if (refTimeInMs != REF_TIME_OUT_OF_BOUNDS && refTimeInMs >= results.logSegmentForecastEndTimeMsForDeleted) {
      refTimeInMs = results.logSegmentLastBucketTimeMsForDeleted;
    }
    return refTimeInMs;
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
    // If the expiration time is Infinite_Time, then nothing will be updated
    if (isWithinRange(results.containerForecastStartTimeMs, results.containerLastBucketTimeMs, expOrDelTimeInMs)) {
      results.updateContainerBucket(results.getContainerBucketKey(expOrDelTimeInMs),
          "A[" + indexValue.getAccountId() + "]", "C[" + indexValue.getContainerId() + "]",
          indexValue.getSize() * operator);
    }
  }

  /**
   * Helper function for log segment buckets for blob expiration/deletion related updates.
   * @param results the {@link ScanResults} to be updated
   * @param indexValue the PUT {@link IndexValue} of the expiring/deleting blob
   * @param deleteTimeInMs delete time of the blob in ms
   * @param operator indicating the operator of the update, 1 for add and -1 for subtract
   */
  private void handleLogSegmentDeletedBucketUpdate(ScanResults results, IndexValue indexValue, long deleteTimeInMs,
      int operator) {
    if (isWithinRange(results.logSegmentForecastStartTimeMsForDeleted, results.logSegmentLastBucketTimeMsForDeleted,
        deleteTimeInMs)) {
      results.updateLogSegmentDeletedBucket(results.getLogSegmentDeletedBucketKey(deleteTimeInMs),
          indexValue.getOffset().getName(), indexValue.getSize() * operator);
    }
  }

  /**
   * Helper function for log segment buckets for blob expiration/deletion related updates.
   * @param results the {@link ScanResults} to be updated
   * @param indexValue the PUT {@link IndexValue} of the expiring/deleting blob
   * @param expiryTimeInMs expiration time of the blob in ms
   * @param operator indicating the operator of the update, 1 for add and -1 for subtract
   */
  private void handleLogSegmentExpiredBucketUpdate(ScanResults results, IndexValue indexValue, long expiryTimeInMs,
      int operator) {
    if (isWithinRange(results.logSegmentForecastStartTimeMsForExpired, results.logSegmentLastBucketTimeMsForExpired,
        expiryTimeInMs)) {
      results.updateLogSegmentExpiredBucket(results.getLogSegmentExpiredBucketKey(expiryTimeInMs),
          indexValue.getOffset().getName(), indexValue.getSize() * operator);
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
      results.updateContainerBaseBucket("A[" + putValue.getAccountId() + "]", "C[" + putValue.getContainerId() + "]",
          putValue.getSize());
      if (expiresAtMs != Utils.Infinite_Time) {
        handleContainerBucketUpdate(results, putValue, expiresAtMs, SUBTRACT);
      }
    }
    if (!isExpired(expiresAtMs, results.logSegmentForecastStartTimeMsForExpired)) {
      results.updateLogSegmentBaseBucket(putValue.getOffset().getName(), putValue.getSize());
      if (expiresAtMs != Utils.Infinite_Time) {
        handleLogSegmentExpiredBucketUpdate(results, putValue, expiresAtMs, SUBTRACT);
      }
    }
  }

  /**
   * Helper function to process new DELETE entries and make appropriate updates to the given {@link ScanResults}.
   * @param results the {@link ScanResults} to apply the updates to
   * @param key the {@link StoreKey} of the new DELETE
   * @param deleteValue the {@link IndexValue} of the new DELETE
   * @param originalPutValue the {@link IndexValue} of the original PUT that is getting deleted
   * @param previousValue the {@link IndexValue} comes prior to the new DELETE
   */
  private void processNewDelete(ScanResults results, StoreKey key, IndexValue deleteValue, IndexValue originalPutValue,
      IndexValue previousValue) {
    long operationTimeInMs = deleteValue.getOperationTimeInMs();
    if (operationTimeInMs == Utils.Infinite_Time) {
      operationTimeInMs =
          index.getIndexSegments().floorEntry(deleteValue.getOffset()).getValue().getLastModifiedTimeMs();
    }
    // First deal with container bucket
    if (previousValue.isDelete()) {
      // The previous value is already DELETE, current DELETE would not change container bucket.
      // Assume the record history looks like this: PUT, DELETE, DELETE.
      // First DELETE already updated container bucket.
    } else {
      // If original PUT expires before this DELETE, then it will not change container bucket.
      if (!isExpired(originalPutValue.getExpiresAtMs(), operationTimeInMs)) {
        handleContainerBucketUpdate(results, originalPutValue, operationTimeInMs, SUBTRACT);
        handleContainerBucketUpdate(results, originalPutValue, originalPutValue.getExpiresAtMs(), ADD);
      }
    }
    // Now deal with log segment bucket
    // Current DELETE is always valid
    if (previousValue.isDelete()) {
      // Previous IndexValue is a DELETE, it will be invalidated by current DELETE. So we have to revert the previous
      // DELETE here as if it doesn't exist. After reverting changes made by previous DELETE, we will process the current
      // DELETE IndexValue.
      processDeleteUpdateLogSegmentHelper(results, key, previousValue, originalPutValue, SUBTRACT);
    } else if (previousValue.isUndelete()) {
      // Previous IndexValue is an UNDELETE, it should already
      // 1. Added it's size to the base bucket of the scan result
      // we need to recover from this operation
      results.updateLogSegmentBaseBucket(previousValue.getOffset().getName(), SUBTRACT * previousValue.getSize());
    }
    // Process the current DELETE IndexValue.
    processDeleteUpdateLogSegmentHelper(results, key, deleteValue, originalPutValue, ADD);
  }

  /**
   * Update deleted bucket based on given DELETE IndexValue. For DELETE IndexValue, we should
   * 1. Add it's size to the base bucket in the {@link ScanResults}.$
   * 2. Subtract original PUT's size from DELETE's operationTimeInMs bucket.
   * 3. If TTL_UPDATE IndexValue exist, subtract TTL_UPDATE's size from DELETE's operationTimeInMs bucket as well.
   * @param results the {@link ScanResults} to update the log segment deleted bucket.
   * @param key the {@link StoreKey} of IndexValue to process.
   * @param deleteValue the DELETE {@link IndexValue} to process.
   * @param originalPutValue the original PUT {@link IndexValue} of the same key.
   * @param operator if the operator is {@link #SUBTRACT}, then adding would become subtracting, and subtracting would
   *                 become adding. Set operator to {@link #SUBTRACT} if you want to revert the operation for the given
   *                 DELETE {@link IndexValue}.
   */
  private void processDeleteUpdateLogSegmentHelper(ScanResults results, StoreKey key, IndexValue deleteValue,
      IndexValue originalPutValue, int operator) {
    long operationTimeInMs = deleteValue.getOperationTimeInMs();
    if (operationTimeInMs == Utils.Infinite_Time) {
      operationTimeInMs =
          index.getIndexSegments().floorEntry(deleteValue.getOffset()).getValue().getLastModifiedTimeMs();
    }
    results.updateLogSegmentBaseBucket(deleteValue.getOffset().getName(), operator * deleteValue.getSize());
    if (!isExpired(originalPutValue.getExpiresAtMs(), operationTimeInMs)) {
      handleLogSegmentDeletedBucketUpdate(results, originalPutValue, operationTimeInMs, operator * SUBTRACT);
      if (originalPutValue.getExpiresAtMs() != Utils.Infinite_Time) {
        // make appropriate updates to avoid double counting
        handleLogSegmentExpiredBucketUpdate(results, originalPutValue, originalPutValue.getExpiresAtMs(),
            operator * ADD);
      } else if (deleteValue.isTtlUpdate()) {
        // This blob has a PUT and TTL_UPDATE, because of DELETE, now TTL_UPDATE is not valid any more.
        try {
          IndexValue ttlUpdateValue =
              index.findKey(key, new FileSpan(originalPutValue.getOffset(), deleteValue.getOffset()),
                  EnumSet.of(PersistentIndex.IndexEntryType.TTL_UPDATE));
          if (ttlUpdateValue != null) {
            handleLogSegmentDeletedBucketUpdate(results, ttlUpdateValue, operationTimeInMs, operator * SUBTRACT);
          }
        } catch (StoreException e) {
          logger.error(
              "Failed to find TTL_UPDATE IndexValue for " + key + " when processing new DELETE: " + deleteValue, e);
        }
      }
    }
  }

  /**
   * Helper function to process new UNDELETE entries and make appropriate updates to the given {@link ScanResults}.
   * @param results the {@link ScanResults} to apply the updates to
   * @param key the {@link StoreKey} of the new UNDELETE
   * @param undeleteValue the {@link IndexValue} of the new UNDELETE
   * @param originalPutValue the {@link IndexValue} of the original PUT
   * @param previousValue the {@link IndexValue} comes prior to the new UNDELETE
   */
  private void processNewUndelete(ScanResults results, StoreKey key, IndexValue undeleteValue,
      IndexValue originalPutValue, IndexValue previousValue) {
    // First deal with container bucket
    if (!previousValue.isDelete()) {
      // This previous value is not DELETE, current UNDELETE would not change container bucket.
      // There are several possibility when the previous value is not DELETE.
      // 1. PUT UNDELETE: UNDELETE doesn't change any bucket
      // 2. PUT TTL_UPDATE UNDELETE: UNDELETE doesn't change any bucket
      // 3. PUT DELETE UNDELETE UNDELETE: the last UNDELETE doesn't change any bucket
      // UNDELETE would resurrect a deleted IndexValue, if the previous value is not DELETE, then the IndexValue doesn't
      // need to be resurrected.
    } else {
      handleContainerBucketUpdate(results, originalPutValue, undeleteValue.getOperationTimeInMs(), ADD);
      handleContainerBucketUpdate(results, originalPutValue, originalPutValue.getExpiresAtMs(), SUBTRACT);
    }

    // Now deal with log segment bucket
    // Current UNDELETE is always valid
    results.updateLogSegmentBaseBucket(undeleteValue.getOffset().getName(), undeleteValue.getSize());
    if (previousValue.isDelete()) {
      // Previous IndexValue is a DELETE, it will be invalidated by current DELETE. So we have to revert the previous
      // DELETE here as if it doesn't exist. After reverting changes made by previous DELETE, we will process the current
      // DELETE IndexValue.
      processDeleteUpdateLogSegmentHelper(results, key, previousValue, originalPutValue, SUBTRACT);
    } else if (previousValue.isUndelete()) {
      // Previous IndexValue is an UNDELETE, it should already
      // 1. Added it's size to the base bucket of the scan result
      // we need to recover from this operation
      results.updateLogSegmentBaseBucket(previousValue.getOffset().getName(), SUBTRACT * previousValue.getSize());
    }
  }

  /**
   * Helper function to process new TTL update entries and make appropriate updates to the given {@link ScanResults}.
   * @param results the {@link ScanResults} to apply the updates to
   * @param ttlUpdateValue the {@link IndexValue} of the new TTL update
   * @param originalPutValue the {@link IndexValue} of the original PUT
   */
  private void processNewTtlUpdate(ScanResults results, IndexValue ttlUpdateValue, IndexValue originalPutValue) {
    // First deal with container bucket
    long originalExpiresAt = originalPutValue.getExpiresAtMs();
    if (originalExpiresAt != Utils.Infinite_Time) {
      handleContainerBucketUpdate(results, originalPutValue, originalExpiresAt, ADD);
    }

    // Now deal with log segment bucket
    results.updateLogSegmentBaseBucket(ttlUpdateValue.getOffset().getName(), ttlUpdateValue.getSize());
    if (originalExpiresAt != Utils.Infinite_Time) {
      handleLogSegmentExpiredBucketUpdate(results, originalPutValue, originalExpiresAt, ADD);
    }
  }

  /**
   * Process a {@link List} of valid {@link IndexEntry} based on forecast boundaries and populate corresponding
   * container buckets in the given {@link ScanResults}.
   * @param results the {@link ScanResults} to be populated
   * @param indexEntry a valid {@link IndexEntry} to be processed
   * @param keyFinalStates a {@link Map} of key to {@link IndexFinalState}.
   */
  private void processEntryForContainerBucket(ScanResults results, IndexEntry indexEntry,
      Map<StoreKey, IndexFinalState> keyFinalStates) {
    IndexValue indexValue = indexEntry.getValue();
    if (indexValue.isPut()) {
      // delete and TTL update records does not count towards valid data size for usage (containers)
      results.updateContainerBaseBucket("A[" + indexValue.getAccountId() + "]",
          "C[" + indexValue.getContainerId() + "]", indexValue.getSize());
      long expOrDelTimeInMs = indexValue.getExpiresAtMs();
      IndexFinalState finalState = keyFinalStates.get(indexEntry.getKey());
      if (finalState != null && finalState.isDelete()) {
        long deleteTimeInMs = finalState.getOperationTime();
        expOrDelTimeInMs =
            expOrDelTimeInMs != Utils.Infinite_Time && expOrDelTimeInMs < deleteTimeInMs ? expOrDelTimeInMs
                : deleteTimeInMs;
      }
      if (expOrDelTimeInMs != Utils.Infinite_Time) {
        handleContainerBucketUpdate(results, indexValue, expOrDelTimeInMs, SUBTRACT);
      }
    }
  }

  /**
   * Process a {@link List} of valid {@link IndexEntry} based on forecast boundaries and populate corresponding
   * log segment buckets in the given {@link ScanResults}.
   * @param results the {@link ScanResults} to be populated
   * @param indexEntry a {@link List} of valid {@link IndexEntry} to be processed
   * @param keyFinalStates a {@link Map} of key to {@link IndexFinalState}.
   * @throws StoreException if there are problems accessing the index
   */
  private void processEntryForLogSegmentBucket(ScanResults results, IndexEntry indexEntry,
      Map<StoreKey, IndexFinalState> keyFinalStates) throws StoreException {
    IndexValue indexValue = indexEntry.getValue();
    results.updateLogSegmentBaseBucket(indexValue.getOffset().getName(), indexValue.getSize());
    if (indexValue.isPut()) {
      long expiresAtMs = indexValue.getExpiresAtMs();
      long deleteAtMs = Utils.Infinite_Time;
      IndexFinalState finalState = keyFinalStates.get(indexEntry.getKey());
      if (finalState != null && finalState.isDelete()) {
        deleteAtMs = finalState.getOperationTime();
      }
      if (expiresAtMs == Utils.Infinite_Time && deleteAtMs != Utils.Infinite_Time) {
        handleLogSegmentDeletedBucketUpdate(results, indexValue, deleteAtMs, SUBTRACT);
      } else if (expiresAtMs != Utils.Infinite_Time && deleteAtMs == Utils.Infinite_Time) {
        handleLogSegmentExpiredBucketUpdate(results, indexValue, expiresAtMs, SUBTRACT);
      } else if (expiresAtMs != Utils.Infinite_Time && deleteAtMs != Utils.Infinite_Time) {
        if (expiresAtMs < deleteAtMs) {
          handleLogSegmentExpiredBucketUpdate(results, indexValue, expiresAtMs, SUBTRACT);
        } else {
          handleLogSegmentDeletedBucketUpdate(results, indexValue, deleteAtMs, SUBTRACT);
        }
      }
    } else if (!indexValue.isDelete() && !indexValue.isUndelete() && indexValue.isTtlUpdate()) {
      IndexFinalState finalState = keyFinalStates.get(indexEntry.getKey());
      if (finalState != null && finalState.isDelete()) {
        Offset beginningOfThisLogSegment = new Offset(indexValue.getOffset().getName(), LogSegment.HEADER_SIZE);
        IndexValue putValue =
            index.findKey(indexEntry.getKey(), new FileSpan(beginningOfThisLogSegment, indexValue.getOffset()),
                EnumSet.of(PersistentIndex.IndexEntryType.PUT));
        if (putValue != null) {
          // When the Put IndexValue is in the same log segment as the TTL_UPDATE value, then TTL_UPDATE value is invalid.
          handleLogSegmentDeletedBucketUpdate(results, indexValue, finalState.getOperationTime(), SUBTRACT);
        }
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
        for (int i = 0; i < entryCount && !cancelled && !isScanning; i++) {
          EntryContext entryContext = recentEntryQueue.poll();
          if (entryContext == null) {
            throw new IllegalStateException(
                String.format("Invalid queue state in store %s. Expected entryCount %d, current index: %d", storeId,
                    entryCount, i));
          }
          metrics.statsRecentEntryQueueSize.update(queueEntryCount.decrementAndGet());
          StoreKey key = entryContext.key;
          IndexValue newValue = entryContext.currentValue;
          IndexValue originalPut = entryContext.originalPutValue;
          IndexValue previousValue = entryContext.previousValue;
          // prevent double counting new entries that were added after enabling the queue and just before the second
          // checkpoint is taken
          if (newValue.getOffset().compareTo(currentScanResults.scannedEndOffset) >= 0) {
            if (newValue.isDelete()) {
              // new delete
              processNewDelete(currentScanResults, key, newValue, originalPut, previousValue);
            } else if (newValue.isUndelete()) {
              // new undelete
              processNewUndelete(currentScanResults, key, newValue, originalPut, previousValue);
            } else if (newValue.isTtlUpdate()) {
              // new ttl update
              processNewTtlUpdate(currentScanResults, newValue, originalPut);
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
        Map<StoreKey, IndexFinalState> keyFinalStates = new HashMap<>();
        // process the active index segment based on the firstCheckpoint in case index segment rolled over after the
        // checkpoint is taken
        if (!cancelled && indexSegments.size() > 0) {
          Map.Entry<Offset, IndexSegment> activeIndexSegmentEntry = indexSegments.floorEntry(firstCheckpoint);
          long indexSegmentStartProcessTime = time.milliseconds();
          logger.trace("Processing index entries in active segment {} before first checkpoint for store {}",
              activeIndexSegmentEntry.getValue().getFile().getName(), storeId);
          processIndexSegmentEntriesBackward(activeIndexSegmentEntry.getValue(),
              entry -> entry.getValue().getOffset().compareTo(firstCheckpoint) < 0, keyFinalStates);
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
              processIndexSegmentEntriesBackward(indexSegment, null, keyFinalStates);
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
        diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID,
            indexSegment.size());
        Iterator<IndexEntry> iterator = indexSegment.iterator();
        Map<StoreKey, IndexValue> seenPuts = new HashMap<>();
        while (iterator.hasNext()) {
          forwardScanEntryCount++;
          IndexEntry entry = iterator.next();
          IndexValue indexValue = entry.getValue();
          if (indexValue.getOffset().compareTo(startOffset) >= 0 && indexValue.getOffset().compareTo(endOffset) < 0) {
            // index value is not yet processed and should be processed
            if (!indexValue.isPut()) {
              IndexValue originalPut;
              if (indexValue.getOriginalMessageOffset() == indexValue.getOffset().getOffset()) {
                // update record with no put record due to compaction and legacy bugs
                originalPut = null;
              } else {
                FileSpan searchSpan = new FileSpan(index.getStartOffset(), indexSegment.getStartOffset());
                originalPut = seenPuts.get(entry.getKey());
                if (originalPut == null) {
                  originalPut =
                      index.findKey(entry.getKey(), searchSpan, EnumSet.of(PersistentIndex.IndexEntryType.PUT));
                }
              }
              IndexValue previousValue = null;
              if (originalPut != null) {
                // Find the IndexValue right before current IndexValue, it might be original PUT as well.
                previousValue =
                    index.findKey(entry.getKey(), new FileSpan(originalPut.getOffset(), indexValue.getOffset()),
                        EnumSet.allOf(PersistentIndex.IndexEntryType.class));
              }
              if (originalPut == null) {
                newScanResults.updateLogSegmentBaseBucket(indexValue.getOffset().getName(), indexValue.getSize());
              } else if (indexValue.isDelete()) {
                processNewDelete(newScanResults, entry.getKey(), indexValue, previousValue, originalPut);
              } else if (indexValue.isUndelete()) {
                processNewUndelete(newScanResults, entry.getKey(), indexValue, previousValue, originalPut);
              } else if (indexValue.isTtlUpdate()) {
                processNewTtlUpdate(newScanResults, indexValue, originalPut);
              }
            } else {
              // put record
              processNewPut(newScanResults, indexValue);
            }
          }
          if (indexValue.isPut()) {
            seenPuts.put(entry.getKey(), entry.getValue());
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
     * @param predicate if not null, then only apply entries from the index segment if the predicate is true.
     * @param keyFinalStates a {@link Map} of key to {@link IndexFinalState}.
     * @throws StoreException
     */
    private void processIndexSegmentEntriesBackward(IndexSegment indexSegment, Predicate<IndexEntry> predicate,
        Map<StoreKey, IndexFinalState> keyFinalStates) throws StoreException {
      diskIOScheduler.getSlice(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, BlobStoreStats.IO_SCHEDULER_JOB_ID,
          indexSegment.size());
      logger.trace("Processing index entries backward by IndexScanner for segment {} for store {}",
          indexSegment.getFile().getName(), storeId);

      // valid index entries wrt log segment reference time
      forEachValidIndexEntry(indexSegment, newScanResults.logSegmentForecastStartTimeMsForDeleted,
          newScanResults.logSegmentForecastStartTimeMsForExpired, keyFinalStates, false, entry -> {
            if (predicate == null || predicate.test(entry)) {
              processEntryForLogSegmentBucket(newScanResults, entry, keyFinalStates);
            }
          });
      // valid index entries wrt container reference time
      forEachValidIndexEntry(indexSegment, newScanResults.containerForecastStartTimeMs,
          newScanResults.containerForecastStartTimeMs, keyFinalStates, true, entry -> {
            if (predicate == null || predicate.test(entry)) {
              processEntryForContainerBucket(newScanResults, entry, keyFinalStates);
            }
          });
    }

    void cancel() {
      cancelled = true;
    }
  }

  /**
   * @return the storeId for this {@link BlobStore}.
   */
  String getStoreId() {
    return this.storeId;
  }

  /**
   * @return the {@link StoreMetrics} for this {@link BlobStore}.
   */
  StoreMetrics getMetrics() {
    return this.metrics;
  }

  /**
   * An action to take on a single {@link IndexEntry}
   */
  @FunctionalInterface
  private interface IndexEntryAction {
    /**
     * @param indexEntry the entry to process.
     * @throws StoreException if there is an error reading from the index.
     */
    void accept(IndexEntry indexEntry) throws StoreException;
  }

  /**
   * A helper class to represents the final state of a blob.
   */
  private class IndexFinalState {
    private final byte flags;
    private final long operationTime;
    private final short lifeVersion;

    /**
     * Constructor to construct an {@link IndexFinalState}.
     * @param flags
     * @param operationTime
     * @param lifeVersion
     */
    IndexFinalState(byte flags, long operationTime, short lifeVersion) {
      this.flags = flags;
      this.operationTime = operationTime;
      this.lifeVersion = lifeVersion;
    }

    /**
     * @return the operation time of the final {@link IndexValue}.
     */
    public long getOperationTime() {
      return operationTime;
    }

    /**
     * @return the lifeVersion of the final {@link IndexValue}.
     */
    public short getLifeVersion() {
      return lifeVersion;
    }

    /**
     * @return true if the final {@link IndexValue}'s TTL_UPDATE flag is true.
     */
    public boolean isTtlUpdate() {
      return isFlagSet(IndexValue.Flags.Ttl_Update_Index);
    }

    /**
     * @return true if the final {@link IndexValue} is an UNDELETE.
     */
    public boolean isUndelete() {
      return isFlagSet(IndexValue.Flags.Undelete_Index);
    }

    /**
     * @return true if the final {@link IndexValue} is an DELETE.
     */
    public boolean isDelete() {
      return isFlagSet(IndexValue.Flags.Delete_Index);
    }

    private boolean isFlagSet(IndexValue.Flags flag) {
      return (flags & (1 << flag.ordinal())) != 0;
    }
  }

  private class EntryContext {
    final StoreKey key; // This is for UNDELETE and DELETE
    final IndexValue currentValue;
    final IndexValue originalPutValue;
    final IndexValue previousValue; // This is for UNDELETE and DELETE

    EntryContext(StoreKey key, IndexValue currentValue, IndexValue originalPutValue, IndexValue previousValue) {
      this.key = key;
      this.currentValue = currentValue;
      this.originalPutValue = originalPutValue;
      this.previousValue = previousValue;
    }
  }
}
