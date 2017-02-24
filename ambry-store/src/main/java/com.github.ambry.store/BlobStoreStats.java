package com.github.ambry.store;

import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Exposes stats related to {@link BlobStore} that is useful to different components.
 *
 * Note: This is the v1 implementation of BlobStoreStats. The v1 implementation walks through the
 * entire index and collect data needed to serve stats related requests for a predefined amount of
 * time. Requests that are outside of this time range will trigger a new scan.
 */
public class BlobStoreStats implements StoreStats {
  private final long bucketCount;
  private final long bucketTimeSpan;
  private final long segmentScanOffset;
  private final boolean autoReschedule;
  private final Log log;
  private final PersistentIndex index;
  private final ScheduledExecutorService scheduler;
  private final ConcurrentLinkedQueue<StatsEvent> bufferedEvents;
  private final Object swapLock = new Object();
  private final Object notifyObject = new Object();
  private final Time time;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private ScheduledFuture<?> nextScan;
  private boolean scanComplete;
  private boolean redirectToBuffer;
  private ScanResults scanResults;
  private long lastScanTime;
  private long servingWindowBoundary;
  private IndexScanner scanner;

  BlobStoreStats(Log log, PersistentIndex index, long bucketCount, long bucketTimeSpan,
      long segmentScanOffset, boolean autoReschedule, Time time) {
    this.log = log;
    this.index = index;
    this.bucketCount = bucketCount;
    this.bucketTimeSpan = bucketTimeSpan;
    this.segmentScanOffset = segmentScanOffset;
    this.autoReschedule = autoReschedule;
    this.redirectToBuffer = true;
    this.time = time;
    this.servingWindowBoundary = -1;
    this.lastScanTime = -1;
    this.scheduler = Utils.newScheduler(1, true);
    ((ScheduledThreadPoolExecutor)scheduler).setRemoveOnCancelPolicy(true);
    this.bufferedEvents = new ConcurrentLinkedQueue<>();
    this.scanner = null;
    this.scanComplete = false;
  }

  @Override
  public long getTotalCapacity() {
    return log.getCapacityInBytes();
  }

  @Override
  public long getUsedCapacity() {
    return log.getCapacityInBytes();
  }

  /**
   * Gets the used capacity of each log segment.
   * @return the used capacity of each log segment.
   */
  public SortedMap<String, Long> getUsedCapacityBySegment() {
    SortedMap<String, Long> usedCapacityBySegments = new TreeMap<>();
    LogSegment logSegment = log.getFirstSegment();
    while (logSegment != null) {
      usedCapacityBySegments.put(logSegment.getName(), logSegment.getEndOffset());
      logSegment = log.getNextSegment(logSegment);
    }
    return usedCapacityBySegments;
  }

  /**
   * Gets the total size of valid data for all log segments
   * @param timeRange the time range for the expected output. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the exact time at which the stats are valid and whose second
   * element is the total valid data size.
   */
  public Pair<Long, Long> getValidDataSize(TimeRange timeRange) {
    checkForInitialScan();
    Long allSegmentValidDataSize = 0L;
    Long lastBucketEndTime = processTimeRange(timeRange);
    if (lastBucketEndTime != -1) {
      synchronized (swapLock) {
        SortedMap<String, Long> map = scanResults.getValidDataSizePerSegment(lastBucketEndTime);
        for (Long value : map.values()) {
          allSegmentValidDataSize += value;
        }
      }
      return new Pair<>(lastBucketEndTime / Time.MsPerSec, allSegmentValidDataSize);
    } else {
      return outOfBoundValidDataSizeRequest(timeRange);
    }
  }

  /**
   * Gets the size of valid data at a particular point in time for all log segments. The caller specifies a reference time and
   * acceptable resolution for the stats in the form of a {@link TimeRange}. The store will return data for a point in time within
   * the specified range.
   * @param timeRange the time range for the expected output. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the exact time at which the stats are valid and whose second element is the valid data
   * size for each segment in the form of a {@link SortedMap} of segment names to valid data sizes.
   */
  public Pair<Long, SortedMap<String, Long>> getValidDataSizeBySegment(TimeRange timeRange) {
    checkForInitialScan();
    SortedMap<String, Long> map;
    Long lastBucketEndTime = processTimeRange(timeRange);
    if (lastBucketEndTime != -1) {
      synchronized (swapLock) {
        map = scanResults.getValidDataSizePerSegment(lastBucketEndTime);
      }
      return new Pair<>(lastBucketEndTime / Time.MsPerSec, map);
    } else {
      return outOfBoundValidDataSizeBySegmentRequest(timeRange);
    }
  }

  /**
   * Gets the valid data size of all containers in this blob store.
   * @return nested {@link HashMap} of serviceIds to containerIds to their corresponding valid data size
   */
  public HashMap<String, HashMap<String, Long>> getValidDataSize() {
    checkForInitialScan();
    Long referenceTime = time.milliseconds();
    if (referenceTime > servingWindowBoundary) {
      scanComplete = false;
      rescheduleScan(referenceTime, -1);
      waitForScanToComplete();
    }
    Long lastBucketEndTime = referenceTime - referenceTime % bucketTimeSpan;
    synchronized (swapLock) {
      return scanResults.getValidDataSizePerContainer(lastBucketEndTime);
    }
  }

  /**
   * Takes a {@link TimeRange} and compute the latest bucket that falls into the given time range.
   * @param timeRange the time range for the expected output. Defines both the reference time and the acceptable resolution.
   * @return The latest bucket end time that is within the given time range or -1 if there are no bucket found within
   * the given time range.
   */
  private long processTimeRange(TimeRange timeRange) {
    long startTimeInMs = timeRange.getStart() * Time.MsPerSec;
    long endTimeInMs = timeRange.getEnd() * Time.MsPerSec;
    if (startTimeInMs <= servingWindowBoundary - segmentScanOffset && endTimeInMs >= lastScanTime - segmentScanOffset) {
      long referenceTime = endTimeInMs > servingWindowBoundary ? servingWindowBoundary : endTimeInMs;
      long previousBucketEndTime = referenceTime - referenceTime % bucketTimeSpan;
      if (previousBucketEndTime >= startTimeInMs) {
        return previousBucketEndTime;
      }
    }
    return -1;
  }

  void start() {
    scanner = new IndexScanner(time.milliseconds(), -1);
    nextScan = scheduler.schedule(scanner, 0, TimeUnit.MILLISECONDS);
  }

  void close () {
    if (nextScan != null) {
      nextScan.cancel(true);
    }
    scanComplete = true;
  }

  private void rescheduleScan(long newScanTime, long requestedReferenceTime) {
    if (nextScan != null && !nextScan.isDone()) {
      nextScan.cancel(false);
    }
    nextScan = scheduler.schedule(new IndexScanner(newScanTime, requestedReferenceTime),
        newScanTime - time.milliseconds(), TimeUnit.MILLISECONDS);
  }

  private Pair<Long, Long> outOfBoundValidDataSizeRequest(TimeRange timeRange) {
    if (!scanComplete && timeRange.getStart() * Time.MsPerSec <= scanner.scanWindowBoundary - segmentScanOffset &&
        timeRange.getEnd() * Time.MsPerSec >= scanner.scanTimeWithOffsetInMs) {
      waitForScanToComplete();
      return getValidDataSize(timeRange);
    } else {
      scanComplete = false;
      rescheduleScan(time.milliseconds(), timeRange.getStart() * Time.MsPerSec);
      waitForScanToComplete();
      Long allSegmentValidDataSize = 0L;
      for (Long value : scanResults.getRequestedSegmentMap().values()) {
        allSegmentValidDataSize += value;
      }
      return new Pair<>(timeRange.getStart(), allSegmentValidDataSize);
    }
  }

  private Pair<Long, SortedMap<String, Long>> outOfBoundValidDataSizeBySegmentRequest(TimeRange timeRange) {
    if (!scanComplete && timeRange.getStart() * Time.MsPerSec <= scanner.scanWindowBoundary - segmentScanOffset &&
        timeRange.getEnd() * Time.MsPerSec >= scanner.scanTimeWithOffsetInMs) {
      waitForScanToComplete();
      return getValidDataSizeBySegment(timeRange);
    } else {
      scanComplete = false;
      rescheduleScan(time.milliseconds(), timeRange.getStart() * Time.MsPerSec);
      waitForScanToComplete();
      return new Pair<>(timeRange.getStart(), scanResults.getRequestedSegmentMap());
    }
  }

  private void checkForInitialScan() {
    if (lastScanTime == -1) {
      waitForScanToComplete();
    }
  }

  private void waitForScanToComplete() {
    synchronized (notifyObject) {
      while (!scanComplete) {
        try {
          notifyObject.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private Offset prepareForScanning() {
    this.redirectToBuffer = true;
    return index.getCurrentEndOffset();
  }

  private void wrapUpScanning(Offset checkPoint) {
    redirectToBuffer = false;
    boolean isCaughtUp = processBufferedStatsEvent(checkPoint);
    while (!isCaughtUp) {
      isCaughtUp = processBufferedStatsEvent(checkPoint);
    }
  }

  private boolean processBufferedStatsEvent(Offset checkPoint) {
    StatsEvent event = bufferedEvents.poll();
    if (event == null) {
      return true;
    } else {
      if (event.getOffset().compareTo(checkPoint) >= 0) {
        if (event.isDelete()) {
          handleNewDelete(event.getMessageInfo(), event.getPutIndexValue(), event.getOffset(), event.getEventTime());
        } else {
          handleNewPut(event.getMessageInfo(), event.getOffset());
        }
      }
      return false;
    }
  }

  private void swapStructs(ScanResults results, long windowBoundary, long scanTime) {
    synchronized (swapLock) {
      this.scanResults = results;
      this.servingWindowBoundary = windowBoundary;
      this.lastScanTime = scanTime;
    }
  }

  void processNewPutEntries(List<MessageInfo> messageInfos, ArrayList<IndexEntry> indexEntries) {
    if (redirectToBuffer) {
      for (int i = 0; i < messageInfos.size(); i++) {
        bufferedEvents.add(new StatsEvent(messageInfos.get(i), indexEntries.get(i).getValue().getOffset(),
            time.milliseconds()));
      }
    } else {
      for (int i = 0; i < messageInfos.size(); i++) {
        handleNewPut(messageInfos.get(i), indexEntries.get(i).getValue().getOffset());
      }
    }
  }

  void processNewDeleteEntry(MessageInfo messageInfo, IndexValue putIndexValue, Offset offset) {
    if (redirectToBuffer) {
      bufferedEvents.add(new StatsEvent(messageInfo, offset, putIndexValue, time.milliseconds(), true));
    } else {
      handleNewDelete(messageInfo, putIndexValue, offset, time.milliseconds());
    }
  }

  private void handleNewPut(MessageInfo messageInfo, Offset offset) {
    scanResults.updateSegmentBaseMap(offset.getName(), messageInfo.getSize());
    long expirationTime = messageInfo.getExpirationTimeInMs();
    if (!isExpired(expirationTime, time.milliseconds())) {
      scanResults.updateContainerBaseMap(messageInfo.getServiceId(), messageInfo.getContainerId(),
          messageInfo.getSize());
      if (expirationTime != -1 && isWithinWindow(expirationTime, servingWindowBoundary, 0)) {
        scanResults.updateContainerBucket(expirationTime, messageInfo.getServiceId(), messageInfo.getContainerId(),
            messageInfo.getSize() * -1);
      }
    }

    if (expirationTime != -1  && isWithinWindow(expirationTime, servingWindowBoundary, segmentScanOffset) &&
        !isExpired(expirationTime, time.milliseconds() - segmentScanOffset)) {
      scanResults.updateSegmentBucket(expirationTime, offset.getName(), messageInfo.getSize() * -1);
    }
  }

  private void handleNewDelete(MessageInfo messageInfo, IndexValue putIndexValue,
      Offset offset, long deleteTimeInMs) {
    processDeleteIndexForContainer(putIndexValue, scanResults, messageInfo.getServiceId(), messageInfo.getContainerId(),
        time.milliseconds());
    scanResults.updateSegmentBaseMap(offset.getName(), messageInfo.getSize());
    if (isWithinWindow(deleteTimeInMs, servingWindowBoundary, segmentScanOffset)) {
      // Delete records that we need to capture in buckets
      String putSegmentName = putIndexValue.getOffset().getName();
      scanResults.updateSegmentBucket(deleteTimeInMs, putSegmentName,putIndexValue.getSize() * -1);
      long putExpirationTime = putIndexValue.getExpiresAtMs();
      if (scanResults.checkIfBucketEntryExists(putExpirationTime, putSegmentName) &&
          putExpirationTime >= deleteTimeInMs) {
        scanResults.updateSegmentBucket(putExpirationTime, putSegmentName, putIndexValue.getSize());
      }
    }
  }

  private boolean isExpired(long expirationTime, long referenceTime) {
    return expirationTime < referenceTime && expirationTime != Utils.Infinite_Time;
  }

  private void processDeleteIndexForSegment(IndexValue putIndexValue, ScanResults results, String key,
      Long referenceTime) {
    if (putIndexValue.getExpiresAtMs() == -1) {
      results.updateSegmentBaseMap(key, putIndexValue.getSize() * -1);
    } else if (!isExpired(putIndexValue.getExpiresAtMs(), referenceTime)) {
      results.updateSegmentBaseMap(key, putIndexValue.getSize() * -1);
      if (results.checkIfBucketEntryExists(putIndexValue.getExpiresAtMs(), key)) {
        results.updateSegmentBucket(putIndexValue.getExpiresAtMs(), key, putIndexValue.getSize());
      }
    }
  }

  private void processDeleteIndexForContainer(IndexValue putIndexValue, ScanResults results, String serviceId,
      String containerId, Long referenceTime) {
    if (putIndexValue.getExpiresAtMs() == -1) {
      results.updateContainerBaseMap(serviceId, containerId, putIndexValue.getSize() * -1);
    } else if (!isExpired(putIndexValue.getExpiresAtMs(), referenceTime)) {
      results.updateContainerBaseMap(serviceId, containerId, putIndexValue.getSize() * -1);
      if (results.checkIfBucketEntryExists(putIndexValue.getExpiresAtMs(), serviceId, containerId)) {
        results.updateContainerBucket(putIndexValue.getExpiresAtMs(), serviceId, containerId,
            putIndexValue.getSize());
      }
    }
  }

  private boolean isWithinWindow(long referenceTime, long window, long offSet) {
    return referenceTime < window - offSet;
  }

  private class IndexScanner implements Runnable {
    final long scanTimeInMs;
    final long scanTimeWithOffsetInMs;
    final ScanResults newScanResults;
    final long scanWindowBoundary;
    final long requestedScanReferenceTime;

    IndexScanner(long scanTimeInMs, long requestedScanReferenceTime) {
      this.scanTimeInMs = scanTimeInMs;
      this.scanTimeWithOffsetInMs = scanTimeInMs - segmentScanOffset;
      this.newScanResults = new ScanResults(bucketCount, bucketTimeSpan);
      this.scanWindowBoundary = newScanResults.createBuckets(scanTimeInMs, segmentScanOffset);
      this.requestedScanReferenceTime = requestedScanReferenceTime;
    }

    @Override
    public void run() {
      Offset checkpoint = prepareForScanning();
      Offset previousSegmentOffset = null;
      try {
        for (Map.Entry<Offset, IndexSegment> indexSegmentEntry : index.indexes.entrySet()) {
          IndexSegment segment = indexSegmentEntry.getValue();
          if (segment.getStartOffset().compareTo(checkpoint) >= 0) {
            break;
          }
          List<MessageInfo> messageInfos = new ArrayList<>();
          segment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE),
              messageInfos, new AtomicLong(0));
          FileSpan currentSpan = new FileSpan(segment.getStartOffset(), segment.getStartOffset());
          Offset backwardSearchEndOffset = previousSegmentOffset == null ?
              segment.getStartOffset() : previousSegmentOffset;
          FileSpan backwardSearchSpan = new FileSpan(index.indexes.firstEntry().getValue().getStartOffset(),
              backwardSearchEndOffset);
          for (MessageInfo messageInfo : messageInfos) {
            IndexValue indexValue = index.findKey(messageInfo.getStoreKey(), currentSpan);
            if (indexValue.getOffset().compareTo(checkpoint) >= 0) {
              continue;
            }
            populateValidSizePerSegment(messageInfo, segment.getLastModifiedTime() * 1000, segment.getLogSegmentName(),
                backwardSearchSpan);
            populateValidSizePerContainer(messageInfo, backwardSearchSpan);

            if (requestedScanReferenceTime != -1) {
              populateRequestedSegmentMap(messageInfo, segment.getLastModifiedTime() * 1000, segment.getLogSegmentName(),
                  backwardSearchSpan);
            }
          }
          previousSegmentOffset = segment.getStartOffset();
        }
      } catch (StoreException e) {
        logger.error("StoreException thrown while scanning for valid data size ", e);
      } catch (IOException e) {
        logger.error("IOException thrown while scanning for valid data size ", e);
      } finally {
        swapStructs(newScanResults, scanWindowBoundary, scanTimeInMs);
        boolean isCaughtUp = processBufferedStatsEvent(checkpoint);
        while(!isCaughtUp) {
          isCaughtUp = processBufferedStatsEvent(checkpoint);
        }
        wrapUpScanning(checkpoint);
        if (autoReschedule) { rescheduleScan(servingWindowBoundary, -1); }
        synchronized (notifyObject) {
          scanComplete = true;
          notifyObject.notify();
        }
      }
    }

    /**
     * Process a record in the index to collect data for valid data size per segment.
     * purpose.
     * @param messageInfo The {@link MessageInfo} to be processed.
     * @param lastModifiedTime The time stamp of the processed index value.
     * @param segmentName The name of the segment where the processed index value belongs to.
     * @param backwardSearchSpan A {@link FileSpan} that spans from the beginning of the index to the end of the
     *                           previous index segment.
     * @throws StoreException
     */
    private void populateValidSizePerSegment(MessageInfo messageInfo, Long lastModifiedTime, String segmentName,
        FileSpan backwardSearchSpan) throws StoreException {
      if (!messageInfo.isDeleted() && isExpired(messageInfo.getExpirationTimeInMs(), scanTimeWithOffsetInMs)) {
        return;
      }
      if (messageInfo.isDeleted()) {
        // Delete record
        newScanResults.updateSegmentBaseMap(segmentName, messageInfo.getSize());
        IndexValue putIndexValue = index.findKey(messageInfo.getStoreKey(), backwardSearchSpan);
        if (putIndexValue == null) {
          if (lastModifiedTime > scanTimeWithOffsetInMs) {
            BlobReadOptions originalPutInfo = index.getBlobReadInfo(messageInfo.getStoreKey(),
                EnumSet.of(StoreGetOptions.Store_Include_Deleted, StoreGetOptions.Store_Include_Expired));
            if (originalPutInfo != null && !isExpired(originalPutInfo.getExpiresAtMs(), scanTimeWithOffsetInMs)) {
              newScanResults.updateSegmentBaseMap(segmentName, originalPutInfo.getSize());
              long removalTime = originalPutInfo.getExpiresAtMs() == Utils.Infinite_Time ?
                  lastModifiedTime : Math.min(lastModifiedTime, originalPutInfo.getExpiresAtMs());
              newScanResults.updateSegmentBucket(removalTime, segmentName, originalPutInfo.getSize() * -1);
            }
          }
        } else {
          String putSegmentName = putIndexValue.getOffset().getName();
          if (lastModifiedTime < scanTimeWithOffsetInMs) {
            processDeleteIndexForSegment(putIndexValue, newScanResults, putSegmentName, scanTimeWithOffsetInMs);
          } else if (isWithinWindow(lastModifiedTime, scanWindowBoundary, segmentScanOffset)) {
            // Delete records that we need to capture in buckets
            newScanResults.updateSegmentBucket(lastModifiedTime, putSegmentName, putIndexValue.getSize() * -1);
            if (newScanResults.checkIfBucketEntryExists(putIndexValue.getExpiresAtMs(), putSegmentName) &&
                putIndexValue.getExpiresAtMs() >= lastModifiedTime) {
              newScanResults.updateSegmentBucket(putIndexValue.getExpiresAtMs(), putSegmentName,
                  putIndexValue.getSize());
            }
          }
        }
      } else {
        // Put record that is not expired
        newScanResults.updateSegmentBaseMap(segmentName, messageInfo.getSize());
        if (messageInfo.getExpirationTimeInMs() != -1 &&
            isWithinWindow(messageInfo.getExpirationTimeInMs(), scanWindowBoundary, segmentScanOffset)) {
          newScanResults.updateSegmentBucket(messageInfo.getExpirationTimeInMs(), segmentName,
              messageInfo.getSize() * -1);
        }
      }
    }

    /**
     * Process a record in the index to collect data for valid data size per container.
     * purpose.
     * @param messageInfo The {@link MessageInfo} to be processed.
     * @param backwardSearchSpan A {@link FileSpan} that spans from the beginning of the index to the end of the
     *                           previous index segment.
     * @throws StoreException
     */
    private void populateValidSizePerContainer(MessageInfo messageInfo, FileSpan backwardSearchSpan) throws StoreException {
      if (isExpired(messageInfo.getExpirationTimeInMs(), scanTimeInMs)) {
        return;
      }
      if (messageInfo.isDeleted()) {
        // delete record (may need to find and remove previously counted put data size).
        IndexValue putIndexValue = index.findKey(messageInfo.getStoreKey(), backwardSearchSpan);
        if (putIndexValue == null) {
          return;
        }
        processDeleteIndexForContainer(putIndexValue, newScanResults, messageInfo.getServiceId(),
            messageInfo.getContainerId(), scanTimeInMs);
      } else {
        // put record that is not expired
        newScanResults.updateContainerBaseMap(messageInfo.getServiceId(), messageInfo.getContainerId(),
            messageInfo.getSize());
        if (messageInfo.getExpirationTimeInMs() != -1 &&
            isWithinWindow(messageInfo.getExpirationTimeInMs(), scanWindowBoundary, 0)) {
          // put record that will be expiring within current window
          newScanResults.updateContainerBucket(messageInfo.getExpirationTimeInMs(),
              messageInfo.getServiceId(), messageInfo.getContainerId(), messageInfo.getSize() * -1);
        }
      }
    }

    /**
     * Process a record in the index to collect data with the requested reference time for valid data size per segment.
     * @param messageInfo The {@link MessageInfo} to be processed.
     * @param lastModifiedTime The time stamp of the processed index value.
     * @param segmentName The name of the segment where the processed index value belongs to.
     * @param backwardSearchSpan A {@link FileSpan} that spans from the beginning of the index to the end of the
     *                           previous index segment.
     * @throws StoreException
     */
    private void populateRequestedSegmentMap(MessageInfo messageInfo, Long lastModifiedTime, String segmentName,
        FileSpan backwardSearchSpan) throws StoreException {
      if (!messageInfo.isDeleted() && isExpired(messageInfo.getExpirationTimeInMs(), requestedScanReferenceTime)) {
        return;
      }
      if (messageInfo.isDeleted()) {
        // Delete record
        newScanResults.updateRequestedSegmentMap(segmentName, messageInfo.getSize());
        IndexValue putIndexValue = index.findKey(messageInfo.getStoreKey(), backwardSearchSpan);
        if (putIndexValue == null) {
          if (lastModifiedTime > requestedScanReferenceTime){
            BlobReadOptions originalPutInfo = index.getBlobReadInfo(messageInfo.getStoreKey(),
                EnumSet.of(StoreGetOptions.Store_Include_Deleted, StoreGetOptions.Store_Include_Expired));
            if (originalPutInfo != null && !isExpired(originalPutInfo.getExpiresAtMs(), requestedScanReferenceTime)) {
              newScanResults.updateRequestedSegmentMap(segmentName, originalPutInfo.getSize());
            }
          }
        }
        else {
          if (lastModifiedTime < requestedScanReferenceTime && !isExpired(putIndexValue.getExpiresAtMs(),
              requestedScanReferenceTime)) {
            String putSegmentName = putIndexValue.getOffset().getName();
            newScanResults.updateRequestedSegmentMap(putSegmentName, putIndexValue.getSize() * -1);
          }
        }
      } else {
        // Put record that is not expired
        newScanResults.updateRequestedSegmentMap(segmentName, messageInfo.getSize());
      }
    }
  }
}