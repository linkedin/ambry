package com.github.ambry.store;

import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The stats engine contains most of the business logic responsible for constructing and maintaining the data structures
 * required by the {@link BlobStoreStats} to forecast the valid data size per segment and container at various time
 * points.
 */
public class StatsEngine {
  private final long SEGMENT_SCAN_OFFSET;
  private final long BUCKET_COUNT;
  private final long BUCKET_TIMESPAN;
  private final HashMap<String, Long> containerMap;
  private final HashMap<String, Long> segmentMap;
  private final TreeMap<Long, Bucket> containerBuckets;
  private final TreeMap<Long, Bucket>  segmentBuckets;
  private final PersistentIndex index;
  private BlobStoreStats blobStoreStats;
  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> nextScan;
  private boolean isScanning;
  private final Time time;
  private final long capacityInBytes;
  private final Object notifyObject;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public StatsEngine(Log log, PersistentIndex index, long capacityInBytes, int bucketCount,
      long bucketTimeSpan, long segmentScanOffset, Time time, Object notifyObject) {
    this.index = index;
    this.time = time;
    this.blobStoreStats = new BlobStoreStats(this, log, segmentScanOffset, time);
    this.containerBuckets = new TreeMap<>();
    this.segmentBuckets = new TreeMap<>();
    this.containerMap = new HashMap<>();
    this.segmentMap = new HashMap<>();
    this.SEGMENT_SCAN_OFFSET = segmentScanOffset;
    this.BUCKET_COUNT = bucketCount;
    this.BUCKET_TIMESPAN = bucketTimeSpan;
    this.capacityInBytes = capacityInBytes;
    this.notifyObject = notifyObject;
    scheduler = Utils.newScheduler(1, true);
    ((ScheduledThreadPoolExecutor)scheduler).setRemoveOnCancelPolicy(true);
  }

  public BlobStoreStats getBlobStoreStats() {
    return blobStoreStats;
  }

  public void start() {
    nextScan = scheduler.schedule(new IndexScanner(time.milliseconds()), 0, TimeUnit.MILLISECONDS);
  }

  public void close() {
    if (nextScan != null) {
      nextScan.cancel(true);
    }
    isScanning = false;
  }

  public long getCapacityInBytes() {
    return capacityInBytes;
  }

  public long getPreviousBucketEndTime(long refTime) {
    return refTime - refTime % BUCKET_TIMESPAN;
  }

  public boolean isScanning() {
    return isScanning;
  }

  public void rescheduleScan(long newScanTime) {
    if (nextScan != null) {
      nextScan.cancel(true);
    }
    nextScan = scheduler.schedule(new IndexScanner(newScanTime), newScanTime - time.milliseconds(),
        TimeUnit.MILLISECONDS);
  }

  public long generateBucketTime(long refTime) {
    long remainder = refTime % BUCKET_TIMESPAN;
    return remainder == 0 ? refTime : refTime - remainder + BUCKET_TIMESPAN;
  }

  /**
   * Helper function for inserting and updating HashMap entries.
   * @param map The {@link HashMap} to be updated.
   * @param key The key of the entry.
   * @param size The value that is going to be added.
   */
  public void updateMap(HashMap<String, Long> map, String key, Long size) {
    Long existingValue = map.get(key);
    if (existingValue == null) {
      existingValue = size;
    } else {
      existingValue += size;
    }
    map.put(key, existingValue);
  }

  /**
   * Helper function for inserting and updating bucket entries.
   * @param buckets The {@link TreeMap} representation of the buckets to be updated.
   * @param bucketKey The key of the bucket (bucket end time).
   * @param key The key for the {@link HashMap} inside the bucket.
   * @param value The value that is going to be added.
   */
  public void updateBuckets(TreeMap<Long, Bucket> buckets, Long bucketKey, String key, Long value) {
    Bucket bucket = buckets.get(bucketKey);
    if (bucket == null) {
      bucket = new Bucket(bucketKey);
      bucket.updateValue(key, value);
      buckets.put(bucketKey, bucket);
    } else {
      bucket.updateValue(key, value);
    }
  }

  /**
   * Helper function that processes deleted put record.
   * @param putIndexValue The {@link IndexValue} of the deleted put record.
   * @param key The key of the deleted record (either segment name or serviceId + containerId).
   * @param refTime A reference time indicating the time of the deletion.
   * @param map The corresponding {@link HashMap} that needs to be updated.
   * @param buckets The corresponding {@link TreeMap} that needs to be updated.
   */
  public void processDeleteIndex(IndexValue putIndexValue, String key, Long refTime,
      HashMap<String, Long> map, TreeMap<Long, Bucket> buckets) {
    if (putIndexValue.getExpiresAtMs() == -1) {
      updateMap(map, key, putIndexValue.getSize() * -1);
    } else if (!isExpired(putIndexValue.getExpiresAtMs(), refTime)) {
      updateMap(map, key, putIndexValue.getSize() * -1);
      if (checkBucketEntryExists(buckets, generateBucketTime(putIndexValue.getExpiresAtMs()), key)) {
        updateBuckets(buckets, generateBucketTime(putIndexValue.getExpiresAtMs()), key, putIndexValue.getSize());
      }
    }
  }

  public IndexValue investigateDeleteIndex(MessageInfo messageInfo, FileSpan backwardSearchSpan) throws StoreException {
    return index.findKey(messageInfo.getStoreKey(), backwardSearchSpan);
  }

  public boolean isWithinWindow(long eventRefTime, long window, long offSet) {
    return eventRefTime < window - offSet;
  }

  public boolean checkBucketEntryExists(TreeMap<Long, Bucket> buckets, long bucketKey, String key) {
    return buckets.containsKey(bucketKey) && buckets.get(bucketKey).contains(key);
  }

  public boolean isExpired(long expirationTime, long referenceTime) {
    return expirationTime < referenceTime && expirationTime != Utils.Infinite_Time;
  }

  private long createStructs(long startRefTime) {
    long containerBucketEndTime = generateBucketTime(startRefTime);
    long segmentBucketEndTime = generateBucketTime(startRefTime - SEGMENT_SCAN_OFFSET);
    for (int i = 0; i < BUCKET_COUNT; i++) {
      containerBuckets.put(containerBucketEndTime, new Bucket(containerBucketEndTime));
      segmentBuckets.put(segmentBucketEndTime, new Bucket(segmentBucketEndTime));
      containerBucketEndTime += BUCKET_TIMESPAN;
      segmentBucketEndTime += BUCKET_TIMESPAN;
    }
    return containerBucketEndTime;
  }

  private void clearStructs() {
    this.containerMap.clear();
    this.segmentMap.clear();
    this.containerBuckets.clear();
    this.segmentBuckets.clear();
  }

  private class IndexScanner implements Runnable {
    private final long scanTimeInMs;
    private final long segmentScanTimeInMs;
    private long windowBoundary;

    public IndexScanner(long scanTimeInMs) {
      this.scanTimeInMs = scanTimeInMs;
      this.segmentScanTimeInMs = scanTimeInMs - SEGMENT_SCAN_OFFSET;
      clearStructs();
      windowBoundary = createStructs(scanTimeInMs);
    }

    @Override
    public void run() {
      Offset checkpoint = blobStoreStats.prepareForScanning();
      isScanning = true;
      Offset previousSegmentOffset = null;
      try {
        outerLoop:
        for (Map.Entry<Offset, IndexSegment> indexSegmentEntry : index.indexes.entrySet()) {
          IndexSegment segment = indexSegmentEntry.getValue();
          List<MessageInfo> messageInfos = new ArrayList<>();
          segment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE),
              messageInfos, new AtomicLong(0));
          FileSpan currentSpan = new FileSpan(segment.getStartOffset(), segment.getEndOffset());
          Offset backwardSearchEndOffset = previousSegmentOffset == null ?
              segment.getStartOffset() : previousSegmentOffset;
          FileSpan backwardSearchSpan = new FileSpan(index.indexes.firstEntry().getValue().getStartOffset(),
              backwardSearchEndOffset);
          for (MessageInfo messageInfo : messageInfos) {
            IndexValue indexValue = index.findKey(messageInfo.getStoreKey(), currentSpan);
            if (segment.getLogSegmentName().equals(checkpoint.getName()) && indexValue.getOffset().getOffset() > checkpoint.getOffset()) {
              break outerLoop;
            }
            processIndexPerSegment(messageInfo, segment.getLastModifiedTime() * 1000, segment.getLogSegmentName(),
                backwardSearchSpan);
            processIndexPerContainer(messageInfo, backwardSearchSpan);
          }
          previousSegmentOffset = segment.getStartOffset();
        }
      } catch (StoreException e) {
        logger.error("StoreException thrown while scanning for valid data size ", e);
      } catch (IOException e) {
        logger.error("IOException thrown while scanning for valid data size ", e);
      } finally {
        isScanning = false;
      }
      blobStoreStats.swapStructs(containerMap, segmentMap, containerBuckets,
          segmentBuckets, windowBoundary, scanTimeInMs);
      boolean isCaughtUp = blobStoreStats.processStatsEvent();
      while(!isCaughtUp) {
        isCaughtUp = blobStoreStats.processStatsEvent();
      }
      blobStoreStats.wrapUpScanning();
      clearStructs();
      rescheduleScan(windowBoundary);
      isScanning = false;
      synchronized (notifyObject) {
        notifyObject.notify();
      }
    }

    /**
     * Helper function to process a {@link MessageInfo} to collect data for valid data size per segment for compaction
     * purpose.
     * @param messageInfo The {@link MessageInfo} to be processed.
     * @param lastModifiedTime The time stamp of the processed index value.
     * @param segmentName The name of the segment where the processed index value belongs to.
     * @param backwardSearchSpan A {@link FileSpan} that spans from the beginning of the index to the end of the
     *                           previous index segment.
     * @throws StoreException
     */
    private void processIndexPerSegment(MessageInfo messageInfo, Long lastModifiedTime, String segmentName,
        FileSpan backwardSearchSpan) throws StoreException {
      if (!messageInfo.isDeleted() && isExpired(messageInfo.getExpirationTimeInMs(), segmentScanTimeInMs)) {
        return;
      }
      if (messageInfo.isDeleted()) {
        // Delete record
        updateMap(segmentMap, segmentName, messageInfo.getSize());
        IndexValue putIndexValue = investigateDeleteIndex(messageInfo, backwardSearchSpan);
        if (putIndexValue == null) {
          if (lastModifiedTime > segmentScanTimeInMs) {
            BlobReadOptions originalPutInfo = index.getBlobReadInfo(messageInfo.getStoreKey(),
                EnumSet.of(StoreGetOptions.Store_Include_Deleted, StoreGetOptions.Store_Include_Expired));
            if (originalPutInfo != null && !isExpired(originalPutInfo.getExpiresAtMs(), segmentScanTimeInMs)) {
              updateMap(segmentMap, segmentName, originalPutInfo.getSize());
              long removalTime = originalPutInfo.getExpiresAtMs() == Utils.Infinite_Time ?
                  lastModifiedTime : Math.min(lastModifiedTime, originalPutInfo.getExpiresAtMs());
              updateBuckets(segmentBuckets, generateBucketTime(removalTime), segmentName,
                  originalPutInfo.getSize() * -1);
            }
          }
        } else {
          String putSegmentName = putIndexValue.getOffset().getName();
          if (lastModifiedTime < segmentScanTimeInMs) {
            processDeleteIndex(putIndexValue, putSegmentName, segmentScanTimeInMs, segmentMap, segmentBuckets);
          } else if (isWithinWindow(lastModifiedTime, windowBoundary, SEGMENT_SCAN_OFFSET)) {
            // Delete records that we need to capture in buckets
            updateBuckets(segmentBuckets, generateBucketTime(lastModifiedTime), putSegmentName,
                putIndexValue.getSize() * -1);
            if (checkBucketEntryExists(segmentBuckets, generateBucketTime(putIndexValue.getExpiresAtMs()),
                putSegmentName) && putIndexValue.getExpiresAtMs() >= lastModifiedTime) {
              updateBuckets(segmentBuckets, generateBucketTime(putIndexValue.getExpiresAtMs()), putSegmentName,
                  putIndexValue.getSize());
            }
          }
        }
      } else if (!isExpired(messageInfo.getExpirationTimeInMs(), segmentScanTimeInMs)) {
        // Put record that is not expired
        updateMap(segmentMap, segmentName, messageInfo.getSize());
        if (messageInfo.getExpirationTimeInMs() != -1 &&
            isWithinWindow(messageInfo.getExpirationTimeInMs(), windowBoundary, SEGMENT_SCAN_OFFSET)) {
          updateBuckets(segmentBuckets, generateBucketTime(messageInfo.getExpirationTimeInMs()),
              segmentName, messageInfo.getSize() * -1);
        }
      }
    }

    /**
     * Helper function to process a {@link MessageInfo} to collect data for valid data size per container for quota
     * purpose.
     * @param messageInfo The {@link MessageInfo} to be processed.
     * @param backwardSearchSpan A {@link FileSpan} that spans from the beginning of the index to the end of the
     *                           previous index segment.
     * @throws StoreException
     */
    private void processIndexPerContainer(MessageInfo messageInfo, FileSpan backwardSearchSpan) throws StoreException {
      if (isExpired(messageInfo.getExpirationTimeInMs(), scanTimeInMs)) {
        return;
      }
      String key = messageInfo.getServiceId().concat("-".concat(messageInfo.getContainerId()));
      if (messageInfo.isDeleted()) {
        // delete record (may need to find and remove previously counted put data size).
        IndexValue putIndexValue = investigateDeleteIndex(messageInfo, backwardSearchSpan);
        if (putIndexValue == null) {
          return;
        }
        processDeleteIndex(putIndexValue, key, scanTimeInMs, containerMap, containerBuckets);
      } else {
        // put record that is not expired
        updateMap(containerMap, key, messageInfo.getSize());
        if (messageInfo.getExpirationTimeInMs() != -1 &&
            isWithinWindow(messageInfo.getExpirationTimeInMs(), windowBoundary, 0)) {
          // put record that will be expiring within current window
          updateBuckets(containerBuckets, generateBucketTime(messageInfo.getExpirationTimeInMs()),
              key, messageInfo.getSize() * -1);
        }
      }
    }
  }
}
