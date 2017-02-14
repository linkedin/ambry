package com.github.ambry.store;

import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
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


public class StatsEngine {
  private final long SEGMENT_SCAN_OFFSET;
  private final long BUCKET_COUNT;
  private final long BUCKET_TIMESPAN;
  private final HashMap<String, SizeStats> containerMap;
  private final HashMap<String, SizeStats> segmentMap;
  private final TreeMap<Long, Bucket> containerBuckets;
  private final TreeMap<Long, Bucket>  segmentBuckets;
  private final Log log;
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
    this.log = log;
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
    /*
    * Deserialize persisted supporting data structures and swap part or all of it to BlobStoreStats
    * so there is something available to answer incoming requests while scanning.
    * deserialize();
    * blolbStoreStats.swapContainerStruct()
    * blobStoreStats.swapSegmentStruct
    **/
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
    return refTime - refTime % BUCKET_TIMESPAN + BUCKET_TIMESPAN;
  }

  public void updateMap(HashMap<String, SizeStats> map, String key, long size) {
    SizeStats sizeStats = map.get(key);
    if (sizeStats == null) {
      sizeStats = new SizeStats();
      sizeStats.setTotalSize(size);
      map.put(key, sizeStats);
    } else {
      sizeStats.setTotalSize(sizeStats.getTotalSize() + size);
    }
  }

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

  public void processDeleteIndex(IndexValue value, String key, long refTime,
      HashMap<String, SizeStats> map, TreeMap<Long, Bucket> buckets) {
    if (value.getExpiresAtMs() == -1) {
      updateMap(map, key, value.getSize() * -1);
    } else if (!isExpired(value.getExpiresAtMs(), refTime)) {
      updateMap(map, key, value.getSize() * -1);
      /** Could check window instead of contains */
      if (checkBucketEntryExists(buckets, generateBucketTime(value.getExpiresAtMs()), key)) {
        updateBuckets(buckets, generateBucketTime(value.getExpiresAtMs()), key, value.getSize());
      }
    }
  }

  public IndexValue investigateDeleteIndex(MessageInfo messageInfo,
      IndexValue indexValue, FileSpan backwardSearchSpan) throws StoreException {
    IndexValue retValue = index.findKey(messageInfo.getStoreKey(), backwardSearchSpan);
    return retValue;
  }

  public boolean isWithinWindow(long eventRefTime, long window, long offSet) {
    return eventRefTime < window - offSet;
  }

  public boolean checkBucketEntryExists(TreeMap<Long, Bucket> buckets, long bucketKey, String key) {
    return buckets.containsKey(bucketKey) && buckets.get(bucketKey).contains(key);
  }

  /**public int popBucket(TreeMap<Long, Bucket> containerBuckets, TreeMap<Long, Bucket> segmentBuckets) {
    long normalizedSegmentEndTime = segmentBuckets.isEmpty() ? 0 : segmentBuckets.lastKey() + SEGMENT_SCAN_OFFSET;
    long containerEndTime = containerBuckets.isEmpty() ? 0 : containerBuckets.lastKey();
    TreeMap<Long, Bucket> selected;
    if (normalizedSegmentEndTime >= containerEndTime) {
      selected = segmentBuckets;
    } else {
      selected = containerBuckets;
    }
    return selected.pollLastEntry().getValue().getEntryCount();
  }

  public long determineNewWindow(TreeMap<Long, Bucket> containerBuckets, TreeMap<Long, Bucket> segmentBuckets, long newEntryTimeRef) {
    long lastContainerBucket = containerBuckets.isEmpty() ? 0L : containerBuckets.lastKey();
    long lastSegmentBucket = segmentBuckets.isEmpty() ? 0L : segmentBuckets.lastKey() + SEGMENT_SCAN_OFFSET;
    return Math.max(Math.max(lastContainerBucket, lastSegmentBucket), newEntryTimeRef);
  }*/

  public boolean isExpired(long expirationTime, long refTime) {
    return expirationTime < refTime && expirationTime != Utils.Infinite_Time;
  }

  /*private void handleMapInsertion(HashMap<String, SizeStats> map, String key, long size) {
    if (!map.containsKey(key)) {
      handleEntryCount(-1);
      totalEntryCount++;
    }
    updateMap(map, key, size);
  }*/

  /*private void handleBucketInsertion(TreeMap<Long, Bucket> buckets, Long bucketEndTime, String key, long size) {
    if (!checkBucketEntryExists(buckets, bucketEndTime, key)) {
      handleEntryCount(bucketEndTime);
      totalEntryCount++;
    }
    updateBuckets(buckets, bucketEndTime, key, size);
  }*/

  /*private void handleEntryCount(long newEntryTime) {
    if (totalEntryCount >= MAX_ENTRY_COUNT) {
      totalEntryCount -= popBucket(containerBuckets, segmentBuckets);
      windowBoundary = determineNewWindow(containerBuckets, segmentBuckets, newEntryTime);
    }
  }*/

  private void clearStructs() {
    this.containerMap.clear();
    this.segmentMap.clear();
    this.containerBuckets.clear();
    this.segmentBuckets.clear();
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
      /**
       * long scanTime = now;
       * long nextScanTime = -1;
       *
       * for (MockIndexSegment indexSegment : index.indexes.values()) {
       *   List<MockMessageInfo> entries = new List<>()
       *   indexSegment.getEntries(entries);
       *   for (MockMessageInfo messageInfo : entries) {
       *
       *    segment mapping:
       *        Non-expired (based on scanTime and segmentScanOffset) PUTs and DELETEs that already occurred are
       *        aggregated into appropriate entries in segmentMap.
       *        PUTs that are expiring within the current window are aggregated into appropriate entries in segmentBuckets.
       *        PUTs and DELETEs after the scanTime - segmentScanOffset are also aggregated into appropriate entries in segmentBuckets.
       *
       *        DELETEs associated with blobs that are expiring within current window will require update to appropriate
       *        buckets to avoid double counting.
       *
       *     serviceId/containerId mapping:
       *        All non-expired (based on scan time) PUTs and DELETEs (including permanent data) are aggregated into
       *        appropriate  entries in containerMap.
       *        Expiring PUTs within the current window are aggregated into appropriate entries in containerBuckets as
       *        negative values.
       *        Delete associated with blobs that are expiring within current window will require update to appropriate
       *        buckets to avoid double counting.
       *
       *     During the process dynamically adjust the window by adjusting nextScanTime
       *   }
       * }
       *
       * Copy local data struct and call blobStoreStats swap struct functions to update the data structs
       * Schedule the nextScan with nextScanTime
       * Free local data struct space
       */
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
            processIndexPerSegment(messageInfo, indexValue,
                segment.getLastModifiedTime() * 1000, segment.getLogSegmentName(), backwardSearchSpan);
            processIndexPerContainer(messageInfo, indexValue, backwardSearchSpan);
          }
          previousSegmentOffset = segment.getStartOffset();
        }
      } catch (StoreException e) {
        logger.error("StoreException thrown while calculating valid data size ", e);
      } catch (IOException e) {
        logger.error("IOException thrown while calculating valid data size ", e);
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

    private void processIndexPerSegment(MessageInfo messageInfo, IndexValue indexValue,
        long lastModifiedTime, String segmentName, FileSpan backwardSearchSpan)
        throws StoreException {
      if (!messageInfo.isDeleted() && isExpired(messageInfo.getExpirationTimeInMs(), segmentScanTimeInMs)) {
        return;
      }
      if (messageInfo.isDeleted()) {
        // Delete record
        updateMap(segmentMap, segmentName, messageInfo.getSize());
        IndexValue putIndexValue = investigateDeleteIndex(messageInfo, indexValue, backwardSearchSpan);
        if (putIndexValue == null) {
          return;
        }
        String putSegmentName = putIndexValue.getOffset().getName();
        if (lastModifiedTime < segmentScanTimeInMs) {
          processDeleteIndex(putIndexValue, putSegmentName, segmentScanTimeInMs, segmentMap, segmentBuckets);
        } else if (isWithinWindow(lastModifiedTime, windowBoundary, SEGMENT_SCAN_OFFSET)) {
          // Delete records that we need to capture in buckets
          updateBuckets(segmentBuckets, generateBucketTime(lastModifiedTime),
              putSegmentName, putIndexValue.getSize() * -1);
          /** Could check for window instead of contains*/
          if (checkBucketEntryExists(segmentBuckets, generateBucketTime(putIndexValue.getExpiresAtMs()),
              putSegmentName) && putIndexValue.getExpiresAtMs() >= lastModifiedTime) {
            updateBuckets(segmentBuckets, generateBucketTime(putIndexValue.getExpiresAtMs()),
                putSegmentName, putIndexValue.getSize());
          }
        }
      } else {
        // Put record that is not expired
        updateMap(segmentMap, segmentName, messageInfo.getSize());
        if (messageInfo.getExpirationTimeInMs() != -1 &&
            isWithinWindow(messageInfo.getExpirationTimeInMs(), windowBoundary, SEGMENT_SCAN_OFFSET)) {
          updateBuckets(segmentBuckets, generateBucketTime(messageInfo.getExpirationTimeInMs()),
              segmentName, messageInfo.getSize() * -1);
        }
      }
    }

    private void processIndexPerContainer(MessageInfo messageInfo, IndexValue indexValue,
        FileSpan backwardSearchSpan) throws StoreException {
      if (isExpired(messageInfo.getExpirationTimeInMs(), scanTimeInMs)) {
        return;
      }
      String key = messageInfo.getServiceId().concat("-".concat(messageInfo.getContainerId()));
      if (messageInfo.isDeleted()) {
        // Delete record (may need to find and remove previously counted put data size).
        IndexValue putIndexValue = investigateDeleteIndex(messageInfo, indexValue, backwardSearchSpan);
        if (putIndexValue == null) {
          return;
        }
        processDeleteIndex(putIndexValue, key, scanTimeInMs, containerMap, containerBuckets);
      } else {
        // Put record that is not expired
        updateMap(containerMap, key, messageInfo.getSize());
        if (messageInfo.getExpirationTimeInMs() != -1 &&
            isWithinWindow(messageInfo.getExpirationTimeInMs(), windowBoundary, 0)) {
          // Put record that will be expiring within current window
          updateBuckets(containerBuckets, generateBucketTime(messageInfo.getExpirationTimeInMs()),
              key, messageInfo.getSize() * -1);
        }
      }
    }
  }
}
