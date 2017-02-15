package com.github.ambry.store;

import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Exposes stats related to {@link BlobStore} that is useful to different components.
 *
 * Note: The initial implementation of this class will scan the indexes completely when any of these API calls are made.
 * It may store some data in memory and "expire" them but the data will not be persisted. Going forward, this will
 * change. We may persist the data, collect stats more "intelligently", actively push data from the {@link BlobStore} or
 * any other multitude of things.
 */
public class BlobStoreStats implements StoreStats {
  private final long SEGMENT_SCAN_OFFSET;
  private StatsEngine statsEngine;
  private Log log;
  private HashMap<String, Long> containerMap;
  private HashMap<String, Long> segmentMap;
  private TreeMap<Long, Bucket> containerBuckets;
  private TreeMap<Long, Bucket> segmentBuckets;
  private long lastScanTime;
  private long windowBoundary;
  private ConcurrentLinkedQueue<StatsEvent> bufferedEvents;
  private boolean redirect;
  private final Object scanLock = new Object();
  private final Object swapLock = new Object();
  private final Time time;

  BlobStoreStats(StatsEngine statsEngine, Log log, long segmentScanOffset, Time time) {
    this.statsEngine = statsEngine;
    this.log = log;
    this.containerMap = new HashMap<>();
    this.segmentMap = new HashMap<>();
    this.containerBuckets = new TreeMap<>();
    this.segmentBuckets = new TreeMap<>();
    this.bufferedEvents = new ConcurrentLinkedQueue<>();
    this.SEGMENT_SCAN_OFFSET = segmentScanOffset;
    this.redirect = false;
    this.time = time;
    this.windowBoundary = -1;
    this.lastScanTime = -1;
  }


  @Override
  public long getTotalCapacity() {
    return statsEngine.getCapacityInBytes();
  }

  @Override
  public long getUsedCapacity() {
    return log.getCapacityInBytes();
  }

  /**
   * Gets the used capacity of each log segment.
   * @return the used capacity of each log segment.
   */
  SortedMap<String, Long> getUsedCapacityBySegment() {
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
  @Override
  public Pair<Long, Long> getValidDataSize(TimeRange timeRange) {
    long lastBucketEndTime = processTimeRange(timeRange);
    if (lastBucketEndTime != -1) {
      long allSegmentValidDataSize = 0;
      synchronized (swapLock) {
        for (String segmentName : segmentMap.keySet()) {
          allSegmentValidDataSize += extractValidDataSize(segmentMap, segmentBuckets, segmentName, lastBucketEndTime);
        }
      }
      return new Pair<>(lastBucketEndTime / time.MsPerSec, allSegmentValidDataSize);
    } else {
      return new Pair<>(-1L, 0L);
    }
  }

  /**
   * Gets the size of valid data at a particular point in time for all log segments. The caller specifies a reference time and
   * acceptable resolution for the stats in the form of a {@code timeRange}. The store will return data for a point in time within
   * the specified range.
   * @param timeRange the time range for the expected output. Defines both the reference time and the acceptable resolution.
   * @return a {@link Pair} whose first element is the exact time at which the stats are valid and whose second element is the valid data
   * size for each segment in the form of a {@link SortedMap} of segment names to valid data sizes.
   */
  Pair<Long, SortedMap<String, Long>> getValidDataSizeBySegment(TimeRange timeRange) {
    TreeMap<String, Long> map = new TreeMap<>();
    long lastBucketEndTime = processTimeRange(timeRange);
    if (lastBucketEndTime != -1) {
      synchronized (swapLock) {
        for (String segmentName : segmentMap.keySet()) {
          map.put(segmentName, extractValidDataSize(segmentMap, segmentBuckets, segmentName, lastBucketEndTime));
        }
      }
      return new Pair<>(lastBucketEndTime / time.MsPerSec, map);
    } else {
      return new Pair<>(-1L, map);
    }
  }

  /**
   * Gets the size of valid data for all containers belonging to the given serviceId.
   * @param serviceId The serviceId at interest.
   * @return A {@link Long} representing the total valid data size for the given serviceId.
   */
  Long getValidDataSize(String serviceId) {
    long lastBucketEndTime = statsEngine.getPreviousBucketEndTime(time.milliseconds());
    long allValidDataSize = 0;
    synchronized (swapLock) {
      for (Map.Entry<String, Long> entry : containerMap.entrySet()) {
        if ((entry.getKey().split("-"))[0].equals(serviceId)) {
          allValidDataSize += extractValidDataSize(containerMap, containerBuckets, entry.getKey(), lastBucketEndTime);
        }
      }
    }
    return allValidDataSize;
  }

  /**
   * Gets the size of valid data for each requested containerId belonging to a single service Id
   * at the time when the API is called.
   * @param serviceId The serviceId at interest.
   * @param containerIds A list of containerIds.
   * @return A {@link SortedMap} of containerIds to their corresponding valid data size.
   */
  SortedMap<String, Long> getValidDataSizeByContainer(String serviceId, List<String> containerIds) {
    TreeMap<String, Long> map = new TreeMap<>();
    long lastBucketEndTime = statsEngine.getPreviousBucketEndTime(time.milliseconds());
    synchronized (swapLock) {
      for (String containerId : containerIds) {
        String key = serviceId.concat("-").concat(containerId);
        map.put(containerId, extractValidDataSize(containerMap, containerBuckets, key, lastBucketEndTime));
      }
    }
    return map;
  }

  /**
   * Helper function that takes a {@link TimeRange} and compute the latest bucket that falls into the given time range.
   * @param timeRange
   * @return The latest bucket end time that is within the given time range or -1 if there are no bucket found within
   * the given time range.
   */
  private long processTimeRange(TimeRange timeRange) {
    long startTimeInMs = timeRange.getStart() * time.MsPerSec;
    long endTimeInMs = timeRange.getEnd() * time.MsPerSec;
    if (lastScanTime != -1 && startTimeInMs <= windowBoundary - SEGMENT_SCAN_OFFSET &&
        endTimeInMs >= lastScanTime - SEGMENT_SCAN_OFFSET) {
      long referenceTime = endTimeInMs > windowBoundary ? windowBoundary : endTimeInMs;
      long previousBucketEndTime = statsEngine.getPreviousBucketEndTime(referenceTime);
      if (previousBucketEndTime >= startTimeInMs) {
        return previousBucketEndTime;
      }
    }
    return -1;
  }

  public Offset prepareForScanning() {
    synchronized (scanLock) {
      Offset checkpoint = log.getEndOffset();
      this.redirect = true;
      return checkpoint;
    }
  }

  public void wrapUpScanning() {
    synchronized (scanLock) {
      redirect = false;
      boolean isCaughtUp = processStatsEvent();
      while (!isCaughtUp) {
        isCaughtUp = processStatsEvent();
      }
    }
  }

  public boolean processStatsEvent() {
    StatsEvent event = bufferedEvents.poll();
    if (event == null) {
      return true;
    } else {
      if (event.isDelete()) {
        handleNewDelete(event.getMessageInfo(), event.getPutIndexValue(), event.getSegmentName(), event.getEventTime());
      } else {
        handleNewPut(event.getMessageInfo(), event.getSegmentName());
      }
      return false;
    }
  }

  public void swapStructs(HashMap<String, Long> containerMap, HashMap<String, Long> segmentMap,
      TreeMap<Long, Bucket> containerBuckets, TreeMap<Long, Bucket> segmentBuckets,
      long windowBoundary, long scanTime) {
    synchronized (swapLock) {
      this.containerMap.clear();
      this.containerBuckets.clear();
      this.segmentMap.clear();
      this.segmentBuckets.clear();
      this.containerMap.putAll(containerMap);
      this.segmentMap.putAll(segmentMap);
      this.containerBuckets.putAll(containerBuckets);
      this.segmentBuckets.putAll(segmentBuckets);
      this.windowBoundary = windowBoundary;
      this.lastScanTime = scanTime;
    }
  }

  public void processNewPutEntries(List<MessageInfo> messageInfos, ArrayList<IndexEntry> indexEntries) {
    synchronized (scanLock) {
      int i = 0;
      for (MessageInfo messageInfo : messageInfos) {
        if (redirect) {
          bufferedEvents.add(new StatsEvent(messageInfo, indexEntries.get(i).getValue().getOffset().getName(),
              time.milliseconds()));
        } else {
          handleNewPut(messageInfo, indexEntries.get(i).getValue().getOffset().getName());
        }
        i++;
      }
    }
  }

  public void processNewDeleteEntry(MessageInfo messageInfo, IndexValue putIndexValue, String segmentName) {
    synchronized (scanLock) {
      if (redirect) {
        bufferedEvents.add(new StatsEvent(messageInfo, segmentName, putIndexValue, time.milliseconds(), true));
      } else {
        handleNewDelete(messageInfo, putIndexValue, segmentName, time.milliseconds());
      }
    }
  }

  private void handleNewPut(MessageInfo messageInfo, String segmentName) {
    String key = messageInfo.getServiceId().concat("-".concat(messageInfo.getContainerId()));
    statsEngine.updateMap(segmentMap, segmentName, messageInfo.getSize());
    long expirationTime = messageInfo.getExpirationTimeInMs();
    if (!statsEngine.isExpired(expirationTime, time.milliseconds())) {
      statsEngine.updateMap(containerMap, key, messageInfo.getSize());
      if (expirationTime != -1 && statsEngine.isWithinWindow(expirationTime, windowBoundary, 0)) {
        statsEngine.updateBuckets(containerBuckets, statsEngine.generateBucketTime(expirationTime),
            key, messageInfo.getSize() * -1);
      }
    }

    if (!statsEngine.isExpired(expirationTime, time.milliseconds() - SEGMENT_SCAN_OFFSET) &&
        expirationTime != -1 && statsEngine.isWithinWindow(expirationTime, windowBoundary, SEGMENT_SCAN_OFFSET)) {
      statsEngine.updateBuckets(segmentBuckets, statsEngine.generateBucketTime(expirationTime),
          segmentName, messageInfo.getSize() * -1);
    }
  }

  private void handleNewDelete(MessageInfo messageInfo, IndexValue putIndexValue,
      String segmentName, long deleteTimeInMs) {
    String key = messageInfo.getServiceId().concat("-".concat(messageInfo.getContainerId()));
    statsEngine.processDeleteIndex(putIndexValue, key, time.milliseconds(), containerMap, containerBuckets);
    statsEngine.updateMap(segmentMap, segmentName, messageInfo.getSize());
    if (statsEngine.isWithinWindow(deleteTimeInMs, windowBoundary, SEGMENT_SCAN_OFFSET)) {
      // Delete records that we need to capture in buckets
      statsEngine.updateBuckets(segmentBuckets, statsEngine.generateBucketTime(deleteTimeInMs),
          putIndexValue.getOffset().getName(), putIndexValue.getSize() * -1);
      /** Could check for window instead contains*/
      String putSegmentName = putIndexValue.getOffset().getName();
      long putExpirationTime = putIndexValue.getExpiresAtMs();
      if (statsEngine.checkBucketEntryExists(segmentBuckets,
          statsEngine.generateBucketTime(putExpirationTime), putSegmentName) && putExpirationTime >= deleteTimeInMs) {
        statsEngine.updateBuckets(segmentBuckets, statsEngine.generateBucketTime(putExpirationTime),
            segmentName, putIndexValue.getSize());
      }
    }
  }

  /**
   * Compute the delta that needs to be applied on the base value by aggregating all bucket values that is before the
   * lastBucketEndTime.
   * @param map The {@link HashMap} containing the base value.
   * @param buckets The {@link TreeMap} holding the buckets.
   * @param key The key of the base value map.
   * @param lastBucketEndTime All buckets that is less than and equal to this reference time is aggregated to compute
   *                          the total delta.
   * @return
   */
  private long extractValidDataSize(HashMap<String, Long> map, TreeMap<Long, Bucket> buckets,
      String key, long lastBucketEndTime) {
    long baseValue = map.containsKey(key) ? map.get(key) : 0;
    long deltaValue = 0;
    if (!buckets.isEmpty() && lastBucketEndTime >= buckets.firstKey()) {
      SortedMap<Long, Bucket> bucketsInRange = buckets.subMap(buckets.firstKey(), true, lastBucketEndTime, true);
      for (Map.Entry<Long, Bucket> bucketEntry : bucketsInRange.entrySet()) {
        if (bucketEntry.getValue().contains(key)) {
          deltaValue += bucketEntry.getValue().getSize(key);
        }
      }
    }
    return baseValue + deltaValue;
  }
}