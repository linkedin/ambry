package com.github.ambry.store;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


/**
 * Hold the data structures needed by {@link BlobStoreStats} to serve requests. The class also holds helper methods
 * used to modify and access the stored data structures.
 */
public class ScanResults {
  private final long bucketCount;
  private final long bucketTimeSpan;
  private final SortedMap<String, Long> requestedSegmentMap = new TreeMap<>();
  private final HashMap<String, HashMap<String, Long>> containerBaseMap = new HashMap<>();
  private final HashMap<String, Long> segmentBaseMap = new HashMap<>();
  private final TreeMap<Long, HashMap<String, HashMap<String, Long>>> containerBuckets = new TreeMap<>();
  private final TreeMap<Long, HashMap<String, Long>> segmentBuckets = new TreeMap<>();

  ScanResults(long bucketCount, long bucketTimeSpan) {
    this.bucketCount = bucketCount;
    this.bucketTimeSpan = bucketTimeSpan;
  }

  /**
   * Create the bucket data structures in advance based on the given scanStartTime and segmentScanTimeOffset.
   * @param scanStartTime
   * @param segmentScanTimeOffset
   * @return the reference time of the last container bucket. This is the coverage boundary of this ScanResult
   */
  long createBuckets(long scanStartTime, long segmentScanTimeOffset) {
    long containerBucketEndTime = generateBucketTime(scanStartTime);
    long segmentBucketEndTime = (scanStartTime - segmentScanTimeOffset);
    for (int i = 0; i < bucketCount; i++) {
      containerBuckets.put(containerBucketEndTime, new HashMap<String, HashMap<String, Long>>());
      segmentBuckets.put(segmentBucketEndTime, new HashMap<String, Long>());
      containerBucketEndTime += bucketTimeSpan;
      segmentBucketEndTime += bucketTimeSpan;
    }
    return containerBucketEndTime;
  }

  /**
   * Update the data structure that is used to hold on demand scan results for valid data size per segment.
   * @param segmentName
   * @param value
   */
  void updateRequestedSegmentMap(String segmentName, Long value) {
    Long existingValue = requestedSegmentMap.get(segmentName);
    if (existingValue == null) {
      existingValue = value;
    } else {
      existingValue += value;
    }
    requestedSegmentMap.put(segmentName, existingValue);
  }

  void updateContainerBaseMap(String serviceId, String containerId, Long value) {
    updateNestedMapHelper(containerBaseMap, serviceId, containerId, value);
  }

  /**
   * Helper function to update nested hash map data structure.
   * @param nestedMap nested hash map to be updated
   * @param firstKey first level hash map key
   * @param secondKey second level hash map key
   * @param value the value to be added
   */
  private void updateNestedMapHelper(HashMap<String, HashMap<String, Long>> nestedMap, String firstKey,
      String secondKey, Long value) {
    if (!nestedMap.containsKey(firstKey)) {
      nestedMap.put(firstKey, new HashMap<String, Long>());
    }
    Long existingValue = nestedMap.get(firstKey).get(secondKey);
    if (existingValue == null) {
      existingValue = value;
    } else {
      existingValue += value;
    }
    nestedMap.get(firstKey).put(secondKey, existingValue);
  }

  void updateSegmentBaseMap(String segmentName, Long value) {
    updateHashMapHelper(segmentBaseMap, segmentName, value);
  }

  /**
   * Helper function to update hash map data structure.
   * @param hashMap hash map to be updated
   * @param key the key of the entry
   * @param value the value to be added
   */
  private void updateHashMapHelper(HashMap<String, Long> hashMap, String key, Long value) {
    Long existingValue = hashMap.get(key);
    if (existingValue == null) {
      existingValue = value;
    } else {
      existingValue += value;
    }
    hashMap.put(key, existingValue);
  }

  void updateContainerBucket(Long referenceTime, String serviceId, String containerId, Long value) {
    HashMap<String, HashMap<String, Long>> containerBucketMap = containerBuckets.get(generateBucketTime(referenceTime));
    if (containerBucketMap != null) {
      updateNestedMapHelper(containerBucketMap, serviceId, containerId, value);
    }
  }

  void updateSegmentBucket(Long referenceTime, String segmentName, Long value) {
    HashMap<String, Long> segmentBucketMap = segmentBuckets.get(generateBucketTime(referenceTime));
    if (segmentBucketMap != null) {
      updateHashMapHelper(segmentBucketMap, segmentName, value);
    }
  }

  boolean checkIfBucketEntryExists(Long referenceTime, String serviceId, String containerId) {
    return containerBuckets.containsKey(referenceTime) && containerBuckets.get(referenceTime).containsKey(serviceId) &&
        containerBuckets.get(referenceTime).get(serviceId).containsKey(containerId);
  }

  boolean checkIfBucketEntryExists(Long referenceTime, String segmentName) {
    Long bucketEndTime = generateBucketTime(referenceTime);
    return segmentBuckets.containsKey(bucketEndTime) && segmentBuckets.get(bucketEndTime).containsKey(segmentName);
  }

  SortedMap<String, Long> getRequestedSegmentMap() {
    return requestedSegmentMap;
  }

  private Long generateBucketTime(long referenceTime) {
    long remainder = referenceTime % bucketTimeSpan;
    return remainder == 0 ? referenceTime : referenceTime - remainder + bucketTimeSpan;
  }

  /**
   * Helper function to extract the valid data size of each segment and put them into a {@link SortedMap}.
   * @param bucketEndTime all buckets prior to and including this bucketEndTime is aggregated as the delta to be applied
   *                      on the base value to compute the valid data size
   * @return mapping of segment names to their corresponding valid data size
   */
  SortedMap<String, Long> getValidDataSizePerSegment(Long bucketEndTime) {
    SortedMap<String, Long> retMap = new TreeMap<>();
    if (!segmentBuckets.isEmpty() && bucketEndTime >= segmentBuckets.firstKey()) {
      SortedMap<Long, HashMap<String, Long>> bucketsInRange =
          segmentBuckets.subMap(segmentBuckets.firstKey(), true, bucketEndTime, true);
      for (Map.Entry<String, Long> entry : segmentBaseMap.entrySet()) {
        Long value = entry.getValue();
        for (HashMap<String, Long> segmentDelta : bucketsInRange.values()) {
          if (segmentDelta.containsKey(entry.getKey())) {
            value += segmentDelta.get(entry.getKey());
          }
        }
        retMap.put(entry.getKey(), value);
      }
    } else {
      retMap.putAll(segmentBaseMap);
    }
    return retMap;
  }

  /**
   * Helper function to extract the valid data size of each container and put then into a nested {@link HashMap}.
   * @param bucketEndTime all buckets prior to and including this bucketEndTime is aggregated as the delta to be applied
   *                      on the base value to compute the valid data size
   * @return nested {@link HashMap} of serviceIds to containerIds to their corresponding valid data size
   */
  HashMap<String, HashMap<String, Long>> getValidDataSizePerContainer(Long bucketEndTime) {
    HashMap<String, HashMap<String, Long>> retMap = new HashMap<>();
    if (!containerBuckets.isEmpty() && bucketEndTime >= containerBuckets.firstKey()) {
      SortedMap<Long, HashMap<String, HashMap<String, Long>>> bucketsInRange =
          containerBuckets.subMap(containerBuckets.firstKey(), true, bucketEndTime, true);
      for (Map.Entry<String, HashMap<String, Long>> entry : containerBaseMap.entrySet()) {
        HashMap<String, Long> containerMap = new HashMap<>();
        for (Map.Entry<String, Long> innerEntry : entry.getValue().entrySet()) {
          Long value = innerEntry.getValue();
          for (HashMap<String, HashMap<String, Long>> containerDelta : bucketsInRange.values()) {
            if (containerDelta.containsKey(entry.getKey()) &&
                containerDelta.get(entry.getKey()).containsKey(innerEntry.getKey())) {
              value += containerDelta.get(entry.getKey()).get(innerEntry.getKey());
            }
          }
          containerMap.put(innerEntry.getKey(), value);
        }
        retMap.put(entry.getKey(), containerMap);
      }
    } else {
      retMap.putAll(containerBaseMap);
    }
    return retMap;
  }
}
