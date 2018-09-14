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

package com.github.ambry.clustermap;

import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Responsible for performing cluster wide stats aggregation.
 */
public class HelixClusterAggregator {
  private static final Logger logger = LoggerFactory.getLogger(HelixClusterAggregator.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private final long relevantTimePeriodInMs;
  private final Map<StatsReportType, List<String>> exceptionOccurredInstances = new HashMap<>();

  HelixClusterAggregator(long relevantTimePeriodInMinutes) {
    relevantTimePeriodInMs = TimeUnit.MINUTES.toMillis(relevantTimePeriodInMinutes);
  }

  /**
   * Take a {@link Map} of instance name to JSON string representation of {@link StatsWrapper} objects and perform cluster wide
   * aggregation with them.
   * @param statsWrappersJSON a {@link Map} of instance name to JSON string representation of {@link StatsWrapper} objects from the
   *                          node level
   * @param type the type of stats report which is enum defined in {@link StatsReportType}
   * @return a {@link Pair} of Strings whose values represents valid quota stats across all partitions.
   * First element is the raw (sum) aggregated stats and second element is largest valid stats for all replicas
   * for each partition. Here "valid" means the timestamp is within valid time range indicating the report is up-to-date.
   * @throws IOException
   * @throws IllegalArgumentException
   */
  Pair<String, String> doWork(Map<String, String> statsWrappersJSON, StatsReportType type)
      throws IOException, IllegalArgumentException {
    StatsSnapshot partitionSnapshot = new StatsSnapshot(0L, new HashMap<>());
    Map<String, Long> partitionTimestampMap = new HashMap<>();
    StatsSnapshot rawPartitionSnapshot = new StatsSnapshot(0L, new HashMap<>());
    exceptionOccurredInstances.remove(type);
    for (Map.Entry<String, String> statsWrapperJSON : statsWrappersJSON.entrySet()) {
      if (statsWrapperJSON != null && statsWrapperJSON.getValue() != null) {
        try {
          StatsWrapper snapshotWrapper = mapper.readValue(statsWrapperJSON.getValue(), StatsWrapper.class);
          StatsWrapper snapshotWrapperCopy = mapper.readValue(statsWrapperJSON.getValue(), StatsWrapper.class);
          combineRawData(rawPartitionSnapshot, snapshotWrapper);
          if (type == StatsReportType.ACCOUNT_REPORT) {
            combineValidDataByAccount(partitionSnapshot, snapshotWrapperCopy, statsWrapperJSON.getKey(),
                partitionTimestampMap);
          } else if (type == StatsReportType.PARTITION_CLASS_REPORT) {
            combineValidDataByPartitionClass(partitionSnapshot, snapshotWrapperCopy, statsWrapperJSON.getKey(),
                partitionTimestampMap);
          }
        } catch (Exception e) {
          logger.error("Exception occurred while processing stats from {}", statsWrapperJSON.getKey(), e);
          exceptionOccurredInstances.computeIfAbsent(type, key -> new ArrayList<>()).add(statsWrapperJSON.getKey());
        }
      }
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Combined raw snapshot {}", mapper.writeValueAsString(rawPartitionSnapshot));
      logger.trace("Combined valid snapshot {}", mapper.writeValueAsString(partitionSnapshot));
    }
    StatsSnapshot reducedRawSnapshot = null;
    StatsSnapshot reducedSnapshot = null;
    switch (type) {
      case ACCOUNT_REPORT:
        reducedRawSnapshot = reduceByAccount(rawPartitionSnapshot);
        reducedSnapshot = reduceByAccount(partitionSnapshot);
        break;
      case PARTITION_CLASS_REPORT:
        reducedRawSnapshot = reduceByPartitionClass(rawPartitionSnapshot);
        reducedSnapshot = reduceByPartitionClass(partitionSnapshot);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized stats report type: " + type);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Reduced raw snapshot {}", mapper.writeValueAsString(reducedRawSnapshot));
      logger.trace("Reduced valid snapshot {}", mapper.writeValueAsString(reducedSnapshot));
    }
    return new Pair<>(mapper.writeValueAsString(reducedRawSnapshot), mapper.writeValueAsString(reducedSnapshot));
  }

  /**
   * Aggregate the given {@link StatsSnapshot} by simply add it to the raw base {@link StatsSnapshot} by partition.
   * @param rawBaseSnapshot the raw base {@link StatsSnapshot} which will contain the aggregated result
   * @param snapshotWrapper the {@link StatsSnapshot} to be added to the raw base {@link StatsSnapshot}
   */
  private void combineRawData(StatsSnapshot rawBaseSnapshot, StatsWrapper snapshotWrapper) {
    Map<String, StatsSnapshot> snapshotMap = snapshotWrapper.getSnapshot().getSubMap();
    if (snapshotMap == null) {
      logger.info("There is no partition in given StatsSnapshot, skip aggregation on it.");
      return;
    }
    long totalValue = rawBaseSnapshot.getValue();
    Map<String, StatsSnapshot> baseSnapshotMap = rawBaseSnapshot.getSubMap();
    for (Map.Entry<String, StatsSnapshot> snapshotEntry : snapshotMap.entrySet()) {
      StatsSnapshot baseSnapshot = baseSnapshotMap.get(snapshotEntry.getKey());
      if (baseSnapshot != null) {
        StatsSnapshot.aggregate(baseSnapshot, snapshotEntry.getValue());
      } else {
        baseSnapshotMap.put(snapshotEntry.getKey(), snapshotEntry.getValue());
      }
      totalValue += snapshotEntry.getValue().getValue();
    }
    rawBaseSnapshot.setValue(totalValue);
  }

  /**
   * Aggregate the given {@link StatsSnapshot} with the base {@link StatsSnapshot} by partition with the following
   * rules:
   * 1. Append the partition entry if the base doesn't already contain an entry for a given partition.
   * 2. If the base already has an entry for a given partition, update/replace the entry only if:
   *    a) Timestamp difference of a given partition entry and base entry are within one aggregation period, and the
   *       given partition entry has a greater value.
   *    b) Timestamp of a given partition entry is one aggregation period newer (greater) than the timestamp of the
   *       base entry.
   * The combined snapshot is represented in following format:
   * {
   *   value: 1000,
   *   subMap: {
   *     Partition[0]: {
   *       value: 400,
   *       subMap: {
   *         Account[0]: {
   *           value: 400,
   *           subMap: {
   *             Container[0]: {
   *               value: 400
   *               subMap: null
   *             }
   *           }
   *         }
   *       }
   *     },
   *     Partition[1]: {
   *       value: 600,
   *       subMap: {
   *         Account[1]: {
   *           value: 600,
   *           subMap: {
   *             Container[1]:{
   *               value: 600,
   *               subMap: null
   *             }
   *           }
   *         }
   *       }
   *     }
   *   }
   * }
   * @param baseSnapshot the base {@link StatsSnapshot} which will contain the aggregated result
   * @param snapshotWrapper the {@link StatsSnapshot} from each instance to be aggregated to the base {@link StatsSnapshot}
   * @param instance new instance from which snapshot is being combined
   * @param partitionTimestampMap a {@link Map} of partition to timestamp to keep track the current timestamp of each
   *                              partition entry in the base {@link StatsSnapshot}
   */
  private void combineValidDataByAccount(StatsSnapshot baseSnapshot, StatsWrapper snapshotWrapper, String instance,
      Map<String, Long> partitionTimestampMap) {
    Map<String, StatsSnapshot> partitionSnapshotMap = snapshotWrapper.getSnapshot().getSubMap();
    if (partitionSnapshotMap == null) {
      logger.info("There is no partition in given StatsSnapshot, skip aggregation on it.");
      return;
    }
    long totalValue = baseSnapshot.getValue();
    long snapshotTimestamp = snapshotWrapper.getHeader().getTimestamp();
    Map<String, StatsSnapshot> basePartitionSnapshotMap = baseSnapshot.getSubMap();
    for (Map.Entry<String, StatsSnapshot> partitionSnapshot : partitionSnapshotMap.entrySet()) {
      String partitionId = partitionSnapshot.getKey();
      if (basePartitionSnapshotMap.containsKey(partitionId)) {
        long deltaInValue =
            partitionSnapshot.getValue().getValue() - basePartitionSnapshotMap.get(partitionId).getValue();
        long deltaInTimeMs = snapshotTimestamp - partitionTimestampMap.get(partitionId);
        if (Math.abs(deltaInTimeMs) < relevantTimePeriodInMs && deltaInValue > 0) {
          logger.trace("Updating partition {} snapshot from instance {} as last updated time is within"
              + "relevant time period and delta value has increased ", partitionId, instance);
          basePartitionSnapshotMap.put(partitionId, partitionSnapshot.getValue());
          partitionTimestampMap.put(partitionId, snapshotTimestamp);
          totalValue += deltaInValue;
        } else if (deltaInTimeMs > relevantTimePeriodInMs) {
          logger.trace("Updating partition {} snapshot from instance {} as new value is updated "
              + "within relevant time period compared to already existing one", partitionId, instance);
          basePartitionSnapshotMap.put(partitionId, partitionSnapshot.getValue());
          partitionTimestampMap.put(partitionId, snapshotTimestamp);
          totalValue += deltaInValue;
        } else {
          logger.trace("Ignoring snapshot from {} for partition {}", instance, partitionId);
        }
      } else {
        logger.debug("First entry for partition {} is from {}", partitionId, instance);
        basePartitionSnapshotMap.put(partitionId, partitionSnapshot.getValue());
        partitionTimestampMap.put(partitionId, snapshotTimestamp);
        totalValue += partitionSnapshot.getValue().getValue();
      }
    }
    baseSnapshot.setValue(totalValue);
  }

  /**
   * Aggregate the given {@link StatsSnapshot} with the base {@link StatsSnapshot} by partition class with same rules defined
   * in {@link #combineValidDataByAccount(StatsSnapshot, StatsWrapper, String, Map)}.
   *
   * The workflow of this method is as follows:
   * 1. check if basePartitionClassMap contains given partitionClass. If yes, go to step 2; If not, directly put it into basePartitionClassMap
   *    and update partitionTimestampMap by adding all < partition, timestamp > pairs associated with given partitionClass
   * 2. for each partition in given partitionClass, check if basePartitionMap contains it. If yes, go to step 3;
   *    if not, put the partition into basePartitionMap and update partitionTimestampMap by adding the partition and its timestamp.
   * 3. compute the delta value and delta time between given partition and existing partition. Update total value and
   *    partitionTimestampMap based on following rules:
   *      a) if abs(delta time) is within relevantTimePeriodInMs and delta value > 0, replace existing partition with given partition.
   *      b) if delta time > relevantTimePeriodInMs, which means given partition is newer than existing partition,
   *         then replace existing partition with given one.
   * 4. otherwise, ignore the partition(replica) because it is either stale or not the replica with largest value.
   * 5. update basePartitionClassMap with up-to-date basePartitionMap and partitionClassTotalVal.
   *
   * The combined snapshot is represented in following format:
   * {
   *   value: 1000,
   *   subMap:{
   *     PartitionClass_1: {
   *       value: 400,
   *       subMap: {
   *         Partition[1]:{
   *           value: 400,
   *           subMap: {
   *             Account[1]_Container[1]:{
   *               value: 400,
   *               subMap: null
   *             }
   *           }
   *         }
   *       }
   *     },
   *     PartitionClass_2: {
   *       value: 600,
   *       subMap:{
   *         Partition[2]:{
   *           value: 600,
   *           subMap:{
   *             Account[2]_Container[2]:{
   *               value: 600,
   *               subMap: null
   *             }
   *           }
   *         }
   *       }
   *     }
   *   }
   * }
   * @param baseSnapshot the base {@link StatsSnapshot} which will contain the aggregated result
   * @param snapshotWrapper the {@link StatsSnapshot} from each instance to be aggregated to the base {@link StatsSnapshot}
   * @param instance new instance from which snapshot is being combined
   * @param partitionTimestampMap a {@link Map} of partition to timestamp to keep track the current timestamp of each
   *                              partition entry in the base {@link StatsSnapshot}
   */
  private void combineValidDataByPartitionClass(StatsSnapshot baseSnapshot, StatsWrapper snapshotWrapper,
      String instance, Map<String, Long> partitionTimestampMap) {
    Map<String, StatsSnapshot> partitionClassSnapshotMap = snapshotWrapper.getSnapshot().getSubMap();
    if (partitionClassSnapshotMap == null) {
      logger.info("There is no partition in given StatsSnapshot, skip aggregation on it.");
      return;
    }
    long totalValue = baseSnapshot.getValue();
    long snapshotTimestamp = snapshotWrapper.getHeader().getTimestamp();
    Map<String, StatsSnapshot> basePartitionClassMap = baseSnapshot.getSubMap();

    for (Map.Entry<String, StatsSnapshot> partitionClassSnapshot : partitionClassSnapshotMap.entrySet()) {
      String partitionClassId = partitionClassSnapshot.getKey();
      if (basePartitionClassMap.containsKey(partitionClassId)) {
        Map<String, StatsSnapshot> basePartitionMap = basePartitionClassMap.get(partitionClassId).getSubMap();
        long partitionClassTotalVal = basePartitionClassMap.get(partitionClassId).getValue();
        for (Map.Entry<String, StatsSnapshot> partitionSnapshot : partitionClassSnapshot.getValue()
            .getSubMap()
            .entrySet()) {
          String partitionId = partitionSnapshot.getKey();
          if (basePartitionMap.containsKey(partitionId)) {
            long deltaInValue = partitionSnapshot.getValue().getValue() - basePartitionMap.get(partitionId).getValue();
            long deltaInTimeMs = snapshotTimestamp - partitionTimestampMap.get(partitionId);
            if (Math.abs(deltaInTimeMs) < relevantTimePeriodInMs && deltaInValue > 0) {
              basePartitionMap.put(partitionId, partitionSnapshot.getValue());
              partitionTimestampMap.put(partitionId, snapshotTimestamp);
              totalValue += deltaInValue;
              partitionClassTotalVal += deltaInValue;
            } else if (deltaInTimeMs > relevantTimePeriodInMs) {
              basePartitionMap.put(partitionId, partitionSnapshot.getValue());
              partitionTimestampMap.put(partitionId, snapshotTimestamp);
              totalValue += deltaInValue;
              partitionClassTotalVal += deltaInValue;
            } else {
              logger.trace("Ignoring snapshot from {} for partition {}", instance, partitionId);
            }
          } else {
            basePartitionMap.put(partitionId, partitionSnapshot.getValue());
            partitionTimestampMap.put(partitionId, snapshotTimestamp);
            totalValue += partitionSnapshot.getValue().getValue();
            partitionClassTotalVal += partitionSnapshot.getValue().getValue();
          }
        }
        //update partitionClass snapshot
        basePartitionClassMap.get(partitionClassId).setSubMap(basePartitionMap);
        basePartitionClassMap.get(partitionClassId).setValue(partitionClassTotalVal);
      } else {
        logger.trace("First entry for partitionClass {} is from {}", partitionClassId, instance);
        basePartitionClassMap.put(partitionClassId, partitionClassSnapshot.getValue());
        // put all partitions associated with this partitionClass into partitionTimestampMap on their first occurrence.
        for (String partitionIdStr : partitionClassSnapshot.getValue().getSubMap().keySet()) {
          partitionTimestampMap.put(partitionIdStr, snapshotTimestamp);
        }
        // add aggregated value in this partition class to totalValue
        totalValue += partitionClassSnapshot.getValue().getValue();
      }
    }
    baseSnapshot.setValue(totalValue);
    baseSnapshot.setSubMap(basePartitionClassMap);
  }

  /**
   * Reduce the given {@link StatsSnapshot} whose first level mapped by partitions to a shallower {@link StatsSnapshot}
   * by adding entries belonging to each partition together and aggregating based on account. The level of partition
   * would be removed after reduce completes.
   * @param statsSnapshot the {@link StatsSnapshot} to be reduced
   * @return the reduced {@link StatsSnapshot}
   */
  private StatsSnapshot reduceByAccount(StatsSnapshot statsSnapshot) {
    StatsSnapshot reducedSnapshot = new StatsSnapshot(0L, null);
    if (statsSnapshot.getSubMap() != null) {
      for (StatsSnapshot snapshot : statsSnapshot.getSubMap().values()) {
        StatsSnapshot.aggregate(reducedSnapshot, snapshot);
      }
    }
    return reducedSnapshot;
  }

  /**
   * Reduce the given {@link StatsSnapshot} whose first level mapped by PartitionClass to a shallower {@link StatsSnapshot}.
   * by adding entries belonging to each partition together and aggregating based on partition class. The partition level
   * would be removed after reduce completes.
   * @param statsSnapshot the {@link StatsSnapshot} to be reduced
   * @return the reduced {@link StatsSnapshot}
   */
  static StatsSnapshot reduceByPartitionClass(StatsSnapshot statsSnapshot) {
    StatsSnapshot returnSnapshot = new StatsSnapshot(statsSnapshot.getValue(), new HashMap<>());
    Map<String, StatsSnapshot> partitionClassSnapshots = statsSnapshot.getSubMap();
    if (partitionClassSnapshots != null) {
      for (Map.Entry<String, StatsSnapshot> partitionClassSnapshotEntry : partitionClassSnapshots.entrySet()) {
        StatsSnapshot partitionClassSnapshot = partitionClassSnapshotEntry.getValue();
        StatsSnapshot reducedPartitionClassSnapshot = new StatsSnapshot(0L, null);
        if (partitionClassSnapshot.getSubMap() != null) {
          for (StatsSnapshot snapshot : partitionClassSnapshot.getSubMap().values()) {
            StatsSnapshot.aggregate(reducedPartitionClassSnapshot, snapshot);
          }
          returnSnapshot.getSubMap().put(partitionClassSnapshotEntry.getKey(), reducedPartitionClassSnapshot);
        }
      }
    }
    return returnSnapshot;
  }

  /**
   * Get exception occurred instances for certain {@link StatsReportType}
   * @param type the type of stats report to use.
   * @return the list of instances on which exception occurred during cluster wide stats aggregation.
   */
  List<String> getExceptionOccurredInstances(StatsReportType type) {
    return exceptionOccurredInstances.get(type);
  }
}
