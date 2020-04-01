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
   * @param type the type of stats report to be aggregated, which is defined in {@link StatsReportType}
   * @return a {@link Pair} of Strings whose values represents aggregated stats across all partitions.
   * First element is the raw (sum) aggregated stats and second element is valid aggregated stats for all replicas
   * for each partition.
   * @throws IOException
   */
  Pair<String, String> doWork(Map<String, String> statsWrappersJSON, StatsReportType type) throws IOException {
    StatsSnapshot partitionSnapshot = new StatsSnapshot(0L, new HashMap<>());
    Map<String, Long> partitionTimestampMap = new HashMap<>();
    StatsSnapshot rawPartitionSnapshot = new StatsSnapshot(0L, new HashMap<>());
    exceptionOccurredInstances.remove(type);
    for (Map.Entry<String, String> statsWrapperJSON : statsWrappersJSON.entrySet()) {
      if (statsWrapperJSON != null && statsWrapperJSON.getValue() != null) {
        try {
          StatsWrapper snapshotWrapper = mapper.readValue(statsWrapperJSON.getValue(), StatsWrapper.class);
          StatsWrapper snapshotWrapperCopy = mapper.readValue(statsWrapperJSON.getValue(), StatsWrapper.class);

          combineRawStats(rawPartitionSnapshot, snapshotWrapper);
          switch (type) {
            case ACCOUNT_REPORT:
              combineValidStatsByAccount(partitionSnapshot, snapshotWrapperCopy, statsWrapperJSON.getKey(),
                  partitionTimestampMap);
              break;
            case PARTITION_CLASS_REPORT:
              combineValidStatsByPartitionClass(partitionSnapshot, snapshotWrapperCopy, statsWrapperJSON.getKey(),
                  partitionTimestampMap);
              break;
            default:
              throw new IllegalArgumentException("Unrecognized stats report type: " + type);
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
    StatsSnapshot reducedRawSnapshot;
    StatsSnapshot reducedSnapshot;
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
  private void combineRawStats(StatsSnapshot rawBaseSnapshot, StatsWrapper snapshotWrapper) {
    Map<String, StatsSnapshot> partitionSnapshotMap = snapshotWrapper.getSnapshot().getSubMap();
    if (partitionSnapshotMap == null) {
      logger.info("There is no partition in given StatsSnapshot, skip aggregation on it.");
      return;
    }
    long totalValue = rawBaseSnapshot.getValue();
    Map<String, StatsSnapshot> basePartitionSnapshotMap = rawBaseSnapshot.getSubMap();
    for (Map.Entry<String, StatsSnapshot> partitionSnapshot : partitionSnapshotMap.entrySet()) {
      if (basePartitionSnapshotMap.containsKey(partitionSnapshot.getKey())) {
        StatsSnapshot.aggregate(basePartitionSnapshotMap.get(partitionSnapshot.getKey()), partitionSnapshot.getValue());
      } else {
        basePartitionSnapshotMap.put(partitionSnapshot.getKey(), partitionSnapshot.getValue());
      }
      totalValue += partitionSnapshot.getValue().getValue();
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
   * The combined valid stats snapshot is represented in following format:
   * <pre>
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
   * </pre>
   * @param baseSnapshot the base {@link StatsSnapshot} which will contain the aggregated result
   * @param snapshotWrapper the {@link StatsSnapshot} to be aggregated to the base {@link StatsSnapshot}
   * @param instance new instance from which snapshot is being combined
   * @param partitionTimestampMap a {@link Map} of partition to timestamp to keep track the current timestamp of each
   *                              partition entry in the base {@link StatsSnapshot}
   */
  private void combineValidStatsByAccount(StatsSnapshot baseSnapshot, StatsWrapper snapshotWrapper, String instance,
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
        logger.trace("First entry for partition {} is from {}", partitionId, instance);
        basePartitionSnapshotMap.put(partitionId, partitionSnapshot.getValue());
        partitionTimestampMap.put(partitionId, snapshotTimestamp);
        totalValue += partitionSnapshot.getValue().getValue();
      }
    }
    baseSnapshot.setValue(totalValue);
  }

  /**
   * Aggregate the given {@link StatsSnapshot} with the base {@link StatsSnapshot} by partition class. The aggregation uses
   * the same rules in {@link #combineValidStatsByAccount(StatsSnapshot, StatsWrapper, String, Map)}.
   *
   * The workflow of this method is as follows:
   * 1. Check if basePartitionClassMap contains given partitionClass. If yes, go to step 2; If not, directly put it into basePartitionClassMap
   *    and update partitionTimestampMap by adding all < partition, timestamp > pairs associated with given partitionClass
   * 2. For each partition in given partitionClass, check if basePartitionMap contains it. If yes, go to step 3;
   *    if not, put the partition into basePartitionMap and update partitionTimestampMap by adding the partition and its timestamp.
   * 3. Compute the delta value and delta time between given partition and existing one. Update partitionClassVal and
   *    partitionTimestampMap based on following rules:
   *      a) if abs(delta time) is within relevantTimePeriodInMs and delta value > 0, replace existing partition with given partition.
   *      b) if delta time > relevantTimePeriodInMs, which means given partition is newer than existing partition,
   *         then replace existing partition with given one.
   *      c) otherwise, ignore the partition(replica) because it is either stale or not the replica with largest value.
   * 4. update basePartitionClassMap with up-to-date basePartitionMap and totalValueOfAllClasses.
   * The combined snapshot is represented in following format:
   * <pre>
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
   * </pre>
   * @param baseSnapshot baseSnapshot the base {@link StatsSnapshot} which will contain the aggregated result
   * @param snapshotWrapper the {@link StatsSnapshot} from each instance to be aggregated to the base {@link StatsSnapshot}
   * @param instance new instance from which snapshot is being combined
   * @param partitionTimestampMap a {@link Map} of partition to timestamp. It keeps track the current timestamp of each
   *                              partition entry in the base {@link StatsSnapshot}
   */
  private void combineValidStatsByPartitionClass(StatsSnapshot baseSnapshot, StatsWrapper snapshotWrapper,
      String instance, Map<String, Long> partitionTimestampMap) {
    Map<String, StatsSnapshot> partitionClassSnapshotMap = snapshotWrapper.getSnapshot().getSubMap();
    if (partitionClassSnapshotMap == null) {
      logger.info("There is no partition in given StatsSnapshot, skip aggregation on it.");
      return;
    }
    long totalValueOfAllClasses = baseSnapshot.getValue();
    long snapshotTimestamp = snapshotWrapper.getHeader().getTimestamp();
    Map<String, StatsSnapshot> basePartitionClassMap = baseSnapshot.getSubMap();

    for (Map.Entry<String, StatsSnapshot> partitionClassSnapshot : partitionClassSnapshotMap.entrySet()) {
      String partitionClassId = partitionClassSnapshot.getKey();
      if (basePartitionClassMap.containsKey(partitionClassId)) {
        long partitionClassVal = basePartitionClassMap.get(partitionClassId).getValue();
        Map<String, StatsSnapshot> basePartitionMap = basePartitionClassMap.get(partitionClassId).getSubMap();
        for (Map.Entry<String, StatsSnapshot> partitionToSnapshot : partitionClassSnapshot.getValue()
            .getSubMap()
            .entrySet()) {
          String partitionId = partitionToSnapshot.getKey();
          StatsSnapshot partitionSnapshot = partitionToSnapshot.getValue();
          if (basePartitionMap.containsKey(partitionId)) {
            long deltaInValue = partitionSnapshot.getValue() - basePartitionMap.get(partitionId).getValue();
            long deltaInTimeMs = snapshotTimestamp - partitionTimestampMap.get(partitionId);
            if (Math.abs(deltaInTimeMs) < relevantTimePeriodInMs && deltaInValue > 0) {
              basePartitionMap.put(partitionId, partitionSnapshot);
              partitionTimestampMap.put(partitionId, snapshotTimestamp);
              partitionClassVal += deltaInValue;
              totalValueOfAllClasses += deltaInValue;
            } else if (deltaInTimeMs > relevantTimePeriodInMs) {
              basePartitionMap.put(partitionId, partitionSnapshot);
              partitionTimestampMap.put(partitionId, snapshotTimestamp);
              partitionClassVal += deltaInValue;
              totalValueOfAllClasses += deltaInValue;
            } else {
              logger.trace("Ignoring snapshot from {} for partition {}", instance, partitionId);
            }
          } else {
            logger.trace("First partition: {} in partitionClass: {}", partitionId, partitionClassId);
            basePartitionMap.put(partitionId, partitionSnapshot);
            partitionTimestampMap.put(partitionId, snapshotTimestamp);
            partitionClassVal += partitionSnapshot.getValue();
            totalValueOfAllClasses += partitionSnapshot.getValue();
          }
        }
        //update partitionClass snapshot
        basePartitionClassMap.get(partitionClassId).setSubMap(basePartitionMap);
        basePartitionClassMap.get(partitionClassId).setValue(partitionClassVal);
      } else {
        logger.trace("First entry for partitionClass {} is from {}", partitionClassId, instance);
        basePartitionClassMap.put(partitionClassId, partitionClassSnapshot.getValue());
        // put all partitions associated with this partitionClass into partitionTimestampMap on their first occurrence.
        for (String partitionIdStr : partitionClassSnapshot.getValue().getSubMap().keySet()) {
          partitionTimestampMap.put(partitionIdStr, snapshotTimestamp);
        }
        // add aggregated value in this partition class to totalValue
        totalValueOfAllClasses += partitionClassSnapshot.getValue().getValue();
      }
    }
    baseSnapshot.setValue(totalValueOfAllClasses);
    baseSnapshot.setSubMap(basePartitionClassMap);
  }

  /**
   * Reduce the given {@link StatsSnapshot} whose first level mapped by partitions to a shallower {@link StatsSnapshot}
   * by adding entries belonging to the same partition together and aggregating based on account. The level of partition
   * would be removed after reduce work completes.
   * @param statsSnapshot the {@link StatsSnapshot} to be reduced
   * @return the reduced {@link StatsSnapshot}
   */
  private StatsSnapshot reduceByAccount(StatsSnapshot statsSnapshot) {
    StatsSnapshot reducedSnapshot = new StatsSnapshot(0L, new HashMap<>());
    if (!statsSnapshot.getSubMap().isEmpty()) {
      for (StatsSnapshot snapshot : statsSnapshot.getSubMap().values()) {
        StatsSnapshot.aggregate(reducedSnapshot, snapshot);
      }
    }
    return reducedSnapshot;
  }

  /**
   * Reduce the given {@link StatsSnapshot} whose first level mapped by PartitionClass to a shallower {@link StatsSnapshot}.
   * by adding entries belonging to each partition together and aggregating based on partition class. The partition level
   * would be removed after reduce work completes.
   * <pre>
   *             Before Reduce                   |             After Reduce
   * --------------------------------------------------------------------------------------------
   * {                                           |        {
   *   value: 1000,                              |          value:1000
   *   subMap:{                                  |          subMap:{
   *     PartitionClass_1: {                     |            PartitionClass_1: {
   *       value: 1000,                          |              value: 1000,
   *       subMap: {                             |              subMap: {
   *         Partition[1]:{                      |                Account[1]_Container[1]:{
   *           value: 400,                       |                  value: 1000,
   *           subMap: {                         |                  subMap: null
   *             Account[1]_Container[1]:{       |                }
   *               value: 400,                   |              }
   *               subMap: null                  |            }
   *             }                               |          }
   *           }                                 |        }
   *         },                                  |
   *         Partition[2]:{                      |
   *           value: 600,                       |
   *           subMap: {                         |
   *             Account[1]_Container[1]:{       |
   *               value: 600,                   |
   *               subMap: null                  |
   *             }                               |
   *           }                                 |
   *         }                                   |
   *       }
   *     }
   *   }
   * }
   * </pre>
   * @param statsSnapshot the {@link StatsSnapshot} to be reduced
   * @return the reduced {@link StatsSnapshot}
   */
  static StatsSnapshot reduceByPartitionClass(StatsSnapshot statsSnapshot) {
    StatsSnapshot returnSnapshot = new StatsSnapshot(statsSnapshot.getValue(), new HashMap<>());
    Map<String, StatsSnapshot> partitionClassSnapshots = statsSnapshot.getSubMap();
    if (!partitionClassSnapshots.isEmpty()) {
      for (Map.Entry<String, StatsSnapshot> partitionClassToSnapshot : partitionClassSnapshots.entrySet()) {
        StatsSnapshot partitionClassSnapshot = partitionClassToSnapshot.getValue();
        StatsSnapshot reducedPartitionClassSnapshot = new StatsSnapshot(0L, null);
        Map<String, StatsSnapshot> partitionToSnapshot = partitionClassSnapshot.getSubMap();
        for (StatsSnapshot snapshot : partitionToSnapshot.values()) {
          StatsSnapshot.aggregate(reducedPartitionClassSnapshot, snapshot);
        }
        returnSnapshot.getSubMap().put(partitionClassToSnapshot.getKey(), reducedPartitionClassSnapshot);
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
