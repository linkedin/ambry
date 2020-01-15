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

package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;
import static com.github.ambry.utils.Utils.*;


/**
 * The stats manager is responsible for periodic aggregation of node level stats and expose/publish such stats to
 * potential consumers.
 */
class StatsManager {
  private static final Logger logger = LoggerFactory.getLogger(StatsManager.class);

  private final StorageManager storageManager;
  private final File statsOutputFile;
  private final long publishPeriodInSecs;
  private final int initialDelayInSecs;
  private final StatsManagerMetrics metrics;
  private final Time time;
  private final ObjectMapper mapper = new ObjectMapper();
  private ScheduledExecutorService scheduler = null;
  private StatsAggregator statsAggregator = null;
  final ConcurrentMap<PartitionId, ReplicaId> partitionToReplicaMap;

  /**
   * Constructs a {@link StatsManager}.
   * @param storageManager the {@link StorageManager} to be used to fetch the {@link Store}s
   * @param replicaIds a {@link List} of {@link ReplicaId}s that are going to be fetched
   * @param registry the {@link MetricRegistry} to be used for {@link StatsManagerMetrics}
   * @param config the {@link StatsManagerConfig} to be used to configure the output file path and publish period
   * @param time the {@link Time} instance to be used for reporting
   * @param clusterParticipant the {@link ClusterParticipant} to register state change listener.
   */
  StatsManager(StorageManager storageManager, List<? extends ReplicaId> replicaIds, MetricRegistry registry,
      StatsManagerConfig config, Time time, ClusterParticipant clusterParticipant) {
    this.storageManager = storageManager;
    statsOutputFile = new File(config.outputFilePath);
    publishPeriodInSecs = config.publishPeriodInSecs;
    initialDelayInSecs = config.initialDelayUpperBoundInSecs;
    metrics = new StatsManagerMetrics(registry);
    partitionToReplicaMap =
        replicaIds.stream().collect(Collectors.toConcurrentMap(ReplicaId::getPartitionId, Function.identity()));
    this.time = time;
    if (clusterParticipant != null) {
      clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.StatsManagerListener,
          new PartitionStateChangeListenerImpl());
      logger.info("Stats Manager's state change listener registered!");
    }
  }

  /**
   * Start the stats manager by scheduling the periodic task that collect, aggregate and publish stats.
   */
  void start() {
    scheduler = Utils.newScheduler(1, false);
    statsAggregator = new StatsAggregator();
    int actualDelay = initialDelayInSecs > 0 ? ThreadLocalRandom.current().nextInt(initialDelayInSecs) : 0;
    logger.info("Scheduling stats aggregation job with an initial delay of {} secs", actualDelay);
    scheduler.scheduleAtFixedRate(statsAggregator, actualDelay, publishPeriodInSecs, TimeUnit.SECONDS);
  }

  /**
   * Stops the periodic task that is collecting, aggregating and publishing stats.
   */
  void shutdown() {
    if (statsAggregator != null) {
      statsAggregator.cancel();
    }
    if (scheduler != null) {
      shutDownExecutorService(scheduler, 30, TimeUnit.SECONDS);
    }
  }

  /**
   * Publishes stats to a local file in JSON format.
   * @param statsWrapper the {@link StatsWrapper} to be published
   * @throws IOException
   */
  void publish(StatsWrapper statsWrapper) throws IOException {
    File tempFile = new File(statsOutputFile.getAbsolutePath() + ".tmp");
    if (tempFile.createNewFile()) {
      mapper.defaultPrettyPrintingWriter().writeValue(tempFile, statsWrapper);
      if (!tempFile.renameTo(statsOutputFile)) {
        throw new IOException(
            "Failed to rename " + tempFile.getAbsolutePath() + " to " + statsOutputFile.getAbsolutePath());
      }
    } else {
      throw new IOException("Temporary file creation failed when publishing stats " + tempFile.getAbsolutePath());
    }
  }

  /**
   * Fetch and aggregate stats from a given {@link Store}
   * @param aggregatedSnapshot the {@link StatsSnapshot} to hold the aggregated result
   * @param partitionId specifies the {@link Store} to be fetched from
   * @param unreachablePartitions a {@link List} containing partition Ids that were unable to successfully fetch from
   */
  void collectAndAggregate(StatsSnapshot aggregatedSnapshot, PartitionId partitionId,
      List<PartitionId> unreachablePartitions) {
    Store store = storageManager.getStore(partitionId, false);
    if (store == null) {
      unreachablePartitions.add(partitionId);
    } else {
      try {
        long fetchAndAggregatePerStoreStartTimeMs = time.milliseconds();
        Map<StatsReportType, StatsSnapshot> snapshotsByType =
            store.getStoreStats().getStatsSnapshots(EnumSet.of(StatsReportType.ACCOUNT_REPORT), time.milliseconds());
        StatsSnapshot.aggregate(aggregatedSnapshot, snapshotsByType.get(StatsReportType.ACCOUNT_REPORT));
        metrics.fetchAndAggregateTimePerStoreMs.update(time.milliseconds() - fetchAndAggregatePerStoreStartTimeMs);
      } catch (StoreException e) {
        unreachablePartitions.add(partitionId);
      }
    }
  }

  /**
   * Fetch the {@link StatsSnapshot} for the given {@link PartitionId}.
   * @param partitionId the {@link PartitionId} to try to fetch the {@link StatsSnapshot} from
   * @param unreachablePartitions a list of partitionIds to keep track of the unreachable partitions
   * @return the generated {@link StatsSnapshot}
   */
  StatsSnapshot fetchSnapshot(PartitionId partitionId, List<PartitionId> unreachablePartitions,
      StatsReportType reportType) {
    StatsSnapshot statsSnapshot = null;
    Store store = storageManager.getStore(partitionId, false);
    if (store == null) {
      unreachablePartitions.add(partitionId);
    } else {
      try {
        Map<StatsReportType, StatsSnapshot> snapshotsByType =
            store.getStoreStats().getStatsSnapshots(EnumSet.of(reportType), time.milliseconds());
        statsSnapshot = snapshotsByType.get(reportType);
      } catch (StoreException e) {
        String reportTypeStr = reportType.toString();
        logger.error("StoreException on fetching {} stats snapshot for store {}",
            reportTypeStr.substring(0, reportTypeStr.lastIndexOf('_')), store, e);
        unreachablePartitions.add(partitionId);
      }
    }
    return statsSnapshot;
  }

  /**
   * Add new {@link ReplicaId} to StatsManager and thus stats of this store will be collected during next round of aggregation.
   * @param id the {@link ReplicaId} associated with store to be added
   * @return {@code true} if adding replica was successful. {@code false} if not.
   */
  boolean addReplica(ReplicaId id) {
    boolean success = partitionToReplicaMap.putIfAbsent(id.getPartitionId(), id) == null;
    if (success) {
      logger.info("Partition " + id.getPartitionId() + " is added into StatsManager");
    } else {
      logger.error("Failed to add partition " + id.getPartitionId() + " because it is already in StatsManager");
    }
    return success;
  }

  /**
   * Remove {@link ReplicaId} from StatsManager and thus it won't be checked during next round of aggregation.
   * @param id the {@link ReplicaId} associated with store to be removed
   * @return {@code true} if removing replica was successful. {@code false} if the replica doesn't exist.
   */
  boolean removeReplica(ReplicaId id) {
    boolean success = partitionToReplicaMap.remove(id.getPartitionId()) != null;
    if (success) {
      logger.info("Partition " + id.getPartitionId() + " is removed from StatsManager");
    } else {
      logger.error("Failed to remove partition " + id.getPartitionId() + " because it doesn't exist in StatsManager");
    }
    return success;
  }

  /**
   * Get the combined {@link StatsSnapshot} of all partitions in this node. This json will contain one entry per partition
   * wrt valid data size. The node level stats format is as follows.
   * <pre>
   *             ACCOUNT_REPORT                   |             PARTITION_CLASS_REPORT
   * ---------------------------------------------------------------------------------------------------
   * {                                            |    {
   *   value: 1000,                               |      value: 1000,
   *   subMap: {                                  |      subMap: {
   *     Partition[1]:{                           |        PartitionClass_1: {
   *       value: 1000,                           |          value: 400,
   *       subMap: {                              |          subMap: {
   *         Account[1]:{                         |            Partition[1]: {
   *           value: 400,                        |              value: 400,
   *           subMap: {                          |              subMap: {
   *             Container[1]:{                   |                Account[1]_Container[1]: {
   *               value: 400,                    |                  value: 400,
   *               subMap: null                   |                  subMap: null
   *             }                                |                }
   *           }                                  |              }
   *         },                                   |            }
   *         Account[2]:{                         |          }
   *           value: 600,                        |        },
   *           subMap: {                          |        PartitionClass_2: {
   *             Container[2]:{                   |          value: 600,
   *               value: 600,                    |          subMap: {
   *               subMap: null                   |            Partition[2]: {
   *             }                                |              value: 600,
   *           }                                  |              subMap: {
   *         }                                    |                Account[2]_Container[2]: {
   *       }                                      |                  value: 600,
   *     }                                        |                  subMap: null
   *   }                                          |                }
   * }                                            |              }
   *                                              |            }
   *                                              |          }
   *                                              |        }
   *                                              |      }
   *                                              |    }
   * </pre>
   * @param statsReportType the {@link StatsReportType} to get from this node
   * @return a combined {@link StatsSnapshot} of this node
   */
  String getNodeStatsInJSON(StatsReportType statsReportType) {
    String statsWrapperJSON = "";
    logger.info("Aggregating node-level stats for Helix report");
    try {
      long totalFetchAndAggregateStartTimeMs = time.milliseconds();
      StatsSnapshot combinedSnapshot = new StatsSnapshot(0L, new HashMap<>());
      long totalValue = 0;
      List<PartitionId> unreachablePartitions = new ArrayList<>();
      Set<PartitionId> partitionsBeforeAggregation = new HashSet<>(partitionToReplicaMap.keySet());
      Iterator<PartitionId> iterator = partitionsBeforeAggregation.iterator();
      while (iterator.hasNext()) {
        PartitionId partitionId = iterator.next();
        long fetchSnapshotStartTimeMs = time.milliseconds();
        StatsSnapshot statsSnapshot = fetchSnapshot(partitionId, unreachablePartitions, statsReportType);
        if (statsSnapshot != null) {
          Map<String, StatsSnapshot> combinedSnapshotSubMap = combinedSnapshot.getSubMap();
          switch (statsReportType) {
            case ACCOUNT_REPORT:
              combinedSnapshotSubMap.put(partitionId.toString(), statsSnapshot);
              break;
            case PARTITION_CLASS_REPORT:
              StatsSnapshot partitionClassSnapshot =
                  combinedSnapshotSubMap.getOrDefault(partitionId.getPartitionClass(),
                      new StatsSnapshot(0L, new HashMap<>()));
              partitionClassSnapshot.setValue(partitionClassSnapshot.getValue() + statsSnapshot.getValue());
              partitionClassSnapshot.getSubMap().put(partitionId.toString(), statsSnapshot);
              combinedSnapshotSubMap.put(partitionId.getPartitionClass(), partitionClassSnapshot);
              break;
            default:
              throw new IllegalArgumentException("Unrecognized stats report type: " + statsReportType);
          }
          totalValue += statsSnapshot.getValue();
        }
        metrics.fetchAndAggregateTimePerStoreMs.update(time.milliseconds() - fetchSnapshotStartTimeMs);
      }
      combinedSnapshot.setValue(totalValue);
      List<String> examinedUnreachableStores = examineUnreachablePartitions(unreachablePartitions);
      metrics.totalFetchAndAggregateTimeMs.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
      StatsHeader statsHeader = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, time.milliseconds(),
          partitionsBeforeAggregation.size(), partitionsBeforeAggregation.size() - unreachablePartitions.size(),
          examinedUnreachableStores);
      statsWrapperJSON = mapper.writeValueAsString(new StatsWrapper(statsHeader, combinedSnapshot));
      logger.info("Node-level stats aggregated for Helix report");
    } catch (Exception | Error e) {
      metrics.statsAggregationFailureCount.inc();
      logger.error("Exception while aggregating stats for Helix report", e);
    }
    return statsWrapperJSON;
  }

  /**
   * Runnable class that collects, aggregate and publish stats via methods in StatsManager.
   */
  private class StatsAggregator implements Runnable {
    private volatile boolean cancelled = false;

    @Override
    public void run() {
      logger.info("Aggregating stats for local report");
      try {
        long totalFetchAndAggregateStartTimeMs = time.milliseconds();
        StatsSnapshot aggregatedSnapshot = new StatsSnapshot(0L, null);
        List<PartitionId> unreachablePartitions = new ArrayList<>();
        Iterator<PartitionId> iterator = (new HashSet<>(partitionToReplicaMap.keySet())).iterator();
        while (!cancelled && iterator.hasNext()) {
          PartitionId partitionId = iterator.next();
          logger.info("Aggregating stats for local report started for store {}", partitionId);
          collectAndAggregate(aggregatedSnapshot, partitionId, unreachablePartitions);
        }
        List<String> unreachableStores = examineUnreachablePartitions(unreachablePartitions);
        if (!cancelled) {
          metrics.totalFetchAndAggregateTimeMs.update(time.milliseconds() - totalFetchAndAggregateStartTimeMs);
          StatsHeader statsHeader = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, time.milliseconds(),
              partitionToReplicaMap.keySet().size(), partitionToReplicaMap.keySet().size() - unreachableStores.size(),
              unreachableStores);
          publish(new StatsWrapper(statsHeader, aggregatedSnapshot));
          logger.info("Local stats snapshot published to {}", statsOutputFile.getAbsolutePath());
        }
      } catch (Exception | Error e) {
        metrics.statsAggregationFailureCount.inc();
        logger.error("Exception while aggregating stats for local report. Stats output file path - {}",
            statsOutputFile.getAbsolutePath(), e);
      }
    }

    void cancel() {
      cancelled = true;
    }
  }

  /**
   * Re-examine the unreachable partitions to preclude those already removed from StatsManager.
   * @param unreachablePartitions a list of unreachable partitions generated during stats aggregation.
   * @return a list of stores which are unreachable and still present in StatsManager.
   */
  List<String> examineUnreachablePartitions(List<PartitionId> unreachablePartitions) {
    List<String> unreachableStores = new ArrayList<>();
    for (PartitionId partition : unreachablePartitions) {
      if (partitionToReplicaMap.containsKey(partition)) {
        unreachableStores.add(partition.toPathString());
      } else {
        logger.info("Removing partition " + partition.toPathString()
            + " from unreachable list because it is no longer in StatsManager");
      }
    }
    return unreachableStores;
  }

  /**
   * {@link PartitionStateChangeListener} to capture changes in partition state.
   */
  private class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      // check if partition exists
      ReplicaId replica = storageManager.getReplica(partitionName);
      if (replica == null) {
        // no matter this is an existing replica or new added one, it should be present in storage manager because new
        // replica is added into storage manager first.
        throw new StateTransitionException("Replica " + partitionName + " is not found on current node",
            ReplicaNotFound);
      }
      if (!partitionToReplicaMap.containsKey(replica.getPartitionId())) {
        // if replica is not present in partitionToReplicaMap, it means this new replica was just added into storage
        // manager. Here we add it into stats manager accordingly.
        logger.info("Didn't find replica {} in stats manager, starting to add it.", partitionName);
        if (!addReplica(replica)) {
          throw new StateTransitionException("Failed to add new replica into stats manager", ReplicaOperationFailure);
        }
      }
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      logger.info("Partition state change notification from Bootstrap to Standby received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {
      logger.info("Partition state change notification from Standby to Leader received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {
      logger.info("Partition state change notification from Leader to Standby received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {
      logger.info("Partition state change notification from Standby to Inactive received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {
      logger.info("Partition state change notification from Inactive to Offline received for partition {}",
          partitionName);
    }

    @Override
    public void onPartitionBecomeDroppedFromOffline(String partitionName) {
      // check if partition exists
      ReplicaId replica = storageManager.getReplica(partitionName);
      if (replica == null) {
        throw new StateTransitionException("Replica " + partitionName + " is not found on current node",
            ReplicaNotFound);
      }
      // remove replica from in-mem data structure. If replica doesn't exist, log info but don't fail the transition
      removeReplica(replica);
    }
  }
}
