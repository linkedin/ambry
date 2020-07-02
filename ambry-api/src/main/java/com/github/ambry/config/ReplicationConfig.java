/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.config;

import com.github.ambry.replication.ReplicationModelType;
import com.github.ambry.utils.Utils;
import java.util.HashSet;
import java.util.Set;


/**
 * The configs for the replication layer "
 */
public class ReplicationConfig {

  public static final String REPLICATION_CLOUD_TOKEN_FACTORY = "replication.cloud.token.factory";
  public static final String REPLICA_TOKEN_FILE_NAME = "replicaTokens";
  public static final String REPLICATION_MODEL_ACROSS_DATACENTERS = "replication.model.across.datacenters";
  public static final String REPLICATION_STANDBY_WAIT_TIMEOUT_TO_TRIGGER_CROSS_COLO_FETCH_SECONDS =
      "replication.standby.wait.timeout.to.trigger.cross.colo.fetch.seconds";

  /**
   * The factory class the replication uses to creates its tokens
   */
  @Config("replication.token.factory")
  @Default("com.github.ambry.store.StoreFindTokenFactory")
  public final String replicationStoreTokenFactory;

  /**
   * The factory class the replication uses to create cloud token
   */
  @Config(REPLICATION_CLOUD_TOKEN_FACTORY)
  @Default("com.github.ambry.cloud.azure.CosmosUpdateTimeFindTokenFactory")
  public final String replicationCloudTokenFactory;

  /**
   * The number of replica threads on each server that runs the replication protocol for intra dc replication
   */
  @Config("replication.no.of.intra.dc.replica.threads")
  @Default("1")
  public final int replicationNumOfIntraDCReplicaThreads;

  /**
   * The number of replica threads on each server that runs the replication protocol for inter dc replication
   */
  @Config("replication.no.of.inter.dc.replica.threads")
  @Default("1")
  public final int replicationNumOfInterDCReplicaThreads;

  /**
   * The timeout to get a connection checkout from the connection pool for replication
   */
  @Config("replication.connection.pool.checkout.timeout.ms")
  @Default("5000")
  public final int replicationConnectionPoolCheckoutTimeoutMs;

  /**
   * The flush interval for persisting the replica tokens to disk
   */
  @Config("replication.token.flush.interval.seconds")
  @Default("300")
  public final int replicationTokenFlushIntervalSeconds;

  /**
   * The initial delay to start the replica token flush thread
   */
  @Config("replication.token.flush.delay.seconds")
  @Default("5")
  public final int replicationTokenFlushDelaySeconds;

  /**
   * The time (in ms) to sleep between replication cycles to throttle the replica thread in case the thread handles
   * intra datacenter replicas
   */
  @Config("replication.intra.replica.thread.throttle.sleep.duration.ms")
  @Default("0")
  public final long replicationIntraReplicaThreadThrottleSleepDurationMs;

  /**
   * The time (in ms) to sleep between replication cycles to throttle the replica thread in case the thread handles
   * inter datacenter replicas
   */
  @Config("replication.inter.replica.thread.throttle.sleep.duration.ms")
  @Default("0")
  public final long replicationInterReplicaThreadThrottleSleepDurationMs;

  /**
   * The time (in ms) to sleep between replication cycles when the replica thread is not doing any useful work
   */
  @Config("replication.replica.thread.idle.sleep.duration.ms")
  @Default("0")
  public final long replicationReplicaThreadIdleSleepDurationMs;

  /**
   * The time (in ms) to temporarily disable replication for a replica in order to reduce wasteful network calls
   */
  @Config("replication.synced.replica.backoff.duration.ms")
  @Default("0")
  public final long replicationSyncedReplicaBackoffDurationMs;

  /**
   * The fetch size is an approximate total size that a remote server would return on a fetch request.
   * This is not guaranteed to be always obeyed. For example, if a single blob is larger than the fetch size
   * the entire blob would be returned
   */
  @Config("replication.fetch.size.in.bytes")
  @Default("1048576")
  public final long replicationFetchSizeInBytes;

  /**
   * The maximum number of partitions on remote node can be replicated within a single replication request. If local
   * node shares more partitions than this number with peer node, the partitions will be split into several lists (with
   * size <= max partition count) and will be replicated sequentially in separate replication cycles. If set to 0, it
   * means there is no limit.
   */
  @Config("replication.max.partition.count.per.request")
  @Default("20")
  public final int replicationMaxPartitionCountPerRequest;

  /**
   * If true, replication Get requests asks for deleted and expired blobs as well to succeed in scenarios where blobs
   * get deleted or expired after replication metadata exchange.
   */
  @Config("replication.include.all")
  @Default("true")
  public final boolean replicationIncludeAll;

  /**
   * If true, replication token will be persisted when shutdown or replica remote.
   * For CloudBackupManger and ReplicationManager, token is persisted when shutdown if this config is true.
   * For CloudBackupManger, where replica may be removed, token is also persisted if this config is true.
   * ReplicationManager doesn't support replica remove now.
   * This is used for test only as of now.
   */
  @Config("replication.persist.token.on.shutdown.or.replica.remove")
  @Default("true")
  public final boolean replicationPersistTokenOnShutdownOrReplicaRemove;

  /**
   * If true, replication will register metric for each partition to track lag between local and remote replicas. This
   * metric indicates how far local replica is behind remote peer replica.
   */
  @Config("replication.track.per.partition.lag.from.remote")
  @Default("false")
  public final boolean replicationTrackPerPartitionLagFromRemote;

  /**
   * If true, register metrics that track remote replica's lag from local at datacenter level. These metrics indicate how
   * far remote replicas (in certain dc) are behind local replicas (on current node). If enabled, it records max/min/avg
   * remote replica lag separately in each datacenter.
   */
  @Config("replication.track.per.datacenter.lag.from.local")
  @Default("true")
  public final boolean replicationTrackPerDatacenterLagFromLocal;

  /**
   * The version of metadata request to be used for replication.
   */
  @Config("replication.metadata.request.version")
  @Default("1")
  public final short replicaMetadataRequestVersion;

  /**
   * If set to true, the Ambry data nodes will also replicate from vcr nodes based on vcr helix cluster map.
   */
  @Config("replication.enabled.with.vcr.cluster")
  @Default("false")
  public final boolean replicationEnabledWithVcrCluster;

  /**
   * Comma separated list of partitions to recover.
   * This config will ignore the partition leadership information for recovering partitions.
   */
  @Config("replication.vcr.recovery.partitions")
  @Default("")
  public final Set<String> replicationVcrRecoveryPartitions;

  /**
   * To specify the type of replication to be used for inter colo replication.
   * It has two values - "ALL_TO_ALL" or "LEADER_BASED". Default value is "ALL_TO_ALL".
   * If set to "LEADER_BASED", inter colo replication between will be limited to the leaders(as elected by Helix) of the partition of each datacenter.
   * If set to "ALL_TO_ALL", inter colo replication will be in an all-to-all fashion, i.e. each replica talks to all other replicas irrespective of their state.
   * Intra colo replication will continue as all-to-all fashion in both the models.
   */
  @Config(REPLICATION_MODEL_ACROSS_DATACENTERS)
  @Default("ALL_TO_ALL")
  public final ReplicationModelType replicationModelAcrossDatacenters;

  /**
   * Use http2 connection for replication if this is true.
   * DO NOT enable http2 replication. Not ready.
   */
  @Config("replication.enable.http2")
  @Default("false")
  public final boolean replicationEnableHttp2;

  /**
   * How long (in days) a container must be replicated before it's been deleted during compaction.
   */
  @Config("replication.container.deletion.retention.days")
  @Default("14")
  public final int replicationContainerDeletionRetentionDays;

  /**
   * True to enable skip deprecated containers in replication.
   */
  @Config("replication.container.deletion.enabled")
  @Default("false")
  public final boolean replicationContainerDeletionEnabled;

  /**
   * The time (in seconds) for standby replicas to wait before fetching missing keys from replicas in cross colo
   * data centers. This is applicable during leader based replication where standby replicas don't fetch the missing
   * keys found in metadata exchange from cross colo replicas and expect them to come from leader replica in
   * local data center via intra-dc replication. This time out ensures that standby replicas are not stuck indefinitely
   * waiting for the missing keys to come via intra-dc replication by doing cross colo fetch themselves.
   * Default value is 120 seconds. If configured to -1, this timeout doesn't take effect, i.e. cross colo fetch for
   * standby replicas is never done.
   */
  @Config(REPLICATION_STANDBY_WAIT_TIMEOUT_TO_TRIGGER_CROSS_COLO_FETCH_SECONDS)
  @Default("120")
  public final int replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds;

  public ReplicationConfig(VerifiableProperties verifiableProperties) {

    replicationStoreTokenFactory =
        verifiableProperties.getString("replication.token.factory", "com.github.ambry.store.StoreFindTokenFactory");
    replicationCloudTokenFactory = verifiableProperties.getString(REPLICATION_CLOUD_TOKEN_FACTORY,
        "com.github.ambry.cloud.azure.CosmosChangeFeedFindTokenFactory");
    replicationNumOfIntraDCReplicaThreads =
        verifiableProperties.getInt("replication.no.of.intra.dc.replica.threads", 1);
    replicationNumOfInterDCReplicaThreads =
        verifiableProperties.getInt("replication.no.of.inter.dc.replica.threads", 1);
    replicationConnectionPoolCheckoutTimeoutMs =
        verifiableProperties.getIntInRange("replication.connection.pool.checkout.timeout.ms", 5000, 1000, 10000);
    replicationTokenFlushIntervalSeconds =
        verifiableProperties.getIntInRange("replication.token.flush.interval.seconds", 300, 5, Integer.MAX_VALUE);
    replicationTokenFlushDelaySeconds =
        verifiableProperties.getIntInRange("replication.token.flush.delay.seconds", 5, 1, Integer.MAX_VALUE);
    replicationIntraReplicaThreadThrottleSleepDurationMs =
        verifiableProperties.getLongInRange("replication.intra.replica.thread.throttle.sleep.duration.ms", 0, 0,
            Long.MAX_VALUE);
    replicationInterReplicaThreadThrottleSleepDurationMs =
        verifiableProperties.getLongInRange("replication.inter.replica.thread.throttle.sleep.duration.ms", 0, 0,
            Long.MAX_VALUE);
    replicationReplicaThreadIdleSleepDurationMs =
        verifiableProperties.getLongInRange("replication.replica.thread.idle.sleep.duration.ms", 0, 0, Long.MAX_VALUE);
    replicationSyncedReplicaBackoffDurationMs =
        verifiableProperties.getLongInRange("replication.synced.replica.backoff.duration.ms", 0, 0, Long.MAX_VALUE);
    replicationFetchSizeInBytes =
        verifiableProperties.getLongInRange("replication.fetch.size.in.bytes", 1048576, 1, Long.MAX_VALUE);
    replicationMaxPartitionCountPerRequest =
        verifiableProperties.getIntInRange("replication.max.partition.count.per.request", 20, 0, Integer.MAX_VALUE);
    replicationIncludeAll = verifiableProperties.getBoolean("replication.include.all", true);
    replicationPersistTokenOnShutdownOrReplicaRemove =
        verifiableProperties.getBoolean("replication.persist.token.on.shutdown.or.replica.remove", true);
    replicationTrackPerPartitionLagFromRemote =
        verifiableProperties.getBoolean("replication.track.per.partition.lag.from.remote", false);
    replicationTrackPerDatacenterLagFromLocal =
        verifiableProperties.getBoolean("replication.track.per.datacenter.lag.from.local", true);
    replicaMetadataRequestVersion =
        verifiableProperties.getShortInRange("replication.metadata.request.version", (short) 1, (short) 1, (short) 2);
    replicationEnabledWithVcrCluster = verifiableProperties.getBoolean("replication.enabled.with.vcr.cluster", false);
    String vcrRecoveryPartitions = verifiableProperties.getString("replication.vcr.recovery.partitions", "");
    replicationVcrRecoveryPartitions = Utils.splitString(vcrRecoveryPartitions, ",", HashSet::new);
    replicationModelAcrossDatacenters = ReplicationModelType.valueOf(
        verifiableProperties.getString(REPLICATION_MODEL_ACROSS_DATACENTERS, ReplicationModelType.ALL_TO_ALL.name()));
    replicationEnableHttp2 = verifiableProperties.getBoolean("replication.enable.http2", false);
    replicationContainerDeletionRetentionDays =
        verifiableProperties.getInt("replication.container.deletion.retention.days", 14);
    replicationContainerDeletionEnabled =
        verifiableProperties.getBoolean("replication.container.deletion.enabled", false);
    replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds =
        verifiableProperties.getIntInRange(REPLICATION_STANDBY_WAIT_TIMEOUT_TO_TRIGGER_CROSS_COLO_FETCH_SECONDS, 120,
            -1, Integer.MAX_VALUE);
  }
}
