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
package com.github.ambry.replication;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


/**
 * Metrics for Replication
 */
public class ReplicationMetrics {

  private final static String MAX_LAG_FROM_PEERS_IN_BYTE_METRIC_NAME_TEMPLATE = "Partition-%s-maxLagFromPeersInBytes";
  private final static String CATCH_POINT_FROM_CLOUD_METRIC_NAME_TEMPLATE = "Partition-%s-catchupPointFromCloud";

  public final Map<String, Meter> interColoReplicationBytesRate = new HashMap<String, Meter>();
  public final Meter intraColoReplicationBytesRate;
  public final Map<String, Meter> plainTextInterColoReplicationBytesRate = new HashMap<String, Meter>();
  public final Meter plainTextIntraColoReplicationBytesRate;
  public final Map<String, Meter> sslInterColoReplicationBytesRate = new HashMap<String, Meter>();
  public final Meter sslIntraColoReplicationBytesRate;
  public final Map<String, Counter> interColoMetadataExchangeCount = new HashMap<>();
  public final Map<String, Counter> interColoReplicationGetRequestCount = new HashMap<>();
  public final Counter intraColoMetadataExchangeCount;
  public final Counter intraColoReplicationGetRequestCount;
  public final Map<String, Counter> interColoBlobsReplicatedCount = new HashMap<String, Counter>();
  public final Counter intraColoBlobsReplicatedCount;
  public final Counter unknownRemoteReplicaRequestCount;
  public final Map<String, Counter> plainTextInterColoMetadataExchangeCount = new HashMap<String, Counter>();
  public final Counter plainTextIntraColoMetadataExchangeCount;
  public final Map<String, Counter> plainTextInterColoBlobsReplicatedCount = new HashMap<String, Counter>();
  public final Counter plainTextIntraColoBlobsReplicatedCount;
  public final Map<String, Counter> sslInterColoMetadataExchangeCount = new HashMap<String, Counter>();
  public final Counter sslIntraColoMetadataExchangeCount;
  public final Map<String, Counter> sslInterColoBlobsReplicatedCount = new HashMap<String, Counter>();
  public final Counter sslIntraColoBlobsReplicatedCount;
  public final Counter replicationErrors;
  public final Counter plainTextReplicationErrors;
  public final Counter sslReplicationErrors;
  public final Counter replicationTokenResetCount;
  public final Counter replicationInvalidMessageStreamErrorCount;
  public final Map<String, Timer> interColoReplicationLatency = new HashMap<String, Timer>();
  public final Timer intraColoReplicationLatency;
  public final Map<String, Timer> plainTextInterColoReplicationLatency = new HashMap<String, Timer>();
  public final Timer plainTextIntraColoReplicationLatency;
  public final Map<String, Timer> sslInterColoReplicationLatency = new HashMap<String, Timer>();
  public final Timer sslIntraColoReplicationLatency;
  public final Histogram remoteReplicaTokensPersistTime;
  public final Histogram remoteReplicaTokensRestoreTime;
  public final Map<String, Histogram> interColoExchangeMetadataTime = new HashMap<String, Histogram>();
  public final Histogram intraColoExchangeMetadataTime;
  public final Map<String, Histogram> plainTextInterColoExchangeMetadataTime = new HashMap<String, Histogram>();
  public final Histogram plainTextIntraColoExchangeMetadataTime;
  public final Map<String, Histogram> sslInterColoExchangeMetadataTime = new HashMap<String, Histogram>();
  public final Histogram sslIntraColoExchangeMetadataTime;
  public final Map<String, Histogram> interColoFixMissingKeysTime = new HashMap<String, Histogram>();
  public final Histogram intraColoFixMissingKeysTime;
  public final Map<String, Histogram> plainTextInterColoFixMissingKeysTime = new HashMap<String, Histogram>();
  public final Histogram plainTextIntraColoFixMissingKeysTime;
  public final Map<String, Histogram> sslInterColoFixMissingKeysTime = new HashMap<String, Histogram>();
  public final Histogram sslIntraColoFixMissingKeysTime;
  public final Map<String, Histogram> interColoReplicationMetadataRequestTime = new HashMap<String, Histogram>();
  public final Histogram intraColoReplicationMetadataRequestTime;
  public final Map<String, Histogram> plainTextInterColoReplicationMetadataRequestTime =
      new HashMap<String, Histogram>();
  public final Histogram plainTextIntraColoReplicationMetadataRequestTime;
  public final Map<String, Histogram> sslInterColoReplicationMetadataRequestTime = new HashMap<String, Histogram>();
  public final Histogram sslIntraColoReplicationMetadataRequestTime;
  public final Histogram intraColoReplicationWaitTime;
  public final Map<String, Histogram> interColoCheckMissingKeysTime = new HashMap<String, Histogram>();
  public final Histogram intraColoCheckMissingKeysTime;
  public final Map<String, Histogram> interColoProcessMetadataResponseTime = new HashMap<String, Histogram>();
  public final Histogram intraColoProcessMetadataResponseTime;
  public final Map<String, Histogram> interColoGetRequestTime = new HashMap<String, Histogram>();
  public final Histogram intraColoGetRequestTime;
  public final Map<String, Histogram> plainTextInterColoGetRequestTime = new HashMap<String, Histogram>();
  public final Histogram plainTextIntraColoGetRequestTime;
  public final Map<String, Histogram> sslInterColoGetRequestTime = new HashMap<String, Histogram>();
  public final Histogram sslIntraColoGetRequestTime;
  public final Map<String, Histogram> interColoBatchStoreWriteTime = new HashMap<String, Histogram>();
  public final Histogram intraColoBatchStoreWriteTime;
  public final Map<String, Histogram> plainTextInterColoBatchStoreWriteTime = new HashMap<String, Histogram>();
  public final Histogram plainTextIntraColoBatchStoreWriteTime;
  public final Map<String, Histogram> sslInterColoBatchStoreWriteTime = new HashMap<String, Histogram>();
  public final Histogram sslIntraColoBatchStoreWriteTime;
  public final Map<String, Histogram> interColoTotalReplicationTime = new HashMap<String, Histogram>();
  public final Histogram intraColoTotalReplicationTime;
  public final Map<String, Histogram> plainTextInterColoTotalReplicationTime = new HashMap<String, Histogram>();
  public final Histogram plainTextIntraColoTotalReplicationTime;
  public final Map<String, Histogram> sslInterColoTotalReplicationTime = new HashMap<String, Histogram>();
  public final Histogram sslIntraColoTotalReplicationTime;
  public final Counter blobDeletedOnGetCount;
  public final Counter blobAuthorizationFailureCount;
  public final Counter intraColoReplicaSyncedBackoffCount;
  public final Counter interColoReplicaSyncedBackoffCount;
  public final Counter intraColoReplicaThreadIdleCount;
  public final Counter interColoReplicaThreadIdleCount;
  public final Counter intraColoReplicaThreadThrottleCount;
  public final Counter interColoReplicaThreadThrottleCount;
  public final Counter remoteReplicaInfoRemoveError;
  public final Counter remoteReplicaInfoAddError;
  public final Counter allResponsedKeysExist;

  // Metrics for replication from cloud
  public final Counter addCloudPartitionErrorCount;
  public final Counter cloudTokenReloadWarnCount;

  private MetricRegistry registry;
  private final Map<String, Counter> metadataRequestErrorMap = new ConcurrentHashMap<>();
  private final Map<String, Counter> getRequestErrorMap = new HashMap<>();
  private final Map<String, Counter> localStoreErrorMap = new HashMap<>();
  private final Map<PartitionId, Counter> partitionIdToInvalidMessageStreamErrorCounter = new HashMap<>();
  // ConcurrentHashMap is used to avoid cache incoherence.
  private final Map<PartitionId, Map<DataNodeId, Long>> partitionLags = new ConcurrentHashMap<>();
  private Map<PartitionId, Long> cloudReplicaCatchUpPoint = new ConcurrentHashMap<>();
  private final Map<String, Set<RemoteReplicaInfo>> remoteReplicaInfosByDc = new ConcurrentHashMap<>();
  private final Map<String, LongSummaryStatistics> dcToReplicaLagStats = new ConcurrentHashMap<>();

  // Metric to track number of cross colo replication get requests sent by standby replicas. This is applicable during
  // leader-based replication.
  // Use case: If leader-based replication is enabled, we should see ideally send cross colo gets only for leader replicas.
  // However, if standby replicas time out waiting for their keys to come from leader, we send cross colo gets for them.
  // This metric tracks number of such cross colo get requests for Standby replicas.
  public final Map<String, Counter> interColoReplicationGetRequestCountForStandbyReplicas = new ConcurrentHashMap<>();

  // Metric to track cross colo replication bytes fetch rate for standby replicas. This is applicable during
  // leader-based replication.
  // Use case: If leader-based replication is enabled, we should see ideally send cross colo gets only for leader replicas.
  // However, if standby replicas time out waiting for their keys to come from leader, we send cross colo gets for them.
  // This metric tracks cross colo get requests bytes rate for Standby replicas.
  public final Map<String, Meter> interColoReplicationFetchBytesRateForStandbyReplicas = new ConcurrentHashMap<>();

  public ReplicationMetrics(MetricRegistry registry, List<? extends ReplicaId> replicaIds) {
    intraColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationBytesRate"));
    plainTextIntraColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoReplicationBytesRate"));
    sslIntraColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "SslIntraColoReplicationBytesRate"));
    intraColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoMetadataExchangeCount"));
    intraColoReplicationGetRequestCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationGetRequestCount"));
    intraColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoBlobsReplicatedCount"));
    unknownRemoteReplicaRequestCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "UnknownRemoteReplicaRequestCount"));
    plainTextIntraColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoMetadataExchangeCount"));
    plainTextIntraColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoBlobsReplicatedCount"));
    sslIntraColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "SslIntraColoMetadataExchangeCount"));
    sslIntraColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "SslIntraColoBlobsReplicatedCount"));
    replicationErrors = registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationErrors"));
    plainTextReplicationErrors =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "PlainTextReplicationErrors"));
    sslReplicationErrors = registry.counter(MetricRegistry.name(ReplicaThread.class, "SslReplicationErrors"));
    replicationTokenResetCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationTokenResetCount"));
    replicationInvalidMessageStreamErrorCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationInvalidMessageStreamErrorCount"));
    intraColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationLatency"));
    plainTextIntraColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoReplicationLatency"));
    sslIntraColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "SslIntraColoReplicationLatency"));
    remoteReplicaTokensPersistTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaTokensPersistTime"));
    remoteReplicaTokensRestoreTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaTokensRestoreTime"));
    intraColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoExchangeMetadataTime"));
    plainTextIntraColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoExchangeMetadataTime"));
    sslIntraColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoExchangeMetadataTime"));
    intraColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoFixMissingKeysTime"));
    plainTextIntraColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoFixMissingKeysTime"));
    sslIntraColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoFixMissingKeysTime"));
    intraColoReplicationWaitTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationWaitTime"));
    intraColoReplicationMetadataRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationMetadataRequestTime"));
    plainTextIntraColoReplicationMetadataRequestTime = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoReplicationMetadataRequestTime"));
    sslIntraColoReplicationMetadataRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoReplicationMetadataRequestTime"));
    intraColoCheckMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoCheckMissingKeysTime"));
    intraColoProcessMetadataResponseTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoProcessMetadataResponseTime"));
    intraColoGetRequestTime = registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoGetRequestTime"));
    plainTextIntraColoGetRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoGetRequestTime"));
    sslIntraColoGetRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoGetRequestTime"));
    intraColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoBatchStoreWriteTime"));
    plainTextIntraColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoBatchStoreWriteTime"));
    sslIntraColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoBatchStoreWriteTime"));
    intraColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoTotalReplicationTime"));
    plainTextIntraColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "PlainTextIntraColoTotalReplicationTime"));
    sslIntraColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslIntraColoTotalReplicationTime"));
    blobDeletedOnGetCount = registry.counter(MetricRegistry.name(ReplicaThread.class, "BlobDeletedOnGetCount"));
    blobAuthorizationFailureCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "BlobAuthorizationFailureCount"));
    intraColoReplicaSyncedBackoffCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicaSyncedBackoffCount"));
    interColoReplicaSyncedBackoffCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "InterColoReplicaSyncedBackoffCount"));
    intraColoReplicaThreadIdleCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicaThreadIdleCount"));
    interColoReplicaThreadIdleCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "InterColoReplicaThreadIdleCount"));
    intraColoReplicaThreadThrottleCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicaThreadThrottleCount"));
    interColoReplicaThreadThrottleCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "InterColoReplicaThreadThrottleCount"));
    remoteReplicaInfoRemoveError =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaInfoRemoveError"));
    remoteReplicaInfoAddError = registry.counter(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaInfoAddError"));
    allResponsedKeysExist = registry.counter(MetricRegistry.name(ReplicaThread.class, "AllResponsedKeysExist"));
    addCloudPartitionErrorCount =
        registry.counter(MetricRegistry.name(CloudToStoreReplicationManager.class, "AddCloudPartitionErrorCount"));
    cloudTokenReloadWarnCount =
        registry.counter(MetricRegistry.name(CloudToStoreReplicationManager.class, "CloudTokenReloadWarnCount"));
    this.registry = registry;
    populateInvalidMessageMetricForReplicas(replicaIds);
  }

  /**
   * Updates given colo metrics.
   * @param datacenter The datacenter to replicate from.
   */
  public void populateSingleColoMetrics(String datacenter) {
    Meter interColoReplicationBytesRatePerDC =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ReplicationBytesRate"));
    interColoReplicationBytesRate.put(datacenter, interColoReplicationBytesRatePerDC);
    Meter plainTextInterColoReplicationBytesRatePerDC = registry.meter(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-ReplicationBytesRate"));
    plainTextInterColoReplicationBytesRate.put(datacenter, plainTextInterColoReplicationBytesRatePerDC);
    Meter sslInterColoReplicationBytesRatePerDC =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-ReplicationBytesRate"));
    sslInterColoReplicationBytesRate.put(datacenter, sslInterColoReplicationBytesRatePerDC);
    Counter interColoMetadataExchangeCountPerDC =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-MetadataExchangeCount"));
    interColoMetadataExchangeCount.put(datacenter, interColoMetadataExchangeCountPerDC);
    Counter interColoReplicationGetRequestCountPerDC = registry.counter(
        MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ReplicationGetRequestCount"));
    interColoReplicationGetRequestCount.put(datacenter, interColoReplicationGetRequestCountPerDC);
    Counter interColoBlobsReplicatedCountPerDC =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ReplicationBlobsCount"));
    interColoBlobsReplicatedCount.put(datacenter, interColoBlobsReplicatedCountPerDC);
    Counter plainTextInterColoMetadataExchangeCountPerDC = registry.counter(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-MetadataExchangeCount"));
    plainTextInterColoMetadataExchangeCount.put(datacenter, plainTextInterColoMetadataExchangeCountPerDC);
    Counter plainTextInterColoBlobsReplicatedCountPerDC = registry.counter(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-BlobsReplicatedCount"));
    plainTextInterColoBlobsReplicatedCount.put(datacenter, plainTextInterColoBlobsReplicatedCountPerDC);
    Counter sslInterColoMetadataExchangeCountPerDC =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-MetadataExchangeCount"));
    sslInterColoMetadataExchangeCount.put(datacenter, sslInterColoMetadataExchangeCountPerDC);
    Counter sslInterColoBlobsReplicatedCountPerDC =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-BlobsReplicatedCount"));
    sslInterColoBlobsReplicatedCount.put(datacenter, sslInterColoBlobsReplicatedCountPerDC);
    Timer interColoReplicationLatencyPerDC =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ReplicationLatency"));
    interColoReplicationLatency.put(datacenter, interColoReplicationLatencyPerDC);
    Timer plainTextInterColoReplicationLatencyPerDC = registry.timer(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-ReplicationLatency"));
    plainTextInterColoReplicationLatency.put(datacenter, plainTextInterColoReplicationLatencyPerDC);
    Timer sslInterColoReplicationLatencyPerDC =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-ReplicationLatency"));
    sslInterColoReplicationLatency.put(datacenter, sslInterColoReplicationLatencyPerDC);
    Histogram interColoExchangeMetadataTimePerDC =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ExchangeMetadataTime"));
    interColoExchangeMetadataTime.put(datacenter, interColoExchangeMetadataTimePerDC);
    Histogram plainTextInterColoExchangeMetadataTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-ExchangeMetadataTime"));
    plainTextInterColoExchangeMetadataTime.put(datacenter, plainTextInterColoExchangeMetadataTimePerDC);
    Histogram sslInterColoExchangeMetadataTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-ExchangeMetadataTime"));
    sslInterColoExchangeMetadataTime.put(datacenter, sslInterColoExchangeMetadataTimePerDC);
    Histogram interColoFixMissingKeysTimePerDC =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-FixMissingKeysTime"));
    interColoFixMissingKeysTime.put(datacenter, interColoFixMissingKeysTimePerDC);
    Histogram plainTextInterColoFixMissingKeysTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-FixMissingKeysTime"));
    plainTextInterColoFixMissingKeysTime.put(datacenter, plainTextInterColoFixMissingKeysTimePerDC);
    Histogram sslInterColoFixMissingKeysTimePerDC =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-FixMissingKeysTime"));
    sslInterColoFixMissingKeysTime.put(datacenter, sslInterColoFixMissingKeysTimePerDC);
    Histogram interColoReplicationMetadataRequestTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ReplicationMetadataRequestTime"));
    interColoReplicationMetadataRequestTime.put(datacenter, interColoReplicationMetadataRequestTimePerDC);
    Histogram plainTextInterColoReplicationMetadataRequestTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-ReplicationMetadataRequestTime"));
    plainTextInterColoReplicationMetadataRequestTime.put(datacenter,
        plainTextInterColoReplicationMetadataRequestTimePerDC);
    Histogram sslInterColoReplicationMetadataRequestTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-ReplicationMetadataRequestTime"));
    sslInterColoReplicationMetadataRequestTime.put(datacenter, sslInterColoReplicationMetadataRequestTimePerDC);
    Histogram interColoCheckMissingKeysTimePerDC =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-CheckMissingKeysTime"));
    interColoCheckMissingKeysTime.put(datacenter, interColoCheckMissingKeysTimePerDC);
    Histogram interColoProcessMetadataResponseTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ProcessMetadataResponseTime"));
    interColoProcessMetadataResponseTime.put(datacenter, interColoProcessMetadataResponseTimePerDC);
    Histogram interColoGetRequestTimePerDC =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-GetRequestTime"));
    interColoGetRequestTime.put(datacenter, interColoGetRequestTimePerDC);
    Histogram plainTextInterColoGetRequestTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-GetRequestTime"));
    plainTextInterColoGetRequestTime.put(datacenter, plainTextInterColoGetRequestTimePerDC);
    Histogram sslInterColoGetRequestTimePerDC =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-GetRequestTime"));
    sslInterColoGetRequestTime.put(datacenter, sslInterColoGetRequestTimePerDC);
    Histogram interColoBatchStoreWriteTimePerDC =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-BatchStoreWriteTime"));
    interColoBatchStoreWriteTime.put(datacenter, interColoBatchStoreWriteTimePerDC);
    Histogram plainTextInterColoBatchStoreWriteTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-BatchStoreWriteTime"));
    plainTextInterColoBatchStoreWriteTime.put(datacenter, plainTextInterColoBatchStoreWriteTimePerDC);
    Histogram sslInterColoBatchStoreWriteTimePerDC =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-BatchStoreWriteTime"));
    sslInterColoBatchStoreWriteTime.put(datacenter, sslInterColoBatchStoreWriteTimePerDC);
    Histogram interColoTotalReplicationTimePerDC =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-TotalReplicationTime"));
    interColoTotalReplicationTime.put(datacenter, interColoTotalReplicationTimePerDC);
    Histogram plainTextInterColoTotalReplicationTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "PlainTextInter-" + datacenter + "-TotalReplicationTime"));
    plainTextInterColoTotalReplicationTime.put(datacenter, plainTextInterColoTotalReplicationTimePerDC);
    Histogram sslInterColoTotalReplicationTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "SslInter-" + datacenter + "-TotalReplicationTime"));
    sslInterColoTotalReplicationTime.put(datacenter, sslInterColoTotalReplicationTimePerDC);
    Counter interColoReplicationGetRequestCountForStandbyReplicasPerDC = registry.counter(
        MetricRegistry.name(ReplicaThread.class,
            "Inter-" + datacenter + "-ReplicationGetRequestCountForStandbyReplicas"));
    interColoReplicationGetRequestCountForStandbyReplicas.put(datacenter,
        interColoReplicationGetRequestCountForStandbyReplicasPerDC);
    Meter interColoReplicationFetchBytesRateForStandbyReplicasPerDC = registry.meter(
        MetricRegistry.name(ReplicaThread.class,
            "Inter-" + datacenter + "-ReplicationFetchBytesRateForStandbyReplicas"));
    interColoReplicationFetchBytesRateForStandbyReplicas.put(datacenter,
        interColoReplicationFetchBytesRateForStandbyReplicasPerDC);
  }

  /**
   * Register metrics for measuring the number of active replica threads.
   *
   * @param replicaThreads A list of {@link ReplicaThread}s handling replication.
   * @param datacenter The datacenter of the {@link ReplicaThread} is running
   */
  void trackLiveThreadsCount(final List<ReplicaThread> replicaThreads, String datacenter) {
    Gauge<Integer> liveThreadsPerDatacenter = () -> getLiveThreads(replicaThreads);

    registry.register(MetricRegistry.name(ReplicaThread.class, "NumberOfReplicaThreadsIn" + datacenter),
        liveThreadsPerDatacenter);
  }

  private int getLiveThreads(List<ReplicaThread> replicaThreads) {
    int count = 0;
    for (ReplicaThread thread : replicaThreads) {
      if (thread.isThreadUp()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Tracks the number of partitions for which replication is disabled.
   * @param replicaThreadPools A map of datacenter names to {@link ReplicaThread}s handling replication from that
   *                           datacenter
   */
  public void trackReplicationDisabledPartitions(final Map<String, List<ReplicaThread>> replicaThreadPools) {
    for (Map.Entry<String, List<ReplicaThread>> entry : replicaThreadPools.entrySet()) {
      String datacenter = entry.getKey();
      List<ReplicaThread> pool = entry.getValue();
      Gauge<Integer> disabledCount = () -> {
        Set<PartitionId> replicationDisabledPartitions = new HashSet<>();
        for (ReplicaThread replicaThread : pool) {
          replicationDisabledPartitions.addAll(replicaThread.getReplicationDisabledPartitions());
        }
        return replicationDisabledPartitions.size();
      };
      registry.register(MetricRegistry.name(ReplicaThread.class, "ReplicationDisabledPartitions-" + datacenter),
          disabledCount);
    }
  }

  private void populateInvalidMessageMetricForReplicas(List<? extends ReplicaId> replicaIds) {
    for (ReplicaId replicaId : replicaIds) {
      PartitionId partitionId = replicaId.getPartitionId();
      if (!partitionIdToInvalidMessageStreamErrorCounter.containsKey(partitionId)) {
        Counter partitionBasedCorruptionErrorCount =
            registry.counter(MetricRegistry.name(ReplicaThread.class, partitionId + "-CorruptionErrorCount"));
        partitionIdToInvalidMessageStreamErrorCounter.put(partitionId, partitionBasedCorruptionErrorCount);
      }
    }
  }

  public void incrementInvalidMessageError(PartitionId partitionId) {
    replicationInvalidMessageStreamErrorCount.inc();
    if (partitionIdToInvalidMessageStreamErrorCounter.containsKey(partitionId)) {
      partitionIdToInvalidMessageStreamErrorCounter.get(partitionId).inc();
    }
  }

  /**
   * Add replication lag metric(local from remote) for given partitionId.
   * @param partitionId partition to add metric for.
   */
  public void addLagMetricForPartition(PartitionId partitionId) {
    if (!partitionLags.containsKey(partitionId)) {
      partitionLags.put(partitionId, new HashMap<>());
      // Set up metrics if and only if no mapping for this partition before.
      Gauge<Long> replicaLag = () -> getMaxLagForPartition(partitionId);
      registry.register(MetricRegistry.name(ReplicaThread.class,
          String.format(MAX_LAG_FROM_PEERS_IN_BYTE_METRIC_NAME_TEMPLATE, partitionId.toPathString())), replicaLag);
    }
  }

  /**
   * Add catchup point metric(local from cloud) for given partitionId.
   * @param partitionId partition to add metric for.
   */
  public void addCatchUpPointMetricForPartition(PartitionId partitionId) {
    if (!cloudReplicaCatchUpPoint.containsKey(partitionId)) {
      cloudReplicaCatchUpPoint.put(partitionId, 0L);
      // Set up metrics if and only if no mapping for this partition before.
      Gauge<Long> catchUpPoint = () -> cloudReplicaCatchUpPoint.get(partitionId);
      registry.register(MetricRegistry.name(ReplicaThread.class,
          String.format(CATCH_POINT_FROM_CLOUD_METRIC_NAME_TEMPLATE, partitionId.toPathString())), catchUpPoint);
    }
  }

  /**
   * Remove replication lag metric of given partition if it's present.
   * @param partitionId the given partition whose lag metric should be removed.
   */
  public void removeLagMetricForPartition(PartitionId partitionId) {
    if (partitionLags.containsKey(partitionId)) {
      registry.remove(MetricRegistry.name(ReplicaThread.class,
          String.format(MAX_LAG_FROM_PEERS_IN_BYTE_METRIC_NAME_TEMPLATE, partitionId.toPathString())));
    }
  }

  /**
   * Remove catch up point metric of given partition if it's present.
   * @param partitionId the given partition whose catch up point metric should be removed.
   */
  public void removeCatchupPointMetricForPartition(PartitionId partitionId) {
    if (cloudReplicaCatchUpPoint.containsKey(partitionId)) {
      registry.remove(MetricRegistry.name(ReplicaThread.class,
          String.format(CATCH_POINT_FROM_CLOUD_METRIC_NAME_TEMPLATE, partitionId.toPathString())));
    }
  }

  /**
   * Add metrics for given remote replica.
   * @param remoteReplicaInfo the {@link RemoteReplicaInfo} that contains all infos associated with remote replica
   * @param trackPerDatacenterLag whether to track remote replicas' lag from local by each datacenter. Specifically, it
   *                              tracks min/max/avg remote replica lag from each datacenter.
   */
  public void addMetricsForRemoteReplicaInfo(RemoteReplicaInfo remoteReplicaInfo, boolean trackPerDatacenterLag) {
    String metricNamePrefix = generateRemoteReplicaMetricPrefix(remoteReplicaInfo);

    String metadataRequestErrorMetricName = metricNamePrefix + "-metadataRequestError";
    if (metadataRequestErrorMap.containsKey(metadataRequestErrorMetricName)) {
      // Metrics already exist. For VCR: Partition add/remove go back and forth.
      return;
    }
    Counter metadataRequestError =
        registry.counter(MetricRegistry.name(ReplicaThread.class, metadataRequestErrorMetricName));
    metadataRequestErrorMap.put(metadataRequestErrorMetricName, metadataRequestError);

    String getRequestErrorMetricName = metricNamePrefix + "-getRequestError";
    Counter getRequestError = registry.counter(MetricRegistry.name(ReplicaThread.class, getRequestErrorMetricName));
    getRequestErrorMap.put(getRequestErrorMetricName, getRequestError);

    String localStoreErrorMetricName = metricNamePrefix + "-localStoreError";
    Counter localStoreError = registry.counter(MetricRegistry.name(ReplicaThread.class, localStoreErrorMetricName));
    localStoreErrorMap.put(localStoreErrorMetricName, localStoreError);
    Gauge<Long> replicaLag = remoteReplicaInfo::getRemoteLagFromLocalInBytes;
    registry.register(MetricRegistry.name(ReplicaThread.class, metricNamePrefix + "-remoteLagInBytes"), replicaLag);
    if (trackPerDatacenterLag) {
      String remoteReplicaDc = remoteReplicaInfo.getReplicaId().getDataNodeId().getDatacenterName();
      remoteReplicaInfosByDc.computeIfAbsent(remoteReplicaDc, k -> {
        Gauge<Double> avgReplicaLag = () -> getAvgLagFromDc(remoteReplicaDc);
        registry.register(MetricRegistry.name(ReplicaThread.class, remoteReplicaDc + "-avgReplicaLagFromLocalInBytes"),
            avgReplicaLag);
        Gauge<Long> maxReplicaLag = () -> {
          LongSummaryStatistics statistics =
              dcToReplicaLagStats.getOrDefault(remoteReplicaDc, new LongSummaryStatistics());
          return statistics.getMax() == Long.MIN_VALUE ? -1 : statistics.getMax();
        };
        registry.register(MetricRegistry.name(ReplicaThread.class, remoteReplicaDc + "-maxReplicaLagFromLocalInBytes"),
            maxReplicaLag);
        Gauge<Long> minReplicaLag = () -> {
          LongSummaryStatistics statistics =
              dcToReplicaLagStats.getOrDefault(remoteReplicaDc, new LongSummaryStatistics());
          return statistics.getMin() == Long.MAX_VALUE ? -1 : statistics.getMin();
        };
        registry.register(MetricRegistry.name(ReplicaThread.class, remoteReplicaDc + "-minReplicaLagFromLocalInBytes"),
            minReplicaLag);
        return ConcurrentHashMap.newKeySet();
      }).add(remoteReplicaInfo);
    }
  }

  public void removeMetricsForRemoteReplicaInfo(RemoteReplicaInfo remoteReplicaInfo) {
    String metricNamePrefix = generateRemoteReplicaMetricPrefix(remoteReplicaInfo);
    String metadataRequestErrorMetricName = metricNamePrefix + "-metadataRequestError";
    if (metadataRequestErrorMap.remove(metadataRequestErrorMetricName) == null) {
      // if there is no metric associated with given remote replica info, this means it has already been removed
      return;
    }
    registry.remove(MetricRegistry.name(ReplicaThread.class, metadataRequestErrorMetricName));
    String getRequestErrorMetricName = metricNamePrefix + "-getRequestError";
    getRequestErrorMap.remove(getRequestErrorMetricName);
    registry.remove(MetricRegistry.name(ReplicaThread.class, getRequestErrorMetricName));
    String localStoreErrorMetricName = metricNamePrefix + "-localStoreError";
    localStoreErrorMap.remove(localStoreErrorMetricName);
    registry.remove(MetricRegistry.name(ReplicaThread.class, localStoreErrorMetricName));
    registry.remove(MetricRegistry.name(ReplicaThread.class, metricNamePrefix + "-remoteLagInBytes"));
  }

  public void updateMetadataRequestError(ReplicaId remoteReplica) {
    String metadataRequestErrorMetricName =
        remoteReplica.getDataNodeId().getHostname() + "-" + remoteReplica.getDataNodeId().getPort() + "-"
            + remoteReplica.getPartitionId().toString() + "-metadataRequestError";
    metadataRequestErrorMap.get(metadataRequestErrorMetricName).inc();
  }

  public void updateGetRequestError(ReplicaId remoteReplica) {
    String getRequestErrorMetricName =
        remoteReplica.getDataNodeId().getHostname() + "-" + remoteReplica.getDataNodeId().getPort() + "-"
            + remoteReplica.getPartitionId().toString() + "-getRequestError";
    getRequestErrorMap.get(getRequestErrorMetricName).inc();
  }

  public void updateLocalStoreError(ReplicaId remoteReplica) {
    String localStoreErrorMetricName =
        remoteReplica.getDataNodeId().getHostname() + "-" + remoteReplica.getDataNodeId().getPort() + "-"
            + remoteReplica.getPartitionId().toString() + "-localStoreError";
    localStoreErrorMap.get(localStoreErrorMetricName).inc();
  }

  public void incrementReplicationErrors(boolean sslEnabled) {
    replicationErrors.inc();
    if (sslEnabled) {
      sslReplicationErrors.inc();
    } else {
      plainTextReplicationErrors.inc();
    }
  }

  public void updateTotalReplicationTime(long totalReplicationTime, boolean remoteColo, boolean sslEnabled,
      String datacenter) {
    if (remoteColo) {
      interColoTotalReplicationTime.get(datacenter).update(totalReplicationTime);
      if (sslEnabled) {
        sslInterColoTotalReplicationTime.get(datacenter).update(totalReplicationTime);
      } else {
        plainTextInterColoTotalReplicationTime.get(datacenter).update(totalReplicationTime);
      }
    } else {
      intraColoTotalReplicationTime.update(totalReplicationTime);
      if (sslEnabled) {
        sslIntraColoTotalReplicationTime.update(totalReplicationTime);
      } else {
        plainTextIntraColoTotalReplicationTime.update(totalReplicationTime);
      }
    }
  }

  public void updateExchangeMetadataTime(long exchangeMetadataTime, boolean remoteColo, boolean sslEnabled,
      String datacenter) {
    if (remoteColo) {
      interColoMetadataExchangeCount.get(datacenter).inc();
      interColoExchangeMetadataTime.get(datacenter).update(exchangeMetadataTime);
      if (sslEnabled) {
        sslInterColoMetadataExchangeCount.get(datacenter).inc();
        sslInterColoExchangeMetadataTime.get(datacenter).update(exchangeMetadataTime);
      } else {
        plainTextInterColoMetadataExchangeCount.get(datacenter).inc();
        plainTextInterColoExchangeMetadataTime.get(datacenter).update(exchangeMetadataTime);
      }
    } else {
      intraColoMetadataExchangeCount.inc();
      intraColoExchangeMetadataTime.update(exchangeMetadataTime);
      if (sslEnabled) {
        sslIntraColoMetadataExchangeCount.inc();
        sslIntraColoExchangeMetadataTime.update(exchangeMetadataTime);
      } else {
        plainTextIntraColoMetadataExchangeCount.inc();
        plainTextIntraColoExchangeMetadataTime.update(exchangeMetadataTime);
      }
    }
  }

  public void updateCheckMissingKeysTime(long checkMissingKeyTime, boolean remoteColo, String datacenterName) {
    if (remoteColo) {
      interColoCheckMissingKeysTime.get(datacenterName).update(checkMissingKeyTime);
    } else {
      intraColoCheckMissingKeysTime.update(checkMissingKeyTime);
    }
  }

  public void updateFixMissingStoreKeysTime(long fixMissingStoreKeysTime, boolean remoteColo, boolean sslEnabled,
      String datacenter) {
    if (remoteColo) {
      interColoFixMissingKeysTime.get(datacenter).update(fixMissingStoreKeysTime);
      if (sslEnabled) {
        sslInterColoFixMissingKeysTime.get(datacenter).update(fixMissingStoreKeysTime);
      } else {
        plainTextInterColoFixMissingKeysTime.get(datacenter).update(fixMissingStoreKeysTime);
      }
    } else {
      intraColoFixMissingKeysTime.update(fixMissingStoreKeysTime);
      if (sslEnabled) {
        sslIntraColoFixMissingKeysTime.update(fixMissingStoreKeysTime);
      } else {
        plainTextIntraColoFixMissingKeysTime.update(fixMissingStoreKeysTime);
      }
    }
  }

  public void updateMetadataRequestTime(long metadataRequestTime, boolean remoteColo, boolean sslEnabled,
      String datacenter) {
    if (remoteColo) {
      interColoReplicationMetadataRequestTime.get(datacenter).update(metadataRequestTime);
      if (sslEnabled) {
        sslInterColoReplicationMetadataRequestTime.get(datacenter).update(metadataRequestTime);
      } else {
        plainTextInterColoReplicationMetadataRequestTime.get(datacenter).update(metadataRequestTime);
      }
    } else {
      intraColoReplicationMetadataRequestTime.update(metadataRequestTime);
      if (sslEnabled) {
        sslIntraColoReplicationMetadataRequestTime.update(metadataRequestTime);
      } else {
        plainTextIntraColoReplicationMetadataRequestTime.update(metadataRequestTime);
      }
    }
  }

  public void updateGetRequestTime(long getRequestTime, boolean remoteColo, boolean sslEnabled, String datacenter,
      boolean remoteColoGetRequestForStandby) {
    if (remoteColo) {
      interColoReplicationGetRequestCount.get(datacenter).inc();
      interColoGetRequestTime.get(datacenter).update(getRequestTime);
      if (sslEnabled) {
        sslInterColoGetRequestTime.get(datacenter).update(getRequestTime);
      } else {
        plainTextInterColoGetRequestTime.get(datacenter).update(getRequestTime);
      }
      if (remoteColoGetRequestForStandby) {
        interColoReplicationGetRequestCountForStandbyReplicas.get(datacenter).inc();
      }
    } else {
      intraColoReplicationGetRequestCount.inc();
      intraColoGetRequestTime.update(getRequestTime);
      if (sslEnabled) {
        sslIntraColoGetRequestTime.update(getRequestTime);
      } else {
        plainTextIntraColoGetRequestTime.update(getRequestTime);
      }
    }
  }

  public void updateBatchStoreWriteTime(long batchStoreWriteTime, long totalBytesFixed, long totalBlobsFixed,
      boolean remoteColo, boolean sslEnabled, String datacenter, boolean remoteColoRequestForStandby) {
    if (remoteColo) {
      interColoReplicationBytesRate.get(datacenter).mark(totalBytesFixed);
      interColoBlobsReplicatedCount.get(datacenter).inc(totalBlobsFixed);
      interColoBatchStoreWriteTime.get(datacenter).update(batchStoreWriteTime);
      if (sslEnabled) {
        sslInterColoReplicationBytesRate.get(datacenter).mark(totalBytesFixed);
        sslInterColoBlobsReplicatedCount.get(datacenter).inc(totalBlobsFixed);
        sslInterColoBatchStoreWriteTime.get(datacenter).update(batchStoreWriteTime);
      } else {
        plainTextInterColoReplicationBytesRate.get(datacenter).mark(totalBytesFixed);
        plainTextInterColoBlobsReplicatedCount.get(datacenter).inc(totalBlobsFixed);
        plainTextInterColoBatchStoreWriteTime.get(datacenter).update(batchStoreWriteTime);
      }

      if (remoteColoRequestForStandby) {
        interColoReplicationFetchBytesRateForStandbyReplicas.get(datacenter).mark(totalBytesFixed);
      }
    } else {
      intraColoReplicationBytesRate.mark(totalBytesFixed);
      intraColoBlobsReplicatedCount.inc(totalBlobsFixed);
      intraColoBatchStoreWriteTime.update(batchStoreWriteTime);
      if (sslEnabled) {
        sslIntraColoReplicationBytesRate.mark(totalBytesFixed);
        sslIntraColoBlobsReplicatedCount.inc(totalBlobsFixed);
        sslIntraColoBatchStoreWriteTime.update(batchStoreWriteTime);
      } else {
        plainTextIntraColoReplicationBytesRate.mark(totalBytesFixed);
        plainTextIntraColoBlobsReplicatedCount.inc(totalBlobsFixed);
        plainTextIntraColoBatchStoreWriteTime.update(batchStoreWriteTime);
      }
    }
  }

  /**
   * Update the lag between local and {@link RemoteReplicaInfo}. The lag indicates how far local replica is behind remote
   * peer replica.
   * @param remoteReplicaInfo the remote replica
   * @param lag the new lag
   */
  public void updateLagMetricForRemoteReplica(RemoteReplicaInfo remoteReplicaInfo, long lag) {
    ReplicaId replicaId = remoteReplicaInfo.getReplicaId();
    // update the partition's lag if and only if it was tracked.
    partitionLags.computeIfPresent(replicaId.getPartitionId(), (k, v) -> {
      v.put(replicaId.getDataNodeId(), lag);
      return v;
    });
  }

  /**
   * Update catch up point of local replica from the cloud replica.
   * @param remoteReplicaInfo {@link RemoteReplicaInfo} of the cloud replica.
   * @param catchUpPoint timestamp upto which local replica has caught with the cloud replica.
   */
  public void updateCatchupPointMetricForCloudReplica(RemoteReplicaInfo remoteReplicaInfo, long catchUpPoint) {
    // update this metric only for cloud peer replica. There will only be one cloud replica peer per partition.
    if (remoteReplicaInfo.getReplicaId().getReplicaType() == ReplicaType.CLOUD_BACKED
        && cloudReplicaCatchUpPoint.containsKey(remoteReplicaInfo.getLocalReplicaId().getPartitionId())) {
      // update the partition's catch up point if and only if it was tracked.
      cloudReplicaCatchUpPoint.put(remoteReplicaInfo.getLocalReplicaId().getPartitionId(), catchUpPoint);
    }
  }

  /**
   * Get a partition's maximum lag between local and its {@link RemoteReplicaInfo}s.
   * @param partitionId the partition to check
   */
  public long getMaxLagForPartition(PartitionId partitionId) {
    Map<DataNodeId, Long> perDataNodeLag = partitionLags.get(partitionId);
    if (perDataNodeLag == null || perDataNodeLag.isEmpty()) {
      return -1;
    }
    Map.Entry<DataNodeId, Long> maxEntry = perDataNodeLag.entrySet().stream().max(Map.Entry.comparingByValue()).get();
    return maxEntry.getValue();
  }

  /**
   * Get tha average replication lag of remote replicas in given datacenter
   * @param dcName the name of dc where remote replicas sit
   * @return the average replication lag
   */
  double getAvgLagFromDc(String dcName) {
    Set<RemoteReplicaInfo> replicaInfos = remoteReplicaInfosByDc.get(dcName);
    if (replicaInfos == null || replicaInfos.isEmpty()) {
      return -1.0;
    }
    LongSummaryStatistics longSummaryStatistics =
        replicaInfos.stream().collect(Collectors.summarizingLong(RemoteReplicaInfo::getRemoteLagFromLocalInBytes));
    dcToReplicaLagStats.put(dcName, longSummaryStatistics);
    return longSummaryStatistics.getAverage();
  }

  private String generateRemoteReplicaMetricPrefix(RemoteReplicaInfo remoteReplicaInfo) {
    ReplicaId replicaId = remoteReplicaInfo.getReplicaId();
    DataNodeId dataNodeId = replicaId.getDataNodeId();
    return dataNodeId.getHostname() + "-" + dataNodeId.getPort() + "-" + replicaId.getPartitionId().toString();
  }
}
