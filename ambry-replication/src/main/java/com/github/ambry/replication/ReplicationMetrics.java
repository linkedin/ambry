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

  private final static String REPLICATION_FETCH_BYTES_RATE_SUFFIX = "-replicationFetchBytesRate";

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
  public final Counter replicationNumBlobsSkippedAfterRetry;
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
  public final Map<String, Histogram> interColoTotalReplicationTime = new HashMap<>();
  public final Map<String, Histogram> interColoOneCycleReplicationTime = new HashMap<>();
  public final Map<String, Histogram> interColoReplicaBatchTotalIdleTimePercentage = new HashMap<>();
  public final Map<String, Histogram> interColoReplicaBatchTotalIdleTimeMs = new HashMap<>();
  public final Histogram intraColoTotalReplicationTime;
  public final Histogram intraColoOneCycleReplicationTime;
  public final Histogram intraColoReplicaBatchTotalIdleTimePercentage;
  public final Histogram intraColoReplicaBatchTotalIdleTimeMs;
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
  public final Counter intraColoContinuousReplicationReplicaThrottleCount;
  public final Counter interColoContinuousReplicationReplicaThrottleCount;
  public final Counter remoteReplicaInfoRemoveError;
  public final Counter remoteReplicaInfoAddError;
  public final Counter unexpectedBlobIdError;
  public final Counter allResponsedKeysExist;

  private MetricRegistry registry;
  private final Map<String, Counter> metadataRequestErrorMap = new ConcurrentHashMap<>();
  private final Map<String, Counter> getRequestErrorMap = new HashMap<>();
  private final Map<String, Counter> localStoreErrorMap = new HashMap<>();
  private final Map<String, Meter> replicaToReplicationFetchBytesRateMap = new HashMap<>();
  private final Map<PartitionId, Counter> partitionIdToInvalidMessageStreamErrorCounter = new HashMap<>();
  // ConcurrentHashMap is used to avoid cache incoherence.
  private final Map<PartitionId, Map<DataNodeId, Long>> partitionLags = new ConcurrentHashMap<>();
  private final Map<String, Set<RemoteReplicaInfo>> remoteReplicaInfosByDc = new ConcurrentHashMap<>();
  private final Map<String, LongSummaryStatistics> dcToReplicaLagStats = new ConcurrentHashMap<>();

  // A map from dcname to a histogram. This histogram records the replication lag in seconds for PUT records.
  private final Map<String, Histogram> dcToReplicaLagInSecondsForBlob = new ConcurrentHashMap<>();
  // A map from dcname to a counter to record the replication error.
  private final Map<String, Counter> dcToReplicationError = new ConcurrentHashMap<>();
  private final Counter intraColoReplicationErrorCount;
  private final Map<String, Counter> dcToTimeoutRequestError = new ConcurrentHashMap<>();
  private final Counter intraColoTimeoutRequestErrorCount;
  private final Map<String, Counter> dcToRequestNetworkError = new ConcurrentHashMap<>();
  private final Counter intraColoRequestNetworkErrorCount;
  private final Map<String, Counter> dcToRetryAfterBackoffError = new ConcurrentHashMap<>();
  private final Counter intraColoRetryAfterBackoffErrorCount;
  private final Map<String, Counter> dcToResponseError = new ConcurrentHashMap<>();
  private final Counter intraColoResponseErrorCount;
  public final Map<String, Counter> replicaThreadsAssignedRemoteReplicaInfo = new ConcurrentHashMap<>();
  public final Map<String, Counter> replicaThreadsCycleIterations = new ConcurrentHashMap<>();
  public final Map<String, Histogram> replicaThreadsOneCycleReplicationTime = new ConcurrentHashMap<>();


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

  public final Counter backupIntegrityError;

  public ReplicationMetrics(MetricRegistry registry, List<? extends ReplicaId> replicaIds) {
    backupIntegrityError =
        registry.counter(MetricRegistry.name(ReplicationMetrics.class, "BackupIntegrityError"));
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
    replicationNumBlobsSkippedAfterRetry =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationNumBlobsSkippedAfterRetry"));
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
    intraColoOneCycleReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoOneCycleReplicationTime"));
    intraColoReplicaBatchTotalIdleTimePercentage =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicaBatchTotalIdleTimePercentage"));
    intraColoReplicaBatchTotalIdleTimeMs =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicaBatchTotalIdleTimeMs"));
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
    intraColoContinuousReplicationReplicaThrottleCount = registry.counter(
        MetricRegistry.name(ReplicaThread.class, "IntraColoContinuousReplicationReplicaThrottleCount"));
    interColoContinuousReplicationReplicaThrottleCount = registry.counter(
        MetricRegistry.name(ReplicaThread.class, "InterColoContinuousReplicationReplicaThrottleCount"));
    remoteReplicaInfoRemoveError =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaInfoRemoveError"));
    remoteReplicaInfoAddError = registry.counter(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaInfoAddError"));
    unexpectedBlobIdError = registry.counter(MetricRegistry.name(ReplicaThread.class, "UnexpectedBlobIdError"));
    allResponsedKeysExist = registry.counter(MetricRegistry.name(ReplicaThread.class, "AllResponsedKeysExist"));
    intraColoReplicationErrorCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationErrorCount"));
    intraColoTimeoutRequestErrorCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoTimeoutRequestErrorCount"));
    intraColoRequestNetworkErrorCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoRequestNetworkErrorCount"));
    intraColoRetryAfterBackoffErrorCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoRetryAfterBackoffErrorCount"));
    intraColoResponseErrorCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoResponseErrorCount"));
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
    Histogram interColoOneCycleReplicationTimePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-OneCycleReplicationTime"));
    interColoOneCycleReplicationTime.put(datacenter, interColoOneCycleReplicationTimePerDC);
    Histogram interColoReplicaBatchTotalIdleTimePercentagePerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ReplicaBatchTotalIdleTimePercentage"));
    interColoReplicaBatchTotalIdleTimePercentage.put(datacenter, interColoReplicaBatchTotalIdleTimePercentagePerDC);
    Histogram interColoReplicaBatchTotalIdleTimeMsPerDC = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ReplicaBatchTotalIdleTimeMs"));
    interColoReplicaBatchTotalIdleTimeMs.put(datacenter, interColoReplicaBatchTotalIdleTimeMsPerDC);
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
    Histogram replicationLagInSecondsForBlob = registry.histogram(
        MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ReplicationLagInSecondsForBlob"));
    dcToReplicaLagInSecondsForBlob.put(datacenter, replicationLagInSecondsForBlob);
    Counter replicationError =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ReplicationErrorCount"));
    dcToReplicationError.put(datacenter, replicationError);
    Counter timeoutRequestError =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-TimeoutRequestErrorCount"));
    dcToTimeoutRequestError.put(datacenter, timeoutRequestError);
    Counter requestNetworkError =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-RequestNetworkErrorCount"));
    dcToRequestNetworkError.put(datacenter, requestNetworkError);
    Counter retryAfterBackoffError = registry.counter(
        MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-RetryAfterBackoffErrorCount"));
    dcToRetryAfterBackoffError.put(datacenter, retryAfterBackoffError);
    Counter responseError =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "Inter-" + datacenter + "-ResponseErrorCount"));
    dcToResponseError.put(datacenter, responseError);
  }

  /**
   * Populates metrics at replicaThread level
   * @param replicaThread name of replica thread
   */

  public void populateReplicaThreadMetrics(String replicaThread) {
    Counter replicaThreadAssignedRemoteReplicaInfo = registry.counter(MetricRegistry.name(ReplicaThread.class,
        replicaThread + "-AssignedRemoteReplicaInfo"));
    replicaThreadsAssignedRemoteReplicaInfo.put(replicaThread, replicaThreadAssignedRemoteReplicaInfo);
    Counter replicaThreadsCycleIteration = registry.counter(MetricRegistry.name(ReplicaThread.class,
        replicaThread + "-CycleIterations"));
    replicaThreadsCycleIterations.put(replicaThread, replicaThreadsCycleIteration);
    Histogram replicaThreadOneCycleReplicationTime = registry.histogram(MetricRegistry.name(ReplicaThread.class,
        replicaThread + "-OneCycleReplicationTimeMS"));
    replicaThreadsOneCycleReplicationTime.put(replicaThread, replicaThreadOneCycleReplicationTime);
  }

  /**
   * Increase the number of replication cycle
   * */
  public void updateReplicaThreadCycleIteration(String replicaThread) {
    replicaThreadsCycleIterations.computeIfAbsent(replicaThread, thread -> registry.counter(MetricRegistry.name(ReplicaThread.class,
        replicaThread + "-CycleIterations"))).inc();
  }

  /**
   * Increase the number of assigned remote replica
   * */
  public void increaseReplicaThreadAssignedRemoteReplicaInfo(String replicaThread) {
    replicaThreadsAssignedRemoteReplicaInfo.computeIfAbsent(replicaThread, thread -> registry.counter(MetricRegistry.name(ReplicaThread.class,
        replicaThread + "-AssignedRemoteReplicaInfo"))).inc();
  }

  /**
   * Update cycle replication time
   * */
  public void updateReplicaThreadOneCycleReplicationTime(String replicaThread, long time) {
    replicaThreadsOneCycleReplicationTime.computeIfAbsent(replicaThread, thread -> registry.histogram(MetricRegistry.name(ReplicaThread.class,
        replicaThread + "-OneCycleReplicationTimeMS"))).update(time);
  }

  /**
   * Decrease the number of assigned remote replica
   * */
  public void decreaseReplicaThreadAssignedRemoteReplicaInfo(String replicaThread) {
    replicaThreadsAssignedRemoteReplicaInfo.computeIfAbsent(replicaThread, thread -> registry.counter(MetricRegistry.name(ReplicaThread.class,
        replicaThread + "-AssignedRemoteReplicaInfo"))).dec();
  }

  /**
   * Register metrics for measuring the number of active replica threads.
   *
   * @param replicaThreads A list of {@link ReplicaThread}s handling replication.
   * @param datacenter The datacenter of the {@link ReplicaThread} is running
   */
  void trackLiveThreadsCount(final List<ReplicaThread> replicaThreads, String datacenter) {
    Gauge<Integer> liveThreadsPerDatacenter = () -> getLiveThreads(replicaThreads);

    registry.gauge(MetricRegistry.name(ReplicaThread.class, "NumberOfReplicaThreadsIn" + datacenter),
        () -> liveThreadsPerDatacenter);
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
      registry.gauge(MetricRegistry.name(ReplicaThread.class, "ReplicationDisabledPartitions-" + datacenter),
          () -> disabledCount);
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
   * @param enableEmmitMetricForReplicaLag If true, replication will register metric for each partition to track lag.
   */
  public void addLagMetricForPartition(PartitionId partitionId, boolean enableEmmitMetricForReplicaLag) {
    if (!partitionLags.containsKey(partitionId)) {
      // Use concurrent hash map for the inner value map as well to avoid concurrent modification exception.
      partitionLags.put(partitionId, new ConcurrentHashMap<>());
      // Set up metrics if and only if no mapping for this partition before.
      if (enableEmmitMetricForReplicaLag) {
        Gauge<Long> replicaLag = () -> getMaxLagForPartition(partitionId);
        registry.gauge(MetricRegistry.name(ReplicaThread.class,
                String.format(MAX_LAG_FROM_PEERS_IN_BYTE_METRIC_NAME_TEMPLATE, partitionId.toPathString())),
            () -> replicaLag);
      }
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
   * Add metrics for given remote replica.
   * @param remoteReplicaInfo the {@link RemoteReplicaInfo} that contains all infos associated with remote replica
   * @param trackPerDatacenterLag whether to track remote replicas' lag from local by each datacenter. Specifically, it
   *                              tracks min/max/avg remote replica lag from each datacenter.
   * @param trackPerReplicaReplicationBytes whether to track replicated bytes per second from each remote replica,
   *                                        replica thread is replicating from.
   */
  public void addMetricsForRemoteReplicaInfo(RemoteReplicaInfo remoteReplicaInfo, boolean trackPerDatacenterLag,
      boolean trackPerReplicaReplicationBytes) {
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

    if (trackPerReplicaReplicationBytes) {
      String replicaToReplicationFetchBytesRateMetricName = metricNamePrefix + REPLICATION_FETCH_BYTES_RATE_SUFFIX;
      Meter replicaToReplicationFetchBytesRate =
          registry.meter(MetricRegistry.name(ReplicaThread.class, replicaToReplicationFetchBytesRateMetricName));
      replicaToReplicationFetchBytesRateMap.put(replicaToReplicationFetchBytesRateMetricName,
          replicaToReplicationFetchBytesRate);
    }

    Gauge<Long> replicaLag = remoteReplicaInfo::getRemoteLagFromLocalInBytes;
    registry.gauge(MetricRegistry.name(ReplicaThread.class, metricNamePrefix + "-remoteLagInBytes"), () -> replicaLag);
    if (trackPerDatacenterLag) {
      String remoteReplicaDc = remoteReplicaInfo.getReplicaId().getDataNodeId().getDatacenterName();
      remoteReplicaInfosByDc.computeIfAbsent(remoteReplicaDc, k -> {
        Gauge<Double> avgReplicaLag = () -> getAvgLagFromDc(remoteReplicaDc);
        registry.gauge(MetricRegistry.name(ReplicaThread.class, remoteReplicaDc + "-avgReplicaLagFromLocalInBytes"),
            () -> avgReplicaLag);
        Gauge<Long> maxReplicaLag = () -> {
          LongSummaryStatistics statistics =
              dcToReplicaLagStats.getOrDefault(remoteReplicaDc, new LongSummaryStatistics());
          return statistics.getMax() == Long.MIN_VALUE ? -1 : statistics.getMax();
        };
        registry.gauge(MetricRegistry.name(ReplicaThread.class, remoteReplicaDc + "-maxReplicaLagFromLocalInBytes"),
            () -> maxReplicaLag);
        Gauge<Long> minReplicaLag = () -> {
          LongSummaryStatistics statistics =
              dcToReplicaLagStats.getOrDefault(remoteReplicaDc, new LongSummaryStatistics());
          return statistics.getMin() == Long.MAX_VALUE ? -1 : statistics.getMin();
        };
        registry.gauge(MetricRegistry.name(ReplicaThread.class, remoteReplicaDc + "-minReplicaLagFromLocalInBytes"),
            () -> minReplicaLag);
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
    String replicaToReplicationFetchBytesRateMetricName = metricNamePrefix + REPLICATION_FETCH_BYTES_RATE_SUFFIX;
    replicaToReplicationFetchBytesRateMap.remove(replicaToReplicationFetchBytesRateMetricName);
    registry.remove(MetricRegistry.name(ReplicaThread.class, replicaToReplicationFetchBytesRateMetricName));
    registry.remove(MetricRegistry.name(ReplicaThread.class, localStoreErrorMetricName));
    registry.remove(MetricRegistry.name(ReplicaThread.class, metricNamePrefix + "-remoteLagInBytes"));
  }

  public void updateMetadataRequestError(ReplicaId remoteReplica) {
    String metadataRequestErrorMetricName =
        remoteReplica.getDataNodeId().getHostname() + "-" + remoteReplica.getDataNodeId().getPort() + "-"
            + remoteReplica.getPartitionId().toString() + "-metadataRequestError";
    if (metadataRequestErrorMap.containsKey(metadataRequestErrorMetricName)) {
      metadataRequestErrorMap.get(metadataRequestErrorMetricName).inc();
    }
  }

  public void updateGetRequestError(ReplicaId remoteReplica) {
    String getRequestErrorMetricName =
        remoteReplica.getDataNodeId().getHostname() + "-" + remoteReplica.getDataNodeId().getPort() + "-"
            + remoteReplica.getPartitionId().toString() + "-getRequestError";
    if (getRequestErrorMap.containsKey(getRequestErrorMetricName)) {
      getRequestErrorMap.get(getRequestErrorMetricName).inc();
    }
  }

  public void updateReplicationFetchBytes(RemoteReplicaInfo remoteReplica, long totalBytesFixed) {
    String replicaFetchBytes = generateRemoteReplicaMetricPrefix(remoteReplica) + REPLICATION_FETCH_BYTES_RATE_SUFFIX;
    if (replicaToReplicationFetchBytesRateMap.containsKey(replicaFetchBytes)) {
      replicaToReplicationFetchBytesRateMap.get(replicaFetchBytes).mark(totalBytesFixed);
    }
  }

  public void updateLocalStoreError(ReplicaId remoteReplica) {
    String localStoreErrorMetricName =
        remoteReplica.getDataNodeId().getHostname() + "-" + remoteReplica.getDataNodeId().getPort() + "-"
            + remoteReplica.getPartitionId().toString() + "-localStoreError";
    if (localStoreErrorMap.containsKey(localStoreErrorMetricName)) {
      localStoreErrorMap.get(localStoreErrorMetricName).inc();
    }
  }

  /**
   * This is not used anywhere.
   * @param sslEnabled
   */
  public void incrementReplicationErrors(boolean sslEnabled) {
    replicationErrors.inc();
    if (sslEnabled) {
      sslReplicationErrors.inc();
    } else {
      plainTextReplicationErrors.inc();
    }
  }

  public void incrementReplicationErrorCount(boolean remoteColo, String datacenter) {
    if (remoteColo) {
      dcToReplicationError.get(datacenter).inc();
    } else {
      intraColoReplicationErrorCount.inc();
    }
  }

  public void incrementTimeoutRequestErrorCount(int errorCount, boolean remoteColo, String datacenter) {
    if (remoteColo) {
      dcToTimeoutRequestError.get(datacenter).inc(errorCount);
      dcToReplicationError.get(datacenter).inc(errorCount);
    } else {
      intraColoTimeoutRequestErrorCount.inc(errorCount);
      intraColoReplicationErrorCount.inc(errorCount);
    }
  }

  public void incrementRequestNetworkErrorCount(boolean remoteColo, String datacenter) {
    if (remoteColo) {
      dcToRequestNetworkError.get(datacenter).inc();
      dcToReplicationError.get(datacenter).inc();
    } else {
      intraColoRequestNetworkErrorCount.inc();
      intraColoReplicationErrorCount.inc();
    }
  }

  public void incrementRetryAfterBackoffErrorCount(boolean remoteColo, String datacenter) {
    if (remoteColo) {
      dcToRetryAfterBackoffError.get(datacenter).inc();
      dcToReplicationError.get(datacenter).inc();
    } else {
      intraColoRetryAfterBackoffErrorCount.inc();
      intraColoReplicationErrorCount.inc();
    }
  }

  public void incrementResponseErrorCount(boolean remoteColo, String datacenter) {
    if (remoteColo) {
      dcToResponseError.get(datacenter).inc();
      dcToReplicationError.get(datacenter).inc();
    } else {
      intraColoResponseErrorCount.inc();
      intraColoReplicationErrorCount.inc();
    }
  }

  public void updateOneCycleReplicationTime(long replicationTime, boolean remoteColo, String datacenter) {
    if (remoteColo) {
      interColoOneCycleReplicationTime.get(datacenter).update(replicationTime);
    } else {
      intraColoOneCycleReplicationTime.update(replicationTime);
    }
  }

  public void updateReplicaBatchTotalIdleTimePercentage(long idleTimePercentage, boolean remoteColo,
      String datacenter) {
    if (remoteColo) {
      interColoReplicaBatchTotalIdleTimePercentage.get(datacenter).update(idleTimePercentage);
    } else {
      intraColoReplicaBatchTotalIdleTimePercentage.update(idleTimePercentage);
    }
  }

  public void updateReplicaBatchTotalIdleTimeMs(long idleTime, boolean remoteColo, String datacenter) {
    if (remoteColo) {
      interColoReplicaBatchTotalIdleTimeMs.get(datacenter).update(idleTime);
    } else {
      intraColoReplicaBatchTotalIdleTimeMs.update(idleTime);
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
   * Update the replication lag in seconds for Put Blob.
   * @param datacenter The datacenter
   * @param timeInSeconds The replication lag in seconds
   */
  public void updateReplicationLagInSecondsForBlob(String datacenter, long timeInSeconds) {
    if (datacenter != null && dcToReplicaLagInSecondsForBlob.containsKey(datacenter)) {
      dcToReplicaLagInSecondsForBlob.get(datacenter).update(timeInSeconds);
    }
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
