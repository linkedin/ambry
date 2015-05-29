package com.github.ambry.replication;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Metrics for Replication
 */
public class ReplicationMetrics {

  public final Meter interColoReplicationBytesRate;
  public final Meter intraColoReplicationBytesRate;
  public final Counter interColoMetadataExchangeCount;
  public final Counter intraColoMetadataExchangeCount;
  public final Counter interColoBlobsReplicatedCount;
  public final Counter intraColoBlobsReplicatedCount;
  public final Counter unknownRemoteReplicaRequestCount;
  public final Counter replicationErrors;
  public final Counter replicationTokenResetCount;
  public final Counter replicationInvalidMessageStreamErrorCount;
  public final Timer interColoReplicationLatency;
  public final Timer intraColoReplicationLatency;
  public final Histogram remoteReplicaTokensPersistTime;
  public final Histogram remoteReplicaTokensRestoreTime;
  public final Histogram interColoExchangeMetadataTime;
  public final Histogram intraColoExchangeMetadataTime;
  public final Histogram interColoFixMissingKeysTime;
  public final Histogram intraColoFixMissingKeysTime;
  public final Histogram interColoReplicationMetadataRequestTime;
  public final Histogram intraColoReplicationMetadataRequestTime;
  public final Histogram interColoReplicationWaitTime;
  public final Histogram intraColoReplicationWaitTime;
  public final Histogram interColoCheckMissingKeysTime;
  public final Histogram intraColoCheckMissingKeysTime;
  public final Histogram interColoProcessMetadataResponseTime;
  public final Histogram intraColoProcessMetadataResponseTime;
  public final Histogram interColoGetRequestTime;
  public final Histogram intraColoGetRequestTime;
  public final Histogram interColoBatchStoreWriteTime;
  public final Histogram intraColoBatchStoreWriteTime;
  public final Histogram interColoTotalReplicationTime;
  public final Histogram intraColoTotalReplicationTime;

  public Gauge<Integer> numberOfIntraDCReplicaThreads;
  public Gauge<Integer> numberOfInterDCReplicaThreads;
  public List<Gauge<Long>> replicaLagInBytes;
  private MetricRegistry registry;
  private Map<String, Counter> metadataRequestErrorMap;
  private Map<String, Counter> getRequestErrorMap;
  private Map<String, Counter> localStoreErrorMap;
  private Map<PartitionId, Counter> partitionIdToInvalidMessageStreamErrorCounter;

  public ReplicationMetrics(MetricRegistry registry, final List<ReplicaThread> replicaIntraDCThreads,
      final List<ReplicaThread> replicaInterDCThreads, List<ReplicaId> replicaIds) {
    metadataRequestErrorMap = new HashMap<String, Counter>();
    getRequestErrorMap = new HashMap<String, Counter>();
    localStoreErrorMap = new HashMap<String, Counter>();
    partitionIdToInvalidMessageStreamErrorCounter = new HashMap<PartitionId, Counter>();
    interColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationBytesRate"));
    intraColoReplicationBytesRate =
        registry.meter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationBytesRate"));
    interColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "InterColoMetadataExchangeCount"));
    intraColoMetadataExchangeCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoMetadataExchangeCount"));
    interColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationBlobsCount"));
    intraColoBlobsReplicatedCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoBlobsReplicatedCount"));
    unknownRemoteReplicaRequestCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "UnknownRemoteReplicaRequestCount"));
    registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationBlobsCount"));
    replicationErrors = registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationErrors"));
    replicationTokenResetCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationTokenResetCount"));
    replicationInvalidMessageStreamErrorCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "ReplicationInvalidMessageStreamErrorCount"));
    interColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationLatency"));
    intraColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationLatency"));
    remoteReplicaTokensPersistTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaTokensPersistTime"));
    remoteReplicaTokensRestoreTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaTokensRestoreTime"));
    interColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoExchangeMetadataTime"));
    intraColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoExchangeMetadataTime"));
    interColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoFixMissingKeysTime"));
    intraColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoFixMissingKeysTime"));
    interColoReplicationMetadataRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationMetadataRequestTime"));
    intraColoReplicationMetadataRequestTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationMetadataRequestTime"));
    interColoReplicationWaitTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationWaitTime"));
    intraColoReplicationWaitTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationWaitTime"));
    interColoCheckMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoCheckMissingKeysTime"));
    intraColoCheckMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoCheckMissingKeysTime"));
    interColoProcessMetadataResponseTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoProcessMetadataResponseTime"));
    intraColoProcessMetadataResponseTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoProcessMetadataResponseTime"));
    interColoGetRequestTime = registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoGetRequestTime"));
    intraColoGetRequestTime = registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoGetRequestTime"));
    interColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoBatchStoreWriteTime"));
    intraColoBatchStoreWriteTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoBatchStoreWriteTime"));
    interColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoTotalReplicationTime"));
    intraColoTotalReplicationTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoTotalReplicationTime"));

    this.registry = registry;
    numberOfIntraDCReplicaThreads = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return getLiveThreads(replicaIntraDCThreads);
      }
    };
    numberOfInterDCReplicaThreads = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return getLiveThreads(replicaInterDCThreads);
      }
    };

    registry.register(MetricRegistry.name(ReplicaThread.class, "NumberOfIntraDCReplicaThreads"),
        numberOfIntraDCReplicaThreads);
    registry.register(MetricRegistry.name(ReplicaThread.class, "NumberOfInterDCReplicaThreads"),
        numberOfInterDCReplicaThreads);
    this.replicaLagInBytes = new ArrayList<Gauge<Long>>();
    populateInvalidMessageMetricForReplicas(replicaIds);
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

  public void addRemoteReplicaToLagMetrics(final RemoteReplicaInfo remoteReplicaInfo) {
    final String metricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-replicaLagInBytes";
    Gauge<Long> replicaLag = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return remoteReplicaInfo.getReplicaLagInBytes();
      }
    };
    registry.register(MetricRegistry.name(ReplicationMetrics.class, metricName), replicaLag);
    replicaLagInBytes.add(replicaLag);
  }

  public void populateInvalidMessageMetricForReplicas(List<ReplicaId> replicaIds) {
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

  public void createRemoteReplicaErrorMetrics(RemoteReplicaInfo remoteReplicaInfo) {
    String metadataRequestErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-metadataRequestError";
    Counter metadataRequestError =
        registry.counter(MetricRegistry.name(ReplicaThread.class, metadataRequestErrorMetricName));
    metadataRequestErrorMap.put(metadataRequestErrorMetricName, metadataRequestError);
    String getRequestErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-getRequestError";
    Counter getRequestError = registry.counter(MetricRegistry.name(ReplicaThread.class, getRequestErrorMetricName));
    getRequestErrorMap.put(getRequestErrorMetricName, getRequestError);
    String localStoreErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-localStoreError";
    Counter localStoreError = registry.counter(MetricRegistry.name(ReplicaThread.class, localStoreErrorMetricName));
    localStoreErrorMap.put(localStoreErrorMetricName, localStoreError);
  }

  public void updateMetadataRequestError(RemoteReplicaInfo remoteReplicaInfo) {
    String metadataRequestErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-metadataRequestError";
    metadataRequestErrorMap.get(metadataRequestErrorMetricName).inc();
  }

  public void updateGetRequestError(RemoteReplicaInfo remoteReplicaInfo) {
    String getRequestErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-getRequestError";
    getRequestErrorMap.get(getRequestErrorMetricName).inc();
  }

  public void updateLocalStoreError(RemoteReplicaInfo remoteReplicaInfo) {
    String localStoreErrorMetricName = remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + "-" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() + "-" +
        remoteReplicaInfo.getReplicaId().getReplicaPath() + "-localStoreError";
    localStoreErrorMap.get(localStoreErrorMetricName).inc();
  }
}
