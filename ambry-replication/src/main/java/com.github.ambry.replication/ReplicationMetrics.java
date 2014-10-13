package com.github.ambry.replication;

import com.codahale.metrics.*;

import java.util.ArrayList;
import java.util.List;


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
  public final Timer interColoReplicationLatency;
  public final Timer intraColoReplicationLatency;
  public final Histogram remoteReplicaTokensPersistTime;
  public final Histogram remoteReplicaTokensRestoreTime;
  public final Histogram intraColoExchangeMetadataTime;
  public final Histogram intraColoFixMissingKeysTime;
  public final Histogram interColoExchangeMetadataTime;
  public final Histogram interColoFixMissingKeysTime;
  public Gauge<Integer> numberOfIntraDCReplicaThreads;
  public Gauge<Integer> numberOfInterDCReplicaThreads;
  public List<Gauge<Long>> replicaLagInBytes;
  private MetricRegistry registry;

  public ReplicationMetrics(MetricRegistry registry, final List<ReplicaThread> replicaIntraDCThreads,
      final List<ReplicaThread> replicaInterDCThreads) {
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
    interColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationLatency"));
    intraColoReplicationLatency =
        registry.timer(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationLatency"));
    remoteReplicaTokensPersistTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaTokensPersistTime"));
    remoteReplicaTokensRestoreTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "RemoteReplicaTokensRestoreTime"));

    intraColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoExchangeMetadataTime"));
    intraColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "IntraColoFixMissingKeysTime"));
    interColoExchangeMetadataTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoExchangeMetadataTime"));
    interColoFixMissingKeysTime =
        registry.histogram(MetricRegistry.name(ReplicaThread.class, "InterColoFixMissingKeysTime"));

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
}
