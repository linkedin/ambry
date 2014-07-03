package com.github.ambry.replication;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.ArrayList;
import java.util.List;


/**
 * Metrics for Replication
 */
public class ReplicationMetrics {

  public final Counter interColoReplicationBytesCount;
  public final Counter intraColoReplicationBytesCount;
  public final Counter interColoBlobsReplicatedCount;
  public final Counter intraColoBlobsReplicatedCount;
  public final Counter unknownRemoteReplicaRequestCount;
  public final Counter replicationErrors;
  public final Timer interColoReplicationLatency;
  public final Timer intraColoReplicationLatency;
  public final Histogram remoteReplicaTokensPersistTime;
  public final Histogram remoteReplicaTokensRestoreTime;
  public Gauge<Integer> numberOfReplicaThreads;
  private List<ReplicaThread> replicaThreads;
  public List<Gauge<Long>> replicaLagInBytes;
  private MetricRegistry registry;

  public ReplicationMetrics(MetricRegistry registry, List<ReplicaThread> replicaThreads) {
    interColoReplicationBytesCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "InterColoReplicationBytesCount"));
    intraColoReplicationBytesCount =
        registry.counter(MetricRegistry.name(ReplicaThread.class, "IntraColoReplicationBytesCount"));
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
    this.replicaThreads = replicaThreads;
    this.registry = registry;
    numberOfReplicaThreads = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return getLiveThreads();
      }
    };

    registry.register(MetricRegistry.name(ReplicaThread.class, "NumberOfReplicaThreads"), numberOfReplicaThreads);
    this.replicaLagInBytes = new ArrayList<Gauge<Long>>();
  }

  private int getLiveThreads() {
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
