package com.github.ambry.replication;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.Log;

import java.util.List;

/**
 * Metrics for Replication
 */
public class ReplicationMetrics {

  public final Counter interColoReplicationBytesCount;
  public final Counter intraColoReplicationBytesCount;
  public final Counter interColoBlobsReplicatedCount;
  public final Counter intraColoBlobsReplicatedCount;
  public final Counter replicationErrors;
  public final Timer interColoReplicationLatency;
  public final Timer intraColoReplicationLatency;
  public Gauge<Integer> numberOfReplicaThreads;
  private List<ReplicaThread> replicaThreads;

  public ReplicationMetrics(String name, MetricRegistry registry, List<ReplicaThread> replicaThreads) {
    interColoReplicationBytesCount =
            registry.counter(MetricRegistry.name(ReplicaThread.class, name + "interColoReplicationBytesCount"));
    intraColoReplicationBytesCount =
            registry.counter(MetricRegistry.name(ReplicaThread.class, name + "intraColoReplicationBytesCount"));
    interColoBlobsReplicatedCount =
            registry.counter(MetricRegistry.name(ReplicaThread.class, name + "interColoReplicationBlobsCount"));
    intraColoBlobsReplicatedCount =
            registry.counter(MetricRegistry.name(ReplicaThread.class, name + "intraColoReplicationBlobsCount"));
    replicationErrors =
            registry.counter(MetricRegistry.name(ReplicaThread.class, name + "replicationErrors"));
    interColoReplicationLatency =
            registry.timer(MetricRegistry.name(ReplicaThread.class, name + "interColoReplicationLatency"));
    intraColoReplicationLatency =
            registry.timer(MetricRegistry.name(ReplicaThread.class, name + "intraColoReplicationLatency"));
    this.replicaThreads = replicaThreads;
    numberOfReplicaThreads = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return getLiveThreads();
      }
    };
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
}
