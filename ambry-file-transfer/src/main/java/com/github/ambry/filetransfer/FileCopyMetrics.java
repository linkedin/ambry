package com.github.ambry.filetransfer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;


public class FileCopyMetrics {
  private final Counter partitionsInFileCopyPath;
  private final Counter partitionsFileCopyInitiated;
  private final Counter partitionsFileCopySkipped;
  private final Counter fileCopyRunningThreadCount;

  public FileCopyMetrics(MetricRegistry registry) {
    partitionsInFileCopyPath =
        registry.counter(MetricRegistry.name(FileCopyBasedReplicationManager.class, "PartitionsInFileCopyPath"));
    partitionsFileCopyInitiated =
        registry.counter(MetricRegistry.name(FileCopyBasedReplicationManager.class, "PartitionsFileCopyInitiated"));
    partitionsFileCopySkipped =
        registry.counter(MetricRegistry.name(FileCopyBasedReplicationManager.class, "PartitionsFileCopySkipped"));
    fileCopyRunningThreadCount =
        registry.counter(MetricRegistry.name(FileCopyThread.class, "FileCopyRunningThreadCount"));
  }

  public void incrementFileCopyInitiated() {
    partitionsFileCopyInitiated.inc();
  }

  public void incrementFileCopySkipped() {
    partitionsFileCopySkipped.inc();
  }

  public void incrementPartitionInFileCopyPath() {
    partitionsInFileCopyPath.inc();
  }

  public void decrementPartitionInFileCopyPath() {
    partitionsInFileCopyPath.dec();
  }

  public void incrementFileCopyRunningThreadCount() {
    fileCopyRunningThreadCount.inc();
  }

  public void decrementFileCopyRunningThreadCount() {
    fileCopyRunningThreadCount.dec();
  }
}
