package com.github.ambry.filetransfer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import jdk.internal.org.jline.reader.History;


public class FileCopyBasedReplicationMetrics {


  public final Counter numPartitionsWaitingForFileCopy;

  public FileCopyBasedReplicationMetrics(MetricRegistry metricRegistry, ) {
    this.numPartitionsWaitingForFileCopy = numPartitionsWaitingForFileCopy;

  }
}
