package com.github.ambry.cloud;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


public class CloudReplicationMetrics {

  public final Counter blobUploadRequestCount;
  public final Counter blobUploadedCount;
  public final Counter blobUploadErrorCount;
  public final Counter blobDeleteRequestCount;
  public final Counter blobDeletedCount;
  public final Counter blobDeleteErrorCount;
  public final Meter blobUploadRate;

  public CloudReplicationMetrics(MetricRegistry registry) {
    blobUploadRequestCount = registry.counter(MetricRegistry.name(CloudBlobReplicator.class, "BlobUploadRequestCount"));
    blobUploadedCount = registry.counter(MetricRegistry.name(CloudBlobReplicator.class, "BlobUploadedCount"));
    blobUploadErrorCount = registry.counter(MetricRegistry.name(CloudBlobReplicator.class, "BlobUploadErrorCount"));
    blobDeleteRequestCount = registry.counter(MetricRegistry.name(CloudBlobReplicator.class, "BlobDeleteRequestCount"));
    blobDeletedCount = registry.counter(MetricRegistry.name(CloudBlobReplicator.class, "BlobDeletedCount"));
    blobDeleteErrorCount = registry.counter(MetricRegistry.name(CloudBlobReplicator.class, "BlobDeleteErrorCount"));
    blobUploadRate = registry.meter(MetricRegistry.name(CloudBlobReplicator.class, "BlobUploadRate"));
  }
}
