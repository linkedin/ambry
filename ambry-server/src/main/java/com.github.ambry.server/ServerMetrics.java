package com.github.ambry.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Meter;
import com.github.ambry.metrics.MetricsHistogram;

/**
 * Metrics for the server
 */
public class ServerMetrics {

  public final Histogram putBlobRequestQueueTime;
  public final Histogram putBlobProcessingTime;
  public final Histogram putBlobResponseQueueTime;
  public final Histogram putBlobSendTime;

  public final Histogram getBlobRequestQueueTime;
  public final Histogram getBlobProcessingTime;
  public final Histogram getBlobResponseQueueTime;
  public final Histogram getBlobSendTime;

  public final Histogram getBlobPropertiesRequestQueueTime;
  public final Histogram getBlobPropertiesProcessingTime;
  public final Histogram getBlobPropertiesResponseQueueTime;
  public final Histogram getBlobPropertiesSendTime;

  public final Histogram getBlobUserMetadataRequestQueueTime;
  public final Histogram getBlobUserMetadataProcessingTime;
  public final Histogram getBlobUserMetadataResponseQueueTime;
  public final Histogram getBlobUserMetadataSendTime;

  public final Histogram deleteBlobRequestQueueTime;
  public final Histogram deleteBlobProcessingTime;
  public final Histogram deleteBlobResponseQueueTime;
  public final Histogram deleteBlobSendTime;

  public final Histogram ttlBlobRequestQueueTime;
  public final Histogram ttlBlobProcessingTime;
  public final Histogram ttlBlobResponseQueueTime;
  public final Histogram ttlBlobSendTime;

  public final Meter putBlobRequestRate;
  public final Meter getBlobRequestRate;
  public final Meter getBlobPropertiesRequestRate;
  public final Meter getBlobUserMetadataRequestRate;
  public final Meter deleteBlobRequestRate;
  public final Meter ttlBlobRequestRate;

  public final Counter partitionUnknownError;
  public final Counter diskUnavailableError;
  public final Counter partitionReadOnlyError;
  public final Counter storeIOError; 
  public final Counter unExpectedStorePutError;
  public final Counter unExpectedStoreGetError;
  public final Counter unExpectedStoreTTLError;
  public final Counter unExpectedStoreDeleteError;
  public final Counter idAlreadyExistError;
  public final Counter dataCorruptError;
  public final Counter unknownFormatError;
  public final Counter idNotFoundError;
  public final Counter idDeletedError;
  public final Counter ttlExpiredError;

  public ServerMetrics(MetricRegistry registry) {
    putBlobRequestQueueTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobRequestQueueTime"));
    putBlobProcessingTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobProcessingTime"));
    putBlobResponseQueueTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobResponseQueueTime"));
    putBlobSendTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobSendTime"));

    getBlobRequestQueueTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobRequestQueueTime"));
    getBlobProcessingTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobProcessingTime"));
    getBlobResponseQueueTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobResponseQueueTime"));
    getBlobSendTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobSendTime"));

    getBlobPropertiesRequestQueueTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesRequestQueueTime"));
    getBlobPropertiesProcessingTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesProcessingTime"));
    getBlobPropertiesResponseQueueTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesResponseQueueTime"));
    getBlobPropertiesSendTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesSendTime"));

    getBlobUserMetadataRequestQueueTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataRequestQueueTime"));
    getBlobUserMetadataProcessingTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataProcessingTime"));
    getBlobUserMetadataResponseQueueTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataResponseQueueTime"));
    getBlobUserMetadataSendTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataSendTime"));

    deleteBlobRequestQueueTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobRequestQueueTime"));
    deleteBlobProcessingTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobProcessingTime"));
    deleteBlobResponseQueueTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobResponseQueueTime"));
    deleteBlobSendTime =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobSendTime"));

    ttlBlobRequestQueueTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobRequestQueueTime"));
    ttlBlobProcessingTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobProcessingTime"));
    ttlBlobResponseQueueTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobResponseQueueTime"));
    ttlBlobSendTime = registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobSendTime"));

    putBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "PutBlobRequestRate"));
    getBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobRequestRate"));
    getBlobPropertiesRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesRequestRate"));
    getBlobUserMetadataRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataRequestRate"));
    deleteBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "DeleteBlobRequestRate"));
    ttlBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "TTLBlobRequestRate"));

    partitionUnknownError = registry.counter(MetricRegistry.name(AmbryRequests.class, "PartitionUnknownError"));
    diskUnavailableError = registry.counter(MetricRegistry.name(AmbryRequests.class, "DiskUnavailableError"));
    partitionReadOnlyError = registry.counter(MetricRegistry.name(AmbryRequests.class, "PartitionReadOnlyError"));
    storeIOError = registry.counter(MetricRegistry.name(AmbryRequests.class, "StoreIOError"));
    idAlreadyExistError = registry.counter(MetricRegistry.name(AmbryRequests.class, "IDAlreadyExistError"));
    dataCorruptError = registry.counter(MetricRegistry.name(AmbryRequests.class, "DataCorruptError"));
    unknownFormatError = registry.counter(MetricRegistry.name(AmbryRequests.class, "UnknownFormatError"));
    idNotFoundError = registry.counter(MetricRegistry.name(AmbryRequests.class, "IDNotFoundError"));
    idDeletedError = registry.counter(MetricRegistry.name(AmbryRequests.class, "IDDeletedError"));
    ttlExpiredError = registry.counter(MetricRegistry.name(AmbryRequests.class, "TTLExpiredError"));
    unExpectedStorePutError = registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStorePutError"));
    unExpectedStoreGetError = registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreGetError"));
    unExpectedStoreDeleteError = registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreDeleteError"));
    unExpectedStoreTTLError = registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreTTLError"));
  }
}

class HistogramMeasurement implements MetricsHistogram {

  private final Histogram histogram;

  public HistogramMeasurement(Histogram histogram) {
    this.histogram = histogram;
  }

  @Override
  public void update(long value) {
    histogram.update(value);
  }

  @Override
  public void update(int value) {
    histogram.update(value);
  }
}
