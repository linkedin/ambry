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

  public final Histogram putBlobRequestQueueTimeInMs;
  public final Histogram putBlobProcessingTimeInMs;
  public final Histogram putBlobResponseQueueTimeInMs;
  public final Histogram putBlobSendTimeInMs;
  public final Histogram putBlobTotalTimeInMs;

  public final Histogram getBlobRequestQueueTimeInMs;
  public final Histogram getBlobProcessingTimeInMs;
  public final Histogram getBlobResponseQueueTimeInMs;
  public final Histogram getBlobSendTimeInMs;
  public final Histogram getBlobTotalTimeInMs;

  public final Histogram getBlobPropertiesRequestQueueTimeInMs;
  public final Histogram getBlobPropertiesProcessingTimeInMs;
  public final Histogram getBlobPropertiesResponseQueueTimeInMs;
  public final Histogram getBlobPropertiesSendTimeInMs;
  public final Histogram getBlobPropertiesTotalTimeInMs;

  public final Histogram getBlobUserMetadataRequestQueueTimeInMs;
  public final Histogram getBlobUserMetadataProcessingTimeInMs;
  public final Histogram getBlobUserMetadataResponseQueueTimeInMs;
  public final Histogram getBlobUserMetadataSendTimeInMs;
  public final Histogram getBlobUserMetadataTotalTimeInMs;

  public final Histogram getBlobAllRequestQueueTimeInMs;
  public final Histogram getBlobAllProcessingTimeInMs;
  public final Histogram getBlobAllResponseQueueTimeInMs;
  public final Histogram getBlobAllSendTimeInMs;
  public final Histogram getBlobAllTotalTimeInMs;

  public final Histogram deleteBlobRequestQueueTimeInMs;
  public final Histogram deleteBlobProcessingTimeInMs;
  public final Histogram deleteBlobResponseQueueTimeInMs;
  public final Histogram deleteBlobSendTimeInMs;
  public final Histogram deleteBlobTotalTimeInMs;

  public final Histogram ttlBlobRequestQueueTimeInMs;
  public final Histogram ttlBlobProcessingTimeInMs;
  public final Histogram ttlBlobResponseQueueTimeInMs;
  public final Histogram ttlBlobSendTimeInMs;
  public final Histogram ttlBlobTotalTimeInMs;

  public final Histogram replicaMetadataRequestQueueTimeInMs;
  public final Histogram replicaMetadataRequestProcessingTimeInMs;
  public final Histogram replicaMetadataResponseQueueTimeInMs;
  public final Histogram replicaMetadataSendTimeInMs;
  public final Histogram replicaMetadataTotalTimeInMs;

  public final Histogram blobSizeInBytes;
  public final Histogram blobUserMetadataSizeInBytes;

  public final Meter putBlobRequestRate;
  public final Meter getBlobRequestRate;
  public final Meter getBlobPropertiesRequestRate;
  public final Meter getBlobUserMetadataRequestRate;
  public final Meter getBlobAllRequestRate;
  public final Meter deleteBlobRequestRate;
  public final Meter ttlBlobRequestRate;
  public final Meter replicaMetadataRequestRate;

  public final Counter partitionUnknownError;
  public final Counter diskUnavailableError;
  public final Counter partitionReadOnlyError;
  public final Counter storeIOError; 
  public final Counter unExpectedStorePutError;
  public final Counter unExpectedStoreGetError;
  public final Counter unExpectedStoreTTLError;
  public final Counter unExpectedStoreDeleteError;
  public final Counter unExpectedStoreFindEntriesError;
  public final Counter idAlreadyExistError;
  public final Counter dataCorruptError;
  public final Counter unknownFormatError;
  public final Counter idNotFoundError;
  public final Counter idDeletedError;
  public final Counter ttlExpiredError;

  public ServerMetrics(MetricRegistry registry) {
    putBlobRequestQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobRequestQueueTime"));
    putBlobProcessingTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobProcessingTime"));
    putBlobResponseQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobResponseQueueTime"));
    putBlobSendTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobSendTime"));
    putBlobTotalTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobTotalTime"));

    getBlobRequestQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobRequestQueueTime"));
    getBlobProcessingTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobProcessingTime"));
    getBlobResponseQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobResponseQueueTime"));
    getBlobSendTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobSendTime"));
    getBlobTotalTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobTotalTime"));

    getBlobPropertiesRequestQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesRequestQueueTime"));
    getBlobPropertiesProcessingTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesProcessingTime"));
    getBlobPropertiesResponseQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesResponseQueueTime"));
    getBlobPropertiesSendTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesSendTime"));
    getBlobPropertiesTotalTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesTotalTime"));

    getBlobUserMetadataRequestQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataRequestQueueTime"));
    getBlobUserMetadataProcessingTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataProcessingTime"));
    getBlobUserMetadataResponseQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataResponseQueueTime"));
    getBlobUserMetadataSendTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataSendTime"));
    getBlobUserMetadataTotalTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataTotalTime"));

    getBlobAllRequestQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobAllRequestQueueTime"));
    getBlobAllProcessingTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobAllProcessingTime"));
    getBlobAllResponseQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobAllResponseQueueTime"));
    getBlobAllSendTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobAllSendTime"));
    getBlobAllTotalTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobAllTotalTime"));

    deleteBlobRequestQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobRequestQueueTime"));
    deleteBlobProcessingTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobProcessingTime"));
    deleteBlobResponseQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobResponseQueueTime"));
    deleteBlobSendTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobSendTime"));
    deleteBlobTotalTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobTotalTime"));

    ttlBlobRequestQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobRequestQueueTime"));
    ttlBlobProcessingTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobProcessingTime"));
    ttlBlobResponseQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobResponseQueueTime"));
    ttlBlobSendTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobSendTime"));
    ttlBlobTotalTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobTotalTime"));

    replicaMetadataRequestQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicaMetadataRequestQueueTime"));
    replicaMetadataRequestProcessingTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicaMetadataRequestProcessingTime"));
    replicaMetadataResponseQueueTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicaMetadataResponseQueueTime"));
    replicaMetadataSendTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicaMetadataSendTime"));
    replicaMetadataTotalTimeInMs =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicaMetadataTotalTime"));

    blobSizeInBytes =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "BlobSize"));
    blobUserMetadataSizeInBytes =
            registry.histogram(MetricRegistry.name(AmbryRequests.class, "BlobUserMetadataSize"));

    putBlobRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "PutBlobRequestRate"));
    getBlobRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobRequestRate"));
    getBlobPropertiesRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesRequestRate"));
    getBlobUserMetadataRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataRequestRate"));
    getBlobAllRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobAllRequestRate"));
    deleteBlobRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "DeleteBlobRequestRate"));
    ttlBlobRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "TTLBlobRequestRate"));
    replicaMetadataRequestRate =
            registry.meter(MetricRegistry.name(AmbryRequests.class, "ReplicaMetadataRequestRate"));

    partitionUnknownError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "PartitionUnknownError"));
    diskUnavailableError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "DiskUnavailableError"));
    partitionReadOnlyError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "PartitionReadOnlyError"));
    storeIOError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "StoreIOError"));
    idAlreadyExistError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "IDAlreadyExistError"));
    dataCorruptError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "DataCorruptError"));
    unknownFormatError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "UnknownFormatError"));
    idNotFoundError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "IDNotFoundError"));
    idDeletedError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "IDDeletedError"));
    ttlExpiredError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "TTLExpiredError"));
    unExpectedStorePutError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStorePutError"));
    unExpectedStoreGetError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreGetError"));
    unExpectedStoreDeleteError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreDeleteError"));
    unExpectedStoreTTLError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreTTLError"));
    unExpectedStoreFindEntriesError =
            registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreFindEntriesError"));
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
