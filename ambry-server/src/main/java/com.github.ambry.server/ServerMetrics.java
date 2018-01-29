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
package com.github.ambry.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/**
 * Metrics for the server
 */
public class ServerMetrics {

  public static final long smallBlob = 50 * 1024; // up to and including 50KB
  public static final long mediumBlob = 1 * 1024 * 1024; // up to and including 1MB
  // largeBlob is everything larger than mediumBlob

  public final Histogram putBlobRequestQueueTimeInMs;
  public final Histogram putBlobProcessingTimeInMs;
  public final Histogram putBlobResponseQueueTimeInMs;
  public final Histogram putBlobSendTimeInMs;
  public final Histogram putBlobTotalTimeInMs;

  public final Histogram putSmallBlobProcessingTimeInMs;
  public final Histogram putSmallBlobSendTimeInMs;
  public final Histogram putSmallBlobTotalTimeInMs;

  public final Histogram putMediumBlobProcessingTimeInMs;
  public final Histogram putMediumBlobSendTimeInMs;
  public final Histogram putMediumBlobTotalTimeInMs;

  public final Histogram putLargeBlobProcessingTimeInMs;
  public final Histogram putLargeBlobSendTimeInMs;
  public final Histogram putLargeBlobTotalTimeInMs;

  public final Histogram getBlobRequestQueueTimeInMs;
  public final Histogram getBlobProcessingTimeInMs;
  public final Histogram getBlobResponseQueueTimeInMs;
  public final Histogram getBlobSendTimeInMs;
  public final Histogram getBlobTotalTimeInMs;

  public final Histogram getSmallBlobProcessingTimeInMs;
  public final Histogram getSmallBlobSendTimeInMs;
  public final Histogram getSmallBlobTotalTimeInMs;

  public final Histogram getMediumBlobProcessingTimeInMs;
  public final Histogram getMediumBlobSendTimeInMs;
  public final Histogram getMediumBlobTotalTimeInMs;

  public final Histogram getLargeBlobProcessingTimeInMs;
  public final Histogram getLargeBlobSendTimeInMs;
  public final Histogram getLargeBlobTotalTimeInMs;

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

  public final Histogram getBlobInfoRequestQueueTimeInMs;
  public final Histogram getBlobInfoProcessingTimeInMs;
  public final Histogram getBlobInfoResponseQueueTimeInMs;
  public final Histogram getBlobInfoSendTimeInMs;
  public final Histogram getBlobInfoTotalTimeInMs;

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

  public final Histogram triggerCompactionRequestQueueTimeInMs;
  public final Histogram triggerCompactionRequestProcessingTimeInMs;
  public final Histogram triggerCompactionResponseQueueTimeInMs;
  public final Histogram triggerCompactionResponseSendTimeInMs;
  public final Histogram triggerCompactionRequestTotalTimeInMs;

  public final Histogram requestControlRequestQueueTimeInMs;
  public final Histogram requestControlRequestProcessingTimeInMs;
  public final Histogram requestControlResponseQueueTimeInMs;
  public final Histogram requestControlResponseSendTimeInMs;
  public final Histogram requestControlRequestTotalTimeInMs;

  public final Histogram replicationControlRequestQueueTimeInMs;
  public final Histogram replicationControlRequestProcessingTimeInMs;
  public final Histogram replicationControlResponseQueueTimeInMs;
  public final Histogram replicationControlResponseSendTimeInMs;
  public final Histogram replicationControlRequestTotalTimeInMs;

  public final Histogram catchupStatusRequestQueueTimeInMs;
  public final Histogram catchupStatusRequestProcessingTimeInMs;
  public final Histogram catchupStatusResponseQueueTimeInMs;
  public final Histogram catchupStatusResponseSendTimeInMs;
  public final Histogram catchupStatusRequestTotalTimeInMs;

  public final Histogram blobSizeInBytes;
  public final Histogram blobUserMetadataSizeInBytes;

  public final Histogram serverStartTimeInMs;
  public final Histogram serverShutdownTimeInMs;

  public final Meter putBlobRequestRate;
  public final Meter getBlobRequestRate;
  public final Meter getBlobPropertiesRequestRate;
  public final Meter getBlobUserMetadataRequestRate;
  public final Meter getBlobAllRequestRate;
  public final Meter getBlobInfoRequestRate;
  public final Meter deleteBlobRequestRate;
  public final Meter ttlBlobRequestRate;
  public final Meter replicaMetadataRequestRate;
  public final Meter triggerCompactionRequestRate;
  public final Meter requestControlRequestRate;
  public final Meter replicationControlRequestRate;
  public final Meter catchupStatusRequestRate;

  public final Meter putSmallBlobRequestRate;
  public final Meter getSmallBlobRequestRate;

  public final Meter putMediumBlobRequestRate;
  public final Meter getMediumBlobRequestRate;

  public final Meter putLargeBlobRequestRate;
  public final Meter getLargeBlobRequestRate;

  public final Counter partitionUnknownError;
  public final Counter diskUnavailableError;
  public final Counter partitionReadOnlyError;
  public final Counter storeIOError;
  public final Counter unExpectedStorePutError;
  public final Counter unExpectedStoreGetError;
  public final Counter unExpectedStoreTTLError;
  public final Counter unExpectedStoreDeleteError;
  public final Counter unExpectedAdminOperationError;
  public final Counter unExpectedStoreFindEntriesError;
  public final Counter idAlreadyExistError;
  public final Counter dataCorruptError;
  public final Counter unknownFormatError;
  public final Counter idNotFoundError;
  public final Counter idDeletedError;
  public final Counter ttlExpiredError;
  public final Counter badRequestError;
  public final Counter temporarilyDisabledError;
  public final Counter getAuthorizationFailure;
  public final Counter deleteAuthorizationFailure;

  public ServerMetrics(MetricRegistry registry) {
    putBlobRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobRequestQueueTime"));
    putBlobProcessingTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobProcessingTime"));
    putBlobResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobResponseQueueTime"));
    putBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobSendTime"));
    putBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutBlobTotalTime"));

    putSmallBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutSmallBlobProcessingTime"));
    putSmallBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutSmallBlobSendTime"));
    putSmallBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutSmallBlobTotalTime"));

    putMediumBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutMediumBlobProcessingTime"));
    putMediumBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutMediumBlobSendTime"));
    putMediumBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutMediumBlobTotalTime"));

    putLargeBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutLargeBlobProcessingTime"));
    putLargeBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutLargeBlobSendTime"));
    putLargeBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "PutLargeBlobTotalTime"));

    getBlobRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobRequestQueueTime"));
    getBlobProcessingTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobProcessingTime"));
    getBlobResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobResponseQueueTime"));
    getBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobSendTime"));
    getBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobTotalTime"));

    getSmallBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetSmallBlobProcessingTime"));
    getSmallBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetSmallBlobSendTime"));
    getSmallBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetSmallBlobTotalTime"));

    getMediumBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetMediumBlobProcessingTime"));
    getMediumBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetMediumBlobSendTime"));
    getMediumBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetMediumBlobTotalTime"));

    getLargeBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetLargeBlobProcessingTime"));
    getLargeBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetLargeBlobSendTime"));
    getLargeBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetLargeBlobTotalTime"));

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
    getBlobAllSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobAllSendTime"));
    getBlobAllTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobAllTotalTime"));

    getBlobInfoRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobInfoRequestQueueTime"));
    getBlobInfoProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobInfoProcessingTime"));
    getBlobInfoResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobInfoResponseQueueTime"));
    getBlobInfoSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobInfoSendTime"));
    getBlobInfoTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "GetBlobInfoTotalTime"));

    deleteBlobRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobRequestQueueTime"));
    deleteBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobProcessingTime"));
    deleteBlobResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobResponseQueueTime"));
    deleteBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobSendTime"));
    deleteBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "DeleteBlobTotalTime"));

    ttlBlobRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobRequestQueueTime"));
    ttlBlobProcessingTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobProcessingTime"));
    ttlBlobResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobResponseQueueTime"));
    ttlBlobSendTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobSendTime"));
    ttlBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(AmbryRequests.class, "TTLBlobTotalTime"));

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

    triggerCompactionRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "TriggerCompactionRequestQueueTimeInMs"));
    triggerCompactionRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "TriggerCompactionRequestProcessingTimeInMs"));
    triggerCompactionResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "TriggerCompactionResponseQueueTimeInMs"));
    triggerCompactionResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "TriggerCompactionResponseSendTimeInMs"));
    triggerCompactionRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "TriggerCompactionRequestTotalTimeInMs"));

    requestControlRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "RequestControlRequestQueueTimeInMs"));
    requestControlRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "RequestControlRequestProcessingTimeInMs"));
    requestControlResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "RequestControlResponseQueueTimeInMs"));
    requestControlResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "RequestControlResponseSendTimeInMs"));
    requestControlRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "RequestControlRequestTotalTimeInMs"));

    replicationControlRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicationControlRequestQueueTimeInMs"));
    replicationControlRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicationControlRequestProcessingTimeInMs"));
    replicationControlResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicationControlResponseQueueTimeInMs"));
    replicationControlResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicationControlResponseSendTimeInMs"));
    replicationControlRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "ReplicationControlRequestTotalTimeInMs"));

    catchupStatusRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "CatchupStatusRequestQueueTimeInMs"));
    catchupStatusRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "CatchupStatusRequestProcessingTimeInMs"));
    catchupStatusResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "CatchupStatusResponseQueueTimeInMs"));
    catchupStatusResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "CatchupStatusResponseSendTimeInMs"));
    catchupStatusRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryRequests.class, "CatchupStatusRequestTotalTimeInMs"));

    blobSizeInBytes = registry.histogram(MetricRegistry.name(AmbryRequests.class, "BlobSize"));
    blobUserMetadataSizeInBytes = registry.histogram(MetricRegistry.name(AmbryRequests.class, "BlobUserMetadataSize"));

    serverStartTimeInMs = registry.histogram(MetricRegistry.name(AmbryServer.class, "ServerStartTimeInMs"));
    serverShutdownTimeInMs = registry.histogram(MetricRegistry.name(AmbryServer.class, "ServerShutdownTimeInMs"));

    putBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "PutBlobRequestRate"));
    getBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobRequestRate"));
    getBlobPropertiesRequestRate =
        registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobPropertiesRequestRate"));
    getBlobUserMetadataRequestRate =
        registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobUserMetadataRequestRate"));
    getBlobAllRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobAllRequestRate"));
    getBlobInfoRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "GetBlobInfoRequestRate"));
    deleteBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "DeleteBlobRequestRate"));
    ttlBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "TTLBlobRequestRate"));
    replicaMetadataRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "ReplicaMetadataRequestRate"));
    triggerCompactionRequestRate =
        registry.meter(MetricRegistry.name(AmbryRequests.class, "TriggerCompactionRequestRate"));
    requestControlRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "RequestControlRequestRate"));
    replicationControlRequestRate =
        registry.meter(MetricRegistry.name(AmbryRequests.class, "ReplicationControlRequestRate"));
    catchupStatusRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "CatchupStatusRequestRate"));

    putSmallBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "PutSmallBlobRequestRate"));
    getSmallBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "GetSmallBlobRequestRate"));

    putMediumBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "PutMediumBlobRequestRate"));
    getMediumBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "GetMediumBlobRequestRate"));

    putLargeBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "PutLargeBlobRequestRate"));
    getLargeBlobRequestRate = registry.meter(MetricRegistry.name(AmbryRequests.class, "GetLargeBlobRequestRate"));

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
    temporarilyDisabledError = registry.counter(MetricRegistry.name(AmbryRequests.class, "TemporarilyDisabledError"));
    badRequestError = registry.counter(MetricRegistry.name(AmbryRequests.class, "BadRequestError"));
    unExpectedStorePutError = registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStorePutError"));
    unExpectedStoreGetError = registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreGetError"));
    unExpectedStoreDeleteError =
        registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreDeleteError"));
    unExpectedAdminOperationError =
        registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedAdminOperationError"));
    unExpectedStoreTTLError = registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreTTLError"));
    unExpectedStoreFindEntriesError =
        registry.counter(MetricRegistry.name(AmbryRequests.class, "UnexpectedStoreFindEntriesError"));
    getAuthorizationFailure =
        registry.counter(MetricRegistry.name(AmbryRequests.class, "GetAuthorizationFailure"));
    deleteAuthorizationFailure =
        registry.counter(MetricRegistry.name(AmbryRequests.class, "DeleteAuthorizationFailure"));
  }

  public void markPutBlobRequestRateBySize(long blobSize) {
    if (blobSize <= smallBlob) {
      putSmallBlobRequestRate.mark();
    } else if (blobSize <= mediumBlob) {
      putMediumBlobRequestRate.mark();
    } else {
      putLargeBlobRequestRate.mark();
    }
  }

  public void markGetBlobRequestRateBySize(long blobSize) {
    if (blobSize <= smallBlob) {
      getSmallBlobRequestRate.mark();
    } else if (blobSize <= mediumBlob) {
      getMediumBlobRequestRate.mark();
    } else {
      getLargeBlobRequestRate.mark();
    }
  }
}
