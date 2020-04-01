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
package com.github.ambry.commons;

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

  public final Histogram getBlobAllByReplicaRequestQueueTimeInMs;
  public final Histogram getBlobAllByReplicaProcessingTimeInMs;
  public final Histogram getBlobAllByReplicaResponseQueueTimeInMs;
  public final Histogram getBlobAllByReplicaSendTimeInMs;
  public final Histogram getBlobAllByReplicaTotalTimeInMs;

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

  public final Histogram undeleteBlobRequestQueueTimeInMs;
  public final Histogram undeleteBlobProcessingTimeInMs;
  public final Histogram undeleteBlobResponseQueueTimeInMs;
  public final Histogram undeleteBlobSendTimeInMs;
  public final Histogram undeleteBlobTotalTimeInMs;

  public final Histogram updateBlobTtlRequestQueueTimeInMs;
  public final Histogram updateBlobTtlProcessingTimeInMs;
  public final Histogram updateBlobTtlResponseQueueTimeInMs;
  public final Histogram updateBlobTtlSendTimeInMs;
  public final Histogram updateBlobTtlTotalTimeInMs;

  public final Histogram replicaMetadataRequestQueueTimeInMs;
  public final Histogram replicaMetadataRequestProcessingTimeInMs;
  public final Histogram replicaMetadataResponseQueueTimeInMs;
  public final Histogram replicaMetadataSendTimeInMs;
  public final Histogram replicaMetadataTotalTimeInMs;
  public final Histogram replicaMetadataTotalSizeOfMessages;

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

  public final Histogram blobStoreControlRequestQueueTimeInMs;
  public final Histogram blobStoreControlRequestProcessingTimeInMs;
  public final Histogram blobStoreControlResponseQueueTimeInMs;
  public final Histogram blobStoreControlResponseSendTimeInMs;
  public final Histogram blobStoreControlRequestTotalTimeInMs;

  public final Histogram blobSizeInBytes;
  public final Histogram blobUserMetadataSizeInBytes;

  public final Histogram serverStartTimeInMs;
  public final Histogram serverShutdownTimeInMs;

  public final Meter putBlobRequestRate;
  public final Meter getBlobRequestRate;
  public final Meter getBlobPropertiesRequestRate;
  public final Meter getBlobUserMetadataRequestRate;
  public final Meter getBlobAllRequestRate;
  public final Meter getBlobAllByReplicaRequestRate;
  public final Meter getBlobInfoRequestRate;
  public final Meter deleteBlobRequestRate;
  public final Meter undeleteBlobRequestRate;
  public final Meter updateBlobTtlRequestRate;
  public final Meter replicaMetadataRequestRate;
  public final Meter triggerCompactionRequestRate;
  public final Meter requestControlRequestRate;
  public final Meter replicationControlRequestRate;
  public final Meter catchupStatusRequestRate;
  public final Meter blobStoreControlRequestRate;

  public final Meter putSmallBlobRequestRate;
  public final Meter getSmallBlobRequestRate;

  public final Meter putMediumBlobRequestRate;
  public final Meter getMediumBlobRequestRate;

  public final Meter putLargeBlobRequestRate;
  public final Meter getLargeBlobRequestRate;

  public final Counter partitionUnknownError;
  public final Counter diskUnavailableError;
  public final Counter replicaUnavailableError;
  public final Counter partitionReadOnlyError;
  public final Counter storeIOError;
  public final Counter unExpectedStorePutError;
  public final Counter unExpectedStoreGetError;
  public final Counter unExpectedStoreTtlUpdateError;
  public final Counter unExpectedStoreDeleteError;
  public final Counter unExpectedStoreUndeleteError;
  public final Counter unExpectedAdminOperationError;
  public final Counter unExpectedStoreFindEntriesError;
  public final Counter idAlreadyExistError;
  public final Counter dataCorruptError;
  public final Counter unknownFormatError;
  public final Counter idNotFoundError;
  public final Counter idDeletedError;
  public final Counter idUndeletedError;
  public final Counter idNotDeletedError;
  public final Counter lifeVersionConflictError;
  public final Counter ttlExpiredError;
  public final Counter badRequestError;
  public final Counter temporarilyDisabledError;
  public final Counter getAuthorizationFailure;
  public final Counter deleteAuthorizationFailure;
  public final Counter undeleteAuthorizationFailure;
  public final Counter ttlUpdateAuthorizationFailure;
  public final Counter ttlAlreadyUpdatedError;
  public final Counter ttlUpdateRejectedError;
  public final Counter replicationResponseMessageSizeTooHigh;

  public ServerMetrics(MetricRegistry registry, Class<?> requestClass) {
    this(registry, requestClass, null);
  }

  public ServerMetrics(MetricRegistry registry, Class<?> requestClass, Class<?> serverClass) {
    putBlobRequestQueueTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutBlobRequestQueueTime"));
    putBlobProcessingTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutBlobProcessingTime"));
    putBlobResponseQueueTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutBlobResponseQueueTime"));
    putBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutBlobSendTime"));
    putBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutBlobTotalTime"));

    putSmallBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "PutSmallBlobProcessingTime"));
    putSmallBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutSmallBlobSendTime"));
    putSmallBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutSmallBlobTotalTime"));

    putMediumBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "PutMediumBlobProcessingTime"));
    putMediumBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutMediumBlobSendTime"));
    putMediumBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutMediumBlobTotalTime"));

    putLargeBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "PutLargeBlobProcessingTime"));
    putLargeBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutLargeBlobSendTime"));
    putLargeBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PutLargeBlobTotalTime"));

    getBlobRequestQueueTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobRequestQueueTime"));
    getBlobProcessingTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobProcessingTime"));
    getBlobResponseQueueTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobResponseQueueTime"));
    getBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobSendTime"));
    getBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobTotalTime"));

    getSmallBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetSmallBlobProcessingTime"));
    getSmallBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetSmallBlobSendTime"));
    getSmallBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetSmallBlobTotalTime"));

    getMediumBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetMediumBlobProcessingTime"));
    getMediumBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetMediumBlobSendTime"));
    getMediumBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetMediumBlobTotalTime"));

    getLargeBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetLargeBlobProcessingTime"));
    getLargeBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetLargeBlobSendTime"));
    getLargeBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetLargeBlobTotalTime"));

    getBlobPropertiesRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobPropertiesRequestQueueTime"));
    getBlobPropertiesProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobPropertiesProcessingTime"));
    getBlobPropertiesResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobPropertiesResponseQueueTime"));
    getBlobPropertiesSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobPropertiesSendTime"));
    getBlobPropertiesTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobPropertiesTotalTime"));

    getBlobUserMetadataRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobUserMetadataRequestQueueTime"));
    getBlobUserMetadataProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobUserMetadataProcessingTime"));
    getBlobUserMetadataResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobUserMetadataResponseQueueTime"));
    getBlobUserMetadataSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobUserMetadataSendTime"));
    getBlobUserMetadataTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobUserMetadataTotalTime"));

    getBlobAllRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllRequestQueueTime"));
    getBlobAllProcessingTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllProcessingTime"));
    getBlobAllResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllResponseQueueTime"));
    getBlobAllSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllSendTime"));
    getBlobAllTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllTotalTime"));

    getBlobAllByReplicaRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllByReplicaRequestQueueTime"));
    getBlobAllByReplicaProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllByReplicaProcessingTime"));
    getBlobAllByReplicaResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllByReplicaResponseQueueTime"));
    getBlobAllByReplicaSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllByReplicaSendTime"));
    getBlobAllByReplicaTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobAllByReplicaTotalTime"));

    getBlobInfoRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobInfoRequestQueueTime"));
    getBlobInfoProcessingTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobInfoProcessingTime"));
    getBlobInfoResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "GetBlobInfoResponseQueueTime"));
    getBlobInfoSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobInfoSendTime"));
    getBlobInfoTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "GetBlobInfoTotalTime"));

    deleteBlobRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "DeleteBlobRequestQueueTime"));
    deleteBlobProcessingTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "DeleteBlobProcessingTime"));
    deleteBlobResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "DeleteBlobResponseQueueTime"));
    deleteBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "DeleteBlobSendTime"));
    deleteBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "DeleteBlobTotalTime"));

    undeleteBlobRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "UndeleteBlobRequestQueueTime"));
    undeleteBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "UndeleteBlobProcessingTime"));
    undeleteBlobResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "UndeleteBlobResponseQueueTime"));
    undeleteBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "UndeleteBlobSendTime"));
    undeleteBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "UndeleteBlobTotalTime"));

    updateBlobTtlRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "UpdateBlobTtlRequestQueueTime"));
    updateBlobTtlProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "UpdateBlobTtlProcessingTime"));
    updateBlobTtlResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "UpdateBlobTtlResponseQueueTime"));
    updateBlobTtlSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "UpdateBlobTtlSendTime"));
    updateBlobTtlTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "UpdateBlobTtlTotalTime"));

    replicaMetadataRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicaMetadataRequestQueueTime"));
    replicaMetadataRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicaMetadataRequestProcessingTime"));
    replicaMetadataResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicaMetadataResponseQueueTime"));
    replicaMetadataSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "ReplicaMetadataSendTime"));
    replicaMetadataTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "ReplicaMetadataTotalTime"));
    replicaMetadataTotalSizeOfMessages =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicaMetadataTotalSizeOfMessages"));

    triggerCompactionRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "TriggerCompactionRequestQueueTimeInMs"));
    triggerCompactionRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "TriggerCompactionRequestProcessingTimeInMs"));
    triggerCompactionResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "TriggerCompactionResponseQueueTimeInMs"));
    triggerCompactionResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "TriggerCompactionResponseSendTimeInMs"));
    triggerCompactionRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "TriggerCompactionRequestTotalTimeInMs"));

    requestControlRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "RequestControlRequestQueueTimeInMs"));
    requestControlRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "RequestControlRequestProcessingTimeInMs"));
    requestControlResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "RequestControlResponseQueueTimeInMs"));
    requestControlResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "RequestControlResponseSendTimeInMs"));
    requestControlRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "RequestControlRequestTotalTimeInMs"));

    replicationControlRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicationControlRequestQueueTimeInMs"));
    replicationControlRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicationControlRequestProcessingTimeInMs"));
    replicationControlResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicationControlResponseQueueTimeInMs"));
    replicationControlResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicationControlResponseSendTimeInMs"));
    replicationControlRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicationControlRequestTotalTimeInMs"));

    catchupStatusRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "CatchupStatusRequestQueueTimeInMs"));
    catchupStatusRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "CatchupStatusRequestProcessingTimeInMs"));
    catchupStatusResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "CatchupStatusResponseQueueTimeInMs"));
    catchupStatusResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "CatchupStatusResponseSendTimeInMs"));
    catchupStatusRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "CatchupStatusRequestTotalTimeInMs"));

    blobStoreControlRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobStoreControlRequestQueueTimeInMs"));
    blobStoreControlRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobStoreControlRequestProcessingTimeInMs"));
    blobStoreControlResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobStoreControlResponseQueueTimeInMs"));
    blobStoreControlResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobStoreControlResponseSendTimeInMs"));
    blobStoreControlRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobStoreControlRequestTotalTimeInMs"));

    blobSizeInBytes = registry.histogram(MetricRegistry.name(requestClass, "BlobSize"));
    blobUserMetadataSizeInBytes = registry.histogram(MetricRegistry.name(requestClass, "BlobUserMetadataSize"));

    if (serverClass != null) {
      serverStartTimeInMs = registry.histogram(MetricRegistry.name(serverClass, "ServerStartTimeInMs"));
      serverShutdownTimeInMs = registry.histogram(MetricRegistry.name(serverClass, "ServerShutdownTimeInMs"));
    } else {
      serverStartTimeInMs = null;
      serverShutdownTimeInMs = null;
    }

    putBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "PutBlobRequestRate"));
    getBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetBlobRequestRate"));
    getBlobPropertiesRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetBlobPropertiesRequestRate"));
    getBlobUserMetadataRequestRate =
        registry.meter(MetricRegistry.name(requestClass, "GetBlobUserMetadataRequestRate"));
    getBlobAllRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetBlobAllRequestRate"));
    getBlobAllByReplicaRequestRate =
        registry.meter(MetricRegistry.name(requestClass, "GetBlobAllByReplicaRequestRate"));
    getBlobInfoRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetBlobInfoRequestRate"));
    deleteBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "DeleteBlobRequestRate"));
    undeleteBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "UndeleteBlobRequestRate"));
    updateBlobTtlRequestRate = registry.meter(MetricRegistry.name(requestClass, "UpdateBlobTtlRequestRate"));
    replicaMetadataRequestRate = registry.meter(MetricRegistry.name(requestClass, "ReplicaMetadataRequestRate"));
    triggerCompactionRequestRate = registry.meter(MetricRegistry.name(requestClass, "TriggerCompactionRequestRate"));
    requestControlRequestRate = registry.meter(MetricRegistry.name(requestClass, "RequestControlRequestRate"));
    replicationControlRequestRate = registry.meter(MetricRegistry.name(requestClass, "ReplicationControlRequestRate"));
    catchupStatusRequestRate = registry.meter(MetricRegistry.name(requestClass, "CatchupStatusRequestRate"));
    blobStoreControlRequestRate = registry.meter(MetricRegistry.name(requestClass, "BlobStoreControlRequestRate"));

    putSmallBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "PutSmallBlobRequestRate"));
    getSmallBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetSmallBlobRequestRate"));

    putMediumBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "PutMediumBlobRequestRate"));
    getMediumBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetMediumBlobRequestRate"));

    putLargeBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "PutLargeBlobRequestRate"));
    getLargeBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetLargeBlobRequestRate"));

    partitionUnknownError = registry.counter(MetricRegistry.name(requestClass, "PartitionUnknownError"));
    diskUnavailableError = registry.counter(MetricRegistry.name(requestClass, "DiskUnavailableError"));
    replicaUnavailableError = registry.counter(MetricRegistry.name(requestClass, "ReplicaUnavailableError"));
    partitionReadOnlyError = registry.counter(MetricRegistry.name(requestClass, "PartitionReadOnlyError"));
    storeIOError = registry.counter(MetricRegistry.name(requestClass, "StoreIOError"));
    idAlreadyExistError = registry.counter(MetricRegistry.name(requestClass, "IDAlreadyExistError"));
    dataCorruptError = registry.counter(MetricRegistry.name(requestClass, "DataCorruptError"));
    unknownFormatError = registry.counter(MetricRegistry.name(requestClass, "UnknownFormatError"));
    idNotFoundError = registry.counter(MetricRegistry.name(requestClass, "IDNotFoundError"));
    idDeletedError = registry.counter(MetricRegistry.name(requestClass, "IDDeletedError"));
    idUndeletedError = registry.counter(MetricRegistry.name(requestClass, "IDUndeletedError"));
    idNotDeletedError = registry.counter(MetricRegistry.name(requestClass, "IDNotDeletedError"));
    lifeVersionConflictError = registry.counter(MetricRegistry.name(requestClass, "lifeVersionConflictError"));
    ttlExpiredError = registry.counter(MetricRegistry.name(requestClass, "TTLExpiredError"));
    temporarilyDisabledError = registry.counter(MetricRegistry.name(requestClass, "TemporarilyDisabledError"));
    badRequestError = registry.counter(MetricRegistry.name(requestClass, "BadRequestError"));
    unExpectedStorePutError = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStorePutError"));
    unExpectedStoreGetError = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreGetError"));
    unExpectedStoreDeleteError = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreDeleteError"));
    unExpectedStoreUndeleteError = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreUndeleteError"));
    unExpectedAdminOperationError =
        registry.counter(MetricRegistry.name(requestClass, "UnexpectedAdminOperationError"));
    unExpectedStoreTtlUpdateError =
        registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreTtlUpdateError"));
    unExpectedStoreFindEntriesError =
        registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreFindEntriesError"));
    getAuthorizationFailure = registry.counter(MetricRegistry.name(requestClass, "GetAuthorizationFailure"));
    deleteAuthorizationFailure = registry.counter(MetricRegistry.name(requestClass, "DeleteAuthorizationFailure"));
    undeleteAuthorizationFailure = registry.counter(MetricRegistry.name(requestClass, "UndeleteAuthorizationFailure"));
    ttlUpdateAuthorizationFailure =
        registry.counter(MetricRegistry.name(requestClass, "TtlUpdateAuthorizationFailure"));
    ttlAlreadyUpdatedError = registry.counter(MetricRegistry.name(requestClass, "TtlAlreadyUpdatedError"));
    ttlUpdateRejectedError = registry.counter(MetricRegistry.name(requestClass, "TtlUpdateRejectedError"));
    replicationResponseMessageSizeTooHigh =
        registry.counter(MetricRegistry.name(requestClass, "ReplicationResponseMessageSizeTooHigh"));
  }

  /**
   * Update put blob request rate based on blob size.
   */
  public void markPutBlobRequestRateBySize(long blobSize) {
    if (blobSize <= smallBlob) {
      putSmallBlobRequestRate.mark();
    } else if (blobSize <= mediumBlob) {
      putMediumBlobRequestRate.mark();
    } else {
      putLargeBlobRequestRate.mark();
    }
  }

  /**
   * Update get blob request rate based on blob size.
   */
  public void markGetBlobRequestRateBySize(long blobSize) {
    if (blobSize <= smallBlob) {
      getSmallBlobRequestRate.mark();
    } else if (blobSize <= mediumBlob) {
      getMediumBlobRequestRate.mark();
    } else {
      getLargeBlobRequestRate.mark();
    }
  }

  /**
   * Update get blob processing time based on blob size.
   */
  public void updateGetBlobProcessingTimeBySize(long blobSize, long processingTime) {
    if (blobSize <= ServerMetrics.smallBlob) {
      getSmallBlobProcessingTimeInMs.update(processingTime);
    } else if (blobSize <= ServerMetrics.mediumBlob) {
      getMediumBlobProcessingTimeInMs.update(processingTime);
    } else {
      getLargeBlobProcessingTimeInMs.update(processingTime);
    }
  }

  /**
   * Update put blob processing time based on blob size.
   */
  public void updatePutBlobProcessingTimeBySize(long blobSize, long processingTime) {
    if (blobSize <= ServerMetrics.smallBlob) {
      putSmallBlobProcessingTimeInMs.update(processingTime);
    } else if (blobSize <= ServerMetrics.mediumBlob) {
      putMediumBlobProcessingTimeInMs.update(processingTime);
    } else {
      putLargeBlobProcessingTimeInMs.update(processingTime);
    }
  }
}
