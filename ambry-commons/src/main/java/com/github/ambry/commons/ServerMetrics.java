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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


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

  public final Histogram fileCopyGetMetadataRequestQueueTimeInMs;
  public final Histogram fileCopyGetMetadataProcessingTimeInMs;
  public final Histogram fileCopyGetMetadataResponseQueueTimeInMs;
  public final Histogram fileCopyGetMetadataSendTimeInMs;
  public final Histogram fileCopyGetMetadataTotalTimeInMs;
  public final Meter fileCopyGetMetadataRequestRate;
  public final Meter fileCopyGetMetadataDroppedRate;

  public final Histogram fileCopyGetChunkRequestQueueTimeInMs;
  public final Histogram fileCopyGetChunkProcessingTimeInMs;
  public final Histogram fileCopyGetChunkResponseQueueTimeInMs;
  public final Histogram fileCopyGetChunkSendTimeInMs;
  public final Histogram fileCopyGetChunkTotalTimeInMs;
  public final Meter fileCopyGetChunkRequestRate;
  public final Meter fileCopyGetChunkDroppedRate;

  public final Histogram fileCopyDataVerificationRequestQueueTimeInMs;
  public final Histogram fileCopyDataVerificationProcessingTimeInMs;
  public final Histogram fileCopyDataVerificationResponseQueueTimeInMs;
  public final Histogram fileCopyDataVerificationSendTimeInMs;
  public final Histogram fileCopyDataVerificationTotalTimeInMs;
  public final Meter fileCopyDataVerificationRequestRate;
  public final Meter fileCopyDataVerificationDroppedRate;


  public final Histogram batchDeleteBlobRequestQueueTimeInMs;
  public final Histogram batchDeleteBlobProcessingTimeInMs;
  public final Histogram batchDeleteBlobResponseQueueTimeInMs;
  public final Histogram purgeBlobResponseQueueTimeInMs;
  public final Histogram deleteBlobSendTimeInMs;

  public final Histogram batchDeleteBlobSendTimeInMs;
  public final Histogram purgeBlobSendTimeInMs;
  public final Histogram deleteBlobTotalTimeInMs;
  public final Histogram batchDeleteBlobTotalTimeInMs;
  public final Histogram purgeBlobTotalTimeInMs;

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

  public final Histogram replicateBlobRequestQueueTimeInMs;
  public final Histogram replicateBlobProcessingTimeInMs;
  public final Histogram replicateBlobResponseQueueTimeInMs;
  public final Histogram replicateBlobSendTimeInMs;
  public final Histogram replicateBlobTotalTimeInMs;

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

  public final Histogram healthCheckRequestQueueTimeInMs;
  public final Histogram healthCheckRequestProcessingTimeInMs;
  public final Histogram healthCheckResponseQueueTimeInMs;
  public final Histogram healthCheckResponseSendTimeInMs;
  public final Histogram healthCheckRequestTotalTimeInMs;

  public final Histogram blobIndexRequestQueueTimeInMs;
  public final Histogram blobIndexRequestProcessingTimeInMs;
  public final Histogram blobIndexResponseQueueTimeInMs;
  public final Histogram blobIndexResponseSendTimeInMs;
  public final Histogram blobIndexRequestTotalTimeInMs;

  public final Histogram forceDeleteRequestQueueTimeInMs;
  public final Histogram forceDeleteRequestProcessingTimeInMs;
  public final Histogram forceDeleteResponseQueueTimeInMs;
  public final Histogram forceDeleteResponseSendTimeInMs;
  public final Histogram forceDeleteRequestTotalTimeInMs;

  public final Histogram blobSizeInBytes;
  public final Histogram blobUserMetadataSizeInBytes;

  public final Histogram serverStartTimeInMs;
  public final Histogram serverShutdownTimeInMs;

  // AmbryServerSecurityService
  public final Histogram securityServiceValidateConnectionTimeInMs;
  public final Histogram securityServiceValidateRequestTimeInMs;

  public final Meter putBlobRequestRate;
  public final Meter getBlobRequestRate;
  public final Meter getBlobPropertiesRequestRate;
  public final Meter getBlobUserMetadataRequestRate;
  public final Meter getBlobAllRequestRate;
  public final Meter getBlobAllByReplicaRequestRate;
  public final Meter getBlobInfoRequestRate;
  public final Meter deleteBlobRequestRate;
  public final Meter replicateBlobRequestRate;
  public final Meter replicateBlobRequestOnTtlUpdateRate;
  public final Meter replicateBlobRequestOnDeleteRate;
  public final Meter replicateDeleteRecordRate;
  public final Meter undeleteBlobRequestRate;
  public final Meter updateBlobTtlRequestRate;
  public final Meter replicaMetadataRequestRate;
  public final Meter triggerCompactionRequestRate;
  public final Meter requestControlRequestRate;
  public final Meter replicationControlRequestRate;
  public final Meter catchupStatusRequestRate;
  public final Meter blobStoreControlRequestRate;
  public final Meter healthCheckRequestRate;
  public final Meter blobIndexRequestRate;
  public final Meter forceDeleteRequestRate;

  public final Meter putBlobDroppedRate;
  public final Meter getBlobDroppedRate;
  public final Meter getBlobPropertiesDroppedRate;
  public final Meter getBlobUserMetadataDroppedRate;
  public final Meter getBlobAllDroppedRate;
  public final Meter getBlobAllByReplicaDroppedRate;
  public final Meter getBlobInfoDroppedRate;
  public final Meter deleteBlobDroppedRate;
  public final Meter replicateBlobDroppedRate;
  public final Meter undeleteBlobDroppedRate;
  public final Meter updateBlobTtlDroppedRate;
  public final Meter replicaMetadataDroppedRate;
  public final Meter triggerCompactionDroppedRate;
  public final Meter requestControlDroppedRate;
  public final Meter replicationControlDroppedRate;
  public final Meter catchupStatusDroppedRate;
  public final Meter blobStoreControlDroppedRate;
  public final Meter healthCheckDroppedRate;
  public final Meter blobIndexDroppedRate;
  public final Meter forceDeleteDroppedRate;

  public final Meter totalRequestDroppedRate;

  public final Meter putSmallBlobRequestRate;
  public final Meter getSmallBlobRequestRate;

  public final Meter putMediumBlobRequestRate;
  public final Meter getMediumBlobRequestRate;

  public final Meter putLargeBlobRequestRate;
  public final Meter getLargeBlobRequestRate;

  // AmbryServerSecurityService
  public final Meter securityServiceValidateConnectionRate;
  public final Meter securityServiceValidateRequestRate;

  public final Counter partitionUnknownError;
  public final Counter diskUnavailableError;
  public final Counter replicaUnavailableError;
  public final Counter partitionReadOnlyError;
  public final Counter storeIOError;
  public final Counter unExpectedStorePutError;
  public final Counter unExpectedStoreGetError;
  public final Counter unExpectedStoreTtlUpdateError;
  public final Counter unExpectedStoreDeleteError;

  public final Counter unExpectedStoreDeleteErrorInBatchDelete;

  public final Counter unExpectedStoreBatchDeleteError;
  public final Counter unExpectedStorePurgeError;
  public final Counter unExpectedStoreUndeleteError;
  public final Counter unExpectedAdminOperationError;
  public final Counter unExpectedStoreFindEntriesError;
  public final Counter idAlreadyExistError;
  public final Counter dataCorruptError;
  public final Counter unknownFormatError;
  public final Counter idNotFoundError;

  public final Counter idNotFoundErrorInBatchDelete;
  public final Counter idDeletedError;

  public final Counter idDeletedErrorInBatchDelete;
  public final Counter idPurgedError;
  public final Counter idUndeletedError;
  public final Counter idNotDeletedError;
  public final Counter lifeVersionConflictError;
  public final Counter ttlExpiredError;

  public final Counter ttlExpiredErrorInBatchDelete;
  public final Counter badRequestError;
  public final Counter temporarilyDisabledError;
  public final Counter getAuthorizationFailure;
  public final Counter deleteAuthorizationFailure;

  public final Counter deleteAuthorizationFailureInBatchDelete;
  public final Counter purgeAuthorizationFailure;
  public final Counter undeleteAuthorizationFailure;
  public final Counter ttlUpdateAuthorizationFailure;
  public final Counter ttlAlreadyUpdatedError;
  public final Counter ttlUpdateRejectedError;
  public final Counter replicationResponseMessageSizeTooHigh;
  public final Counter batchDeleteOperationError;
  public Counter sealedReplicasMismatchCount = null;
  public Counter partiallySealedReplicasMismatchCount = null;
  public Counter stoppedReplicasMismatchCount = null;
  // AmbryServerSecurityService
  public final Counter serverValidateConnectionSuccess;
  public final Counter serverValidateConnectionFailure;

  public final Map<String, Meter> crossColoFetchBytesRate = new ConcurrentHashMap<>();
  public final Map<String, Meter> crossColoMetadataExchangeBytesRate = new ConcurrentHashMap<>();

  private final MetricRegistry registry;
  private final Class<?> requestClass;

  public Gauge<Integer> activeRequestsQueueSize;

  public ServerMetrics(MetricRegistry registry, Class<?> requestClass) {
    this(registry, requestClass, null);
  }

  public ServerMetrics(MetricRegistry registry, Class<?> requestClass, Class<?> serverClass) {
    this.registry = registry;
    this.requestClass = requestClass;
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

    fileCopyGetMetadataRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetMetadataRequestQueueTimeInMs"));
    fileCopyGetMetadataProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetMetadataProcessingTimeInMs"));
    fileCopyGetMetadataResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetMetadataResponseQueueTimeInMs"));
    fileCopyGetMetadataSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetMetadataSendTimeInMs"));
    fileCopyGetMetadataTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetMetadataTotalTimeInMs"));
    fileCopyGetMetadataRequestRate =
        registry.meter(MetricRegistry.name(requestClass, "FileCopyGetMetadataRequestRate"));
    fileCopyGetMetadataDroppedRate =
        registry.meter(MetricRegistry.name(requestClass, "FileCopyGetMetadataDroppedRate"));

    fileCopyGetChunkRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetChunkRequestQueueTimeInMs"));
    fileCopyGetChunkProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetChunkProcessingTimeInMs"));
    fileCopyGetChunkResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetChunkResponseQueueTimeInMs"));
    fileCopyGetChunkSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetChunkSendTimeInMs"));
    fileCopyGetChunkTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyGetChunkTotalTimeInMs"));
    fileCopyGetChunkRequestRate =
        registry.meter(MetricRegistry.name(requestClass, "FileCopyGetChunkRequestRate"));
    fileCopyGetChunkDroppedRate =
        registry.meter(MetricRegistry.name(requestClass, "FileCopyGetChunkDroppedRate"));

    fileCopyDataVerificationRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyDataVerificationRequestQueueTimeInMs"));
    fileCopyDataVerificationProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyDataVerificationProcessingTimeInMs"));
    fileCopyDataVerificationResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyDataVerificationResponseQueueTimeInMs"));
    fileCopyDataVerificationSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyDataVerificationSendTimeInMs"));
    fileCopyDataVerificationTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "FileCopyDataVerificationTotalTimeInMs"));
    fileCopyDataVerificationRequestRate =
        registry.meter(MetricRegistry.name(requestClass, "FileCopyDataVerificationRequestRate"));
    fileCopyDataVerificationDroppedRate =
        registry.meter(MetricRegistry.name(requestClass, "FileCopyDataVerificationDroppedRate"));

    batchDeleteBlobRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BatchDeleteBlobRequestQueueTimeInMs"));
    batchDeleteBlobProcessingTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "BatchDeleteBlobProcessingTimeInMs"));
    batchDeleteBlobResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BatchDeleteBlobResponseQueueTimeInMs"));
    purgeBlobResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "PurgeBlobResponseQueueTimeInMs"));
    deleteBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "DeleteBlobSendTime"));
    batchDeleteBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "BatchDeleteBlobSendTime"));
    purgeBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PurgeBlobSendTimeInMs"));
    deleteBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "DeleteBlobTotalTime"));
    batchDeleteBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "BatchDeleteBlobTotalTimeInMs"));
    purgeBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "PurgeBlobTotalTimeInMs"));

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

    replicateBlobRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicateBlobRequestQueueTime"));
    replicateBlobProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicateBlobProcessingTime"));
    replicateBlobResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ReplicateBlobResponseQueueTime"));
    replicateBlobSendTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "ReplicateBlobSendTime"));
    replicateBlobTotalTimeInMs = registry.histogram(MetricRegistry.name(requestClass, "ReplicateBlobTotalTime"));

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

    healthCheckRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "HealthCheckRequestQueueTimeInMs"));
    healthCheckRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "HealthCheckRequestProcessingTimeInMs"));
    healthCheckResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "HealthCheckResponseQueueTimeInMs"));
    healthCheckResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "HealthCheckResponseSendTimeInMs"));
    healthCheckRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "HealthCheckRequestTotalTimeInMs"));

    blobIndexRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobIndexRequestQueueTimeInMs"));
    blobIndexRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobIndexRequestProcessingTimeInMs"));
    blobIndexResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobIndexResponseQueueTimeInMs"));
    blobIndexResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobIndexResponseSendTimeInMs"));
    blobIndexRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "BlobIndexRequestTotalTimeInMs"));

    forceDeleteRequestQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ForceDeleteRequestQueueTimeInMs"));
    forceDeleteRequestProcessingTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ForceDeleteRequestProcessingTimeInMs"));
    forceDeleteResponseQueueTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ForceDeleteResponseQueueTimeInMs"));
    forceDeleteResponseSendTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ForceDeleteResponseSendTimeInMs"));
    forceDeleteRequestTotalTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "ForceDeleteRequestTotalTimeInMs"));

    blobSizeInBytes = registry.histogram(MetricRegistry.name(requestClass, "BlobSize"));
    blobUserMetadataSizeInBytes = registry.histogram(MetricRegistry.name(requestClass, "BlobUserMetadataSize"));

    if (serverClass != null) {
      serverStartTimeInMs = registry.histogram(MetricRegistry.name(serverClass, "ServerStartTimeInMs"));
      serverShutdownTimeInMs = registry.histogram(MetricRegistry.name(serverClass, "ServerShutdownTimeInMs"));
    } else {
      serverStartTimeInMs = null;
      serverShutdownTimeInMs = null;
    }

    securityServiceValidateConnectionTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "SecurityServiceValidateConnectionTimeInMs"));
    securityServiceValidateRequestTimeInMs =
        registry.histogram(MetricRegistry.name(requestClass, "SecurityServiceValidateRequestTimeInMs"));

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
    replicateBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "ReplicateBlobRequestRate"));
    replicateBlobRequestOnTtlUpdateRate = registry.meter(MetricRegistry.name(requestClass, "ReplicateBlobRequestOnTtlUpdateRate"));
    replicateBlobRequestOnDeleteRate = registry.meter(MetricRegistry.name(requestClass, "ReplicateBlobRequestOnDeleteRate"));
    replicateDeleteRecordRate = registry.meter(MetricRegistry.name(requestClass, "ReplicateDeleteRecordRate"));
    replicaMetadataRequestRate = registry.meter(MetricRegistry.name(requestClass, "ReplicaMetadataRequestRate"));
    triggerCompactionRequestRate = registry.meter(MetricRegistry.name(requestClass, "TriggerCompactionRequestRate"));
    requestControlRequestRate = registry.meter(MetricRegistry.name(requestClass, "RequestControlRequestRate"));
    replicationControlRequestRate = registry.meter(MetricRegistry.name(requestClass, "ReplicationControlRequestRate"));
    catchupStatusRequestRate = registry.meter(MetricRegistry.name(requestClass, "CatchupStatusRequestRate"));
    blobStoreControlRequestRate = registry.meter(MetricRegistry.name(requestClass, "BlobStoreControlRequestRate"));
    healthCheckRequestRate = registry.meter(MetricRegistry.name(requestClass, "HealthCheckRequestRate"));
    blobIndexRequestRate = registry.meter(MetricRegistry.name(requestClass, "BlobIndexRequestRate"));
    forceDeleteRequestRate = registry.meter(MetricRegistry.name(requestClass, "ForceDeleteRequestRate"));

    putBlobDroppedRate = registry.meter(MetricRegistry.name(requestClass, "PutBlobDroppedRate"));
    getBlobDroppedRate = registry.meter(MetricRegistry.name(requestClass, "GetBlobDroppedRate"));
    getBlobPropertiesDroppedRate = registry.meter(MetricRegistry.name(requestClass, "GetBlobPropertiesDroppedRate"));
    getBlobUserMetadataDroppedRate =
        registry.meter(MetricRegistry.name(requestClass, "GetBlobUserMetadataDroppedRate"));
    getBlobAllDroppedRate = registry.meter(MetricRegistry.name(requestClass, "GetBlobAllDroppedRate"));
    getBlobAllByReplicaDroppedRate =
        registry.meter(MetricRegistry.name(requestClass, "GetBlobAllByReplicaDroppedRate"));
    getBlobInfoDroppedRate = registry.meter(MetricRegistry.name(requestClass, "GetBlobInfoDroppedRate"));
    deleteBlobDroppedRate = registry.meter(MetricRegistry.name(requestClass, "DeleteBlobDroppedRate"));
    undeleteBlobDroppedRate = registry.meter(MetricRegistry.name(requestClass, "UndeleteBlobDroppedRate"));
    updateBlobTtlDroppedRate = registry.meter(MetricRegistry.name(requestClass, "UpdateBlobTtlDroppedRate"));
    replicateBlobDroppedRate = registry.meter(MetricRegistry.name(requestClass, "ReplicateBlobDroppedRate"));
    replicaMetadataDroppedRate = registry.meter(MetricRegistry.name(requestClass, "ReplicaMetadataDroppedRate"));
    triggerCompactionDroppedRate = registry.meter(MetricRegistry.name(requestClass, "TriggerCompactionDroppedRate"));
    requestControlDroppedRate = registry.meter(MetricRegistry.name(requestClass, "RequestControlDroppedRate"));
    replicationControlDroppedRate = registry.meter(MetricRegistry.name(requestClass, "ReplicationControlDroppedRate"));
    catchupStatusDroppedRate = registry.meter(MetricRegistry.name(requestClass, "CatchupStatusDroppedRate"));
    blobStoreControlDroppedRate = registry.meter(MetricRegistry.name(requestClass, "BlobStoreControlDroppedRate"));
    healthCheckDroppedRate = registry.meter(MetricRegistry.name(requestClass, "HealthCheckDroppedRate"));
    blobIndexDroppedRate = registry.meter(MetricRegistry.name(requestClass, "BlobIndexDroppedRate"));
    forceDeleteDroppedRate = registry.meter(MetricRegistry.name(requestClass, "ForceDeleteDroppedRate"));

    totalRequestDroppedRate = registry.meter(MetricRegistry.name(requestClass, "TotalRequestDroppedRate"));

    putSmallBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "PutSmallBlobRequestRate"));
    getSmallBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetSmallBlobRequestRate"));

    putMediumBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "PutMediumBlobRequestRate"));
    getMediumBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetMediumBlobRequestRate"));

    putLargeBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "PutLargeBlobRequestRate"));
    getLargeBlobRequestRate = registry.meter(MetricRegistry.name(requestClass, "GetLargeBlobRequestRate"));

    securityServiceValidateConnectionRate =
        registry.meter(MetricRegistry.name(requestClass, "SecurityServiceValidateConnectionRate"));
    securityServiceValidateRequestRate =
        registry.meter(MetricRegistry.name(requestClass, "SecurityServiceValidateRequestRate"));

    partitionUnknownError = registry.counter(MetricRegistry.name(requestClass, "PartitionUnknownError"));
    diskUnavailableError = registry.counter(MetricRegistry.name(requestClass, "DiskUnavailableError"));
    replicaUnavailableError = registry.counter(MetricRegistry.name(requestClass, "ReplicaUnavailableError"));
    partitionReadOnlyError = registry.counter(MetricRegistry.name(requestClass, "PartitionReadOnlyError"));
    storeIOError = registry.counter(MetricRegistry.name(requestClass, "StoreIOError"));
    idAlreadyExistError = registry.counter(MetricRegistry.name(requestClass, "IDAlreadyExistError"));
    dataCorruptError = registry.counter(MetricRegistry.name(requestClass, "DataCorruptError"));
    unknownFormatError = registry.counter(MetricRegistry.name(requestClass, "UnknownFormatError"));
    idNotFoundError = registry.counter(MetricRegistry.name(requestClass, "IDNotFoundError"));
    idNotFoundErrorInBatchDelete = registry.counter(MetricRegistry.name(requestClass, "IDNotFoundErrorInBatchDelete"));
    idDeletedError = registry.counter(MetricRegistry.name(requestClass, "IDDeletedError"));
    idDeletedErrorInBatchDelete = registry.counter(MetricRegistry.name(requestClass, "IDDeletedErrorInBatchDelete"));
    idPurgedError = registry.counter(MetricRegistry.name(requestClass, "IDPurgedError"));
    idUndeletedError = registry.counter(MetricRegistry.name(requestClass, "IDUndeletedError"));
    idNotDeletedError = registry.counter(MetricRegistry.name(requestClass, "IDNotDeletedError"));
    lifeVersionConflictError = registry.counter(MetricRegistry.name(requestClass, "lifeVersionConflictError"));
    ttlExpiredError = registry.counter(MetricRegistry.name(requestClass, "TTLExpiredError"));
    ttlExpiredErrorInBatchDelete = registry.counter(MetricRegistry.name(requestClass, "TTLExpiredErrorInBatchDelete"));
    temporarilyDisabledError = registry.counter(MetricRegistry.name(requestClass, "TemporarilyDisabledError"));
    badRequestError = registry.counter(MetricRegistry.name(requestClass, "BadRequestError"));
    unExpectedStorePutError = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStorePutError"));
    unExpectedStoreGetError = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreGetError"));
    unExpectedStoreDeleteError = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreDeleteError"));
    unExpectedStoreDeleteErrorInBatchDelete = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreDeleteErrorInBatchDelete"));
    unExpectedStoreBatchDeleteError = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreBatchDeleteError"));
    unExpectedStorePurgeError =
        registry.counter(MetricRegistry.name(requestClass, "UnExpectedStorePurgeError"));
    unExpectedStoreUndeleteError = registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreUndeleteError"));
    unExpectedAdminOperationError =
        registry.counter(MetricRegistry.name(requestClass, "UnexpectedAdminOperationError"));
    unExpectedStoreTtlUpdateError =
        registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreTtlUpdateError"));
    unExpectedStoreFindEntriesError =
        registry.counter(MetricRegistry.name(requestClass, "UnexpectedStoreFindEntriesError"));
    getAuthorizationFailure = registry.counter(MetricRegistry.name(requestClass, "GetAuthorizationFailure"));
    deleteAuthorizationFailure = registry.counter(MetricRegistry.name(requestClass, "DeleteAuthorizationFailure"));
    deleteAuthorizationFailureInBatchDelete = registry.counter(MetricRegistry.name(requestClass, "DeleteAuthorizationFailureInBatchDelete"));
    batchDeleteOperationError = registry.counter(MetricRegistry.name(requestClass, "BatchDeleteOperationError"));
    purgeAuthorizationFailure =
        registry.counter(MetricRegistry.name(requestClass, "PurgeAuthorizationFailure"));
    undeleteAuthorizationFailure = registry.counter(MetricRegistry.name(requestClass, "UndeleteAuthorizationFailure"));
    ttlUpdateAuthorizationFailure =
        registry.counter(MetricRegistry.name(requestClass, "TtlUpdateAuthorizationFailure"));
    ttlAlreadyUpdatedError = registry.counter(MetricRegistry.name(requestClass, "TtlAlreadyUpdatedError"));
    ttlUpdateRejectedError = registry.counter(MetricRegistry.name(requestClass, "TtlUpdateRejectedError"));
    replicationResponseMessageSizeTooHigh =
        registry.counter(MetricRegistry.name(requestClass, "ReplicationResponseMessageSizeTooHigh"));
    serverValidateConnectionSuccess =
        registry.counter(MetricRegistry.name(requestClass, "ServerValidateConnectionSuccess"));
    serverValidateConnectionFailure =
        registry.counter(MetricRegistry.name(requestClass, "ServerValidateConnectionFailure"));
  }

  public void registerRequestQueuesMetrics(Gauge<Integer> activeRequestsQueueSize) {
    this.activeRequestsQueueSize = registry.gauge(MetricRegistry.name(ServerMetrics.class, "ActiveRequestsQueueSize"),
        () -> activeRequestsQueueSize);
  }

  public void registerParticipantsMismatchMetrics() {
    sealedReplicasMismatchCount =
        registry.counter(MetricRegistry.name(ServerMetrics.class, "SealedReplicasMismatchCount"));
    partiallySealedReplicasMismatchCount =
        registry.counter(MetricRegistry.name(ServerMetrics.class, "PartiallySealedReplicasMismatchCount"));
    stoppedReplicasMismatchCount =
        registry.counter(MetricRegistry.name(ServerMetrics.class, "StoppedReplicasMismatchCount"));
  }

  public void updateCrossColoFetchBytesRate(String dcName, long bytes) {
    crossColoFetchBytesRate.computeIfAbsent(dcName,
        dc -> registry.meter(MetricRegistry.name(requestClass, dcName + "-CrossColoFetchBytesRate"))).mark(bytes);
  }

  public void updateCrossColoMetadataExchangeBytesRate(String dcName, long bytes) {
    crossColoMetadataExchangeBytesRate.computeIfAbsent(dcName,
        dc -> registry.meter(MetricRegistry.name(requestClass, dcName + "-CrossColoMetadataExchangeBytesRate")))
        .mark(bytes);
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
