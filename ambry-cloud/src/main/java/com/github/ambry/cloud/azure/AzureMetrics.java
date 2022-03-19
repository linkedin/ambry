/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


public class AzureMetrics {

  // Metric name constants
  public static final String BLOB_UPLOAD_REQUEST_COUNT = "BlobUploadRequestCount";
  public static final String BLOB_UPLOAD_SUCCESS_COUNT = "BlobUploadSuccessCount";
  public static final String BLOB_DOWNLOAD_REQUEST_COUNT = "BlobDownloadRequestCount";
  public static final String BLOB_DOWNLOAD_SUCCESS_COUNT = "BlobDownloadSuccessCount";
  public static final String BLOB_DOWNLOAD_ERROR_COUNT = "BlobDownloadErrorCount";
  public static final String BLOB_UPLOAD_CONFLICT_COUNT = "BlobUploadConflictCount";
  public static final String BLOB_UPLOAD_TIME = "BlobUploadTime";
  public static final String BLOB_DOWNLOAD_TIME = "BlobDownloadTime";
  public static final String BLOB_UPDATE_TIME = "BlobUpdateTime";
  public static final String BLOB_UPDATED_COUNT = "BlobUpdatedCount";
  public static final String BLOB_UPDATE_CONFLICT_COUNT = "BlobUpdateConflictCount";
  public static final String DOCUMENT_CREATE_TIME = "DocumentCreateTime";
  public static final String CONTAINER_DELETION_DOCUMENT_CREATE_TIME = "ContainerDeletionDocumentCreateTime";
  public static final String DOCUMENT_READ_TIME = "DocumentReadTime";
  public static final String CONTAINER_DELETION_ENTRY_READ_TIME = "ContainerDeletionEntryReadTime";
  public static final String DOCUMENT_UPDATE_TIME = "DocumentUpdateTime";
  public static final String DOCUMENT_DELETE_TIME = "DocumentDeleteTime";
  public static final String DOCUMENT_QUERY_COUNT = "DocumentQueryCount";
  public static final String CHANGEFEED_QUERY_COUNT = "ChangeFeedQueryCount";
  public static final String CHANGEFEED_QUERY_FAILURE_COUNT = "ChangeFeedFailureQueryCount";
  public static final String MISSING_KEYS_QUERY_TIME = "MissingKeysQueryTime";
  public static final String CHANGE_FEED_QUERY_TIME = "ChangeFeedQueryTime";
  public static final String REPLICATION_FEED_QUERY_TIME = "ReplicationFeedQueryTime";
  public static final String CHANGE_FEED_CACHE_HIT_RATE = "ChangeFeedCacheHitRate";
  public static final String CHANGE_FEED_CACHE_MISS_RATE = "ChangeFeedCacheMissRate";
  public static final String CHANGE_FEED_CACHE_REFRESH_RATE = "ChangeFeedCacheRefreshRate";
  public static final String DEAD_BLOBS_QUERY_TIME = "DeadBlobsQueryTime";
  public static final String DELETED_CONTAINER_BLOBS_QUERY_TIME = "DeletedContainerBlobsQueryTime";
  public static final String FIND_SINCE_QUERY_TIME = "FindSinceQueryTime";
  public static final String BLOB_UPDATE_ERROR_COUNT = "BlobUpdateErrorCount";
  public static final String BLOB_UPDATE_RECOVER_COUNT = "BlobUpdateRecoverCount";
  public static final String STORAGE_ERROR_COUNT = "StorageErrorCount";
  public static final String DOCUMENT_ERROR_COUNT = "DocumentErrorCount";
  public static final String BLOB_DELETE_REQUEST_COUNT = "BlobDeleteRequestCount";
  public static final String BLOB_DELETED_COUNT = "BlobDeletedCount";
  public static final String BLOB_DELETION_TIME = "BlobDeletionTime";
  public static final String BLOB_DELETE_ERROR_COUNT = "BlobDeleteErrorCount";
  public static final String CONFIG_ERROR_COUNT = "ConfigErrorCount";
  public static final String BACKUP_SUCCESS_LATENCY = "BackupSuccessLatency";
  public static final String BACKUP_SUCCESS_BYTE_RATE = "BackupSuccessByteRate";
  public static final String BACKUP_ERROR_COUNT = "BackupErrorCount";
  public static final String COMPACTION_PROGRESS_READ_ERROR_COUNT = "CompactionProgressReadErrorCount";
  public static final String COMPACTION_PROGRESS_WRITE_ERROR_COUNT = "CompactionProgressWriteErrorCount";
  public static final String ABS_TOKEN_REFRESH_ATTEMPT_COUNT = "ABSTokenRefreshAttemptCount";
  public static final String ABS_TOKEN_PERSIST_FAILURE_COUNT = "ABSTokenPersistFailureCount";
  public static final String ABS_FORBIDDEN_EXCEPTION_COUNT = "ABSForbiddenExceptionCount";
  public static final String STORAGE_CLIENT_OPERATION_RETRY_COUNT = "StorageClientOperationRetryCount";
  public static final String STORAGE_CLIENT_OPERATION_EXCEPTION_COUNT = "StorageClientOperationExceptionCount";
  public static final String STORAGE_CLIENT_FAILURE_AFTER_RETRY_COUNT = "StorageClientOperationFailureAfterRetryCount";
  public static final String LAST_CONTAINER_DELETION_TIMESTAMP = "LastContainerDeletionTimestamp";
  public static final String DEPRECATED_CONTAINER_COMPACTION_FAILURE_COUNT =
      "DeprecatedContainerCompactionFailureCount";
  public static final String DEPRECATED_CONTAINER_COMPACTION_SUCCESS_COUNT =
      "DeprecatedContainerCompactionSuccessCount";

  // Metrics
  public final Counter blobUploadRequestCount;
  public final Counter blobUploadSuccessCount;
  public final Counter blobDownloadRequestCount;
  public final Counter blobDownloadSuccessCount;
  public final Counter blobDownloadErrorCount;
  public final Counter blobUploadConflictCount;
  public final Counter blobUpdatedCount;
  /** Attempts to update blob metadata that fail due to concurrent update (412) */
  public final Counter blobUpdateConflictCount;
  public final Timer blobUploadTime;
  public final Timer blobDownloadTime;
  public final Timer blobUpdateTime;
  public final Timer documentCreateTime;
  public final Timer containerDeprecationDocumentCreateTime;
  public final Timer documentReadTime;
  public final Timer continerDeletionEntryReadTime;
  public final Timer documentUpdateTime;
  public final Timer documentDeleteTime;
  public final Timer missingKeysQueryTime;
  public final Timer changeFeedQueryTime;
  public final Timer replicationFeedQueryTime;
  public final Meter changeFeedCacheHitRate;
  public final Meter changeFeedCacheMissRate;
  public final Meter changeFeedCacheRefreshRate;
  public final Counter documentQueryCount;
  public final Counter changeFeedQueryCount;
  public final Counter changeFeedQueryFailureCount;
  public final Timer deadBlobsQueryTime;
  public final Timer deletedContainerBlobsQueryTime;
  public final Timer findSinceQueryTime;
  public final Counter blobUpdateErrorCount;
  /* Tracks updates that recovered a missing Cosmos record */
  public final Counter blobUpdateRecoverCount;
  public final Counter storageErrorCount;
  public final Counter documentErrorCount;
  public final Counter blobDeleteRequestCount;
  public final Counter blobDeletedCount;
  public final Timer blobDeletionTime;
  public final Counter blobDeleteErrorCount;
  public final Counter configErrorCount;
  public final Timer backupSuccessLatency;
  public final Meter backupSuccessByteRate;
  public final Counter backupErrorCount;
  public final Counter compactionProgressReadErrorCount;
  public final Counter compactionProgressWriteErrorCount;
  public final Counter absTokenRefreshAttemptCount;
  public final Counter absTokenPersistFailureCount;
  public final Counter absForbiddenExceptionCount;
  public final Counter storageClientOperationRetryCount;
  public final Counter storageClientOperationExceptionCount;
  public final Counter storageClientFailureAfterRetryCount;
  public final Counter deprecatedContainerCompactionFailureCount;
  public final Counter deprecatedContainerCompactionSuccessCount;
  private final MetricRegistry metricRegistry;
  Gauge<Long> lastContainerDeletionTimestamp;

  public AzureMetrics(MetricRegistry registry) {
    this.metricRegistry = registry;

    blobUploadRequestCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOAD_REQUEST_COUNT));
    blobUploadSuccessCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOAD_SUCCESS_COUNT));
    blobDownloadRequestCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_DOWNLOAD_REQUEST_COUNT));
    blobDownloadSuccessCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_DOWNLOAD_SUCCESS_COUNT));
    blobDownloadErrorCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_DOWNLOAD_ERROR_COUNT));
    blobUploadConflictCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOAD_CONFLICT_COUNT));
    blobUpdatedCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATED_COUNT));
    blobUpdateConflictCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATE_CONFLICT_COUNT));
    blobUploadTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOAD_TIME));
    blobDownloadTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_DOWNLOAD_TIME));
    blobUpdateTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATE_TIME));
    documentCreateTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_CREATE_TIME));
    containerDeprecationDocumentCreateTime =
        registry.timer(MetricRegistry.name(AzureCloudDestination.class, CONTAINER_DELETION_DOCUMENT_CREATE_TIME));
    documentReadTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_READ_TIME));
    continerDeletionEntryReadTime =
        registry.timer(MetricRegistry.name(AzureContainerCompactor.class, CONTAINER_DELETION_ENTRY_READ_TIME));
    documentUpdateTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_UPDATE_TIME));
    documentDeleteTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_DELETE_TIME));
    documentQueryCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_QUERY_COUNT));
    changeFeedQueryCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, CHANGEFEED_QUERY_COUNT));
    changeFeedQueryFailureCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, CHANGEFEED_QUERY_FAILURE_COUNT));
    missingKeysQueryTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, MISSING_KEYS_QUERY_TIME));
    changeFeedQueryTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, CHANGE_FEED_QUERY_TIME));
    replicationFeedQueryTime =
        registry.timer(MetricRegistry.name(AzureCloudDestination.class, REPLICATION_FEED_QUERY_TIME));
    changeFeedCacheHitRate =
        registry.meter(MetricRegistry.name(AzureCloudDestination.class, CHANGE_FEED_CACHE_HIT_RATE));
    changeFeedCacheMissRate =
        registry.meter(MetricRegistry.name(AzureCloudDestination.class, CHANGE_FEED_CACHE_MISS_RATE));
    changeFeedCacheRefreshRate =
        registry.meter(MetricRegistry.name(AzureCloudDestination.class, CHANGE_FEED_CACHE_REFRESH_RATE));
    deadBlobsQueryTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DEAD_BLOBS_QUERY_TIME));
    deletedContainerBlobsQueryTime =
        registry.timer(MetricRegistry.name(AzureCloudDestination.class, DELETED_CONTAINER_BLOBS_QUERY_TIME));
    findSinceQueryTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, FIND_SINCE_QUERY_TIME));
    blobUpdateErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATE_ERROR_COUNT));
    blobUpdateRecoverCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATE_RECOVER_COUNT));
    storageErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, STORAGE_ERROR_COUNT));
    documentErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_ERROR_COUNT));
    blobDeleteRequestCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_DELETE_REQUEST_COUNT));
    blobDeletedCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_DELETED_COUNT));
    blobDeletionTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_DELETION_TIME));
    blobDeleteErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_DELETE_ERROR_COUNT));
    configErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, CONFIG_ERROR_COUNT));
    backupSuccessByteRate = registry.meter(MetricRegistry.name(AzureCloudDestination.class, BACKUP_SUCCESS_BYTE_RATE));
    backupSuccessLatency = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BACKUP_SUCCESS_LATENCY));
    backupErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BACKUP_ERROR_COUNT));
    compactionProgressReadErrorCount =
        registry.counter(MetricRegistry.name(AzureStorageCompactor.class, COMPACTION_PROGRESS_READ_ERROR_COUNT));
    compactionProgressWriteErrorCount =
        registry.counter(MetricRegistry.name(AzureStorageCompactor.class, COMPACTION_PROGRESS_WRITE_ERROR_COUNT));
    absTokenRefreshAttemptCount =
        registry.counter(MetricRegistry.name(StorageClient.class, ABS_TOKEN_REFRESH_ATTEMPT_COUNT));
    absTokenPersistFailureCount =
        registry.counter(MetricRegistry.name(AzureCloudDestination.class, ABS_TOKEN_PERSIST_FAILURE_COUNT));
    absForbiddenExceptionCount =
        registry.counter(MetricRegistry.name(StorageClient.class, ABS_FORBIDDEN_EXCEPTION_COUNT));
    storageClientOperationRetryCount =
        registry.counter(MetricRegistry.name(StorageClient.class, STORAGE_CLIENT_OPERATION_RETRY_COUNT));
    storageClientOperationExceptionCount =
        registry.counter(MetricRegistry.name(StorageClient.class, STORAGE_CLIENT_OPERATION_EXCEPTION_COUNT));
    storageClientFailureAfterRetryCount =
        registry.counter(MetricRegistry.name(StorageClient.class, STORAGE_CLIENT_FAILURE_AFTER_RETRY_COUNT));
    deprecatedContainerCompactionFailureCount = registry.counter(
        MetricRegistry.name(AzureContainerCompactor.class, DEPRECATED_CONTAINER_COMPACTION_FAILURE_COUNT));
    deprecatedContainerCompactionSuccessCount = registry.counter(
        MetricRegistry.name(AzureContainerCompactor.class, DEPRECATED_CONTAINER_COMPACTION_SUCCESS_COUNT));
  }

  /**
   * Tracks the timestamp upto which the cloud container compaction has caught up with the account service.
   * @param azureContainerCompactor {@link AzureContainerCompactor} object.
   */
  public void trackLatestContainerDeletionTimestamp(AzureContainerCompactor azureContainerCompactor) {
    lastContainerDeletionTimestamp = metricRegistry.gauge(MetricRegistry.name(AzureContainerCompactor.class, LAST_CONTAINER_DELETION_TIMESTAMP),
        () -> azureContainerCompactor::getLatestContainerDeletionTimestamp);
  }
}
