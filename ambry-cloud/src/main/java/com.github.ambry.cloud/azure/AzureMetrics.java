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
  public static final String DOCUMENT_CREATE_TIME = "DocumentCreateTime";
  public static final String DOCUMENT_READ_TIME = "DocumentReadTime";
  public static final String DOCUMENT_UPDATE_TIME = "DocumentUpdateTime";
  public static final String DOCUMENT_DELETE_TIME = "DocumentDeleteTime";
  public static final String DOCUMENT_QUERY_COUNT = "DocumentQueryCount";
  public static final String MISSING_KEYS_QUERY_TIME = "MissingKeysQueryTime";
  public static final String DEAD_BLOBS_QUERY_TIME = "DeadBlobsQueryTime";
  public static final String FIND_SINCE_QUERY_TIME = "FindSinceQueryTime";
  public static final String BLOB_UPDATE_ERROR_COUNT = "BlobUpdateErrorCount";
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
  public static final String RETRY_COUNT = "RetryCount";
  public static final String RETRY_WAIT_TIME = "RetryWaitTime";

  // Metrics
  public final Counter blobUploadRequestCount;
  public final Counter blobUploadSuccessCount;
  public final Counter blobDownloadRequestCount;
  public final Counter blobDownloadSuccessCount;
  public final Counter blobDownloadErrorCount;
  public final Counter blobUploadConflictCount;
  public final Counter blobUpdatedCount;
  public final Timer blobUploadTime;
  public final Timer blobDownloadTime;
  public final Timer blobUpdateTime;
  public final Timer documentCreateTime;
  public final Timer documentReadTime;
  public final Timer documentUpdateTime;
  public final Timer documentDeleteTime;
  public final Timer missingKeysQueryTime;
  public final Counter documentQueryCount;
  public final Timer deadBlobsQueryTime;
  public final Timer findSinceQueryTime;
  public final Counter blobUpdateErrorCount;
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
  public final Counter retryCount;
  public final Timer retryWaitTime;

  public AzureMetrics(MetricRegistry registry) {
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
    blobUploadTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOAD_TIME));
    blobDownloadTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_DOWNLOAD_TIME));
    blobUpdateTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATE_TIME));
    documentCreateTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_CREATE_TIME));
    documentReadTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_READ_TIME));
    documentUpdateTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_UPDATE_TIME));
    documentDeleteTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_DELETE_TIME));
    documentQueryCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_QUERY_COUNT));
    missingKeysQueryTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, MISSING_KEYS_QUERY_TIME));
    deadBlobsQueryTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DEAD_BLOBS_QUERY_TIME));
    findSinceQueryTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, FIND_SINCE_QUERY_TIME));
    blobUpdateErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATE_ERROR_COUNT));
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
    retryCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, RETRY_COUNT));
    retryWaitTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, RETRY_WAIT_TIME));
  }
}
