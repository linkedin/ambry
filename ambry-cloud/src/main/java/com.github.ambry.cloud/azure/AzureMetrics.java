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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


public class AzureMetrics {

  // Metric name constants
  public static final String BLOB_UPLOAD_REQUEST_COUNT = "BlobUploadRequestCount";
  public static final String BLOB_UPLOADED_COUNT = "BlobUploadedCount";
  public static final String BLOB_UPLOAD_TIME = "BlobUploadTime";
  public static final String BLOB_UPDATE_TIME = "BlobUpdateTime";
  public static final String BLOB_UPDATED_COUNT = "BlobUpdatedCount";
  public static final String DOCUMENT_CREATE_TIME = "DocumentCreateTime";
  public static final String DOCUMENT_UPDATE_TIME = "DocumentUpdateTime";
  public static final String DOCUMENT_QUERY_TIME = "DocumentQueryTime";
  public static final String DOCUMENT_QUERY_COUNT = "DocumentErrorCount";
  public static final String BLOB_UPLOAD_ERROR_COUNT = "BlobUploadErrorCount";
  public static final String BLOB_UPDATE_ERROR_COUNT = "BlobUpdateErrorCount";
  public static final String STORAGE_ERROR_COUNT = "StorageErrorCount";
  public static final String DOCUMENT_ERROR_COUNT = "DocumentErrorCount";
  public static final String BLOB_DELETE_REQUEST_COUNT = "BlobDeleteRequestCount";
  public static final String BLOB_DELETED_COUNT = "BlobDeletedCount";
  public static final String BLOB_DELETION_TIME = "BlobDeletionTime";
  public static final String BLOB_DELETE_ERROR_COUNT = "BlobDeleteErrorCount";
  public static final String BLOB_SKIPPED_COUNT = "BlobSkippedCount";
  public static final String CONFIG_ERROR_COUNT = "ConfigErrorCount";
  public static final String BLOB_UPLOAD_RATE = "BlobUploadRate";

  // Metrics
  public final Counter blobUploadRequestCount;
  public final Counter blobUploadedCount;
  public final Counter blobUpdatedCount;
  public final Timer blobUploadTime;
  public final Timer blobUpdateTime;
  public final Timer documentCreateTime;
  public final Timer documentUpdateTime;
  public final Timer documentQueryTime;
  public final Counter documentQueryCount;
  public final Counter blobUploadErrorCount;
  public final Counter blobUpdateErrorCount;
  public final Counter storageErrorCount;
  public final Counter documentErrorCount;
  public final Counter blobDeleteRequestCount;
  public final Counter blobDeletedCount;
  public final Timer blobDeletionTime;
  public final Counter blobDeleteErrorCount;
  public final Counter blobSkippedCount;
  public final Counter configErrorCount;
  public final Meter blobUploadRate;

  public AzureMetrics(MetricRegistry registry) {
    blobUploadRequestCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOAD_REQUEST_COUNT));
    blobUploadedCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOADED_COUNT));
    blobUpdatedCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATED_COUNT));
    blobUploadTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOAD_TIME));
    blobUpdateTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATE_TIME));
    documentCreateTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_CREATE_TIME));
    documentUpdateTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_UPDATE_TIME));
    documentQueryTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_QUERY_TIME));
    documentQueryCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_QUERY_COUNT));
    blobUploadErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOAD_ERROR_COUNT));
    blobUpdateErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPDATE_ERROR_COUNT));
    storageErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, STORAGE_ERROR_COUNT));
    documentErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, DOCUMENT_ERROR_COUNT));
    blobDeleteRequestCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_DELETE_REQUEST_COUNT));
    blobDeletedCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_DELETED_COUNT));
    blobDeletionTime = registry.timer(MetricRegistry.name(AzureCloudDestination.class, BLOB_DELETION_TIME));
    blobDeleteErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_DELETE_ERROR_COUNT));
    blobSkippedCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, BLOB_SKIPPED_COUNT));
    configErrorCount = registry.counter(MetricRegistry.name(AzureCloudDestination.class, CONFIG_ERROR_COUNT));
    blobUploadRate = registry.meter(MetricRegistry.name(AzureCloudDestination.class, BLOB_UPLOAD_RATE));
  }
}
