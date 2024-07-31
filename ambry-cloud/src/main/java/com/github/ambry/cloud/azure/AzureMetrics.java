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

// extend AzureMetricsUnused to not break old-code
public class AzureMetrics extends AzureMetricsUnused {

  // Metric name constants
  public static final String BLOB_UPLOAD_SUCCESS_RATE = "BlobUploadSuccessRate";
  public static final String BLOB_UPDATE_TTL_SUCCESS_RATE = "BlobUpdateTTLSuccessRate";
  public static final String BLOB_UPDATE_TTL_LATENCY = "blobUpdateTTLLatency";
  public static final String BLOB_UPDATE_TTL_ERROR_COUNT = "BlobUpdateTTLErrorCount";
  public static final String BLOB_UPDATE_DELETE_TIME_SUCCESS_RATE = "BlobUpdateDeleteTimeSuccessRate";
  public static final String BLOB_UPDATE_DELETE_TIME_LATENCY = "blobUpdateDeleteTimeLatency";
  public static final String BLOB_UPDATE_DELETE_TIME_ERROR_COUNT = "BlobUpdateDeleteTimeErrorCount";
  public static final String BLOB_UNDELETE_SUCCESS_RATE = "BlobUndeleteTimeSuccessRate";
  public static final String BLOB_UNDELETE_LATENCY = "blobUndeleteLatency";
  public static final String BLOB_UNDELETE_ERROR_COUNT = "blobUndeleteErrorCount";
  public static final String BLOB_UPLOAD_ERROR_COUNT = "BlobUploadErrorCount";
  public static final String BLOB_GET_PROPERTIES_SUCCESS_RATE = "BlobGetPropertiesSuccessRate";
  public static final String BLOB_GET_PROPERTIES_LATENCY = "BlobGetPropertiesLatency";
  public static final String BLOB_GET_PROPERTIES_ERROR_COUNT = "BlobGetPropertiesErrorCount";
  public static final String BLOB_DOWNLOAD_SUCCESS_RATE = "BlobDownloadSuccessRate";
  public static final String BLOB_DOWNLOAD_ERROR_COUNT = "BlobDownloadErrorCount";
  public static final String BLOB_UPLOAD_CONFLICT_COUNT = "BlobUploadConflictCount";
  public static final String BLOB_UPLOAD_LATENCY = "BlobUploadLatency";
  public static final String BLOB_DOWNLOAD_LATENCY = "BlobDownloadLatency";
  public static final String BLOB_UPLOAD_BYTE_RATE = "BlobUploadByteRate";
  public static final String PARTITION_COMPACTION_ERROR_COUNT = "PartitionCompactionErrorCount";
  public static final String BLOB_COMPACTION_ERROR_COUNT = "BlobCompactionErrorCount";
  public static final String BLOB_COMPACTION_SUCCESS_RATE = "BlobCompactionSuccessRate";
  public static final String REPLICA_TOKEN_WRITE_ERROR_COUNT = "ReplicaTokenWriteErrorCount";
  public static final String REPLICA_TOKEN_READ_ERROR_COUNT = "ReplicaTokenReadErrorCount";
  public static final String BLOB_CONTAINER_ERROR_COUNT = "BlobContainerErrorCount";
  public static final String BLOB_COMPACTION_LATENCY = "BlobCompactionLatency";
  public static final String PARTITION_COMPACTION_LATENCY = "PartitionCompactionLatency";
  public static final String TABLE_CREATE_ERROR_COUNT = "TableCreateErrorCount";
  public static final String TABLE_ENTITY_CREATE_ERROR_COUNT = "TableEntityCreateErrorCount";
  public static final String REPLICA_TOKEN_WRITE_RATE = "ReplicaTokenWriteRate";
  public static final String COMPACTION_TASK_ERROR_COUNT = "CompactionTaskErrorCount";
  public static final String BLOB_CHECK_ERROR = "BlobCheckError";
  public static final String BLOB_BATCH_UPLOAD_LATENCY = "BlobBatchUploadLatency";

  // Azure Storage metrics
  public final Counter blobContainerErrorCount;
  public final Timer blobCompactionLatency;
  public final Timer partitionCompactionLatency;
  public final Meter blobUploadSuccessRate;
  public final Meter blobUpdateDeleteTimeSuccessRate;
  public final Timer blobUpdateDeleteTimeLatency;
  public final Counter blobUpdateDeleteTimeErrorCount;
  public final Meter blobUndeleteSucessRate;
  public final Timer blobUndeleteLatency;
  public final Counter blobUndeleteErrorCount;
  public final Meter blobUpdateTTLSucessRate;
  public final Timer blobUpdateTTLLatency;
  public final Counter blobUpdateTTLErrorCount;
  public final Meter blobGetPropertiesSuccessRate;
  public final Timer blobGetPropertiesLatency;
  public final Counter blobGetPropertiesErrorCount;
  public final Counter blobUploadErrorCount;
  public final Meter blobDownloadSuccessRate;
  public final Counter blobDownloadErrorCount;
  public final Counter blobUploadConflictCount;
  public final Timer blobUploadLatency;
  public final Meter blobUploadByteRate;
  public final Counter blobCompactionErrorCount;
  public final Meter blobCompactionSuccessRate;
  public final Counter partitionCompactionErrorCount;
  public final Counter replicaTokenWriteErrorCount;
  public final Counter replicaTokenReadErrorCount;
  public final Counter tableCreateErrorCount;
  public final Counter tableEntityCreateErrorCount;
  public final Meter replicaTokenWriteRate;
  public final Counter compactionTaskErrorCount;
  public final Counter blobCheckError;
  public final Timer blobBatchUploadLatency;
  public final Timer blobDownloadLatency;

  public AzureMetrics(MetricRegistry registry) {
    super(registry);
    // V2 metrics
    // These are registered in the closed-source version of Ambry
    blobBatchUploadLatency = registry.timer(MetricRegistry.name(AzureMetrics.class, BLOB_BATCH_UPLOAD_LATENCY));
    blobCheckError = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_CHECK_ERROR));
    blobCompactionErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_COMPACTION_ERROR_COUNT));
    blobCompactionLatency = registry.timer(MetricRegistry.name(AzureMetrics.class, BLOB_COMPACTION_LATENCY));
    blobCompactionSuccessRate = registry.meter(MetricRegistry.name(AzureMetrics.class, BLOB_COMPACTION_SUCCESS_RATE));
    blobContainerErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_CONTAINER_ERROR_COUNT));
    blobDownloadErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_DOWNLOAD_ERROR_COUNT));
    blobDownloadLatency = registry.timer(MetricRegistry.name(AzureMetrics.class, BLOB_DOWNLOAD_LATENCY));
    blobDownloadSuccessRate = registry.meter(MetricRegistry.name(AzureMetrics.class, BLOB_DOWNLOAD_SUCCESS_RATE));
    blobGetPropertiesErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_GET_PROPERTIES_ERROR_COUNT));
    blobGetPropertiesLatency = registry.timer(MetricRegistry.name(AzureMetrics.class, BLOB_GET_PROPERTIES_LATENCY));;
    blobGetPropertiesSuccessRate = registry.meter(MetricRegistry.name(AzureMetrics.class, BLOB_GET_PROPERTIES_SUCCESS_RATE));
    blobUndeleteErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_UNDELETE_ERROR_COUNT));
    blobUndeleteLatency = registry.timer(MetricRegistry.name(AzureMetrics.class, BLOB_UNDELETE_LATENCY));
    blobUndeleteSucessRate = registry.meter(MetricRegistry.name(AzureMetrics.class, BLOB_UNDELETE_SUCCESS_RATE));
    blobUpdateDeleteTimeErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_UPDATE_DELETE_TIME_ERROR_COUNT));
    blobUpdateDeleteTimeLatency = registry.timer(MetricRegistry.name(AzureMetrics.class, BLOB_UPDATE_DELETE_TIME_LATENCY));
    blobUpdateDeleteTimeSuccessRate = registry.meter(MetricRegistry.name(AzureMetrics.class, BLOB_UPDATE_DELETE_TIME_SUCCESS_RATE));
    blobUpdateTTLErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_UPDATE_TTL_ERROR_COUNT));
    blobUpdateTTLLatency = registry.timer(MetricRegistry.name(AzureMetrics.class, BLOB_UPDATE_TTL_LATENCY));
    blobUpdateTTLSucessRate = registry.meter(MetricRegistry.name(AzureMetrics.class, BLOB_UPDATE_TTL_SUCCESS_RATE));
    blobUploadByteRate = registry.meter(MetricRegistry.name(AzureMetrics.class, BLOB_UPLOAD_BYTE_RATE));
    blobUploadConflictCount = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_UPLOAD_CONFLICT_COUNT));
    blobUploadErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, BLOB_UPLOAD_ERROR_COUNT));
    blobUploadLatency = registry.timer(MetricRegistry.name(AzureMetrics.class, BLOB_UPLOAD_LATENCY));
    blobUploadSuccessRate = registry.meter(MetricRegistry.name(AzureMetrics.class, BLOB_UPLOAD_SUCCESS_RATE));
    compactionTaskErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, COMPACTION_TASK_ERROR_COUNT));
    partitionCompactionErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, PARTITION_COMPACTION_ERROR_COUNT));
    partitionCompactionLatency = registry.timer(MetricRegistry.name(AzureMetrics.class, PARTITION_COMPACTION_LATENCY));
    replicaTokenReadErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, REPLICA_TOKEN_READ_ERROR_COUNT));
    replicaTokenWriteErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, REPLICA_TOKEN_WRITE_ERROR_COUNT));
    replicaTokenWriteRate = registry.meter(MetricRegistry.name(AzureMetrics.class, REPLICA_TOKEN_WRITE_RATE));
    tableCreateErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, TABLE_CREATE_ERROR_COUNT));
    tableEntityCreateErrorCount = registry.counter(MetricRegistry.name(AzureMetrics.class, TABLE_ENTITY_CREATE_ERROR_COUNT));
  }

}
