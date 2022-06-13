/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudStorageException;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Utility class to help with blob compaction.
 */
public class AzureCompactionUtil {
  /**
   * Permanently delete the specified blobs in Azure storage.
   * @param blobMetadataList the list of {@link CloudBlobMetadata} referencing the blobs to purge.
   * @param azureBlobDataAccessor {@link AzureBlobDataAccessor} object for calls to ABS.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param cosmosDataAccessor {@link CosmosDataAccessor} object for calls to Cosmos Db.
   * @return the number of blobs successfully purged.
   * @throws CloudStorageException if the purge operation fails for any blob.
   */
  static int purgeBlobs(List<CloudBlobMetadata> blobMetadataList, AzureBlobDataAccessor azureBlobDataAccessor,
      AzureMetrics azureMetrics, CosmosDataAccessor cosmosDataAccessor) throws CloudStorageException {
    if (blobMetadataList.isEmpty()) {
      return 0;
    }
    azureMetrics.blobDeleteRequestCount.inc(blobMetadataList.size());
    long t0 = System.currentTimeMillis();
    try {
      List<CloudBlobMetadata> deletedBlobs = azureBlobDataAccessor.purgeBlobsAsync(blobMetadataList).join();
      long t1 = System.currentTimeMillis();
      int deletedCount = deletedBlobs.size();
      azureMetrics.blobDeleteErrorCount.inc(blobMetadataList.size() - deletedCount);
      if (deletedCount > 0) {
        // Record as time per single blob deletion
        azureMetrics.blobDeletionTime.update((t1 - t0) / deletedCount, TimeUnit.MILLISECONDS);
      } else {
        // Note: should not get here since purgeBlobs throws exception if any blob exists and could not be deleted.
        return 0;
      }

      // Remove them from Cosmos too
      cosmosDataAccessor.deleteMetadata(deletedBlobs);
      long t2 = System.currentTimeMillis();
      // Record as time per single record deletion
      azureMetrics.documentDeleteTime.update((t2 - t1) / deletedCount, TimeUnit.MILLISECONDS);
      azureMetrics.blobDeletedCount.inc(deletedCount);
      return deletedCount;
    } catch (Exception ex) {
      azureMetrics.blobDeleteErrorCount.inc(blobMetadataList.size());
      throw AzureCloudDestination.toCloudStorageException("Failed to purge all blobs", ex, azureMetrics);
    }
  }
}
