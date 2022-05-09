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

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.commons.BlobId;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface to encapsulate ABS client operations.
 */
public interface AzureStorageClient {
  /**
   * Visible for testing.
   * @return the underlying {@link BlobServiceClient}.
   */
  BlobServiceAsyncClient getStorageClient();

  /**
   * Creates a new block blob, or updates the content of an existing block blob asynchronously.
   * @param blobId {@link BlobId} of the blob to upload.
   * @param data The data to write to the blob.
   * @param length The exact length of the data. It is important that this value match precisely the length of the
   * data provided in the {@link InputStream}.
   * @param headers {@link BlobHttpHeaders}
   * @param metadata Metadata to associate with the blob.
   * @param tier {@link AccessTier} for the destination blob.
   * @param contentMd5 An MD5 hash of the block content.
   * @param requestConditions {@link BlobRequestConditions}
   * @return a {@link CompletableFuture} of type {@link Void} that will eventually complete when block blob is uploaded
   *         or will contain an exception if an error occurred.
   */
  CompletableFuture<Void> uploadWithResponse(BlobId blobId, InputStream data, long length,
      BlobHttpHeaders headers, Map<String, String> metadata, AccessTier tier, byte[] contentMd5,
      BlobRequestConditions requestConditions);

  /**
   * Creates a new block blob, or updates the content of an existing block blob asynchronously.
   * @param containerName name of the Azure container where the blob lives.
   * @param blobName name of the blob.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @param data The data to write to the blob.
   * @param length The exact length of the data. It is important that this value match precisely the length of the
   * data provided in the {@link InputStream}.
   * @param headers {@link BlobHttpHeaders}
   * @param metadata Metadata to associate with the blob.
   * @param tier {@link AccessTier} for the destination blob.
   * @param contentMd5 An MD5 hash of the block content.
   * @param requestConditions {@link BlobRequestConditions}
   * @return a {@link CompletableFuture} of type {@link Void} that will eventually complete when block blob is uploaded
   *         or an exception if an error occurred.
   */
   CompletableFuture<Void> uploadWithResponse(String containerName, String blobName, boolean autoCreateContainer,
      InputStream data, long length, BlobHttpHeaders headers, Map<String, String> metadata, AccessTier tier,
      byte[] contentMd5, BlobRequestConditions requestConditions);

  /**
   * Downloads a range of bytes from a blob into an output stream.
   * @param containerName name of the Azure container where the blob lives.
   * @param blobName name of the blob.
   * @param blobId Ambry {@link BlobId} associated with the {@code fileName}.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @param stream A non-null {@link OutputStream} instance where the downloaded data will be written.
   * @param range {@link BlobRange}
   * @param options {@link DownloadRetryOptions}
   * @param requestConditions {@link BlobRequestConditions}
   * @param getRangeContentMd5 Whether the contentMD5 for the specified blob range should be returned.
   * @throws UncheckedIOException If an I/O error occurs.
   * @throws NullPointerException if {@code stream} is null
   */
  CompletableFuture<Void> downloadWithResponse(String containerName, String blobName, BlobId blobId,
      boolean autoCreateContainer, OutputStream stream, BlobRange range, DownloadRetryOptions options,
      BlobRequestConditions requestConditions, boolean getRangeContentMd5);

  /**
   * Returns the blob's metadata and properties.
   * @param blobId {@link BlobId}
   * @param requestConditions {@link BlobRequestConditions}
   * @return a {@link CompletableFuture} that will eventually contain blob properties and metadata or an exception
   *         if an error occurred.
   */
  CompletableFuture<BlobProperties> getPropertiesWithResponse(BlobId blobId,
      BlobRequestConditions requestConditions);

  /**
   * Changes a blob's metadata. The specified metadata in this method will replace existing metadata. If old values
   * must be preserved, they must be downloaded and included in the call to this method.
   * @param blobId {@link BlobId} object.
   * @param metadata Metadata to associate with the blob.
   * @param requestConditions {@link BlobRequestConditions}
   * @param context Additional context that is passed through the Http pipeline during the service call.
   * @return a {@link CompletableFuture} of type {@link Void} that will eventually complete successfully when blob
   *         metadata is changed or completes exceptionally if an error occurred.
   */
  CompletableFuture<Void> setMetadataWithResponse(BlobId blobId, Map<String, String> metadata,
      BlobRequestConditions requestConditions, Context context);

  /**
   * Deletes a list of blobs asynchronously.
   * @param batchOfBlobs {@link List} of {@link CloudBlobMetadata} objects.
   * @return {@link List} of {@link Response}s for the blobs in the batch.
   * @return a {@link CompletableFuture} that will eventually contain {@link List} of {@link Response}s for the blobs
   *         in the batch or an exception if an error occurred.
   */
  CompletableFuture<List<Response<Void>>>  deleteBatch(List<CloudBlobMetadata> batchOfBlobs);

  /**
   * Delete a file from blob storage asynchronously, if it exists.
   * @param containerName name of the container containing file to delete.
   * @param fileName name of the file to delete.
   * @return a {@link CompletableFuture} of type {@link Boolean} that will eventually complete successfully when the file
   *         is deleted or will complete exceptionally if an error occurs.
   */
  CompletableFuture<Boolean> deleteFile(String containerName, String fileName) throws BlobStorageException;

  /**
   * Perform basic connectivity test.
   */
  void testConnectivity();
}
