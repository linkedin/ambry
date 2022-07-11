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
package com.github.ambry.cloud;

import com.github.ambry.account.Container;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.FutureUtils;
import com.github.ambry.replication.FindToken;
import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * An interface representing an interaction with a cloud destination, that allows replicating blob operations.
 */
public interface CloudDestination extends Closeable {

  /**
   * Upload blob to the cloud destination.
   * @param blobId of the Ambry blob
   * @param inputLength the length of the input stream, if known, -1 if unknown.
   * @param cloudBlobMetadata the {@link CloudBlobMetadata} for the blob being uploaded.
   * @param blobInputStream the stream to read blob data
   * @return flag indicating whether the blob was uploaded
   * @throws CloudStorageException if the upload encounters an error.
   */
  boolean uploadBlob(BlobId blobId, long inputLength, CloudBlobMetadata cloudBlobMetadata, InputStream blobInputStream)
      throws CloudStorageException;

  /**
   * Upload blob to the cloud destination asynchronously.
   * @param blobId of the Ambry blob
   * @param inputLength the length of the input stream, if known, -1 if unknown.
   * @param cloudBlobMetadata the {@link CloudBlobMetadata} for the blob being uploaded.
   * @param blobInputStream the stream to read blob data
   * @return a {@link CompletableFuture} that will eventually contain {@link Boolean} flag indicating whether the blob
   *         was uploaded successfully or an exception if an error occurred.
   */
  CompletableFuture<Boolean> uploadBlobAsync(BlobId blobId, long inputLength, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream);

  /**
   * Download blob from the cloud destination.
   * @param blobId of the Ambry blob to be downloaded
   * @param outputStream outputstream to populate the downloaded data with
   * @throws CloudStorageException if the download encounters an error.
   */
  void downloadBlob(BlobId blobId, OutputStream outputStream) throws CloudStorageException;

  /**
   * Download blob from the cloud destination asynchronously.
   * @param blobId of the Ambry blob to be downloaded
   * @param outputStream outputstream to populate the downloaded data with
   * @return a {@link CompletableFuture} of type {@link Void} that will eventually complete when the blob was downloaded
   *        successfully or an exception if an error occurred.
   */
  CompletableFuture<Void> downloadBlobAsync(BlobId blobId, OutputStream outputStream);

  /**
   * Mark a blob as deleted in the cloud destination, if {@code lifeVersion} is less than or equal to life version of
   * the existing blob.
   * @param blobId of the Ambry blob
   * @param deletionTime time of blob deletion
   * @param lifeVersion life version of the blob to be deleted.
   * @param cloudUpdateValidator {@link CloudUpdateValidator} object passed by caller to validate the delete.
   * @return flag indicating whether the blob was deleted
   * @throws CloudStorageException if the deletion encounters an error.
   */
  boolean deleteBlob(BlobId blobId, long deletionTime, short lifeVersion, CloudUpdateValidator cloudUpdateValidator)
      throws CloudStorageException;

  /**
   * Mark a blob as deleted in the cloud destination asynchronously, if {@code lifeVersion} is less than or equal to
   * life version of the existing blob.
   * @param blobId of the Ambry blob
   * @param deletionTime time of blob deletion
   * @param lifeVersion life version of the blob to be deleted.
   * @param cloudUpdateValidator {@link CloudUpdateValidator} object passed by caller to validate the delete.
   * @return a {@link CompletableFuture} that will eventually contain {@link Boolean} flag indicating whether the blob
   *         was deleted successfully or an exception if an error occurred.
   */
  CompletableFuture<Boolean> deleteBlobAsync(BlobId blobId, long deletionTime, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator);

  /**
   * Undelete the blob from cloud destination, and update the new life version.
   * @param blobId of the Ambry blob.
   * @param lifeVersion new life version to update.
   * @param cloudUpdateValidator {@link CloudUpdateValidator} object passed by caller to validate the undelete.
   * @return final live version of the undeleted blob.
   * @throws CloudStorageException if the undelete encounters an error.
   */
  short undeleteBlob(BlobId blobId, short lifeVersion, CloudUpdateValidator cloudUpdateValidator)
      throws CloudStorageException;

  /**
   * Undelete the blob from cloud destination, and update the new life version asynchronously.
   * @param blobId of the Ambry blob.
   * @param lifeVersion new life version to update.
   * @param cloudUpdateValidator {@link CloudUpdateValidator} object passed by caller to validate the undelete.
   * @return a {@link CompletableFuture} that will eventually contain final live version of the undeleted blob if the
   *         blob was undeleted successfully or an exception if an error occurred.
   */
  CompletableFuture<Short> undeleteBlobAsync(BlobId blobId, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator);

  /**
   * Update expiration time of blob in the cloud destination.
   * @param blobId of the Ambry blob
   * @param expirationTime the new expiration time
   * @param cloudUpdateValidator {@link CloudUpdateValidator} object passed by caller to validate the update.
   * @return the life version of the blob if the blob was updated, otherwise -1.
   * @throws CloudStorageException if the update encounters an error.
   */
  short updateBlobExpiration(BlobId blobId, long expirationTime, CloudUpdateValidator cloudUpdateValidator)
      throws CloudStorageException;

  /**
   * Update expiration time of blob in the cloud destination asynchronously.
   * @param blobId of the Ambry blob
   * @param expirationTime the new expiration time
   * @param cloudUpdateValidator {@link CloudUpdateValidator} object passed by caller to validate the update.
   * @return a {@link CompletableFuture} that will eventually contain the life version of the blob if the blob was updated
   *         successfully, otherwise -1 or an exception if an error occurred.
   */
  CompletableFuture<Short> updateBlobExpirationAsync(BlobId blobId, long expirationTime,
      CloudUpdateValidator cloudUpdateValidator);

  /**
   * Query the blob metadata for the specified blobs.
   * @param blobIds list of blob Ids to query.
   * @return a {@link Map} of blobId strings to {@link CloudBlobMetadata}.  If metadata for a blob could not be found,
   * it will not be included in the returned map.
   */
  Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) throws CloudStorageException;

  /**
   * Query the blob metadata for the specified blobs asynchronously.
   * @param blobIds list of blob Ids to query.
   * @return a {@link CompletableFuture} that will eventually contain a {@link Map} of blobId strings to {@link CloudBlobMetadata}
   *         or an exception if an error occurred. If metadata for a blob could not be found, it will not be included in
   *         the returned map.
   */
  CompletableFuture<Map<String, CloudBlobMetadata>> getBlobMetadataAsync(List<BlobId> blobIds);

  /**
   * Returns an ordered sequenced list of blobs within the specified partition and updated
   * {@link com.github.ambry.replication.FindToken}, such that total size of all blobs in the list are less or equal to
   * {@code maxTotalSizeOfEntries}
   * @param partitionPath the partition to query.
   * @param findToken the {@link com.github.ambry.replication.FindToken} specifying the boundary for the query.
   * @param maxTotalSizeOfEntries the cumulative size limit for the list of blobs returned.
   * @return {@link FindResult} instance that contains updated {@link FindToken} object which can act as a bookmark for
   * subsequent requests, and {@link List} of {@link CloudBlobMetadata} entries referencing the blobs returned by the query.
   * @throws CloudStorageException
   */
  FindResult findEntriesSince(String partitionPath, FindToken findToken, long maxTotalSizeOfEntries)
      throws CloudStorageException;

  /**
   * Compact the specified partition, removing blobs that have been deleted or expired for at least the
   * configured retention period.
   * @param partitionPath the path of the partitions to compact.
   * @throws CloudStorageException
   */
  int compactPartition(String partitionPath) throws CloudStorageException;

  /**
   * Upload and persist the replica tokens for the specified Ambry partition in cloud storage.
   * @param partitionPath the string form of the partitionId
   * @param tokenFileName the name of the token file to store in the cloud.
   * @param inputStream the InputStream containing the replica tokens.
   * @throws CloudStorageException if the upload encounters an error.
   */
  void persistTokens(String partitionPath, String tokenFileName, InputStream inputStream) throws CloudStorageException;

  /**
   * Retrieve the persisted replica tokens, if any, for the specified Ambry partition.
   * @param partitionPath the string form of the partitionId
   * @param tokenFileName the name of the token file stored in the cloud.
   * @param outputStream the OutputStream to which the replica tokens are written.
   * @throws CloudStorageException if the upload encounters an error.
   * @return {@code true} if tokens were found, otherwise {@code false}.
   */
  boolean retrieveTokens(String partitionPath, String tokenFileName, OutputStream outputStream)
      throws CloudStorageException;

  /**
   * Halt any compactions in progress.
   */
  void stopCompaction();

  /**
   * Deprecate the specified {@link Container}s in cloud.
   * @param deprecatedContainers {@link Collection} of deprecated {@link Container}s.
   * @throws {@link CloudStorageException} if the operation fails.
   */
  void deprecateContainers(Collection<Container> deprecatedContainers) throws CloudStorageException;

  /**
   * @return {@link CloudContainerCompactor} object that would do container compaction for cloud.
   */
  CloudContainerCompactor getContainerCompactor();
}
