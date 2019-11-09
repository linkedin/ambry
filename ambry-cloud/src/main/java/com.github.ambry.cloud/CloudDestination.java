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

import com.github.ambry.commons.BlobId;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;


/**
 * An interface representing an interaction with a cloud destination, that allows replicating blob operations.
 */
public interface CloudDestination {

  /**
   * Upload blob to the cloud destination.
   * @param blobId id of the Ambry blob
   * @param inputLength the length of the input stream, if known, -1 if unknown.
   * @param cloudBlobMetadata the {@link CloudBlobMetadata} for the blob being uploaded.
   * @param blobInputStream the stream to read blob data
   * @return flag indicating whether the blob was uploaded
   * @throws CloudStorageException if the upload encounters an error.
   */
  boolean uploadBlob(BlobId blobId, long inputLength, CloudBlobMetadata cloudBlobMetadata, InputStream blobInputStream)
      throws CloudStorageException;

  /**
   * Download blob from the cloud destination.
   * @param blobId id of the Ambry blob to be downloaded
   * @param outputStream outputstream to populate the downloaded data with
   * @throws CloudStorageException if the download encounters an error.
   */
  void downloadBlob(BlobId blobId, OutputStream outputStream) throws CloudStorageException;

  /**
   * Mark a blob as deleted in the cloud destination.
   * @param blobId id of the Ambry blob
   * @param deletionTime time of blob deletion
   * @return flag indicating whether the blob was deleted
   * @throws CloudStorageException if the deletion encounters an error.
   */
  boolean deleteBlob(BlobId blobId, long deletionTime) throws CloudStorageException;

  /**
   * Update expiration time of blob in the cloud destination.
   * @param blobId id of the Ambry blob
   * @param expirationTime the new expiration time
   * @return flag indicating whether the blob was updated
   * @throws CloudStorageException if the update encounters an error.
   */
  boolean updateBlobExpiration(BlobId blobId, long expirationTime) throws CloudStorageException;

  /**
   * Query the blob metadata for the specified blobs.
   * @param blobIds list of blob Ids to query.
   * @return a {@link Map} of blobId strings to {@link CloudBlobMetadata}.  If metadata for a blob could not be found,
   * it will not be included in the returned map.
   */
  Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) throws CloudStorageException;

  /**
   * Get the list of blobs in the specified partition that have been deleted or expired for at least the
   * configured retention period.
   * @param partitionPath the partition to query.
   * @return a List of {@link CloudBlobMetadata} referencing the dead blobs found.
   * @throws CloudStorageException
   */
  List<CloudBlobMetadata> getDeadBlobs(String partitionPath) throws CloudStorageException;

  /**
   * Returns a sequenced list of blobs in the specified partition, ordered by update time starting from the
   * specified time.
   * @param partitionPath the partition to query.
   * @param findToken the {@link CloudFindToken} specifying the boundary for the query.
   * @param maxTotalSizeOfEntries the cumulative size limit for the list of blobs returned.
   * @return a List of {@link CloudBlobMetadata} referencing the blobs returned by the query.
   * @throws CloudStorageException
   */
  List<CloudBlobMetadata> findEntriesSince(String partitionPath, CloudFindToken findToken, long maxTotalSizeOfEntries)
      throws CloudStorageException;

  /**
   * Permanently delete the specified blob in the cloud destination.
   * @param blobMetadata the {@link CloudBlobMetadata} referencing the blob to purge.
   * @return flag indicating whether the blob was successfully purged.
   * @throws CloudStorageException if the purge operation fails.
   */
  boolean purgeBlob(CloudBlobMetadata blobMetadata) throws CloudStorageException;

  /**
   * Permanently delete the specified blobs in the cloud destination.
   * @param blobMetadataList the list of {@link CloudBlobMetadata} referencing the blobs to purge.
   * @return the number of blobs successfully purged.
   * @throws CloudStorageException if the purge operation fails for any blob.
   */
  int purgeBlobs(List<CloudBlobMetadata> blobMetadataList) throws CloudStorageException;

  /**
   * Checks whether the blob exists in the cloud destination.
   * @param blobId id of the Ambry blob to check.
   * @return {@code true} if the blob exists, otherwise {@code false}.
   * @throws CloudStorageException if the existence check encounters an error.
   */
  boolean doesBlobExist(BlobId blobId) throws CloudStorageException;

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
}
