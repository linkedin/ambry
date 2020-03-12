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
import com.github.ambry.replication.FindToken;
import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;


/**
 * An interface representing an interaction with a cloud destination, that allows replicating blob operations.
 */
public interface CloudDestination extends Closeable {

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
   * Get the list of blobs in the specified partition that have been deleted for at least the
   * configured retention period.
   * @param partitionPath the partition to query.
   * @param startTime the start of the query time range.
   * @param endTime the end of the query time range.
   * @param maxEntries the max number of metadata records to return.
   * @return a List of {@link CloudBlobMetadata} referencing the deleted blobs found.
   * @throws CloudStorageException
   */
  List<CloudBlobMetadata> getDeletedBlobs(String partitionPath, long startTime, long endTime, int maxEntries)
      throws CloudStorageException;

  /**
   * Get the list of blobs in the specified partition that have been expired for at least the
   * configured retention period.
   * @param partitionPath the partition to query.
   * @param startTime the start of the query time range.
   * @param endTime the end of the query time range.
   * @param maxEntries the max number of metadata records to return.
   * @return a List of {@link CloudBlobMetadata} referencing the expired blobs found.
   * @throws CloudStorageException
   */
  List<CloudBlobMetadata> getExpiredBlobs(String partitionPath, long startTime, long endTime, int maxEntries)
      throws CloudStorageException;

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
   * Permanently delete the specified blobs in the cloud destination.
   * @param blobMetadataList the list of {@link CloudBlobMetadata} referencing the blobs to purge.
   * @return the number of blobs successfully purged.
   * @throws CloudStorageException if the purge operation fails for any blob.
   */
  int purgeBlobs(List<CloudBlobMetadata> blobMetadataList) throws CloudStorageException;

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
