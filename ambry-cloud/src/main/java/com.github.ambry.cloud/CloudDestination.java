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
import java.util.List;
import java.util.Map;


/**
 * An interface representing an interaction with a cloud destination, that allows replicating blob operations.
 */
public interface CloudDestination {

  /**
   * Upload blob to the cloud destination.
   * @param blobId id of the Ambry blob
   * @param blobSize size of the blob in bytes
   * @param cloudBlobMetadata the {@link CloudBlobMetadata} for the blob being uploaded.
   * @param blobInputStream the stream to read blob data
   * @return flag indicating whether the blob was uploaded
   * @throws CloudStorageException if the upload encounters an error.
   */
  boolean uploadBlob(BlobId blobId, long blobSize, CloudBlobMetadata cloudBlobMetadata, InputStream blobInputStream)
      throws CloudStorageException;

  /**
   * Delete blob in the cloud destination.
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
   * Checks whether the blob exists in the cloud destination.
   * @param blobId id of the Ambry blob to check
   * @return {@code true} if the blob exists, otherwise {@code false}.
   * @throws CloudStorageException if the existence check encounters an error.
   */
  boolean doesBlobExist(BlobId blobId) throws CloudStorageException;
}
