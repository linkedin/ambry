/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.protocol;

import com.github.ambry.network.NetworkRequest;
import java.io.IOException;


/**
 * This defines the server request API. The commands below are the requests that can be issued against the server.
 */
public interface RequestAPI {

  /**
   * Handle a request.
   * @param request The request to handle.
   * @throws InterruptedException if request processing is interrupted.
   */
  void handleRequests(NetworkRequest request) throws InterruptedException;

  /**
   * Puts a blob into the store. It accepts a blob property, user metadata and the blob
   * as a stream and stores them.
   * @param request The request that contains the blob property, user metadata and blob as a stream.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  void handlePutRequest(NetworkRequest request) throws IOException, InterruptedException;

  /**
   * Gets blob property, user metadata or the blob from the specified partition.
   * @param request The request that contains the partition and id of the blob whose blob property, user metadata or
   *                blob needs to be returned.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  void handleGetRequest(NetworkRequest request) throws IOException, InterruptedException;

  /**
   * Deletes the blob from the store.
   * @param request The request that contains the partition and id of the blob that needs to be deleted.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  void handleDeleteRequest(NetworkRequest request) throws IOException, InterruptedException;

  /**
   * Deletes a list of blobs from the store. Also handles the scenarios of complete success, partial success and full
   * failure by returning appropriate response {@code BatchDeleteResponse} to network channel.
   * @param request The request that contains the list of partitions and ids of the blob that needs to be deleted.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */

  void handleBatchDeleteRequest(NetworkRequest request) throws IOException, InterruptedException;

  /**
   * Purges the blob from the store.
   * @param request The request that contains the partition and id of the blob that needs to be purged.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  void handlePurgeRequest(NetworkRequest request) throws IOException, InterruptedException;

  /**
   * Updates the TTL of a blob as required in {@code request}.
   * @param request The request that contains the partition and id of the blob that needs to be updated.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  void handleTtlUpdateRequest(NetworkRequest request) throws IOException, InterruptedException;

  /**
   * Gets the metadata required for replication.
   * @param request The request that contains the partition for which the metadata is needed.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  void handleReplicaMetadataRequest(NetworkRequest request) throws IOException, InterruptedException;

  /**
   * Replicate one specific Blob from a remote host to the local store.
   * @param request The request that contains the remote host information and the blob id to be replicated.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  void handleReplicateBlobRequest(NetworkRequest request) throws IOException, InterruptedException;

  /**
   * Handles an administration request. These requests can query for or change the internal state of the server.
   * @param request the request that needs to be handled.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  default void handleAdminRequest(NetworkRequest request) throws InterruptedException, IOException {
    throw new UnsupportedOperationException("Admin request not supported on this node");
  }

  /**
   * Undelete the blob from the store.
   * @param request the request that contains the partition and the id of the blob that needs to be undeleted.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  default void handleUndeleteRequest(NetworkRequest request) throws InterruptedException, IOException {
    throw new UnsupportedOperationException("Undelete request not supported on this node");
  }

  /**
   * Get metadata about log segments in a partition.
   * @param request the request that contains the partition and the id of the blob that needs to be undeleted.
   * @throws IOException if there are I/O errors carrying our the required operation.
   * @throws InterruptedException if request processing is interrupted.
   */
  void handleFileCopyGetMetaDataRequest(NetworkRequest request) throws InterruptedException, IOException;

  /**
   * Get a chunk of a file (log segment, index file, bloom file) in a partition.
   * @param request the request that contains the partition, filename, size and offset of the requested file chunk.
   * @throws InterruptedException if request processing is interrupted.
   * @throws IOException if there are I/O errors carrying our the required operation.
   */
  void handleFileCopyGetChunkRequest(NetworkRequest request) throws InterruptedException, IOException;

  /**
   * Handles a request to verify the data copied during a file copy operation.
   * This is typically used to ensure that the data integrity is maintained after a file copy.
   * @param request The request that contains the necessary information for data verification.
   * @throws InterruptedException if request processing is interrupted.
   * @throws IOException if there are I/O errors carrying out the required operation.
   */
  void handleFileCopyDataVerificationRequest(NetworkRequest request) throws InterruptedException, IOException;
}
