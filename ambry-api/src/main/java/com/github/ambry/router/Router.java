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
package com.github.ambry.router;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;


/**
 * The router interface for Ambry that helps to interact with Ambry server.
 */
public interface Router extends Closeable {

  /**
   * Requests for the blob (info, data, or both) asynchronously and invokes the {@link Callback} when the request
   * completes.
   * @param blobId The ID of the blob for which blob data is requested.
   * @param options The options associated with the request. This cannot be null.
   * @param callback The callback which will be invoked on the completion of the request.
   * @return A future that would eventually contain a {@link GetBlobResult} that can contain either
   *         the {@link BlobInfo}, the {@link ReadableStreamChannel} containing the blob data, or both.
   */
  Future<GetBlobResult> getBlob(String blobId, GetBlobOptions options, Callback<GetBlobResult> callback);

  /**
   * Requests for a new blob to be put asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobProperties The properties of the blob. Note that the size specified in the properties is ignored. The
   *                       channel is consumed fully, and the size of the blob is the number of bytes read from it.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @param options The {@link PutBlobOptions} associated with the request. This cannot be null.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   * @return A future that would contain the BlobId eventually.
   */
  Future<String> putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
      PutBlobOptions options, Callback<String> callback);

  /**
   * Requests for a new metadata blob to be put asynchronously and invokes the {@link Callback} when the request
   * completes. This metadata blob will contain references to the chunks provided as an argument. The blob ID returned
   * by this operation can be used to fetch the chunks as if they were a single blob.
   * @param blobProperties The properties of the blob. Note that the size specified in the properties is ignored. The
   *                       channel is consumed fully, and the size of the blob is the number of bytes read from it.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param chunksToStitch the list of data chunks to stitch together. The router will treat the metadata in the
   *                       {@link ChunkInfo} object as a source of truth, so the caller should ensure that these
   *                       fields are set accurately.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   * @return A future that would contain the BlobId eventually.
   */
  Future<String> stitchBlob(BlobProperties blobProperties, byte[] userMetadata, List<ChunkInfo> chunksToStitch,
      Callback<String> callback);

  /**
   * Requests for a blob to be deleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob that needs to be deleted.
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  Future<Void> deleteBlob(String blobId, String serviceId, Callback<Void> callback);

  /**
   * Requests that a blob's TTL be updated asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob that needs its TTL updated.
   * @param serviceId The service ID of the service updating the blob. This can be null if unknown.
   * @param expiresAtMs The new expiry time (in ms) of the blob. Using {@link Utils#Infinite_Time} makes the blob
   *                    permanent
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @return A future that would contain information about whether the update succeeded or not, eventually.
   */
  Future<Void> updateBlobTtl(String blobId, String serviceId, long expiresAtMs, Callback<Void> callback);

  /**
   * Requests for a blob to be undeleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob that needs to be undeleted.
   * @param serviceId The service ID of the service undeleting the blob. This can be null if unknown.
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @return A future that would contain information about whether the undelete succeeded or not, eventually.
   */
  Future<Void> undeleteBlob(String blobId, String serviceId, Callback<Void> callback);

  /**
   * Closes the router and releases any resources held by the router. If the router is already closed, then this
   * method has no effect.
   * <p/>
   * After a router is closed, any further attempt to invoke Router operations will cause a {@link RouterException} with
   * error code {@link RouterErrorCode#RouterClosed} to be returned as part of the {@link Future} and {@link Callback}
   * if any.
   * @throws IOException if an I/O error occurs.
   */
  void close() throws IOException;

  /**
   * Requests for blob (info, data, or both) asynchronously with user-set {@link GetBlobOptions} and returns a future
   * that will eventually contain a {@link GetBlobResult} that can contain either the {@link BlobInfo}, the
   * {@link ReadableStreamChannel} containing the blob data, or both.
   * @param blobId The ID of the blob for which blob data is requested.
   * @param options The options associated with the request.
   * @return A future that would eventually contain a {@link GetBlobResult} that can contain either
   *         the {@link BlobInfo}, the {@link ReadableStreamChannel} containing the blob data, or both.
   */
  default Future<GetBlobResult> getBlob(String blobId, GetBlobOptions options) {
    return getBlob(blobId, options, null);
  }

  /**
   * Requests for a new metadata blob to be put asynchronously and invokes the {@link Callback} when the request
   * completes. This metadata blob will contain references to the chunks provided as an argument. The blob ID returned
   * by this operation can be used to fetch the chunks as if they were a single blob.
   * @param blobProperties The properties of the blob. Note that the size specified in the properties is ignored. The
   *                       channel is consumed fully, and the size of the blob is the number of bytes read from it.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param chunksToStitch the list of data chunks to stitch together. The router will treat the metadata in the
   *                       {@link ChunkInfo} object as a source of truth, so the caller should ensure that these
   *                       fields are set accurately.
   * @return A future that would contain the BlobId eventually.
   */
  default Future<String> stitchBlob(BlobProperties blobProperties, byte[] userMetadata,
      List<ChunkInfo> chunksToStitch) {
    return stitchBlob(blobProperties, userMetadata, chunksToStitch, null);
  }

  /**
   * Requests for a new blob to be put asynchronously and returns a future that will eventually contain the BlobId of
   * the new blob on a successful response.
   * @param blobProperties The properties of the blob. Note that the size specified in the properties is ignored. The
   *                       channel is consumed fully, and the size of the blob is the number of bytes read from it.
   * @param userMetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @param options The {@link PutBlobOptions} associated with the request. This cannot be null.
   * @return A future that would contain the BlobId eventually.
   */
  default Future<String> putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
      PutBlobOptions options) {
    return putBlob(blobProperties, userMetadata, channel, options, null);
  }

  /**
   * Requests for a blob to be deleted asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob that needs to be deleted.
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  default Future<Void> deleteBlob(String blobId, String serviceId) {
    return deleteBlob(blobId, serviceId, null);
  }

  /**
   * Requests that a blob's TTL be updated asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob that needs its TTL updated.
   * @param serviceId The service ID of the service updating the blob. This can be null if unknown.
   * @param expiresAtMs The new expiry time (in ms) of the blob. Using {@link Utils#Infinite_Time} makes the blob
   *                    permanent
   * @return A future that would contain information about whether the update succeeded or not, eventually.
   */
  default Future<Void> updateBlobTtl(String blobId, String serviceId, long expiresAtMs) {
    return updateBlobTtl(blobId, serviceId, expiresAtMs, null);
  }

  /**
   * Requests for a blob to be undeleted asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob that needs to be undeleted.
   * @param serviceId The service ID of the service undeleting the blob. This can be null if unknown.
   * @return A future that would contain information about whether the undelete succeeded or not, eventually.
   */
  default Future<Void> undeleteBlob(String blobId, String serviceId) {
    return undeleteBlob(blobId, serviceId, null);
  }
}
