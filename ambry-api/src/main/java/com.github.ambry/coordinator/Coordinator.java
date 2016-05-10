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
package com.github.ambry.coordinator;

import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;

import java.io.Closeable;
import java.io.InputStream;
import java.nio.ByteBuffer;


/**
 * The Coordinator performs operations on an Ambry cluster.
 */
public interface Coordinator extends Closeable {
  /**
   * Puts a blob into the store with a set of blob properties and user metadata. It returns the id of the blob if it was
   * stored successfully.
   *
   * @param blobProperties The properties of the blob that needs to be stored.
   * @param userMetadata The user metadata that needs to be associated with the blob.
   * @param blob The blob that needs to be stored
   * @return The id of the blob if the blob was successfully stored
   * @throws CoordinatorException If the operation experienced an error
   */
  String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata, InputStream blob)
      throws CoordinatorException;

  /**
   * Deletes the blob that corresponds to the given id.
   *
   * @param id The id of the blob that needs to be deleted.
   * @throws CoordinatorException If the operation experienced an error
   */
  void deleteBlob(String id)
      throws CoordinatorException;

  /**
   * Gets the properties of the blob that corresponds to the specified blob id.
   *
   * @param blobId The id of the blob for which to retrieve its blob properties.
   * @return The blob properties of the blob that corresponds to the blob id provided.
   * @throws CoordinatorException If the operation experienced an error
   */
  BlobProperties getBlobProperties(String blobId)
      throws CoordinatorException;

  /**
   * Gets the user metadata of the blob that corresponds to the specified blob id.
   *
   * @param blobId The id of the blob for which to retrieve its user metadata.
   * @return A buffer that contains the user metadata of the blob that corresponds to the blob id provided.
   * @throws CoordinatorException If the operation experienced an error
   */
  ByteBuffer getBlobUserMetadata(String blobId)
      throws CoordinatorException;

  // TODO: Add interface 'BlobInfo getBlobInfo(String blobId)' to coordinator

  /**
   * Gets the blob that corresponds to the specified blob id.
   *
   * @param blobId The id of the blob for which to retrieve its content.
   * @return The data of  of the blob that corresponds to the blob id provided.
   * @throws CoordinatorException If the operation experienced an error
   */
  BlobOutput getBlob(String blobId)
      throws CoordinatorException;
}
