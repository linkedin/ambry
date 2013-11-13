package com.github.ambry.coordinator;


import com.github.ambry.messageformat.BlobProperties;

import java.io.InputStream;
import java.nio.ByteBuffer;

public interface Coordinator {

  /**
   * Puts a blob into the store with a set of blob properties and user metadata. It returns the id of the blob if
   * it was stored successfully.
   * @param blobProperties The properties of the blob that needs to be stored. The blob size and serviceId are
   *                       mandatory fields.
   * @param userMetadata The user metadata that needs to be associated with the blob.
   * @param blob The blob that needs to be stored
   * @return The id of the blob if the blob was successfully stored
   */
  String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata, InputStream blob);

  /**
   * Deletes the blob that corresponds to the given id.
   * @param id The id of the blob that needs to be deleted.
   * @throws
   * BlobNotFoundException If the blob is not found for the given id
   */
  void deleteBlob(String id) throws BlobNotFoundException;

  /**
   * Updates the ttl of the blob specified
   * @param id The id of the blob that needs its TTL updated
   * @param newTTLInMs The new ttl value to update to. It is the System time in ms
   *                   when the blob needs to be deleted.
   * @throws BlobNotFoundException
   */
  void updateTTL(String id, long newTTLInMs) throws BlobNotFoundException;

  /**
   * Gets the blob that corresponds to the given blob id.
   * @param blobId The id of the blob whose content needs to be retrieved.
   * @return The inputstream that represents the blob.
   * @throws
   * BlobNotFoundException If the blob is not found for the given id
   */
  InputStream getBlob(String blobId) throws BlobNotFoundException;

  /**
   * Gets the user metadata that was associated with a given blob during a put.
   * @param blobId The id of the blob whose user metadata needs to be retrieved.
   * @return The buffer that contains the user metadata for the given blob id.
   * @throws
   * BlobNotFoundException If the blob is not found for the given id
   */
  ByteBuffer getUserMetadata(String blobId) throws BlobNotFoundException;

  /**
   * Gets the blob properties associated with a given blob during a put.
   * @param blobId The id of the blob whose blob properties needs to be retrieved.
   * @return The blob properties of the blob that corresponds to the blob id provided.
   * @throws
   * BlobNotFoundException If the blob is not found for the given id
   */
  BlobProperties getBlobProperties(String blobId) throws BlobNotFoundException;

}
