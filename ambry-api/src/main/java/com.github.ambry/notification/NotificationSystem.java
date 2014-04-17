package com.github.ambry.notification;

import com.github.ambry.messageformat.BlobProperties;

import java.nio.ByteBuffer;

/**
 * A pluggable notification interface that is used to notify any external
 * system about the server operations. The implementation of the APIs
 * should be fast and preferably non blocking
 */
public interface NotificationSystem {

  /**
   * Notifies the underlying system when a new blob is created
   * @param blobId The id of the blob that was created
   * @param blobProperties The blob properties for the blob
   * @param userMetadata The usermetadata for the blob
   */
  public void onBlobCreated(String blobId, BlobProperties blobProperties, ByteBuffer userMetadata);

  /**
   * Notifies the underlying system when an existing blob is deleted
   * @param blobId The id of the blob that was deleted
   */
  public void onBlobDeleted(String blobId);

  /**
   * Notifies the underlying system when a blob is replicated to a node
   * @param blobId The id of the blob that has been replicated
   */
  public void onBlobReplicated(String blobId);

  /**
   * Notifies the underlying system when a deleted state of a blob is replicated to a node
   * @param blobID The id of the blob whose deleted state has been replicated
   */
  public void onDeleteReplicated(String blobID);
}
