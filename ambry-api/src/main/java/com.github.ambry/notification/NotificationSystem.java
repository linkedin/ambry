package com.github.ambry.notification;

import com.github.ambry.messageformat.BlobProperties;


/**
 * A pluggable notification interface that is used to notify any external
 * system about the server operations. The implementation of the APIs
 * should be fast and preferably non blocking
 */
public interface NotificationSystem {

  /**
   * Notifies the underlying system when a new blob is created
   * @param sourceHost The source host from where the notification is being invoked
   * @param blobId The id of the blob that was created
   * @param blobProperties The blob properties for the blob
   * @param userMetadata The usermetadata for the blob
   */
  public void onBlobCreated(String sourceHost, String blobId, BlobProperties blobProperties, byte[] userMetadata);

  /**
   * Notifies the underlying system when an existing blob is deleted
   * @param sourceHost The source host from where the notification is being invoked
   * @param blobId The id of the blob that was deleted
   */
  public void onBlobDeleted(String sourceHost, String blobId);

  /**
   * Notifies the underlying system when a blob is replicated to a node
   * @param sourceHost The source host from where the notification is being invoked
   * @param blobId The id of the blob that has been replicated
   * @param sourceType The source that created the blob replica
   */
  public void onBlobReplicaCreated(String sourceHost, String blobId, BlobReplicaSourceType sourceType);

  /**
   * Notifies the underlying system when a deleted state of a blob is replicated to a node
   * @param sourceHost The source host from where the notification is being invoked
   * @param blobId The id of the blob whose deleted state has been replicated
   * @param sourceType The source that deleted the blob replica
   */
  public void onBlobReplicaDeleted(String sourceHost, String blobId, BlobReplicaSourceType sourceType);
}
