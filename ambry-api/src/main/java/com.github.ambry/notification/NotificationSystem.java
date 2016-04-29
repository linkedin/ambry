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
package com.github.ambry.notification;

import com.github.ambry.messageformat.BlobProperties;
import java.io.Closeable;


/**
 * A pluggable notification interface that is used to notify any external
 * system about the server operations. The implementation of the APIs
 * should be fast and preferably non blocking
 */
public interface NotificationSystem extends Closeable {

  /**
   * Notifies the underlying system when a new blob is created
   * @param blobId The id of the blob that was created
   * @param blobProperties The blob properties for the blob
   * @param userMetadata The usermetadata for the blob
   */
  public void onBlobCreated(String blobId, BlobProperties blobProperties, byte[] userMetadata);

  /**
   * Notifies the underlying system when an existing blob is deleted
   * @param blobId The id of the blob that was deleted
   */
  public void onBlobDeleted(String blobId);

  /**
   * Notifies the underlying system when a blob is replicated to a node
   * @param sourceHost The source host from where the notification is being invoked
   * @param port The port of the source host from where the notification is being invoked.
   * @param blobId The id of the blob that has been replicated
   * @param sourceType The source that created the blob replica
   */
  public void onBlobReplicaCreated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType);

  /**
   * Notifies the underlying system when a deleted state of a blob is replicated to a node
   * @param sourceHost The source host from where the notification is being invoked
   * @param port The port of the source host from where the notification is being invoked.
   * @param blobId The id of the blob whose deleted state has been replicated
   * @param sourceType The source that deleted the blob replica
   */
  public void onBlobReplicaDeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType);
}
