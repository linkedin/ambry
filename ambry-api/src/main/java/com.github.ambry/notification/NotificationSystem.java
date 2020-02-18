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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.store.MessageInfo;
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
   * @param account The {@link Account} for the blob
   * @param container The {@link Container} for the blob
   * @param notificationBlobType The {@link NotificationBlobType} of this blob.
   */
  void onBlobCreated(String blobId, BlobProperties blobProperties, Account account, Container container,
      NotificationBlobType notificationBlobType);

  /**
   * Notifies the underlying system when the ttl of an existing blob is updated
   * @param blobId The id of the blob whose ttl was updated
   * @param serviceId The service ID of the service that updated the tll of the blob. This can be null if unknown
   * @param expiresAtMs The new expiry time (in ms) of the blob
   * @param account The {@link Account} for the blob
   * @param container The {@link Container} for the blob
   */
  void onBlobTtlUpdated(String blobId, String serviceId, long expiresAtMs, Account account, Container container);

  /**
   * Notifies the underlying system when an existing blob is deleted
   * @param blobId The id of the blob that was deleted
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @param account The {@link Account} for the blob
   * @param container The {@link Container} for the blob
   */
  void onBlobDeleted(String blobId, String serviceId, Account account, Container container);

  /**
   * Notifies the underlying system when the blob is undeleted.
   * @param blobId The id of the blob whose undeleted state has been replicated
   * @param serviceId The service ID of the service undeleting the blob. This can be null if unknown.
   * @param account The {@link Account} for the blob
   * @param container The {@link Container} for the blob
   */
  default void onBlobUndeleted(String blobId, String serviceId, Account account, Container container) {
  }

  /**
   * Notifies the underlying system when a blob is replicated to a node
   * @param sourceHost The source host from where the notification is being invoked
   * @param port The port of the source host from where the notification is being invoked.
   * @param blobId The id of the blob that has been replicated
   * @param sourceType The source that created the blob replica
   */
  void onBlobReplicaCreated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType);

  /**
   * Notifies the underlying system when a deleted state of a blob is replicated to a node
   * @param sourceHost The source host from where the notification is being invoked
   * @param port The port of the source host from where the notification is being invoked.
   * @param blobId The id of the blob whose deleted state has been replicated
   * @param sourceType The source that deleted the blob replica
   */
  void onBlobReplicaDeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType);

  /**
   * Notifies the underlying system when a updated state of a blob is replicated to a node
   * @param sourceHost The source host from where the notification is being invoked
   * @param port The port of the source host from where the notification is being invoked.
   * @param blobId The id of the blob whose updated state has been replicated
   * @param sourceType The source that updated the blob replica
   * @param updateType the type of update
   * @param info the {@link MessageInfo} associated with the update
   */
  void onBlobReplicaUpdated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType,
      UpdateType updateType, MessageInfo info);

  /**
   * Notifies the underlying system when a undeleted state of a blob is replicated to a node.
   * @param sourceHost The source host from where the notification is being invoked
   * @param port The port of the source host from where the notification is being invoked.
   * @param blobId The id of the blob whose undeleted state has been replicated
   * @param sourceType The source that undeleted the blob replica
   */
  default void onBlobReplicaUndeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
  }
}
