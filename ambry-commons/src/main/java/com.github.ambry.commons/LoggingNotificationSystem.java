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
package com.github.ambry.commons;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.store.MessageInfo;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Logs all events at DEBUG level.
 */
public class LoggingNotificationSystem implements NotificationSystem {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public void close() throws IOException {
    // No op
  }

  @Override
  public void onBlobCreated(String blobId, BlobProperties blobProperties, Account account, Container container,
      NotificationBlobType notificationBlobType) {
    logger.debug(
        "onBlobCreated " + blobId + ", blobProperties " + blobProperties + ", accountName " + (account == null ? null
            : account.getName()) + ", accountId" + (account == null ? null : account.getId()) + ", containerName " + (
            container == null ? null : container.getName()) + ", containerId " + (container == null ? null
            : container.getId()) + ", blobType " + notificationBlobType);
  }

  @Override
  public void onBlobTtlUpdated(String blobId, String serviceId, long expiresAtMs, Account account,
      Container container) {
    logger.debug("onBlobTtlUpdated " + blobId + ", serviceId " + serviceId + ", accountName " + (account == null ? null
        : account.getName()) + ", accountId" + (account == null ? null : account.getId()) + ", containerName " + (
        container == null ? null : container.getName()) + ", containerId " + (container == null ? null
        : container.getId()) + ", " + expiresAtMs);
  }

  @Override
  public void onBlobDeleted(String blobId, String serviceId, Account account, Container container) {
    logger.debug("onBlobDeleted " + blobId,
        ", " + serviceId + ", accountName " + (account == null ? null : account.getName()) + ", accountId" + (
            account == null ? null : account.getId()) + ", containerName " + (container == null ? null
            : container.getName()) + ", containerId " + (container == null ? null : container.getId()));
  }

  @Override
  public void onBlobUndeleted(String blobId, String serviceId, Account account, Container container) {
    logger.debug("onBlobUndeleted " + blobId,
        ", " + serviceId + ", accountName " + (account == null ? null : account.getName()) + ", accountId" + (
            account == null ? null : account.getId()) + ", containerName " + (container == null ? null
            : container.getName()) + ", containerId " + (container == null ? null : container.getId()));

  }

  @Override
  public void onBlobReplicaCreated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
    logger.debug("onBlobReplicaCreated " + sourceHost + ", " + port + ", " + blobId + ", " + sourceType);
  }

  @Override
  public void onBlobReplicaDeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
    logger.debug("onBlobReplicaDeleted " + sourceHost + ", " + port + ", " + blobId + ", " + sourceType);
  }

  @Override
  public void onBlobReplicaUpdated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType,
      UpdateType updateType, MessageInfo info) {
    logger.debug(
        "onBlobReplicaUpdated " + sourceHost + ", " + port + ", " + blobId + ", " + sourceType + ", " + updateType
            + ", " + info);
  }

  @Override
  public void onBlobReplicaUndeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
    logger.debug("onBlobReplicaUndeleted " + sourceHost + ", " + port + ", " + blobId + ", " + sourceType);
  }
}

