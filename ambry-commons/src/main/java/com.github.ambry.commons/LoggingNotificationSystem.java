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

import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationSystem;
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
  public void onBlobCreated(String blobId, BlobProperties blobProperties, byte[] userMetadata) {
    logger.debug("onBlobCreated " + blobId + "," + blobProperties);
  }

  @Override
  public void onBlobDeleted(String blobId) {
    logger.debug("onBlobDeleted " + blobId);
  }

  @Override
  public void onBlobReplicaCreated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
    logger.debug("onBlobReplicaCreated " + sourceHost + ", " + port + ", " + blobId + "," + sourceType);
  }

  @Override
  public void onBlobReplicaDeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
    logger.debug("onBlobReplicaCreated " + sourceHost + ", " + port + ", " + blobId + "," + sourceType);
  }
}

