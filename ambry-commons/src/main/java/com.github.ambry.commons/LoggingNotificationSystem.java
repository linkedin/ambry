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
  public void close()
      throws IOException {
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

