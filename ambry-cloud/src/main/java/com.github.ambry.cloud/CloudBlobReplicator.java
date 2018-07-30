package com.github.ambry.cloud;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.CloudReplicationConfig;
import com.github.ambry.account.Container;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ReadableStreamChannelInputStream;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.router.CryptoService;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.KeyManagementService;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudBlobReplicator implements BlobEventConsumer {

  private AccountService accountService;
  private Router router;
  private BlobEventSource eventSource;
  private CloudDestinationFactory destinationFactory;
  private CryptoService cryptoService;
  private KeyManagementService kms;
  private CloudReplicationMetrics metrics;

  private static final Logger logger = LoggerFactory.getLogger(CloudBlobReplicator.class);

  public CloudBlobReplicator() {
  }

  public CloudBlobReplicator accountService(AccountService accountService) {
    this.accountService = accountService;
    return this;
  }

  public CloudBlobReplicator router(Router router) {
    this.router = router;
    return this;
  }

  public CloudBlobReplicator eventSource(BlobEventSource eventSource) {
    this.eventSource = eventSource;
    return this;
  }

  public CloudBlobReplicator destinationFactory(CloudDestinationFactory destinationFactory) {
    this.destinationFactory = destinationFactory;
    return this;
  }

  public CloudBlobReplicator metrics(CloudReplicationMetrics metrics) {
    this.metrics = metrics;
    return this;
  }

  public CloudBlobReplicator cryptoService(CryptoService cryptoService) {
    this.cryptoService = cryptoService;
    return this;
  }

  public CloudBlobReplicator keyService(KeyManagementService kms) {
    this.kms = kms;
    return this;
  }

  public void startup() {
    Objects.requireNonNull(eventSource);
    if (destinationFactory == null) {
      destinationFactory = new AmbryCloudDestinationFactory(null);
    }
    eventSource.subscribe(this);
  }

  public void shutdown() {
    eventSource.unsubscribe(this);
  }

  @Override
  // TODO: need to do this async, can stuff in queue and return
  // Then need to acknowledge event when done
  public boolean onBlobEvent(BlobEvent blobEvent) {
    String blobId = blobEvent.getBlobId();
    BlobOperation op = blobEvent.getBlobOperation();

    CloudReplicationConfig config = getCloudReplicationConfig(blobId);
    if (config == null) {
      // Container not configured for replication
      return true;
    }

    try {
      switch (op) {
        case PUT:
        case UPDATE:
          handleBlobReplication(blobId, config);
          break;
        case DELETE:
          handleBlobDeletion(blobId, config);
          break;
      }
    } catch (Exception ex) {
      return false;
    }
    return true;
  }

  private void handleBlobReplication(String blobId, CloudReplicationConfig config) throws Exception {

    metrics.blobUploadRequestCount.inc();

    try {
      GetBlobOptions getOptions = new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All)
          .getOption(GetOption.Include_All)
          .build();

      // Callback: (result, exception) -> { }
      // Perform the Get synchronously for now
      Future<GetBlobResult> resultFuture = router.getBlob(blobId, getOptions, null);
      GetBlobResult result = resultFuture.get();

      CloudDestination destination = destinationFactory.getCloudDestination(config);

      BlobInfo blobInfo = result.getBlobInfo();
      // Only replicate blobs with infinite TTL
      long ttlSec = blobInfo.getBlobProperties().getTimeToLiveInSeconds();
      if (ttlSec != Utils.Infinite_Time) {
        logger.info("Skipping replication of blob {} due to TTL of {}.", blobId, ttlSec);
        return;
      }
      long blobSize = blobInfo.getBlobProperties().getBlobSize();
      ReadableStreamChannel blobStreamChannel = result.getBlobDataChannel();
      InputStream blobInputStream = new ReadableStreamChannelInputStream(blobStreamChannel);

      long startTimeMs = SystemTime.getInstance().milliseconds();
      if (destination.uploadBlob(blobId, blobSize, blobInputStream)) {
        metrics.blobUploadedCount.inc();
        long sendTimeMs = SystemTime.getInstance().milliseconds() - startTimeMs;
        double sendBytesRate = blobSize / ((double) sendTimeMs / SystemTime.MsPerSec);
        metrics.blobUploadRate.mark((long) sendBytesRate);
      }
    } catch (Throwable ex) {
      metrics.blobUploadErrorCount.inc();
      throw ex;
    }
  }

  private void handleBlobDeletion(String blobId, CloudReplicationConfig config) throws Exception {

    metrics.blobDeleteRequestCount.inc();

    try {
      CloudDestination destination = destinationFactory.getCloudDestination(config);

      if (destination.deleteBlob(blobId)) {
        metrics.blobDeletedCount.inc();
      }
    } catch (Throwable ex) {
      metrics.blobDeleteErrorCount.inc();
      throw ex;
    }
  }

  // Returns a sanitized copy of container's replication config
  private CloudReplicationConfig getCloudReplicationConfig(String blobId) {
    try {
      Pair<Short, Short> pair = BlobId.getAccountAndContainerIds(blobId);
      short accountId = pair.getFirst(), containerId = pair.getSecond();
      Account ambryAccount = accountService.getAccountById(accountId);
      Objects.requireNonNull(ambryAccount, "No Ambry account found for accountId " + accountId);
      Container ambryContainer = ambryAccount.getContainerById(containerId);
      Objects.requireNonNull(ambryContainer, "No Ambry container found for containerId " + containerId);
      // Check for valid cloud destination
      CloudReplicationConfig containerConfig = ambryContainer.getCloudReplicationConfig();
      if (containerConfig == null) {
        return null;
      }
      String clearConfigSpec = decryptString(containerConfig.getConfigSpec(), accountId, containerId);
      // Note: if cloud container name not supplied, use ambry container name instead
      String cloudContainerName = (containerConfig.getCloudContainerName() != null) ? containerConfig.getCloudContainerName()
          : ambryContainer.getName();
      return new CloudReplicationConfig(containerConfig.getDestinationType(), clearConfigSpec, cloudContainerName);
    } catch (Exception ex) {
      logger.error("Getting cloud replication config for blob " + blobId, ex);
      return null;
    }
  }

  private String decryptString(String input, short accountId, short containerId) throws GeneralSecurityException {
    if (kms == null || cryptoService == null) {
      return input;
    }
    Object keySpec = kms.getKey(accountId, containerId);
    ByteBuffer clearBuf = cryptoService.decrypt(ByteBuffer.wrap(input.getBytes()), keySpec);
    return new String(clearBuf.array());
  }

}
