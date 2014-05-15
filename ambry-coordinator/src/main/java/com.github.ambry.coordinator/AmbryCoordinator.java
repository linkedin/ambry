package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.CoordinatorConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.ConnectionPoolFactory;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.ambry.utils.Utils.getRandomLong;

/**
 * Ambry Coordinator performs put, delete, cancelTTL, and get(Blob/BlobUserMetadata/BlobProperties) operations.
 */
public class AmbryCoordinator implements Coordinator {

  private final VerifiableProperties properties;
  private final AtomicBoolean shuttingDown;
  private final CoordinatorMetrics coordinatorMetrics;
  private final ClusterMap clusterMap;
  private final NotificationSystem notificationSystem;

  private int operationTimeoutMs;
  private int connectionPoolCheckoutTimeout;

  private String clientId;
  private String datacenterName;
  private ExecutorService requesterPool;
  private ConnectionPool connectionPool;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public AmbryCoordinator(VerifiableProperties properties, ClusterMap clusterMap) {
    this(properties, clusterMap, null);
  }

  public AmbryCoordinator(VerifiableProperties properties,
                          ClusterMap clusterMap,
                          NotificationSystem notificationSystem) {
    this.properties = properties;
    this.shuttingDown = new AtomicBoolean(false);
    this.clusterMap = clusterMap;
    this.coordinatorMetrics = new CoordinatorMetrics(clusterMap);
    this.notificationSystem = notificationSystem;
  }

  @Override
  public void start() {
    logger.info("start started");
    try {
      CoordinatorConfig coordinatorConfig = new CoordinatorConfig(properties);
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      properties.verify();

      this.connectionPoolCheckoutTimeout = coordinatorConfig.connectionPoolCheckoutTimeoutMs;
      this.clientId = coordinatorConfig.hostname;
      this.datacenterName = coordinatorConfig.datacenterName;
      if (!clusterMap.hasDatacenter(datacenterName)) {
        throw new IllegalStateException("Datacenter with name " + datacenterName + " is not part of cluster map. " +
                                        "Coordinator cannot start.");
      }
      this.operationTimeoutMs = coordinatorConfig.operationTimeoutMs;
      this.requesterPool = Executors.newFixedThreadPool(coordinatorConfig.requesterPoolSize);

      ConnectionPoolFactory connectionPoolFactory = Utils.getObj(coordinatorConfig.connectionPoolFactory,
                                                                 connectionPoolConfig);
      this.connectionPool = connectionPoolFactory.getConnectionPool();
      connectionPool.start();

      logger.info("start completed");
    }
    catch (Exception e) {
      logger.error("Error during start {}", e);
      throw new InstantiationError("Error during start " + e);
    }
  }

  @Override
  public void shutdown() {
    if (shuttingDown.getAndSet(true)) {
      return;
    }
    logger.info("shutdown started");

    if (requesterPool != null) {
      try {
        requesterPool.shutdown();
        requesterPool.awaitTermination(1, TimeUnit.MINUTES);
      }
      catch (Exception e) {
        logger.error("Error while shutting down requesterPool in coordinator {}", e);
      }
      this.requesterPool = null;
    }

    if (connectionPool != null) {
      try {
        connectionPool.shutdown();
      }
      catch (Exception e) {
        logger.error("Error while shutting down connectionPool in coordinator {}", e);
      }
      connectionPool = null;
    }

    logger.info("shutdown completed");
  }

  private OperationContext getOperationContext() {
    return new OperationContext(clientId, connectionPoolCheckoutTimeout, coordinatorMetrics);
  }

  private PartitionId getPartitionForPut() throws CoordinatorException {
    if (clusterMap.getWritablePartitionIdsCount() < 1) {
      throw new CoordinatorException("No writable partitions available.", CoordinatorError.AmbryUnavailable);
    }
    long index = getRandomLong(clusterMap.getWritablePartitionIdsCount());
    return clusterMap.getWritablePartitionIdAt(index);
  }

  private BlobId getBlobIdFromString(String blobIdString) throws CoordinatorException {
    if (blobIdString == null || blobIdString.length() == 0) {
      logger.error("BlobIdString argument is null or zero length: {}", blobIdString);
      throw new CoordinatorException("BlobId is empty.", CoordinatorError.InvalidBlobId);
    }

    BlobId blobId;
    try {
      blobId = new BlobId(blobIdString, clusterMap);
    }
    catch (IOException e) {
      logger.info("Caller passed in invalid BlobId.");
      throw new CoordinatorException("BlobId is invalid.", CoordinatorError.InvalidBlobId);
    }
    return blobId;
  }

  @Override
  public String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata,
                        InputStream blobStream) throws CoordinatorException {
    try {
      logger.trace("putBlob. " + blobProperties);
      long startTimeInMs = System.currentTimeMillis();

      if (blobProperties == null) {
        logger.info("Caller passed in null blobProperties.");
        throw new CoordinatorException("BlobProperties argument to put operation is null.",
                                       CoordinatorError.InvalidPutArgument);
      }
      if (userMetadata == null) {
        logger.info("Caller passed in null userMetadata.");
        throw new CoordinatorException("UserMetadata argument to put operation is null.",
                                       CoordinatorError.InvalidPutArgument);
      }
      if (blobStream == null) {
        logger.info("Caller passed in null blobStream.");
        throw new CoordinatorException("Blob stream argument to put operation is null.",
                                       CoordinatorError.InvalidPutArgument);
      }

      PartitionId partitionId = getPartitionForPut();
      BlobId blobId = new BlobId(partitionId);
      PutOperation putOperation = new PutOperation(datacenterName,
                                                   connectionPool,
                                                   requesterPool,
                                                   getOperationContext(),
                                                   blobId,
                                                   operationTimeoutMs,
                                                   blobProperties,
                                                   userMetadata,
                                                   blobStream);
      putOperation.execute();

      if (notificationSystem != null) {
        notificationSystem.onBlobCreated(clientId, blobId.toString(), blobProperties, userMetadata.array());
      }
      coordinatorMetrics.putBlobOperationRate.mark();
      coordinatorMetrics.putBlobOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);

      return blobId.toString();
    }
    catch (CoordinatorException e) {
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.PutBlob, e.getErrorCode());
      throw e;
    }
  }

  @Override
  public void deleteBlob(String blobIdString) throws CoordinatorException {
    try {
      logger.trace("deleteBlob. " + blobIdString);
      long startTimeInMs = System.currentTimeMillis();

      BlobId blobId = getBlobIdFromString(blobIdString);
      DeleteOperation deleteOperation = new DeleteOperation(datacenterName,
                                                            connectionPool,
                                                            requesterPool,
                                                            getOperationContext(),
                                                            blobId,
                                                            operationTimeoutMs);
      deleteOperation.execute();

      if (notificationSystem != null) {
        notificationSystem.onBlobDeleted(clientId, blobIdString);
      }
      coordinatorMetrics.deleteBlobOperationRate.mark();
      coordinatorMetrics.deleteBlobOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);
    }
    catch (CoordinatorException e) {
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.DeleteBlob, e.getErrorCode());
      throw e;
    }
  }

  @Override
  public void cancelTTL(String blobIdString) throws CoordinatorException {
    logger.trace("cancelTTL. " + blobIdString);
    BlobId blobId = getBlobIdFromString(blobIdString);
    CancelTTLOperation cancelTTLOperation = new CancelTTLOperation(datacenterName,
                                                                   connectionPool,
                                                                   requesterPool,
                                                                   getOperationContext(),
                                                                   blobId,
                                                                   operationTimeoutMs);
    cancelTTLOperation.execute();
  }

  @Override
  public BlobProperties getBlobProperties(String blobIdString) throws CoordinatorException {
    try {
      logger.trace("getBlobProperties. " + blobIdString);
      long startTimeInMs = System.currentTimeMillis();

      BlobId blobId = getBlobIdFromString(blobIdString);
      GetBlobPropertiesOperation gbpo = new GetBlobPropertiesOperation(datacenterName,
                                                                       connectionPool,
                                                                       requesterPool,
                                                                       getOperationContext(),
                                                                       blobId,
                                                                       operationTimeoutMs,
                                                                       clusterMap);
      gbpo.execute();

      coordinatorMetrics.getBlobPropertiesOperationRate.mark();
      coordinatorMetrics.getBlobPropertiesOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);

      return gbpo.getBlobProperties();
    }
    catch (CoordinatorException e) {
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.GetBlobProperties, e.getErrorCode());
      throw e;
    }
  }

  @Override
  public ByteBuffer getBlobUserMetadata(String blobIdString) throws CoordinatorException {
    try {
      logger.trace("getBlobUserMetadata. " + blobIdString);
      long startTimeInMs = System.currentTimeMillis();

      BlobId blobId = getBlobIdFromString(blobIdString);
      GetBlobUserMetadataOperation gumo = new GetBlobUserMetadataOperation(datacenterName,
                                                                           connectionPool,
                                                                           requesterPool,
                                                                           getOperationContext(),
                                                                           blobId,
                                                                           operationTimeoutMs,
                                                                           clusterMap);
      gumo.execute();

      coordinatorMetrics.getBlobUserMetadataOperationRate.mark();
      coordinatorMetrics.getBlobUserMetadataOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);

      return gumo.getUserMetadata();
    }
    catch (CoordinatorException e) {
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.GetBlobUserMetadata, e.getErrorCode());
      throw e;
    }
  }

  @Override
  public BlobOutput getBlob(String blobIdString) throws CoordinatorException {
    try {
      logger.trace("getBlob. " + blobIdString);
      long startTimeInMs = System.currentTimeMillis();

      BlobId blobId = getBlobIdFromString(blobIdString);
      GetBlobOperation gbdo = new GetBlobOperation(datacenterName,
                                                   connectionPool,
                                                   requesterPool,
                                                   getOperationContext(),
                                                   blobId,
                                                   operationTimeoutMs,
                                                   clusterMap);
      gbdo.execute();

      coordinatorMetrics.getBlobOperationRate.mark();
      coordinatorMetrics.getBlobOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);

      return gbdo.getBlobOutput();
    }
    catch (CoordinatorException e) {
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.GetBlob, e.getErrorCode());
      throw e;
    }
  }
}

