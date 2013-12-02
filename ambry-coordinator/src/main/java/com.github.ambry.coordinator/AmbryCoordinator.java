package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.CoordinatorConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.shared.BlobId;
import com.github.ambry.utils.BlockingChannelPool;
import com.github.ambry.utils.BlockingChannelPoolFactory;
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
  private final ClusterMap clusterMap;

  private int operationTimeoutMs;

  private String clientId;
  private String datacenterName;
  private ExecutorService requesterPool;
  private BlockingChannelPool connectionPool;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public AmbryCoordinator(VerifiableProperties properties, ClusterMap clusterMap) {
    this.properties = properties;
    this.shuttingDown = new AtomicBoolean(false);
    this.clusterMap = clusterMap;
  }

  @Override
  public void start() {
    logger.info("start started");
    try {
      CoordinatorConfig coordinatorConfig = new CoordinatorConfig(properties);
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      properties.verify();

      this.clientId = coordinatorConfig.hostname;
      this.datacenterName = coordinatorConfig.datacenterName;
      if (!clusterMap.hasDatacenter(datacenterName)) {
        throw new IllegalStateException("Datacenter with name " + datacenterName + " is not part of cluster map. " +
                                        "Coordinator cannot start.");
      }
      this.operationTimeoutMs = coordinatorConfig.operationTimeoutMs;
      this.requesterPool = Executors.newFixedThreadPool(coordinatorConfig.requesterPoolSize);

      BlockingChannelPoolFactory bcpFactory = Utils.getObj(coordinatorConfig.connectionPoolFactory,
                                                           connectionPoolConfig);
      this.connectionPool = bcpFactory.getBlockingChannelPool();
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
    return new OperationContext(clientId);
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
      throw new CoordinatorException("BlobIdString argument is null or zero length.",
                                     CoordinatorError.UnexpectedInternalError);
    }

    BlobId blobId;
    try {
      blobId = new BlobId(blobIdString, clusterMap);
    }
    catch (IOException e) {
      throw new CoordinatorException("Failed to construct BlobId based on specified string.",
                                     CoordinatorError.UnexpectedInternalError);
    }
    return blobId;
  }

  @Override
  public String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata,
                        InputStream blobStream) throws CoordinatorException {
    if (blobProperties == null) {
      throw new IllegalArgumentException("BlobProperties argument to put operation is null.");
    }
    if (userMetadata == null) {
      throw new IllegalArgumentException("User metadata argument to put operation is null.");
    }
    if (blobStream == null) {
      throw new IllegalArgumentException("Blob stream argument to put operation is null.");
    }

    OperationContext oc = getOperationContext();
    PartitionId partitionId = getPartitionForPut();
    BlobId blobId = new BlobId(partitionId);

    new PutOperation(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, blobProperties,
                     userMetadata, blobStream).execute();

    return blobId.toString();
  }

  @Override
  public void deleteBlob(String blobIdString) throws CoordinatorException {
    OperationContext oc = getOperationContext();
    BlobId blobId = getBlobIdFromString(blobIdString);

    new DeleteOperation(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs).execute();
  }

  @Override
  public void cancelTTL(String blobIdString) throws CoordinatorException {
    OperationContext oc = getOperationContext();
    BlobId blobId = getBlobIdFromString(blobIdString);

    new CancelTTLOperation(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs).execute();
  }

  @Override
  public BlobProperties getBlobProperties(String blobIdString) throws CoordinatorException {
    OperationContext oc = getOperationContext();
    BlobId blobId = getBlobIdFromString(blobIdString);

    GetBlobPropertiesOperation gbpo = new GetBlobPropertiesOperation(datacenterName, connectionPool, requesterPool,
                                                                     oc, blobId, operationTimeoutMs, clusterMap);
    gbpo.execute();
    return gbpo.getBlobProperties();
  }

  @Override
  public ByteBuffer getBlobUserMetadata(String blobIdString) throws CoordinatorException {
    OperationContext oc = getOperationContext();
    BlobId blobId = getBlobIdFromString(blobIdString);

    GetBlobUserMetadataOperation gumo = new GetBlobUserMetadataOperation(datacenterName, connectionPool,
                                                                         requesterPool, oc,
                                                                         blobId, operationTimeoutMs, clusterMap);
    gumo.execute();
    return gumo.getUserMetadata();
  }

  @Override
  public BlobOutput getBlob(String blobIdString) throws CoordinatorException {
    OperationContext oc = getOperationContext();
    BlobId blobId = getBlobIdFromString(blobIdString);

    GetBlobOperation gbdo = new GetBlobOperation(datacenterName, connectionPool, requesterPool, oc,
                                                 blobId, operationTimeoutMs, clusterMap);
    gbdo.execute();
    return gbdo.getBlobOutput();
  }
}

