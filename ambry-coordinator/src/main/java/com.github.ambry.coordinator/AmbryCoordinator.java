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
package com.github.ambry.coordinator;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.CoordinatorConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.getRandomLong;


/**
 * Ambry Coordinator performs put, delete, and get(Blob/BlobUserMetadata/BlobProperties) operations.
 */
public class AmbryCoordinator implements Coordinator {
  private final AtomicBoolean shuttingDown;
  private final CoordinatorMetrics coordinatorMetrics;
  private final ClusterMap clusterMap;
  private final ResponseHandler responseHandler;
  private final Random randomForPartitionSelection;
  private int connectionPoolCheckoutTimeout;
  private JmxReporter reporter = null;
  private String clientId;
  private AtomicBoolean crossDCProxyCallsEnabled;
  private ArrayList<String> sslEnabledDatacenters;
  private Logger logger = LoggerFactory.getLogger(getClass());

  protected final NotificationSystem notificationSystem;
  protected int operationTimeoutMs;
  protected String datacenterName;
  protected ExecutorService requesterPool;
  protected ConnectionPool connectionPool;

  public AmbryCoordinator(VerifiableProperties properties, ClusterMap clusterMap) {
    this(properties, clusterMap, new LoggingNotificationSystem());
  }

  public AmbryCoordinator(VerifiableProperties properties, ClusterMap clusterMap,
      NotificationSystem notificationSystem) {
    this.shuttingDown = new AtomicBoolean(false);
    this.clusterMap = clusterMap;
    this.responseHandler = new ResponseHandler(clusterMap);
    this.notificationSystem = notificationSystem;
    this.randomForPartitionSelection = new Random();
    logger.info("coordinator starting");
    try {
      logger.info("Setting up JMX.");
      MetricRegistry registry = clusterMap.getMetricRegistry();
      this.reporter = JmxReporter.forRegistry(registry).build();
      reporter.start();

      logger.info("Creating configs");
      CoordinatorConfig coordinatorConfig = new CoordinatorConfig(properties);
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      SSLConfig sslConfig = new SSLConfig(properties);
      properties.verify();

      this.connectionPoolCheckoutTimeout = coordinatorConfig.connectionPoolCheckoutTimeoutMs;
      this.clientId = coordinatorConfig.hostname;
      this.crossDCProxyCallsEnabled = new AtomicBoolean(coordinatorConfig.crossDCProxyCallEnable);
      this.coordinatorMetrics = new CoordinatorMetrics(clusterMap, crossDCProxyCallsEnabled.get());
      this.datacenterName = coordinatorConfig.datacenterName;
      if (!clusterMap.hasDatacenter(datacenterName)) {
        throw new IllegalStateException("Datacenter with name " + datacenterName + " is not part of cluster map. " +
            "Coordinator cannot start.");
      }
      sslEnabledDatacenters = Utils.splitString(sslConfig.sslEnabledDatacenters, ",");
      this.operationTimeoutMs = coordinatorConfig.operationTimeoutMs;
      logger.info("Creating requester pool");
      this.requesterPool = Executors.newFixedThreadPool(coordinatorConfig.requesterPoolSize);

      logger.info("Getting connection pool");
      ConnectionPoolFactory connectionPoolFactory =
          Utils.getObj(coordinatorConfig.connectionPoolFactory, connectionPoolConfig, sslConfig, registry);
      this.connectionPool = connectionPoolFactory.getConnectionPool();
      connectionPool.start();

      logger.info("coordinator started");
    } catch (Exception e) {
      logger.error("Error during start {}", e);
      throw new InstantiationError("Error during start " + e);
    }
  }

  @Override
  public void close() {
    if (shuttingDown.getAndSet(true)) {
      return;
    }
    logger.info("closing started");

    if (requesterPool != null) {
      try {
        requesterPool.shutdown();
        requesterPool.awaitTermination(1, TimeUnit.MINUTES);
      } catch (Exception e) {
        logger.error("Error while shutting down requesterPool in coordinator {}", e);
      }
      this.requesterPool = null;
    }

    if (connectionPool != null) {
      try {
        connectionPool.shutdown();
      } catch (Exception e) {
        logger.error("Error while shutting down connectionPool in coordinator {}", e);
      }
      connectionPool = null;
    }

    try {
      notificationSystem.close();
    } catch (IOException e) {
      logger.error("Error while closing notification system.", e);
    }

    logger.info("closing completed");
  }

  protected OperationContext getOperationContext() {
    OperationContext oc = new OperationContext(clientId, connectionPoolCheckoutTimeout, crossDCProxyCallsEnabled.get(),
        coordinatorMetrics, responseHandler, sslEnabledDatacenters);
    logger.trace("Operation context " + oc);
    return oc;
  }

  protected PartitionId getPartitionForPut()
      throws CoordinatorException {
    List<PartitionId> partitions = clusterMap.getWritablePartitionIds();
    if (partitions.isEmpty()) {
      throw new CoordinatorException("No writable partitions available.", CoordinatorError.AmbryUnavailable);
    }
    return partitions.get((int) getRandomLong(randomForPartitionSelection, partitions.size()));
  }

  private BlobId getBlobIdFromString(String blobIdString)
      throws CoordinatorException {
    if (blobIdString == null || blobIdString.length() == 0) {
      logger.error("BlobIdString argument is null or zero length: {}", blobIdString);
      throw new CoordinatorException("BlobId is empty.", CoordinatorError.InvalidBlobId);
    }

    BlobId blobId;
    try {
      blobId = new BlobId(blobIdString, clusterMap);
      logger.trace("BlobId created " + blobId + " with partition " + blobId.getPartition());
    } catch (Exception e) {
      logger.error("Caller passed in invalid BlobId " + blobIdString);
      throw new CoordinatorException("BlobId is invalid " + blobIdString, CoordinatorError.InvalidBlobId);
    }
    return blobId;
  }

  @Override
  public String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata, InputStream blobStream)
      throws CoordinatorException {
    long startTimeInMs = System.currentTimeMillis();
    try {
      logger.trace("putBlob. " + blobProperties);
      coordinatorMetrics.putBlobOperationRate.mark();

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
      PutOperation putOperation =
          new PutOperation(datacenterName, connectionPool, requesterPool, getOperationContext(), blobId,
              operationTimeoutMs, blobProperties, userMetadata, blobStream);
      putOperation.execute();

      notificationSystem.onBlobCreated(blobId.getID(), blobProperties, userMetadata.array());
      return blobId.getID();
    } catch (CoordinatorException e) {
      logger.trace("putBlob re-throwing CoordinatorException", e);
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.PutBlob, e.getErrorCode());
      throw e;
    } finally {
      coordinatorMetrics.putBlobOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);
    }
  }

  @Override
  public void deleteBlob(String blobIdString)
      throws CoordinatorException {
    long startTimeInMs = System.currentTimeMillis();
    try {
      logger.trace("deleteBlob. " + blobIdString);
      coordinatorMetrics.deleteBlobOperationRate.mark();
      BlobId blobId = getBlobIdFromString(blobIdString);
      DeleteOperation deleteOperation =
          new DeleteOperation(datacenterName, connectionPool, requesterPool, getOperationContext(), blobId,
              operationTimeoutMs);
      deleteOperation.execute();
      notificationSystem.onBlobDeleted(blobIdString);
    } catch (CoordinatorException e) {
      logger.trace("deleteBlob re-throwing CoordinatorException", e);
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.DeleteBlob, e.getErrorCode());
      throw e;
    } finally {
      coordinatorMetrics.deleteBlobOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);
    }
  }

  @Override
  public BlobProperties getBlobProperties(String blobIdString)
      throws CoordinatorException {

    long startTimeInMs = System.currentTimeMillis();
    try {
      logger.trace("getBlobProperties. " + blobIdString);
      coordinatorMetrics.getBlobPropertiesOperationRate.mark();

      BlobId blobId = getBlobIdFromString(blobIdString);
      GetBlobPropertiesOperation gbpo =
          new GetBlobPropertiesOperation(datacenterName, connectionPool, requesterPool, getOperationContext(), blobId,
              operationTimeoutMs, clusterMap);
      gbpo.execute();
      return gbpo.getBlobProperties();
    } catch (CoordinatorException e) {
      logger.trace("getBlobProperties re-throwing CoordinatorException", e);
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.GetBlobProperties, e.getErrorCode());
      throw e;
    } finally {
      coordinatorMetrics.getBlobPropertiesOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);
    }
  }

  @Override
  public ByteBuffer getBlobUserMetadata(String blobIdString)
      throws CoordinatorException {
    long startTimeInMs = System.currentTimeMillis();
    try {
      logger.trace("getBlobUserMetadata. " + blobIdString);
      coordinatorMetrics.getBlobUserMetadataOperationRate.mark();

      BlobId blobId = getBlobIdFromString(blobIdString);
      GetBlobUserMetadataOperation gumo =
          new GetBlobUserMetadataOperation(datacenterName, connectionPool, requesterPool, getOperationContext(), blobId,
              operationTimeoutMs, clusterMap);
      gumo.execute();
      return gumo.getUserMetadata();
    } catch (CoordinatorException e) {
      logger.trace("getBlobUserMetadata re-throwing CoordinatorException", e);
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.GetBlobUserMetadata, e.getErrorCode());
      throw e;
    } finally {
      coordinatorMetrics.getBlobUserMetadataOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);
    }
  }

  @Override
  public BlobOutput getBlob(String blobIdString)
      throws CoordinatorException {

    long startTimeInMs = System.currentTimeMillis();
    try {
      logger.trace("getBlob. " + blobIdString);
      coordinatorMetrics.getBlobOperationRate.mark();

      BlobId blobId = getBlobIdFromString(blobIdString);
      GetBlobOperation gbdo =
          new GetBlobOperation(datacenterName, connectionPool, requesterPool, getOperationContext(), blobId,
              operationTimeoutMs, clusterMap);
      gbdo.execute();

      return gbdo.getBlobOutput();
    } catch (CoordinatorException e) {
      logger.trace("getBlob re-throwing CoordinatorException", e);
      coordinatorMetrics.countError(CoordinatorMetrics.CoordinatorOperationType.GetBlob, e.getErrorCode());
      throw e;
    } finally {
      coordinatorMetrics.getBlobOperationLatencyInMs.update(System.currentTimeMillis() - startTimeInMs);
    }
  }
}

