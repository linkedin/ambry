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
package com.github.ambry.server;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;

import static org.junit.Assert.*;


/**
 * A mock cluster that is setup with multiple datacenters.
 * The setup configuration is determined by the mock cluster map.
 * For each data node in the mock cluster map, we start up a server.
 * On shutdown we ensure the servers are shutdown.
 */
public class MockCluster {
  private final MockClusterAgentsFactory mockClusterAgentsFactory;
  private final MockClusterMap clusterMap;
  private List<AmbryServer> serverList = null;
  private boolean serverInitialized = false;
  private int generalDataNodeIndex;
  private int prefetchDataNodeIndex;
  private final List<String> sslEnabledDataCenterList;
  private final Properties sslProps;
  private final boolean enableHardDeletes;
  private final Time time;

  public MockCluster(boolean enableHardDeletes, Time time) throws IOException {
    this(new Properties(), enableHardDeletes, time);
  }

  public MockCluster(Properties sslProps, boolean enableHardDeletes, Time time) throws IOException {
    this.sslProps = sslProps;
    this.enableHardDeletes = enableHardDeletes;
    this.time = time;
    // sslEnabledDatacenters represents comma separated list of datacenters to which ssl should be enabled
    String sslEnabledDataCentersStr = sslProps.getProperty("clustermap.ssl.enabled.datacenters");
    sslEnabledDataCenterList =
        sslEnabledDataCentersStr != null ? Utils.splitString(sslEnabledDataCentersStr, ",") : new ArrayList<String>();

    mockClusterAgentsFactory = new MockClusterAgentsFactory(sslEnabledDataCentersStr != null, 9, 3, 3);
    clusterMap = mockClusterAgentsFactory.getClusterMap();

    serverList = new ArrayList<AmbryServer>();
    generalDataNodeIndex = 0;
    prefetchDataNodeIndex = clusterMap.getDataNodes().size() - 1;
  }

  public void initializeServers(NotificationSystem notificationSystem) {
    List<MockDataNodeId> dataNodes = clusterMap.getDataNodes();
    for (int i = 0; i < dataNodes.size(); i++) {
      if (sslEnabledDataCenterList != null) {
        dataNodes.get(i).setSslEnabledDataCenters(sslEnabledDataCenterList);
      }
      initializeServer(dataNodes.get(i), sslProps, enableHardDeletes, prefetchDataNodeIndex == i, notificationSystem,
          time);
    }
  }

  public List<AmbryServer> getServers() {
    return serverList;
  }

  /**
   * Get a {@link MockDataNodeId} whose data prefetch is disabled.
   */
  public MockDataNodeId getGeneralDataNode() {
    return clusterMap.getDataNodes().get(generalDataNodeIndex);
  }

  /**
   * Get a {@link MockDataNodeId} whose data prefetch is enabled.
   */
  public MockDataNodeId getPrefetchDataNode() {
    return clusterMap.getDataNodes().get(prefetchDataNodeIndex);
  }

  public MockClusterMap getClusterMap() {
    return clusterMap;
  }

  private void initializeServer(DataNodeId dataNodeId, Properties sslProperties, boolean enableHardDeletes,
      boolean enableDataPrefetch, NotificationSystem notificationSystem, Time time) {
    Properties props = new Properties();
    props.setProperty("host.name", dataNodeId.getHostname());
    props.setProperty("port", Integer.toString(dataNodeId.getPort()));
    props.setProperty("store.data.flush.interval.seconds", "1");
    props.setProperty("store.enable.hard.delete", Boolean.toString(enableHardDeletes));
    props.setProperty("store.deleted.message.retention.days", "1");
    props.setProperty("replication.token.flush.interval.seconds", "5");
    props.setProperty("replication.validate.message.stream", "true");
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("store.validate.authorization", "true");
    props.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    props.setProperty("server.enable.store.data.prefetch", Boolean.toString(enableDataPrefetch));
    props.putAll(sslProperties);
    VerifiableProperties propverify = new VerifiableProperties(props);
    AmbryServer server = new AmbryServer(propverify, mockClusterAgentsFactory, notificationSystem, time);
    serverList.add(server);
  }

  public void startServers() throws InstantiationException, IOException {
    serverInitialized = true;
    try {
      for (AmbryServer server : serverList) {
        server.startup();
      }
    } catch (Exception e) {
      // clean up other servers which was started already
      cleanup();
      throw e;
    }
  }

  public void cleanup() throws IOException {
    if (serverInitialized) {
      CountDownLatch shutdownLatch = new CountDownLatch(serverList.size());
      for (AmbryServer server : serverList) {
        new Thread(new ServerShutdown(shutdownLatch, server)).start();
      }
      try {
        if (!shutdownLatch.await(1, TimeUnit.MINUTES)) {
          fail("Did not shutdown in 1 minute");
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      clusterMap.cleanup();
    }
  }

  public List<DataNodeId> getOneDataNodeFromEachDatacenter(ArrayList<String> datacenterList) {
    HashSet<String> datacenters = new HashSet<String>();
    List<DataNodeId> toReturn = new ArrayList<DataNodeId>();
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      if (datacenterList.contains(dataNodeId.getDatacenterName())) {
        if (!datacenters.contains(dataNodeId.getDatacenterName())) {
          datacenters.add(dataNodeId.getDatacenterName());
          toReturn.add(dataNodeId);
        }
      }
    }
    return toReturn;
  }
}

class ServerShutdown implements Runnable {
  private final CountDownLatch latch;
  private final AmbryServer server;

  public ServerShutdown(CountDownLatch latch, AmbryServer ambryServer) {
    this.latch = latch;
    this.server = ambryServer;
  }

  @Override
  public void run() {
    server.shutdown();
    latch.countDown();
  }
}

/**
 * Tracks the arrival of events and allows waiting on all events of a particular type to arrive
 */
class EventTracker {
  private final int numberOfReplicas;
  private final Helper creationHelper;
  private final Helper deletionHelper;
  private final ConcurrentMap<UpdateType, Helper> updateHelpers = new ConcurrentHashMap<>();

  /**
   * Helper class that encapsulates the information needed to track a type of event
   */
  private class Helper {
    private final ConcurrentHashMap<String, Boolean> hosts = new ConcurrentHashMap<>();
    private final AtomicInteger notificationsReceived = new AtomicInteger(0);
    private CountDownLatch latch = new CountDownLatch(numberOfReplicas);

    /**
     * Tracks the event that arrived on {@code host}:{@code port}.
     * @param host the host that received the event
     * @param port the port of the host that describes the instance along with {@code host}.
     */
    void track(String host, int port) {
      notificationsReceived.incrementAndGet();
      if (hosts.putIfAbsent(getKey(host, port), true) == null) {
        latch.countDown();
      }
    }

    /**
     * Waits until the all replicas receive the event.
     * @param duration the duration to wait for all the events to arrive
     * @param timeUnit the time unit of {@code duration}
     * @return {@code true} if events were received in all replicas within the {@code duration} specified.
     * @throws InterruptedException
     */
    boolean await(long duration, TimeUnit timeUnit) throws InterruptedException {
      return latch.await(duration, timeUnit);
    }

    /**
     * Nullifies the notification received (if any) on the given {@code host}:{@code port}.
     * This method is NOT thread safe and should not be used concurrently with other methods in this class like
     * await() and track().
     * @param host the host that to decrement on
     * @param port the port of the host that describes the instance along with {@code host}.
     */
    void decrementCount(String host, int port) {
      if (hosts.remove(getKey(host, port)) != null) {
        long finalCount = latch.getCount() + 1;
        if (finalCount > numberOfReplicas) {
          throw new IllegalArgumentException("Cannot add more replicas than the max possible replicas");
        }
        latch = new CountDownLatch((int) finalCount);
      }
    }

    /**
     * @param host the host that received the event
     * @param port the port of the host that describes the instance along with {@code host}.
     * @return the unique key created for this host:port
     */
    private String getKey(String host, int port) {
      return host + ":" + port;
    }
  }

  /**
   * @param expectedNumberOfReplicas the total number of replicas that will fire events
   */
  EventTracker(int expectedNumberOfReplicas) {
    numberOfReplicas = expectedNumberOfReplicas;
    creationHelper = new Helper();
    deletionHelper = new Helper();
  }

  /**
   * Tracks the creation event that arrived on {@code host}:{@code port}.
   * @param host the host that received the create
   * @param port the port of the host that describes the instance along with {@code host}.
   */
  void trackCreation(String host, int port) {
    creationHelper.track(host, port);
  }

  /**
   * Tracks the deletion event that arrived on {@code host}:{@code port}.
   * @param host the host that received the delete
   * @param port the port of the host that describes the instance along with {@code host}.
   */
  void trackDeletion(String host, int port) {
    deletionHelper.track(host, port);
  }

  /**
   * Tracks the update event of type {@code updateType} that arrived on {@code host}:{@code port}.
   * @param host the host that received the update
   * @param port the port of the host that describes the instance along with {@code host}.
   * @param updateType the {@link UpdateType} received
   */
  void trackUpdate(String host, int port, UpdateType updateType) {
    updateHelpers.computeIfAbsent(updateType, type -> new Helper()).track(host, port);
  }

  /**
   * Waits for blob creations on all replicas
   * @return {@code true} if creations were received in all replicas within the {@code duration} specified.
   * @throws InterruptedException
   */
  boolean awaitBlobCreations() throws InterruptedException {
    return creationHelper.await(10, TimeUnit.SECONDS);
  }

  /**
   * Waits for blob deletions on all replicas
   * @return {@code true} if deletions were received in all replicas within the {@code duration} specified.
   * @throws InterruptedException
   */
  boolean awaitBlobDeletions() throws InterruptedException {
    return deletionHelper.await(10, TimeUnit.SECONDS);
  }

  /**
   * Waits for blob updates of type {@code updateType} on all replicas
   * @param updateType the type of update to wait for
   * @return {@code true} if updates of type {@code updateType} were received in all replicas within the
   * {@code duration} specified.
   * @throws InterruptedException
   */
  boolean awaitBlobUpdates(UpdateType updateType) throws InterruptedException {
    return updateHelpers.computeIfAbsent(updateType, type -> new Helper()).await(10, TimeUnit.SECONDS);
  }

  /**
   * Nullifies the creation notification on {@code host}:{@code port}.
   * This method is NOT thread safe and should not be used concurrently with other methods in this class like
   * await() and track().
   * @param host the host that to decrement on
   * @param port the port of the host that describes the instance along with {@code host}.
   */
  void decrementCreated(String host, int port) {
    creationHelper.decrementCount(host, port);
  }

  /**
   * Nullifies the delete notification on {@code host}:{@code port}.
   * This method is NOT thread safe and should not be used concurrently with other methods in this class like
   * await() and track().
   * @param host the host that to decrement on
   * @param port the port of the host that describes the instance along with {@code host}.
   */
  void decrementDeleted(String host, int port) {
    deletionHelper.decrementCount(host, port);
  }

  /**
   * Nullifies the update notification for {@code updateType} on {@code host}:{@code port}.
   * This method is NOT thread safe and should not be used concurrently with other methods in this class like
   * await() and track().
   * @param host the host that to decrement on
   * @param port the port of the host that describes the instance along with {@code host}.
   * @param updateType the {@link UpdateType} to nullify the notification for
   */
  void decrementUpdated(String host, int port, UpdateType updateType) {
    updateHelpers.computeIfAbsent(updateType, type -> new Helper()).decrementCount(host, port);
  }
}

/**
 * A mock notification system that helps to identify when blobs
 * get replicated. This class is not thread safe
 */
class MockNotificationSystem implements NotificationSystem {

  private final ConcurrentHashMap<String, EventTracker> objectTracker = new ConcurrentHashMap<String, EventTracker>();
  private final ClusterMap clusterMap;

  public MockNotificationSystem(ClusterMap clusterMap) {
    this.clusterMap = clusterMap;
  }

  @Override
  public void onBlobCreated(String blobId, BlobProperties blobProperties, Account account, Container container,
      NotificationBlobType notificationBlobType) {
    // ignore
  }

  @Override
  public void onBlobTtlUpdated(String blobId, String serviceId, long expiresAtMs, Account account,
      Container container) {
    // ignore
  }

  @Override
  public void onBlobDeleted(String blobId, String serviceId, Account account, Container container) {
    // ignore
  }

  @Override
  public synchronized void onBlobReplicaCreated(String sourceHost, int port, String blobId,
      BlobReplicaSourceType sourceType) {
    objectTracker.computeIfAbsent(blobId, k -> new EventTracker(getNumReplicas(blobId)))
        .trackCreation(sourceHost, port);
  }

  @Override
  public synchronized void onBlobReplicaDeleted(String sourceHost, int port, String blobId,
      BlobReplicaSourceType sourceType) {
    objectTracker.computeIfAbsent(blobId, k -> new EventTracker(getNumReplicas(blobId)))
        .trackDeletion(sourceHost, port);
  }

  @Override
  public synchronized void onBlobReplicaUpdated(String sourceHost, int port, String blobId,
      BlobReplicaSourceType sourceType, UpdateType updateType, MessageInfo info) {
    objectTracker.computeIfAbsent(blobId, k -> new EventTracker(getNumReplicas(blobId)))
        .trackUpdate(sourceHost, port, updateType);
  }

  @Override
  public void close() {
    // ignore
  }

  /**
   * Waits for blob creations on all replicas for {@code blobId}
   * @param blobId the ID of the blob
   */
  void awaitBlobCreations(String blobId) {
    try {
      if (!objectTracker.get(blobId).awaitBlobCreations()) {
        Assert.fail("Failed awaiting for " + blobId + " creations");
      }
    } catch (InterruptedException e) {
      // ignore
    }
  }

  /**
   * Waits for blob deletions on all replicas for {@code blobId}
   * @param blobId the ID of the blob
   */
  void awaitBlobDeletions(String blobId) {
    try {
      if (!objectTracker.get(blobId).awaitBlobDeletions()) {
        Assert.fail("Failed awaiting for " + blobId + " deletions");
      }
    } catch (InterruptedException e) {
      // ignore
    }
  }

  /**
   * Waits for blob updates of type {@code updateType} on all replicas for {@code blobId}
   * @param blobId the ID of the blob
   * @param updateType the {@link UpdateType} to wait for
   */
  void awaitBlobUpdates(String blobId, UpdateType updateType) {
    try {
      if (!objectTracker.get(blobId).awaitBlobUpdates(updateType)) {
        Assert.fail("Failed awaiting for " + blobId + " updates of type " + updateType);
      }
    } catch (InterruptedException e) {
      // ignore
    }
  }

  /**
   * Nullifies the creation notification for {@code blobId} on {@code host}:{@code port}.
   * This method should not be used concurrently with the await functions
   * @param blobId the blob ID whose creation notification needs to be nullified
   * @param host the host that to decrement on
   * @param port the port of the host that describes the instance along with {@code host}.
   */
  synchronized void decrementCreatedReplica(String blobId, String host, int port) {
    objectTracker.get(blobId).decrementCreated(host, port);
  }

  /**
   * Nullifies the deletion notification for {@code blobId} on {@code host}:{@code port}.
   * This method should not be used concurrently with the await functions
   * @param blobId the blob ID whose deletion notification needs to be nullified
   * @param host the host that to decrement on
   * @param port the port of the host that describes the instance along with {@code host}.
   */
  synchronized void decrementDeletedReplica(String blobId, String host, int port) {
    objectTracker.get(blobId).decrementDeleted(host, port);
  }

  /**
   * Nullifies the update notification of type {@code updateType} for {@code blobId} on {@code host}:{@code port}.
   * This method should not be used concurrently with the await functions
   * @param blobId the blob ID whose update notification needs to be nullified
   * @param host the host that to decrement on
   * @param port the port of the host that describes the instance along with {@code host}.
   * @param updateType the {@link UpdateType} to nullify the notification for
   */
  synchronized void decrementUpdatedReplica(String blobId, String host, int port, UpdateType updateType) {
    objectTracker.get(blobId).decrementDeleted(host, port);
  }

  /**
   * @param blobId the blob ID received
   * @return the number of replicas of {@code blobId}
   */
  private int getNumReplicas(String blobId) {
    try {
      BlobId blobIdObj = new BlobId(blobId, clusterMap);
      PartitionId partitionId = blobIdObj.getPartition();
      return partitionId.getReplicaIds().size();
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid blob ID: " + blobId, e);
    }
  }
}
