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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
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
  private NotificationSystem notificationSystem;
  private boolean serverInitialized = false;

  public MockCluster(NotificationSystem notificationSystem, boolean enableHardDeletes, Time time)
      throws IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
    this(notificationSystem, new Properties(), enableHardDeletes, time);
  }

  public MockCluster(NotificationSystem notificationSystem, Properties sslProps, boolean enableHardDeletes, Time time)
      throws IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
    // sslEnabledDatacenters represents comma separated list of datacenters to which ssl should be enabled
    String sslEnabledDataCentersStr = sslProps.getProperty("clustermap.ssl.enabled.datacenters");
    ArrayList<String> sslEnabledDataCenterList =
        sslEnabledDataCentersStr != null ? Utils.splitString(sslEnabledDataCentersStr, ",") : new ArrayList<String>();

    this.notificationSystem = notificationSystem;
    mockClusterAgentsFactory = new MockClusterAgentsFactory(sslEnabledDataCentersStr != null, 9, 3, 3);
    clusterMap = mockClusterAgentsFactory.getClusterMap();

    serverList = new ArrayList<AmbryServer>();
    List<MockDataNodeId> dataNodes = clusterMap.getDataNodes();
    try {
      for (MockDataNodeId dataNodeId : dataNodes) {
        if (sslEnabledDataCentersStr != null) {
          dataNodeId.setSslEnabledDataCenters(sslEnabledDataCenterList);
        }
        initializeServer(dataNodeId, sslProps, enableHardDeletes, time);
      }
    } catch (InstantiationException e) {
      // clean up other servers which was started already
      cleanup();
      throw e;
    }
  }

  public List<AmbryServer> getServers() {
    return serverList;
  }

  public MockClusterMap getClusterMap() {
    return clusterMap;
  }

  private void initializeServer(DataNodeId dataNodeId, Properties sslProperties, boolean enableHardDeletes, Time time)
      throws IOException, InstantiationException, URISyntaxException {
    Properties props = new Properties();
    props.setProperty("host.name", dataNodeId.getHostname());
    props.setProperty("port", Integer.toString(dataNodeId.getPort()));
    props.setProperty("store.data.flush.interval.seconds", "1");
    props.setProperty("store.enable.hard.delete", Boolean.toString(enableHardDeletes));
    props.setProperty("store.deleted.message.retention.days", "1");
    props.setProperty("replication.token.flush.interval.seconds", "5");
    props.setProperty("replication.wait.time.between.replicas.ms", "50");
    props.setProperty("replication.validate.message.stream", "true");
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    props.putAll(sslProperties);
    VerifiableProperties propverify = new VerifiableProperties(props);
    AmbryServer server = new AmbryServer(propverify, mockClusterAgentsFactory, notificationSystem, time);
    serverList.add(server);
  }

  public void startServers() throws InstantiationException {
    serverInitialized = true;
    for (AmbryServer server : serverList) {
      server.startup();
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

class Tracker {
  private final int numberOfReplicas;
  private CountDownLatch totalReplicasCreated;
  private CountDownLatch totalReplicasDeleted;

  private ConcurrentHashMap<String, Boolean> creationSrcHosts = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Boolean> deletionSrcHosts = new ConcurrentHashMap<>();

  private AtomicInteger creationNotificationsReceived = new AtomicInteger(0);
  private AtomicInteger deletionNotificationsReceived = new AtomicInteger(0);

  Tracker(int expectedNumberOfReplicas) {
    numberOfReplicas = expectedNumberOfReplicas;
    totalReplicasCreated = new CountDownLatch(expectedNumberOfReplicas);
    totalReplicasDeleted = new CountDownLatch(expectedNumberOfReplicas);
  }

  void trackCreation(String srcHost, int srcPort) {
    creationNotificationsReceived.incrementAndGet();
    if (creationSrcHosts.putIfAbsent(getKey(srcHost, srcPort), true) == null) {
      totalReplicasCreated.countDown();
    }
  }

  void trackDeletion(String srcHost, int srcPort) {
    deletionNotificationsReceived.incrementAndGet();
    if (deletionSrcHosts.putIfAbsent(getKey(srcHost, srcPort), true) == null) {
      totalReplicasDeleted.countDown();
    }
  }

  boolean awaitBlobCreations() throws InterruptedException {
    return totalReplicasCreated.await(10, TimeUnit.SECONDS);
  }

  boolean awaitBlobDeletions() throws InterruptedException {
    return totalReplicasDeleted.await(10, TimeUnit.SECONDS);
  }

  void decrementCreated(String host, int port) {
    if (creationSrcHosts.remove(getKey(host, port)) != null) {
      totalReplicasCreated = decrementCount(totalReplicasCreated);
    }
  }

  void decrementDeleted(String host, int port) {
    if (deletionSrcHosts.remove(getKey(host, port)) != null) {
      totalReplicasDeleted = decrementCount(totalReplicasDeleted);
    }
  }

  private CountDownLatch decrementCount(CountDownLatch latch) {
    long finalCount = latch.getCount() + 1;
    if (finalCount > numberOfReplicas) {
      throw new IllegalArgumentException("Cannot add more replicas than the max possible replicas");
    }
    return new CountDownLatch((int) finalCount);
  }

  private String getKey(String srcHost, int srcPort) {
    return srcHost + ":" + srcPort;
  }
}

/**
 * A mock notification system that helps to identify when blobs
 * get replicated. This class is not thread safe
 */
class MockNotificationSystem implements NotificationSystem {

  private ConcurrentHashMap<String, Tracker> objectTracker = new ConcurrentHashMap<String, Tracker>();
  private int numberOfReplicas;

  public MockNotificationSystem(int numberOfReplicas) {
    this.numberOfReplicas = numberOfReplicas;
  }

  @Override
  public void onBlobCreated(String blobId, BlobProperties blobProperties, NotificationBlobType notificationBlobType) {
    // ignore
  }

  @Override
  public void onBlobDeleted(String blobId, String serviceId) {
    // ignore
  }

  @Override
  public synchronized void onBlobReplicaCreated(String sourceHost, int port, String blobId,
      BlobReplicaSourceType sourceType) {
    objectTracker.computeIfAbsent(blobId, k -> new Tracker(numberOfReplicas)).trackCreation(sourceHost, port);
  }

  @Override
  public synchronized void onBlobReplicaDeleted(String sourceHost, int port, String blobId,
      BlobReplicaSourceType sourceType) {
    objectTracker.get(blobId).trackDeletion(sourceHost, port);
  }

  @Override
  public void close() throws IOException {
    // ignore
  }

  void awaitBlobCreations(String blobId) {
    try {
      Tracker tracker = objectTracker.get(blobId);
      if (!tracker.awaitBlobCreations()) {
        Assert.fail("Failed awaiting for " + blobId + " creations");
      }
    } catch (InterruptedException e) {
      // ignore
    }
  }

  void awaitBlobDeletions(String blobId) {
    try {
      Tracker tracker = objectTracker.get(blobId);
      if (!tracker.awaitBlobDeletions()) {
        Assert.fail("Failed awaiting for " + blobId + " deletions");
      }
    } catch (InterruptedException e) {
      // ignore
    }
  }

  synchronized void decrementCreatedReplica(String blobId, String host, int port) {
    Tracker tracker = objectTracker.get(blobId);
    tracker.decrementCreated(host, port);
  }

  synchronized void decrementDeletedReplica(String blobId, String host, int port) {
    Tracker tracker = objectTracker.get(blobId);
    tracker.decrementDeleted(host, port);
  }
}
