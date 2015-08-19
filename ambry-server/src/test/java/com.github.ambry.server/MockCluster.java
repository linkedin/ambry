package com.github.ambry.server;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;


/**
 * A mock cluster that is setup with multiple datacenters.
 * The setup configuration is determined by the mock cluster map.
 * For each data node in the mock cluster map, we start up a server.
 * On shutdown we ensure the servers are shutdown.
 */
public class MockCluster {
  private final MockClusterMap clusterMap;
  private List<AmbryServer> serverList = null;
  private NotificationSystem notificationSystem;

  public MockCluster(NotificationSystem notificationSystem)
      throws IOException, InstantiationException {
    this(notificationSystem, false, "");
  }

  public MockCluster(NotificationSystem notificationSystem, boolean enableSSL, String sslEnabledDatacenters)
      throws IOException, InstantiationException {
    // sslEnabledDatacenters represents comma separated list of datacenters to which ssl should be enabled
    this.notificationSystem = notificationSystem;
    clusterMap = new MockClusterMap(enableSSL);
    serverList = new ArrayList<AmbryServer>();
    ArrayList<String> sslEnabledDatacenterList = Utils.splitString(sslEnabledDatacenters, ",");
    List<MockDataNodeId> dataNodes = clusterMap.getDataNodes();
    try {
      for (MockDataNodeId dataNodeId : dataNodes) {
        startServer(dataNodeId, getSSLEnabledDatacenterValue(dataNodeId.getDatacenterName(), sslEnabledDatacenterList));
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

  private void startServer(DataNodeId dataNodeId, String sslEnabledDatacenters)
      throws IOException, InstantiationException {
    Properties props = new Properties();
    props.setProperty("host.name", dataNodeId.getHostname());
    props.setProperty("port", Integer.toString(dataNodeId.getPort()));
    props.setProperty("store.data.flush.interval.seconds", "1");
    props.setProperty("store.deleted.message.retention.days", "0");
    props.setProperty("store.enable.hard.delete", "true");
    props.setProperty("replication.token.flush.interval.seconds", "5");
    props.setProperty("replication.wait.time.between.replicas.ms", "50");
    props.setProperty("replication.validate.message.stream", "true");
    props.setProperty("replication.ssl.enabled.datacenters", sslEnabledDatacenters);
    props.setProperty("replication.ssl.protocol", "TLS");
    props.setProperty("replication.ssl.keystore.type", "JKS");
    props.setProperty("replication.ssl.keystore.path", "./ambry-network/src/test/resources/selfsigned-keystore.jks");
    props.setProperty("replication.ssl.keystore.password", "unittestonly");
    props.setProperty("replication.ssl.key.password", "unittestonly");
    props.setProperty("replication.ssl.truststore.type", "JKS");
    props.setProperty("replication.ssl.truststore.path", "./ambry-network/src/test/resources/selfsigned-truststore.ts");
    props.setProperty("replication.ssl.truststore.password", "unittestonly");
    props.setProperty("replication.ssl.cipher.suites", "TLS_RSA_WITH_AES_128_CBC_SHA256");
    VerifiableProperties propverify = new VerifiableProperties(props);
    AmbryServer server = new AmbryServer(propverify, clusterMap, notificationSystem);
    serverList.add(server);
    server.startup();
  }

  public void cleanup() {
    CountDownLatch shutdownLatch = new CountDownLatch(serverList.size());
    for (AmbryServer server : serverList) {

      new Thread(new ServerShutdown(shutdownLatch, server)).start();
    }
    try {
      shutdownLatch.await();
    } catch (Exception e) {
      assertTrue(false);
    }

    clusterMap.cleanup();
  }

  /**
   * Find the value for sslEnabledDatacenter config for the given datacenter
   * @param datacenter for which sslEnabledDatacenter config value has to be determinded
   * @param sslEnabledDataCenterList list of datacenters upon which ssl should be enabled
   * @return the config value for sslEnabledDatacenters for the given datacenter
   */
  private String getSSLEnabledDatacenterValue(String datacenter, ArrayList<String> sslEnabledDataCenterList) {
    sslEnabledDataCenterList.remove(datacenter);
    String sslEnabledDatacenters = Utils.concatenateString(sslEnabledDataCenterList, ",");
    return sslEnabledDatacenters;
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
  public CountDownLatch totalReplicasDeleted;
  public CountDownLatch totalReplicasCreated;

  public Tracker(int expectedNumberOfReplicas) {
    totalReplicasDeleted = new CountDownLatch(expectedNumberOfReplicas);
    totalReplicasCreated = new CountDownLatch(expectedNumberOfReplicas);
  }
}

/**
 * A mock notification system that helps to identify when blobs
 * get replicated. This class is not thread safe
 */
class MockNotificationSystem implements NotificationSystem {

  ConcurrentHashMap<String, Tracker> objectTracker = new ConcurrentHashMap<String, Tracker>();
  int numberOfReplicas;

  public MockNotificationSystem(int numberOfReplicas) {
    this.numberOfReplicas = numberOfReplicas;
  }

  @Override
  public void onBlobCreated(String blobId, BlobProperties blobProperties, byte[] userMetadata) {
    // ignore
  }

  @Override
  public void onBlobDeleted(String blobId) {
    // ignore
  }

  @Override
  public synchronized void onBlobReplicaCreated(String sourceHost, int port, String blobId,
      BlobReplicaSourceType sourceType) {
    Tracker tracker = objectTracker.get(blobId);
    if (tracker == null) {
      tracker = new Tracker(numberOfReplicas);
      objectTracker.put(blobId, tracker);
    }
    tracker.totalReplicasCreated.countDown();
  }

  @Override
  public void onBlobReplicaDeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
    Tracker tracker = objectTracker.get(blobId);
    tracker.totalReplicasDeleted.countDown();
  }

  @Override
  public void close()
      throws IOException {
    // ignore
  }

  public void awaitBlobCreations(String blobId) {
    try {
      Tracker tracker = objectTracker.get(blobId);
      tracker.totalReplicasCreated.await();
    } catch (InterruptedException e) {
      // ignore
    }
  }

  public void awaitBlobDeletions(String blobId) {
    try {
      Tracker tracker = objectTracker.get(blobId);
      tracker.totalReplicasDeleted.await();
    } catch (InterruptedException e) {
      // ignore
    }
  }

  public synchronized void decrementCreatedReplica(String blobId) {
    Tracker tracker = objectTracker.get(blobId);
    long currentCount = tracker.totalReplicasCreated.getCount();
    long finalCount = currentCount + 1;
    if (finalCount > numberOfReplicas) {
      throw new IllegalArgumentException("Cannot add more replicas than the max possible replicas");
    }
    tracker.totalReplicasCreated = new CountDownLatch(numberOfReplicas);
    while (tracker.totalReplicasCreated.getCount() > finalCount) {
      tracker.totalReplicasCreated.countDown();
    }
  }

  public synchronized void decrementDeletedReplica(String blobId) {
    Tracker tracker = objectTracker.get(blobId);
    long currentCount = tracker.totalReplicasDeleted.getCount();
    long finalCount = currentCount + 1;
    if (finalCount > numberOfReplicas) {
      throw new IllegalArgumentException("Cannot add more replicas than the max possible replicas");
    }
    tracker.totalReplicasDeleted = new CountDownLatch(numberOfReplicas);
    while (tracker.totalReplicasDeleted.getCount() > finalCount) {
      tracker.totalReplicasDeleted.countDown();
    }
  }
}
