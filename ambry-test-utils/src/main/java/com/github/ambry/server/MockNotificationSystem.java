package com.github.ambry.server;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import junit.framework.Assert;


/**
 * A mock notification system that helps to identify when blobs
 * get replicated. This class is not thread safe
 */
public class MockNotificationSystem implements NotificationSystem {

  private final ConcurrentHashMap<String, EventTracker> objectTracker = new ConcurrentHashMap<String, EventTracker>();
  private final ClusterMap clusterMap;
  private static final int TRACKER_TIMEOUT_MS = 60000;

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
  public void onBlobUndeleted(String blobId, String serviceId, Account account, Container container) {
    // ignore
  }

  @Override
  public void onBlobReplicated(String blobId, String serviceId, Account account, Container container,
      DataNodeId sourceHost) {
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
  public synchronized void onBlobReplicaPurged(String sourceHost, int port, String blobId,
      BlobReplicaSourceType sourceType) {
    objectTracker.computeIfAbsent(blobId, k -> new EventTracker(getNumReplicas(blobId))).trackPurge(sourceHost, port);
  }

  @Override
  public synchronized void onBlobReplicaUpdated(String sourceHost, int port, String blobId,
      BlobReplicaSourceType sourceType, UpdateType updateType, MessageInfo info) {
    objectTracker.computeIfAbsent(blobId, k -> new EventTracker(getNumReplicas(blobId)))
        .trackUpdate(sourceHost, port, updateType);
  }

  @Override
  public void onBlobReplicaUndeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
    objectTracker.computeIfAbsent(blobId, k -> new EventTracker(getNumReplicas(blobId)))
        .trackUndelete(sourceHost, port);
  }

  @Override
  public void onBlobReplicaReplicated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
    objectTracker.computeIfAbsent(blobId, k -> new EventTracker(1)).trackReplicate(sourceHost, port);
  }

  @Override
  public void close() {
    // ignore
  }

  public List<String> getBlobIds() {
    return new ArrayList<>(objectTracker.keySet());
  }

  /**
   * Waits for blob creations on all replicas for {@code blobId}
   * @param blobId the ID of the blob
   */
  public void awaitBlobCreations(String blobId) {
    try {
      waitForTracker(blobId);
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
      waitForTracker(blobId);
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
      waitForTracker(blobId);
      if (!objectTracker.get(blobId).awaitBlobUpdates(updateType)) {
        Assert.fail("Failed awaiting for " + blobId + " updates of type " + updateType);
      }
    } catch (InterruptedException e) {
      // ignore
    }
  }

  /**
   * Waits for blob undeletes on all replicas for {@code blobId}
   * @param blobId the ID of the blob
   */
  void awaitBlobUndeletes(String blobId) {
    try {
      waitForTracker(blobId);
      if (!objectTracker.get(blobId).awaitBlobUndeletes()) {
        Assert.fail("Failed awaiting for " + blobId + " undeletes");
      }
    } catch (InterruptedException e) {
      // ignore
    }
  }

  /**
   * Waits for blob on-demand-replication happens on one single replica for {@code blobId}
   * For ODR, we usually only replicate to one single replica
   * @param blobId the ID of the blob
   */
  void awaitBlobReplicates(String blobId) {
    try {
      waitForTracker(blobId);
      EventTracker et = objectTracker.get(blobId);
      if (!et.awaitBlobReplicates()) {
        Assert.fail("Failed awaiting for " + blobId + " on-demand-replication");
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
   * Wait for event tracker to be created for {@code blobId}
   * @param blobId the ID of the blob
   */
  private void waitForTracker(String blobId) {
    if (!TestUtils.checkAndSleep(() -> objectTracker.containsKey(blobId), TRACKER_TIMEOUT_MS)) {
      Assert.fail("Tracker not found for " + blobId);
    }
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
