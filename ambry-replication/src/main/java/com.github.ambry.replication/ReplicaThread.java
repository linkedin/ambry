package com.github.ambry.replication;

import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ConnectedChannel;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.ReplicaMetadataRequest;
import com.github.ambry.shared.ReplicaMetadataResponse;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A replica thread is responsible for handling replication for a set of partitions assigned to it
 */
class ReplicaThread implements Runnable {

  private final List<PartitionInfo> partitionsToReplicate;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private volatile boolean running;
  private final FindTokenFactory findTokenFactory;
  private final ClusterMap clusterMap;
  private final AtomicInteger correlationIdGenerator;
  private final DataNodeId dataNodeId;
  private final ConnectionPool connectionPool;
  private final ReplicationConfig replicationConfig;
  private final ReplicationMetrics replicationMetrics;
  private final String threadName;
  private final NotificationSystem notification;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public ReplicaThread(String threadName, List<PartitionInfo> partitionsToReplicate, FindTokenFactory findTokenFactory,
      ClusterMap clusterMap, AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, ConnectionPool connectionPool,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification) {
    this.threadName = threadName;
    this.partitionsToReplicate = partitionsToReplicate;
    this.running = true;
    this.findTokenFactory = findTokenFactory;
    this.clusterMap = clusterMap;
    this.correlationIdGenerator = correlationIdGenerator;
    this.dataNodeId = dataNodeId;
    this.connectionPool = connectionPool;
    this.replicationConfig = replicationConfig;
    this.replicationMetrics = replicationMetrics;
    this.notification = notification;
  }

  public String getName() {
    return threadName;
  }

  @Override
  public void run() {
    try {
      logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() + " Thread name: " + threadName);
      for (PartitionInfo partitionInfo : partitionsToReplicate) {
        logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
            " Thread name: " + threadName +
            " Partition id " + partitionInfo.getPartitionId());
      }
      while (running) {
        // shuffle the partitions
        Collections.shuffle(partitionsToReplicate);
        for (PartitionInfo partitionInfo : partitionsToReplicate) {
          if (!running) {
            break;
          }
          List<RemoteReplicaInfo> remoteReplicas = partitionInfo.getRemoteReplicaInfos();
          Collections.shuffle(remoteReplicas);
          for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicas) {
            if (!running) {
              break;
            }
            logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                " Thread name " + threadName +
                " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                remoteReplicaInfo.getReplicaId().getDataNodeId().getPort());
            boolean remoteColo = true;
            if (dataNodeId.getDatacenterName()
                .equals(remoteReplicaInfo.getReplicaId().getDataNodeId().getDatacenterName())) {
              remoteColo = false;
            }
            Timer.Context context = null;
            if (remoteColo) {
              context = replicationMetrics.interColoReplicationLatency.time();
            } else {
              context = replicationMetrics.intraColoReplicationLatency.time();
            }
            ConnectedChannel connectedChannel = null;
            try {
              connectedChannel = connectionPool
                  .checkOutConnection(remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname(),
                      remoteReplicaInfo.getReplicaId().getDataNodeId().getPort(),
                      replicationConfig.replicationConnectionPoolCheckoutTimeoutMs);

              ExchangeMetadataResponse exchangeMetadataResponse =
                  exchangeMetadata(connectedChannel, partitionInfo, remoteReplicaInfo);
              fixMissingStoreKeys(exchangeMetadataResponse.missingStoreKeys, partitionInfo, connectedChannel,
                  remoteColo);
              remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
              logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                  " Thread name " + threadName +
                  " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                  remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
                  " Token after speaking to remote node " +
                  exchangeMetadataResponse.remoteToken);
            } catch (Exception e) {
              logger.error("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                  " Thread name " + threadName +
                  " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                  remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
                  " Error while replicating with remote replica " + e);
              replicationMetrics.replicationErrors.inc();
              if (connectedChannel != null) {
                connectionPool.destroyConnection(connectedChannel);
                connectedChannel = null;
              }
            } finally {
              if (connectedChannel != null) {
                connectionPool.checkInConnection(connectedChannel);
              }
              context.stop();
            }
          }
        }
      }
    } finally {
      running = false;
      shutdownLatch.countDown();
    }
  }

  /**
   * Gets all the metadata about messages from the remote replica since last token. Checks the messages with the local store
   * and finds all the messages that are missing. For the messages that are not missing, updates the delete
   * and ttl state.
   * @param connectedChannel The connected channel that represents a connection to the remote replica
   * @param partitionInfo The information about the partition that is being replicated
   * @param remoteReplicaInfo The information about the remote replica that is being replicated
   * @return ExchangeMetadataResponse - Contains the set of store keys that are missing from the local store
   *         and are present in the remote replica and also the new token from the remote replica
   * @throws IOException
   * @throws StoreException
   * @throws MessageFormatException
   * @throws ReplicationException
   * @throws InterruptedException
   */
  protected ExchangeMetadataResponse exchangeMetadata(ConnectedChannel connectedChannel, PartitionInfo partitionInfo,
      RemoteReplicaInfo remoteReplicaInfo)
      throws IOException, StoreException, MessageFormatException, ReplicationException, InterruptedException {

    // 1. Sends a ReplicaMetadataRequest to the remote replica and gets all the message entries since the last
    //    token
    logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
        " Thread name " + threadName +
        " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
        " Token sent to remote " + remoteReplicaInfo.getToken());

    ReplicaMetadataRequest request = new ReplicaMetadataRequest(correlationIdGenerator.incrementAndGet(),
        "replication-metadata-" + dataNodeId.getHostname(), partitionInfo.getPartitionId(),
        remoteReplicaInfo.getToken(), dataNodeId.getHostname(), partitionInfo.getLocalReplicaId().getReplicaPath(),
        replicationConfig.replicationFetchSizeInBytes);
    connectedChannel.send(request);
    InputStream stream = connectedChannel.receive();
    ReplicaMetadataResponse response =
        ReplicaMetadataResponse.readFrom(new DataInputStream(stream), findTokenFactory, clusterMap);
    if (response.getError() != ServerErrorCode.No_Error) {
      logger.error("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
          " Thread name " + threadName +
          " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
          remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
          " Replica Metadata Response Error " + response.getError());
      throw new ReplicationException("Replica Metadata Response Error " + response.getError());
    }
    logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
        " Thread name " + threadName +
        " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
        remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
        " Token from remote " + response.getFindToken());
    List<MessageInfo> messageInfoList = response.getMessageInfoList();

    // We apply the wait time between replication from remote replicas here. Any new objects that get written
    // in the remote replica are given time to be written to the local replica and avoids failing the request
    // from the client.
    Thread.sleep(replicationConfig.replicaWaitTimeBetweenReplicasMs);

    // 2. Check the local store to find the messages that are missing locally
    // find ids that are missing
    List<StoreKey> storeKeysToCheck = new ArrayList<StoreKey>(messageInfoList.size());
    for (MessageInfo messageInfo : messageInfoList) {
      storeKeysToCheck.add(messageInfo.getStoreKey());
      logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
          " Thread name " + threadName +
          " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
          remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
          " Key from remote " + " id " + messageInfo.getStoreKey());
    }

    Set<StoreKey> missingStoreKeys = partitionInfo.getStore().findMissingKeys(storeKeysToCheck);
    for (StoreKey storeKey : missingStoreKeys) {
      logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
          " Thread name " + threadName +
          " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
          remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
          " key missing id" + storeKey);
    }

    // 3. For the keys that are not missing, check the deleted and ttl state. If the message in the remote
    //    replica is marked for deletion and is not deleted locally, delete it.
    for (MessageInfo messageInfo : messageInfoList) {
      if (!missingStoreKeys.contains(messageInfo.getStoreKey())) {
        // the key is present in the local store. Mark it for deletion if it is deleted in the remote store
        if (messageInfo.isDeleted() && !partitionInfo.getStore().isKeyDeleted(messageInfo.getStoreKey())) {
          MessageFormatInputStream deleteStream = new DeleteMessageFormatInputStream(messageInfo.getStoreKey());
          MessageInfo info = new MessageInfo(messageInfo.getStoreKey(), deleteStream.getSize(), true);
          ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
          infoList.add(info);
          MessageFormatWriteSet writeset = new MessageFormatWriteSet(deleteStream, infoList);
          partitionInfo.getStore().delete(writeset);
          logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
              " Thread name " + threadName +
              " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
              remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
              " Key deleted. mark for deletion id " + messageInfo.getStoreKey());
          if (notification != null) {
            notification.onBlobReplicaDeleted(dataNodeId.getHostname(), dataNodeId.getPort(),
                messageInfo.getStoreKey().toString(), BlobReplicaSourceType.REPAIRED);
          }
        }
      } else {
        if (messageInfo.isDeleted()) {
          // if the remote replica has the message in deleted state, it is not considered missing locally
          missingStoreKeys.remove(messageInfo.getStoreKey());
          logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
              " Thread name " + threadName +
              " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
              remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
              " key in deleted state remotely. " + messageInfo.getStoreKey());
          if (notification != null) {
            notification.onBlobReplicaDeleted(dataNodeId.getHostname(), dataNodeId.getPort(),
                messageInfo.getStoreKey().toString(), BlobReplicaSourceType.REPAIRED);
          }
        } else if (messageInfo.isExpired()) {
          // if the remote replica has an object that is expired, it is not considered missing locally
          missingStoreKeys.remove(messageInfo.getStoreKey());
          logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
              " Thread name " + threadName +
              " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
              remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
              " key in expired state remotely. " + messageInfo.getStoreKey());
        }
      }
    }
    return new ExchangeMetadataResponse(missingStoreKeys, response.getFindToken());
  }

  /**
   * Gets all the messages from the remote node for the missing keys and writes them to the local store
   * @param missingStoreKeys The missing keys in the local store whose message needs to be retrieved
   *                         from the remote store
   * @param partitionInfo  The information about the partition that is being replicated
   * @param connectedChannel The channel to the remote node
   * @throws IOException
   * @throws StoreException
   * @throws MessageFormatException
   * @throws ReplicationException
   */
  protected void fixMissingStoreKeys(Set<StoreKey> missingStoreKeys, PartitionInfo partitionInfo,
      ConnectedChannel connectedChannel, boolean remoteColo)
      throws IOException, StoreException, MessageFormatException, ReplicationException {
    if (missingStoreKeys.size() > 0) {
      ArrayList<BlobId> keysToFetch = new ArrayList<BlobId>();
      for (StoreKey storeKey : missingStoreKeys) {
        keysToFetch.add((BlobId) storeKey);
      }
      GetRequest getRequest =
          new GetRequest(correlationIdGenerator.incrementAndGet(), "replication-fetch-" + dataNodeId.getHostname(),
              MessageFormatFlags.All, partitionInfo.getPartitionId(), keysToFetch);
      connectedChannel.send(getRequest);
      InputStream getStream = connectedChannel.receive();
      GetResponse getResponse = GetResponse.readFrom(new DataInputStream(getStream), clusterMap);
      if (getResponse.getError() != ServerErrorCode.No_Error) {
        logger.error("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
            " Thread name " + threadName +
            " Remote " + connectedChannel.getRemoteHost() + ":" +
            connectedChannel.getRemotePort() +
            " getResponse from replication " + getResponse.getError());
        throw new ReplicationException(
            " Get Request returned error when trying to get missing keys " + getResponse.getError());
      }
      MessageFormatWriteSet writeset =
          new MessageFormatWriteSet(getResponse.getInputStream(), getResponse.getMessageInfoList());
      partitionInfo.getStore().put(writeset);

      long totalSizeInBytesReplicated = 0;
      for (MessageInfo messageInfo : getResponse.getMessageInfoList()) {
        totalSizeInBytesReplicated += messageInfo.getSize();
        logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
            " Thread name " + threadName +
            " Remote " + connectedChannel.getRemoteHost() + ":" + connectedChannel.getRemotePort() +
            " Message Replicated " + messageInfo.getStoreKey() +
            " Partition " + partitionInfo.getPartitionId() +
            " Mount Path " + partitionInfo.getPartitionId().getReplicaIds().get(0).getMountPath() +
            " Message size " + messageInfo.getSize());
        if (notification != null) {
          notification.onBlobReplicaCreated(dataNodeId.getHostname(), dataNodeId.getPort(),
              messageInfo.getStoreKey().toString(), BlobReplicaSourceType.REPAIRED);
        }
      }
      if (remoteColo) {
        replicationMetrics.interColoReplicationBytesCount.inc(totalSizeInBytesReplicated);
        replicationMetrics.interColoBlobsReplicatedCount.inc(getResponse.getMessageInfoList().size());
      } else {
        replicationMetrics.intraColoReplicationBytesCount.inc(totalSizeInBytesReplicated);
        replicationMetrics.intraColoBlobsReplicatedCount.inc(getResponse.getMessageInfoList().size());
      }
    }
  }

  class ExchangeMetadataResponse {
    public final Set<StoreKey> missingStoreKeys;
    public final FindToken remoteToken;

    public ExchangeMetadataResponse(Set<StoreKey> missingStoreKeys, FindToken remoteToken) {
      this.missingStoreKeys = missingStoreKeys;
      this.remoteToken = remoteToken;
    }
  }

  public boolean isThreadUp() {
    return running;
  }

  public void shutdown()
      throws InterruptedException {
    running = false;
    shutdownLatch.await();
  }
}
