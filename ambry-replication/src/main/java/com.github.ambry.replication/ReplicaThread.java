package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.messageformat.*;
import com.github.ambry.shared.*;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
  private final int connectionPoolCheckoutTimeout;
  private final String threadName;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public ReplicaThread(String threadName,
                       List<PartitionInfo> partitionsToReplicate,
                       FindTokenFactory findTokenFactory,
                       ClusterMap clusterMap,
                       AtomicInteger correlationIdGenerator,
                       DataNodeId dataNodeId,
                       ConnectionPool connectionPool,
                       int connectionPoolCheckoutTimeout) {
    this.threadName = threadName;
    this.partitionsToReplicate = partitionsToReplicate;
    this.running = true;
    this.findTokenFactory = findTokenFactory;
    this.clusterMap = clusterMap;
    this.correlationIdGenerator = correlationIdGenerator;
    this.dataNodeId = dataNodeId;
    this.connectionPool = connectionPool;
    this.connectionPoolCheckoutTimeout = connectionPoolCheckoutTimeout;
  }

  public String getName() {
    return threadName;
  }

  @Override
  public void run() {
    logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() + " Thread name: " + threadName);
    for (PartitionInfo partitionInfo : partitionsToReplicate) {
      logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                   " Thread name: " + threadName +
                   " Partition id " + partitionInfo.getPartitionId());
    }
    while (running) {
      for (PartitionInfo partitionInfo : partitionsToReplicate) {
        for (RemoteReplicaInfo remoteReplicaInfo : partitionInfo.getRemoteReplicaInfo()) {
          logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                       " Thread name " + threadName +
                       " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                       remoteReplicaInfo.getReplicaId().getDataNodeId().getPort());
          ConnectedChannel connectedChannel = null;
          try {

            connectedChannel =
                    connectionPool.checkOutConnection(remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname(),
                                                      remoteReplicaInfo.getReplicaId().getDataNodeId().getPort(),
                                                      connectionPoolCheckoutTimeout);

            ExchangeMetadataResponse exchangeMetadataResponse =
                    exchangeMetadata(connectedChannel, partitionInfo, remoteReplicaInfo);
            fixMissingStoreKeys(exchangeMetadataResponse.missingStoreKeys, partitionInfo, connectedChannel);
            remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
            logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                         " Thread name " + threadName +
                         " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                         remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
                         " Token after speaking to remote node " +
                         " token " + exchangeMetadataResponse.remoteToken);
          }
          catch (Exception e) {
            logger.error("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                         " Thread name " + threadName +
                         " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                         remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
                         "Error while replicating for remote replica " +
                         " with error ", e);
            if (connectedChannel != null) {
              connectionPool.destroyConnection(connectedChannel);
              connectedChannel = null;
            }
          }
          finally {
            if (connectedChannel != null)
              connectionPool.checkInConnection(connectedChannel);
          }
        }
      }
    }
    shutdownLatch.countDown();
  }

  /**
   * Gets all the messages from the remote replica since last token. Checks the messages with the local store
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
   */
  protected ExchangeMetadataResponse exchangeMetadata(ConnectedChannel connectedChannel,
                                                      PartitionInfo partitionInfo,
                                                      RemoteReplicaInfo remoteReplicaInfo)
          throws IOException, StoreException, MessageFormatException {

    // 1. Sends a ReplicaMetadataRequest to the remote replica and gets all the message entries since the last
    //    token
    ReplicaMetadataRequest request = new ReplicaMetadataRequest(correlationIdGenerator.incrementAndGet(),
                                                                "replication-metadata" + dataNodeId.getHostname(),
                                                                partitionInfo.getPartitionId(),
                                                                remoteReplicaInfo.getToken());
    connectedChannel.send(request);
    InputStream stream = connectedChannel.receive();
    ReplicaMetadataResponse response = ReplicaMetadataResponse.readFrom(new DataInputStream(stream),
                                                                        findTokenFactory,
                                                                        clusterMap);
    logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                 " Thread name " + threadName +
                 " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                 remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
                 " token from remote " + response.getFindToken());
    List<MessageInfo> messageInfoList = response.getMessageInfoList();

    // 2. Check the local store to find the messages that are missing locally
    // find ids that are missing
    List<StoreKey> storeKeysToCheck = new ArrayList<StoreKey>(messageInfoList.size());
    for (MessageInfo messageInfo : messageInfoList) {
      storeKeysToCheck.add(messageInfo.getStoreKey());
      logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                   " Thread name " + threadName +
                   " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                   remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
                   " key from remote " + " id " + messageInfo.getStoreKey());
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
    //    replica is marked for deletion and is not deleted locally, delete it. If the message in the remote
    //    replica has a ttl that is Infinite_TTL, mark the ttl of the message in the local replica to hav
    //    infinite ttl
    for (MessageInfo messageInfo : messageInfoList) {
      if (!missingStoreKeys.contains(messageInfo.getStoreKey())) {
        // the key is found. Mark it for deletion if it is deleted
        if (messageInfo.isDeleted() && partitionInfo.getStore().isKeyDeleted(messageInfo.getStoreKey())) {
          MessageFormatInputStream deleteStream = new DeleteMessageFormatInputStream(messageInfo.getStoreKey());
          MessageInfo info = new MessageInfo(messageInfo.getStoreKey(), deleteStream.getSize());
          ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
          infoList.add(info);
          MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
          partitionInfo.getStore().delete(writeset);
          logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                       " Thread name " + threadName +
                       " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                       remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
                       " Key deleted. mark for deletion id " + messageInfo.getStoreKey());
        }
      }
      else {
        if (messageInfo.isDeleted()) {
          // if the remote replica has the message in deleted state, it is not considered missing locally
          missingStoreKeys.remove(messageInfo.getStoreKey());
          logger.trace("Node : " + dataNodeId.getHostname() + ":" + dataNodeId.getPort() +
                       " Thread name " + threadName +
                       " Remote " + remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname() + ":" +
                       remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() +
                       " key in deleted state remotely. " + messageInfo.getStoreKey());
        }
      }
    }
    return new ExchangeMetadataResponse(missingStoreKeys, response.getFindToken());
  }

  protected void fixMissingStoreKeys(Set<StoreKey> missingStoreKeys,
                                     PartitionInfo partitionInfo,
                                     ConnectedChannel connectedChannel)
          throws IOException, StoreException, MessageFormatException {
    if (missingStoreKeys.size() > 0) {
      ArrayList<BlobId> keysToFetch = new ArrayList<BlobId>();
      for (StoreKey storeKey : missingStoreKeys) {
          keysToFetch.add((BlobId)storeKey);
      }
      GetRequest getRequest = new GetRequest(correlationIdGenerator.incrementAndGet(),
                                             "replication-fetch",
                                             MessageFormatFlags.All,
                                             partitionInfo.getPartitionId(),
                                             keysToFetch);
      connectedChannel.send(getRequest);
      InputStream getStream = connectedChannel.receive();
      GetResponse getResponse = GetResponse.readFrom(new DataInputStream(getStream), clusterMap);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(getResponse.getInputStream(),
                                                                 getResponse.getMessageInfoList());
      partitionInfo.getStore().put(writeset);
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

  public void shutdown() throws InterruptedException {
    running = false;
    shutdownLatch.await();
  }
}
