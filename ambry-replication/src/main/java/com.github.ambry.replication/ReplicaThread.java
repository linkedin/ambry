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
import com.github.ambry.shared.*;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A replica thread is responsible for handling replication for a set of partitions assigned to it
 */
class ReplicaThread implements Runnable {

  private final Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateGroupedByNode;
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

  public ReplicaThread(String threadName, Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateGroupedByNode,
                       FindTokenFactory findTokenFactory, ClusterMap clusterMap, AtomicInteger correlationIdGenerator,
                       DataNodeId dataNodeId, ConnectionPool connectionPool, ReplicationConfig replicationConfig,
                       ReplicationMetrics replicationMetrics, NotificationSystem notification) {
    this.threadName = threadName;
    this.replicasToReplicateGroupedByNode = replicasToReplicateGroupedByNode;
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
      logger.trace("Local node: " + dataNodeId + " Thread name: " + threadName);
      List<List<RemoteReplicaInfo>> replicasToReplicate =
              new ArrayList<List<RemoteReplicaInfo>>(replicasToReplicateGroupedByNode.size());
      for (Map.Entry<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateEntry :
              replicasToReplicateGroupedByNode.entrySet()) {
        logger.trace("Remote node: " + replicasToReplicateEntry.getKey() +
                " Thread name: " + threadName +
                " ReplicasToReplicate: " + replicasToReplicateEntry.getValue());
        replicasToReplicate.add(replicasToReplicateEntry.getValue());
      }
      while (running) {
        // shuffle the nodes
        Collections.shuffle(replicasToReplicate);
        for (List<RemoteReplicaInfo> replicasToReplicatePerNode : replicasToReplicate) {
          if (!running) {
            break;
          }
          DataNodeId remoteNode = replicasToReplicatePerNode.get(0).getReplicaId().getDataNodeId();
          logger.trace("Remote node: {} Thread name: {} Remote replicas: {}",
                  remoteNode, threadName, replicasToReplicatePerNode);
          boolean remoteColo = true;
          if (dataNodeId.getDatacenterName()
              .equals(remoteNode.getDatacenterName())) {
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
                .checkOutConnection(remoteNode.getHostname(), remoteNode.getPort(),
                        replicationConfig.replicationConnectionPoolCheckoutTimeoutMs);

            List<ExchangeMetadataResponse> exchangeMetadataResponseList =
                exchangeMetadata(connectedChannel, replicasToReplicatePerNode, remoteColo);
            fixMissingStoreKeys(connectedChannel, replicasToReplicatePerNode, remoteColo, exchangeMetadataResponseList);
          } catch (Exception e) {
            if (logger.isTraceEnabled()) {
              logger.error("Remote node: " + remoteNode +
                  " Thread name: " + threadName +
                  " Remote replicas: " + replicasToReplicatePerNode +
                  " Error while replicating with remote replica ", e);
            } else {
              logger.error("Remote node: " + remoteNode +
                  " Thread name: " + threadName +
                  " Remote replicas: " + replicasToReplicatePerNode +
                  " Error while replicating with remote replica " + e);
            }
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
    } finally {
      running = false;
      shutdownLatch.countDown();
    }
  }


  /**
   * Gets all the metadata about messages from the remote replicas since last token. Checks the messages with the local
   * store and finds all the messages that are missing. For the messages that are not missing, updates the delete
   * and ttl state.
   * @param connectedChannel The connected channel that represents a connection to the remote replica
   * @param replicasToReplicatePerNode The information about the replicas that is being replicated
   * @param remoteColo True, if the replicas are from remote DC. False, if the replicas are from local DC
   * @return - List of ExchangeMetadataResponse that contains the set of store keys that are missing from the local
   *           store and are present in the remote replicas and also the new token from the remote replicas
   * @throws IOException
   * @throws StoreException
   * @throws MessageFormatException
   * @throws ReplicationException
   * @throws InterruptedException
   */
  protected List<ExchangeMetadataResponse> exchangeMetadata(ConnectedChannel connectedChannel,
      List<RemoteReplicaInfo> replicasToReplicatePerNode, boolean remoteColo)
      throws IOException, StoreException, MessageFormatException, ReplicationException, InterruptedException {

    long exchangeMetadataStartTimeInMs = SystemTime.getInstance().milliseconds();
    List<ExchangeMetadataResponse> exchangeMetadataResponseList = new ArrayList<ExchangeMetadataResponse>();
    if (replicasToReplicatePerNode.size() > 0) {
      DataNodeId remoteNode = replicasToReplicatePerNode.get(0).getReplicaId().getDataNodeId();
      // 1. Sends a ReplicaMetadataRequest to the remote replicas and gets all the message entries since the last
      //    token

      List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList = new ArrayList<ReplicaMetadataRequestInfo>();
      for (RemoteReplicaInfo remoteReplicaInfo : replicasToReplicatePerNode) {
        ReplicaMetadataRequestInfo replicaMetadataRequestInfo =
            new ReplicaMetadataRequestInfo(remoteReplicaInfo.getReplicaId().getPartitionId(), remoteReplicaInfo.getToken(),
                dataNodeId.getHostname(), remoteReplicaInfo.getLocalReplicaId().getReplicaPath());
        replicaMetadataRequestInfoList.add(replicaMetadataRequestInfo);
        logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token sent to remote: {} ",
                remoteNode, threadName, remoteReplicaInfo.getReplicaId(), remoteReplicaInfo.getToken());
      }
      ReplicaMetadataRequest request = new ReplicaMetadataRequest(correlationIdGenerator.incrementAndGet(),
          "replication-metadata-" + dataNodeId.getHostname(), replicaMetadataRequestInfoList,
          replicationConfig.replicationFetchSizeInBytes);
      connectedChannel.send(request);
      InputStream stream = connectedChannel.receive();
      ReplicaMetadataResponse response =
          ReplicaMetadataResponse.readFrom(new DataInputStream(stream), findTokenFactory, clusterMap);
      if (response.getError() != ServerErrorCode.No_Error ||
              response.getReplicaMetadataResponseInfoList().size() != replicasToReplicatePerNode.size()) {
        logger.error("Remote node: " + remoteNode +
            " Thread name: " + threadName +
            " Remote replicas: " + replicasToReplicatePerNode +
            " Replica metadata response error: " + response.getError());
        throw new ReplicationException("Replica Metadata Response Error " + response.getError());
      }
      boolean needToWaitForReplicaLag = true;
      for (int i = 0; i < response.getReplicaMetadataResponseInfoList().size(); i++) {
        RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
        ReplicaMetadataResponseInfo replicaMetadataResponseInfo = response.getReplicaMetadataResponseInfoList().get(i);
        if (replicaMetadataResponseInfo.getError() == ServerErrorCode.No_Error) {
          logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token from remote: {} Replica lag: {} ",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getFindToken(),
                  replicaMetadataResponseInfo.getRemoteReplicaLagInBytes());

          List<MessageInfo> messageInfoList = replicaMetadataResponseInfo.getMessageInfoList();
          long remoteReplicaLag = replicaMetadataResponseInfo.getRemoteReplicaLagInBytes();

          if (remoteReplicaLag < replicationConfig.replicationMaxLagForWaitTimeInBytes && needToWaitForReplicaLag) {
            logger.trace("Remote node: {} Thread name: {} Remote replica: {} Remote replica lag: {} " +
                    "ReplicationMaxLagForWaitTimeInBytes: {} Waiting for {} ms",
                    remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                    replicaMetadataResponseInfo.getRemoteReplicaLagInBytes(),
                    replicationConfig.replicationMaxLagForWaitTimeInBytes,
                    replicationConfig.replicaWaitTimeBetweenReplicasMs);
            // We apply the wait time between replication from remote replicas here. Any new objects that get written
            // in the remote replica are given time to be written to the local replica and avoids failing the request
            // from the client. This is done only when the replication lag with that node is less than
            // replicationMaxLagForWaitTimeInBytes
            Thread.sleep(replicationConfig.replicaWaitTimeBetweenReplicasMs);
            needToWaitForReplicaLag = false;
          }

          // 2. Check the local store to find the messages that are missing locally
          // find ids that are missing
          List<StoreKey> storeKeysToCheck = new ArrayList<StoreKey>(messageInfoList.size());
          for (MessageInfo messageInfo : messageInfoList) {
            storeKeysToCheck.add(messageInfo.getStoreKey());
            logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key from remote: {}",
                    remoteNode, threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
          }

          Set<StoreKey> missingStoreKeys = remoteReplicaInfo.getLocalStore().findMissingKeys(storeKeysToCheck);
          for (StoreKey storeKey : missingStoreKeys) {
            logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key missing id: {}",
                    remoteNode, threadName, remoteReplicaInfo.getReplicaId(), storeKey);
          }

          // 3. For the keys that are not missing, check the deleted and ttl state. If the message in the remote
          //    replica is marked for deletion and is not deleted locally, delete it.
          for (MessageInfo messageInfo : messageInfoList) {
            if (!missingStoreKeys.contains(messageInfo.getStoreKey())) {
              // the key is present in the local store. Mark it for deletion if it is deleted in the remote store
              if (messageInfo.isDeleted() && !remoteReplicaInfo.getLocalStore().isKeyDeleted(messageInfo.getStoreKey())) {
                MessageFormatInputStream deleteStream = new DeleteMessageFormatInputStream(messageInfo.getStoreKey());
                MessageInfo info = new MessageInfo(messageInfo.getStoreKey(), deleteStream.getSize(), true);
                ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
                infoList.add(info);
                MessageFormatWriteSet writeset =
                    new MessageFormatWriteSet(deleteStream, infoList, replicationConfig.replicationMaxDeleteWriteTimeMs,
                        false);
                remoteReplicaInfo.getLocalStore().delete(writeset);
                logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key deleted. mark for deletion id: {}",
                        remoteNode, threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
                if (notification != null) {
                  notification.onBlobReplicaDeleted(dataNodeId.getHostname(), dataNodeId.getPort(),
                      messageInfo.getStoreKey().toString(), BlobReplicaSourceType.REPAIRED);
                }
              }
            } else {
              if (messageInfo.isDeleted()) {
                // if the remote replica has the message in deleted state, it is not considered missing locally
                missingStoreKeys.remove(messageInfo.getStoreKey());
                logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key in deleted state remotely: {}",
                        remoteNode, threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
                if (notification != null) {
                  notification.onBlobReplicaDeleted(dataNodeId.getHostname(), dataNodeId.getPort(),
                      messageInfo.getStoreKey().toString(), BlobReplicaSourceType.REPAIRED);
                }
              } else if (messageInfo.isExpired()) {
                // if the remote replica has an object that is expired, it is not considered missing locally
                missingStoreKeys.remove(messageInfo.getStoreKey());
                logger.trace("Remote node: {} Thread name: {} Remote replica: {} Key in expired state remotely {}",
                        remoteNode, threadName, remoteReplicaInfo.getReplicaId(), messageInfo.getStoreKey());
              }
            }
          }
        } else {
          // TODO need partition level replication error metric
          logger.error("Remote node: {} Thread name: {} Remote replica: {} Server error: ",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
          ExchangeMetadataResponse exchangeMetadataResponse =
                  new ExchangeMetadataResponse(replicaMetadataResponseInfo.getError());
          exchangeMetadataResponseList.add(exchangeMetadataResponse);
        }
      }
      if (remoteColo) {
        replicationMetrics.interColoMetadataExchangeCount.inc();
        replicationMetrics.interColoExchangeMetadataTime.update(
                SystemTime.getInstance().milliseconds() - exchangeMetadataStartTimeInMs);
      } else {
        replicationMetrics.intraColoMetadataExchangeCount.inc();
        replicationMetrics.intraColoExchangeMetadataTime.update(
                SystemTime.getInstance().milliseconds() - exchangeMetadataStartTimeInMs);
      }
    }
    return exchangeMetadataResponseList;
  }

  /**
   * Gets all the messages from the remote node for the missing keys and writes them to the local store
   * @param connectedChannel The connected channel that represents a connection to the remote replica
   * @param replicasToReplicatePerNode The information about the replicas that is being replicated
   * @param remoteColo True, if the replicas are from remote DC. False, if the replicas are from local DC
   * @param exchangeMetadataResponseList The missing keys in the local stores whose message needs to be retrieved
   *                                     from the remote stores
   * @throws IOException
   * @throws StoreException
   * @throws MessageFormatException
   * @throws ReplicationException
   */
  protected void fixMissingStoreKeys(ConnectedChannel connectedChannel,
      List<RemoteReplicaInfo> replicasToReplicatePerNode, boolean remoteColo,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList)
      throws IOException, StoreException, MessageFormatException, ReplicationException {
    long fixMissingStoreKeysStartTimeInMs = SystemTime.getInstance().milliseconds();
    if (exchangeMetadataResponseList.size() != replicasToReplicatePerNode.size() ||
            replicasToReplicatePerNode.size() == 0) {
      throw new IllegalArgumentException("ExchangeMetadataResponseList size " + exchangeMetadataResponseList.size() +
              " and replicasToReplicatePerNode size " + replicasToReplicatePerNode.size() +
              " should be the same and greater than zero");
    }
    DataNodeId remoteNode = replicasToReplicatePerNode.get(0).getReplicaId().getDataNodeId();
    List<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.No_Error) {
        Set<StoreKey> missingStoreKeys = exchangeMetadataResponse.missingStoreKeys;
        if (missingStoreKeys.size() > 0) {
          ArrayList<BlobId> keysToFetch = new ArrayList<BlobId>();
          for (StoreKey storeKey : missingStoreKeys) {
            keysToFetch.add((BlobId) storeKey);
          }
          PartitionRequestInfo partitionRequestInfo =
                  new PartitionRequestInfo(remoteReplicaInfo.getReplicaId().getPartitionId(), keysToFetch);
          partitionRequestInfoList.add(partitionRequestInfo);
        }
      }
    }
    GetRequest getRequest =
        new GetRequest(correlationIdGenerator.incrementAndGet(), "replication-fetch-" + dataNodeId.getHostname(),
            MessageFormatFlags.All, partitionRequestInfoList);
    connectedChannel.send(getRequest);
    InputStream getStream = connectedChannel.receive();
    GetResponse getResponse = GetResponse.readFrom(new DataInputStream(getStream), clusterMap);
    if (getResponse.getError() != ServerErrorCode.No_Error) {
      logger.error("Remote node: " + remoteNode +
          " Thread name: " + threadName +
          " Remote replicas: " + replicasToReplicatePerNode +
          " GetResponse from replication: " + getResponse.getError());
      throw new ReplicationException(
          " Get Request returned error when trying to get missing keys " + getResponse.getError());
    }

    int partitionResponseInfoIndex = 0;
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.No_Error &&
              exchangeMetadataResponse.missingStoreKeys.size() > 0) {
        PartitionResponseInfo partitionResponseInfo =
                getResponse.getPartitionResponseInfoList().get(partitionResponseInfoIndex);
        partitionResponseInfoIndex++;
        if (partitionResponseInfo.getPartition().compareTo(remoteReplicaInfo.getReplicaId().getPartitionId()) != 0) {
          throw new IllegalStateException("The partition id from partitionResponseInfo " +
                  partitionResponseInfo.getPartition() + " and from remoteReplicaInfo " +
                  remoteReplicaInfo.getReplicaId().getPartitionId() + " are not the same");
        }
        if (partitionResponseInfo.getErrorCode() == ServerErrorCode.No_Error) {
          List<MessageInfo> messageInfoList = getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList();
          logger.trace("Remote node: {} Thread name: {} Remote replica: {} Messages to fix: {} " +
                  "Partition: {} Local mount path: {}", remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                  exchangeMetadataResponse.missingStoreKeys, remoteReplicaInfo.getReplicaId().getPartitionId(),
                  remoteReplicaInfo.getLocalReplicaId().getMountPath());

          MessageFormatWriteSet writeset = new MessageFormatWriteSet(getResponse.getInputStream(), messageInfoList,
              replicationConfig.replicationMaxPutWriteTimeMs, true);
          remoteReplicaInfo.getLocalStore().put(writeset);

          long totalSizeInBytesReplicated = 0;
          for (MessageInfo messageInfo : messageInfoList) {
            totalSizeInBytesReplicated += messageInfo.getSize();
            logger.trace("Remote node: {} Thread name: {} Remote replica: {} Message replicated: {} Partition: {} " +
                    "Local mount path: {} Message size: {}", remoteNode, threadName, remoteReplicaInfo.getReplicaId(),
                    messageInfo.getStoreKey(), remoteReplicaInfo.getReplicaId().getPartitionId(),
                    remoteReplicaInfo.getLocalReplicaId().getMountPath(), messageInfo.getSize());
            if (notification != null) {
              notification.onBlobReplicaCreated(dataNodeId.getHostname(), dataNodeId.getPort(),
                  messageInfo.getStoreKey().toString(), BlobReplicaSourceType.REPAIRED);
            }
          }
          if (remoteColo) {
            replicationMetrics.interColoReplicationBytesRate.mark(totalSizeInBytesReplicated);
            replicationMetrics.interColoBlobsReplicatedCount.inc(messageInfoList.size());
            replicationMetrics.interColoFixMissingKeysTime
                .update(SystemTime.getInstance().milliseconds() - fixMissingStoreKeysStartTimeInMs);
          } else {
            replicationMetrics.intraColoReplicationBytesRate.mark(totalSizeInBytesReplicated);
            replicationMetrics.intraColoBlobsReplicatedCount.inc(messageInfoList.size());
            replicationMetrics.intraColoFixMissingKeysTime
                .update(SystemTime.getInstance().milliseconds() - fixMissingStoreKeysStartTimeInMs);
          }
          remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
          logger.trace("Remote node: {} Thread name: {} Remote replica: {} Token after speaking to remote node: {}",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), exchangeMetadataResponse.remoteToken);
        } else {
          // TODO Need partition  level replication error metric
          logger.trace("Remote node: {} Thread name: {} Remote replica: {} Server error: ",
                  remoteNode, threadName, remoteReplicaInfo.getReplicaId(), partitionResponseInfo.getErrorCode());
        }
      }
    }
  }

  class ExchangeMetadataResponse {
    public final Set<StoreKey> missingStoreKeys;
    public final FindToken remoteToken;
    public final ServerErrorCode serverErrorCode;

    public ExchangeMetadataResponse(Set<StoreKey> missingStoreKeys, FindToken remoteToken) {
      this.missingStoreKeys = missingStoreKeys;
      this.remoteToken = remoteToken;
      this.serverErrorCode = ServerErrorCode.No_Error;
    }

    public ExchangeMetadataResponse(ServerErrorCode errorCode) {
      missingStoreKeys = null;
      remoteToken = null;
      this.serverErrorCode = errorCode;
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
