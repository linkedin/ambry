/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageInfoType;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends the replication logic encapsulated in ReplicaThread. Instead of apply updates
 * from various replicas, it checks if the blobs are already in the updated state in local store.
 * If the local store was recovered from a backup, then we are effectively checking the blob state in backup.
 * The backup is a union of all on-prem replicas. So the latest state of the blob should be reflected in the backup.
 * There are two possibilities:
 * 1> The backup may be lagging behind the replica and if we recover from such a lagging backup, then we'd be recovering
 * to a point in time in the past.
 * 2> The backup may be ahead of the replica. This can happen if the replica is lagging behind its peers. If we recover
 * from such a backup, we'd still be consistent from the user's point of view. The lagging replica must catch up with
 * its peers and this checker will detect such lagging replicas as well.
 * TODO: Redirect to a different file
 * TODO: Testing on sample partitions
 */
public class BackupCheckerThread extends ReplicaThread {

  private final Logger logger = LoggerFactory.getLogger(BackupCheckerThread.class);
  protected final BackupCheckerFileManager fileManager;
  protected final ReplicationConfig replicationConfig;
  public static final String DR_Verifier_Keyword = "dr";
  public static final String MISSING_KEYS_FILE = "missingKeys";
  public static final String REPLICA_STATUS_FILE = "replicaCheckStatus";

  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS");

  public BackupCheckerThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, ConnectionPool connectionPool, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    super(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, connectionPool, networkClient,
        replicationConfig, replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry,
        replicatingOverSsl, datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate,
        leaderBasedReplicationAdmin);
    try {
      fileManager = Utils.getObj(replicationConfig.backupCheckFileManagerType, replicationConfig, metricRegistry);
    } catch (ReflectiveOperationException e) {
      logger.error("Failed to create file manager. ", e.toString());
      throw new RuntimeException(e);
    }
    this.replicationConfig = replicationConfig;
    logger.info("Created BackupCheckerThread {}", threadName);
  }

  /**
   *  This checks for GDPR compliance.
   *  If a blob has been deleted from remote on-prem servers, it must be absent in local-store
   *  @param remoteBlob the {@link MessageInfo} that will be transformed into a delete-operation
   *  @param remoteReplicaInfo The remote replica that is being replicated from
   */
  @Override
  protected void applyDelete(MessageInfo remoteBlob, RemoteReplicaInfo remoteReplicaInfo) {
    // If we are here, then the blob exists locally and is not deleted.
    EnumSet<MessageInfoType> acceptableLocalBlobStates = EnumSet.of(MessageInfoType.DELETE);
    EnumSet<StoreErrorCodes> acceptableStoreErrorCodes = EnumSet.noneOf(StoreErrorCodes.class);
    // Check local store once before logging an error
    checkLocalStore(remoteBlob, remoteReplicaInfo, acceptableLocalBlobStates, acceptableStoreErrorCodes);
  }

  /**
   * This method checks if permanent blob from remote on-prem server is permanent in local-store
   * @param remoteBlob the {@link MessageInfo} that will be transformed into a TTL update
   * @param remoteReplicaInfo The remote replica that is being replicated from
   */
  @Override
  protected void applyTtlUpdate(MessageInfo remoteBlob, RemoteReplicaInfo remoteReplicaInfo) {
    // If we are here, then the blob exists locally and is not ttl-updated.
    EnumSet<MessageInfoType> acceptableLocalBlobStates = EnumSet.of(MessageInfoType.TTL_UPDATE);
    EnumSet<StoreErrorCodes> acceptableStoreErrorCodes = EnumSet.noneOf(StoreErrorCodes.class);
    // Check local store once before logging an error
    checkLocalStore(remoteBlob, remoteReplicaInfo, acceptableLocalBlobStates, acceptableStoreErrorCodes);
  }

  /**
   * This method checks if an un-deleted blob from remote on-prem server has been un-deleted from local-store
   * @param remoteBlob the {@link MessageInfo} that will be transformed into an un-delete
   * @param remoteReplicaInfo The remote replica that is being replicated from
   */
  @Override
  protected void applyUndelete(MessageInfo remoteBlob, RemoteReplicaInfo remoteReplicaInfo) {
    // If we are here, then the blob exists locally and is not un-deleted.
    EnumSet<MessageInfoType> acceptableLocalBlobStates = EnumSet.of(MessageInfoType.UNDELETE);
    EnumSet<StoreErrorCodes> acceptableStoreErrorCodes = EnumSet.noneOf(StoreErrorCodes.class);
    // Check local store once before logging an error
    checkLocalStore(remoteBlob, remoteReplicaInfo, acceptableLocalBlobStates, acceptableStoreErrorCodes);
  }

  /**
   * Checks if a blob from remote replica is present locally
   * We should not be here since we are overriding fixMissingStoreKeys but still need to override in case someone
   * makes a change in the future, and we end up here.
   * @param validMessageDetectionInputStream Stream of valid blob IDs
   * @param remoteReplicaInfo Info about remote replica from which we are replicating
   */
  @Override
  protected void applyPut(MessageSievingInputStream validMessageDetectionInputStream, RemoteReplicaInfo remoteReplicaInfo) {
    throw new IllegalStateException("We should not be in applyPut since we override fixMissingStoreKeys");
  }

  /**
   * Checks if missing blobs from remote replica are present locally and then logs an error if they are missing.
   * @param connectedChannel The connected channel that represents a connection to the remote replica
   * @param replicasToReplicatePerNode The information about the replicas that is being replicated
   * @param exchangeMetadataResponseList The missing keys in the local stores whose message needs to be retrieved
   *                                     from the remote stores
   * @param remoteColoGetRequestForStandby boolean which indicates if we are getting missing keys for standby or
   *                                       non-leader replica pairs during leader-based replication.
   */
  @Override
  protected void fixMissingStoreKeys(
      ConnectedChannel connectedChannel, List<RemoteReplicaInfo> replicasToReplicatePerNode,
      List<ExchangeMetadataResponse> exchangeMetadataResponseList, boolean remoteColoGetRequestForStandby) {
    /**
     * Instead of over-riding applyPut, it is better to override fixMissingStoreKeys.
     * We already know the missing keys. We know they are not deleted or expired on the remote node.
     * There is no benefit in fetching the entire blob only to discard it, unless we are doing a data comparison also.
     * TODO: We should probably fetch just checksums if we want to compare blob-content in the future.
     */
    EnumSet<MessageInfoType> acceptableLocalBlobStates = EnumSet.of(MessageInfoType.PUT);
    EnumSet<StoreErrorCodes> acceptableStoreErrorCodes = EnumSet.noneOf(StoreErrorCodes.class);
    for (int i = 0; i < exchangeMetadataResponseList.size(); i++) {
      ExchangeMetadataResponse exchangeMetadataResponse = exchangeMetadataResponseList.get(i);
      RemoteReplicaInfo remoteReplicaInfo = replicasToReplicatePerNode.get(i);
      if (exchangeMetadataResponse.serverErrorCode == ServerErrorCode.No_Error) {
        for (MessageInfo messageInfo: exchangeMetadataResponse.getMissingStoreMessages()) {
          // Check local store once before logging an error
          checkLocalStore(messageInfo, replicasToReplicatePerNode.get(0), acceptableLocalBlobStates, acceptableStoreErrorCodes);
        }
        // Advance token so that we make progress in spite of missing keys,
        // else the replication code will be stuck waiting for missing keys to appear in local store.
        advanceToken(remoteReplicaInfo, exchangeMetadataResponse);
      }
    }
  }

  /**
   * Advances local token to make progress on replication
   * @param remoteReplicaInfo Remote replica info object
   * @param exchangeMetadataResponse Metadata object exchanged between replicas
   */
  protected void advanceToken(RemoteReplicaInfo remoteReplicaInfo, ExchangeMetadataResponse exchangeMetadataResponse) {
    remoteReplicaInfo.setToken(exchangeMetadataResponse.remoteToken);
    remoteReplicaInfo.setLocalLagFromRemoteInBytes(exchangeMetadataResponse.localLagFromRemoteInBytes);
    // reset stored metadata response for this replica so that we send next request for metadata
    remoteReplicaInfo.setExchangeMetadataResponse(new ExchangeMetadataResponse(ServerErrorCode.No_Error));
    logReplicationStatus(remoteReplicaInfo, exchangeMetadataResponse);
  }

  /**
   * This method checks if the blob is in local store in an acceptable state or encounters an acceptable error code
   * @param remoteBlob the {@link MessageInfo} that will be checked for in the local-store
   * @param remoteReplicaInfo The remote replica that is being replicated from
   * @param acceptableLocalBlobStates Acceptable states in which the blob must be present in the local-store
   * @param acceptableStoreErrorCodes Acceptable error codes when retrieving the blob from local-store
   */
  protected void checkLocalStore(MessageInfo remoteBlob, RemoteReplicaInfo remoteReplicaInfo,
      EnumSet<MessageInfoType> acceptableLocalBlobStates, EnumSet<StoreErrorCodes> acceptableStoreErrorCodes) {
    String errMsg = "%s | Missing %s | %s | Optime = %s | RemoteBlobState = %s | LocalBlobState = %s \n";
    try {
      EnumSet<MessageInfoType> messageInfoTypes = EnumSet.copyOf(acceptableLocalBlobStates);
      // findKey() is better than get() since findKey() returns index records even if blob is marked for deletion.
      // However, get() can also do this with additional and appropriate StoreGetOptions.
      MessageInfo localBlob = remoteReplicaInfo.getLocalStore().findKey(remoteBlob.getStoreKey());
      // retainAll is set intersection. If the result is empty, then the result of intersection is a null set
      messageInfoTypes.retainAll(getBlobStates(localBlob));
      if (messageInfoTypes.isEmpty()) {
        fileManager.appendToFile(getFilePath(remoteReplicaInfo, MISSING_KEYS_FILE),
            String.format(errMsg, remoteReplicaInfo, acceptableLocalBlobStates, remoteBlob.getStoreKey(),
                DATE_FORMAT.format(remoteBlob.getOperationTimeMs()), getBlobStates(remoteBlob), getBlobStates(localBlob)));
      }
    } catch (StoreException e) {
      EnumSet<StoreErrorCodes> storeErrorCodes = EnumSet.copyOf(acceptableStoreErrorCodes);
      // retainAll is set intersection. If the result is empty, then the result of intersection is a null set
      storeErrorCodes.retainAll(Collections.singleton(e.getErrorCode()));
      if (storeErrorCodes.isEmpty()) {
        fileManager.appendToFile(getFilePath(remoteReplicaInfo, MISSING_KEYS_FILE),
            String.format(errMsg, remoteReplicaInfo, acceptableLocalBlobStates, remoteBlob.getStoreKey(),
                DATE_FORMAT.format(remoteBlob.getOperationTimeMs()), getBlobStates(remoteBlob), e.getErrorCode()));
      }
    }
  }

  /**
   * Returns an enum corresponding to blob state
   * @param messageInfo Blob state as message info object
   * @return Blob state as enum
   */
  protected EnumSet<MessageInfoType> getBlobStates(MessageInfo messageInfo) {
    // Blob must be PUT to begin with
    EnumSet<MessageInfoType> messageInfoTypes = EnumSet.of(MessageInfoType.PUT);
    if (messageInfo.isDeleted()) {
      messageInfoTypes.add(MessageInfoType.DELETE);
    }
    if (messageInfo.isExpired()) {
      messageInfoTypes.add(MessageInfoType.EXPIRED);
    }
    if (messageInfo.isTtlUpdated()) {
      messageInfoTypes.add(MessageInfoType.TTL_UPDATE);
    }
    if (messageInfo.isUndeleted()) {
      messageInfoTypes.add(MessageInfoType.UNDELETE);
    }
    return messageInfoTypes;
  }

  /**
   * Prints a log if local store has caught up with remote store
   * @param remoteReplicaInfo Info about remote replica
   * @param exchangeMetadataResponse Metadata response object
   */
  @Override
  protected void logReplicationStatus(RemoteReplicaInfo remoteReplicaInfo,
      ExchangeMetadataResponse exchangeMetadataResponse) {
    // This will help us know when to stop DR process for sealed partitions
    String text = String.format("%s | isSealed = %s | Token = %s | localLagFromRemoteInBytes = %s \n",
        remoteReplicaInfo, remoteReplicaInfo.getReplicaId().isSealed(), remoteReplicaInfo.getToken().toString(),
        exchangeMetadataResponse.localLagFromRemoteInBytes);
    fileManager.truncateAndWriteToFile(getFilePath(remoteReplicaInfo, REPLICA_STATUS_FILE), text);
  }

  /**
   * Returns a concatenated file path
   * @param remoteReplicaInfo Info about remote replica
   * @param fileName Name of file to write text to
   * @return Returns a concatenated file path
   */
  protected String getFilePath(RemoteReplicaInfo remoteReplicaInfo, String fileName) {
    return String.join(File.separator, replicationConfig.backupCheckerReportDir,
        Long.toString(remoteReplicaInfo.getReplicaId().getPartitionId().getId()),
        remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname(),
        fileName);
  }

  /**
   * Returns local replica mount path of the partition
   * @param remoteReplicaInfo Info about remote replica
   * @return Local replica mount path of the partition
   */
  @Override
  protected String getLocalReplicaPath(RemoteReplicaInfo remoteReplicaInfo) {
    // This will let the DR advertise its replicas as "dr/" replicas to other servers
    // And prevent servers from complaining of requests from unknown peers.
    return DR_Verifier_Keyword + File.separator + super.getLocalReplicaPath(remoteReplicaInfo);
  }
}
