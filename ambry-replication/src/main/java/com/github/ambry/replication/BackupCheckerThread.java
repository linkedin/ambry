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
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.BlobMatchStatus;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Time;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.store.BlobMatchStatus.*;


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
 */
public class BackupCheckerThread extends ReplicaThread {

  private final Logger logger = LoggerFactory.getLogger(BackupCheckerThread.class);
  protected final BackupCheckerFileManager fileManager;
  protected final ReplicationConfig replicationConfig;
  protected HashMap<String, MessageInfo> azureBlobMap;
  public static final String DR_Verifier_Keyword = "dr";
  public static final String BLOB_STATE_MISMATCHES_FILE = "blob_state_mismatches";

  public static final String REPLICA_STATUS_FILE = "server_replica_token";

  public BackupCheckerThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    super(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient, replicationConfig,
        replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry, replicatingOverSsl,
        datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin);
    fileManager = new BackupCheckerFileManager(replicationConfig, metricRegistry);
    this.replicationConfig = replicationConfig;
    azureBlobMap = new HashMap<>();
    logger.info("Created BackupCheckerThread {}", threadName);
  }

  /**
   * Return the {@link BackupCheckerFileManager}. This is only for test
   * @return
   */
  BackupCheckerFileManager getFileManager() {
    return this.fileManager;
  }

  public void setAzureBlobMap(HashMap<String, MessageInfo> azureBlobMap) {
    if (azureBlobMap == null) {
      logger.error("Azure blob map cannot be null");
      return;
    }
    this.azureBlobMap = azureBlobMap;
  }

  /**
   *  This checks for GDPR compliance.
   *  If a blob has been deleted from remote on-prem servers, it must be absent in local-store
   *  @param remoteBlob the {@link MessageInfo} that will be transformed into a delete-operation
   *  @param remoteReplicaInfo The remote replica that is being replicated from
   */
  @Override
  protected void applyDelete(MessageInfo remoteBlob, RemoteReplicaInfo remoteReplicaInfo) {
    throw new UnsupportedOperationException("applyDelete is unsupported");
  }

  /**
   * This method checks if permanent blob from remote on-prem server is permanent in local-store
   * @param remoteBlob the {@link MessageInfo} that will be transformed into a TTL update
   * @param remoteReplicaInfo The remote replica that is being replicated from
   */
  @Override
  protected void applyTtlUpdate(MessageInfo remoteBlob, RemoteReplicaInfo remoteReplicaInfo) {
    throw new UnsupportedOperationException("applyTtlUpdate is unsupported");
  }

  /**
   * This method checks if an un-deleted blob from remote on-prem server has been un-deleted from local-store
   * @param remoteBlob the {@link MessageInfo} that will be transformed into an un-delete
   * @param remoteReplicaInfo The remote replica that is being replicated from
   */
  @Override
  protected void applyUndelete(MessageInfo remoteBlob, RemoteReplicaInfo remoteReplicaInfo) {
    throw new UnsupportedOperationException("applyUndelete is unsupported");
  }

  /**
   * Instead of over-riding applyPut, it is better to override the {@link #afterHandleReplicaMetadataResponse}.
   * We already know the missing keys. We know they are not deleted or expired on the remote node.
   * There is no benefit in fetching the entire blob only to discard it, unless we are doing a data comparison also.
   * That's why we set the state of this group to be DONE.
   * TODO: We should probably fetch just checksums if we want to compare blob-content in the future.
   */
  @Override
  protected void afterHandleReplicaMetadataResponse(RemoteReplicaGroup group) {
    group.setState(ReplicaGroupReplicationState.DONE);
  }

  /**
   * Checks each blob from server with its counterpart in cloud backup.
   * There are 4 cases.
   * server-blob : cloud-blob
   * absent : absent  => nothing to do
   * absent : present => print to file, blob may be compacted or missing on server
   * present: absent  => print to file, blob may not have been backed up or somehow missing in cloud backup
   * present: present => check blob-state matches, if not then print to file
   * @param response The {@link ReplicaMetadataResponse}.
   * @param replicas
   * @param server The remote {@link DataNodeId}.
   * @return
   */
  List<ExchangeMetadataResponse> handleReplicaMetadataResponse(ReplicaMetadataResponse response,
      List<RemoteReplicaInfo> replicas, DataNodeId server) {
    // For each replica-response
    IntStream.range(0, response.getReplicaMetadataResponseInfoList().size())
        .filter(i -> response.getReplicaMetadataResponseInfoList().get(i).getError() == ServerErrorCode.No_Error)
        .forEach(i -> {
          ReplicaMetadataResponseInfo respinfo = response.getReplicaMetadataResponseInfoList().get(i);
          RemoteReplicaInfo repinfo = replicas.get(i);
          String output = getFilePath(repinfo, BLOB_STATE_MISMATCHES_FILE);
          // For each message from replica
          respinfo.getMessageInfoList().forEach(serverBlob -> {
            repinfo.setReplicatedUntilUTC(Math.max(repinfo.getReplicatedUntilUTC(), serverBlob.getOperationTimeMs()));
            try {
              // Don't do batch-convert, if one replica in batch fails, then if affect handling others
              storeKeyConverter.convert(Collections.singleton(serverBlob.getStoreKey()))
                  .values().stream()
                  .filter(serverKeyConvert -> serverKeyConvert != null)
                  .map(serverKeyConvert -> {
                    MessageInfo serverBlobConvert = new MessageInfo(serverKeyConvert, serverBlob);
                    MessageInfo azureBlob = azureBlobMap.remove(serverBlobConvert.getStoreKey()); // can be null
                    return new ImmutableTriple(serverBlobConvert, azureBlob, serverBlobConvert.isEqual(azureBlob));
                  }).filter(tuple ->
                      !((Set<BlobMatchStatus>) tuple.getRight()).contains(BLOB_STATE_MATCH))
                  .forEach(tuple -> {
                    MessageInfo serverBlobConvert = (MessageInfo) tuple.getLeft();
                    MessageInfo azureBlob = (MessageInfo) tuple.getMiddle();
                    Set<BlobMatchStatus> status = (Set<BlobMatchStatus>) tuple.getRight();
                    String msg = String.join(BackupCheckerFileManager.COLUMN_SEPARATOR,
                        status.stream().map(s -> s.name()).collect(Collectors.joining(",")),
                        MessageInfo.toText(serverBlobConvert), MessageInfo.toText(azureBlob), "\n");
                    fileManager.appendToFile(output, msg);
                  });
            } catch (Throwable e) {
              // TODO: metric
              logger.error("Failed to verify blobID {} due to ", serverBlob.getStoreKey().getID(), e.toString());
            }
          }); // For each message from replica
          // TODO: To compare data, treat all keys as missing, do not mark rgroup as "done" and compare blob-crc.
          advanceToken(repinfo, new ExchangeMetadataResponse(Collections.emptySet(), respinfo.getFindToken(),
              respinfo.getRemoteReplicaLagInBytes(), Collections.emptyMap(), time));
    }); // For each replica-response
    return Collections.emptyList();
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
   * Add a replica to be copied from server.
   * Additionally, clears some local files to store results and calls parent method.
   * @param rinfo {@link RemoteReplicaInfo} to add.
   */
  @Override
  public void addRemoteReplicaInfo(RemoteReplicaInfo rinfo) {
    String msg = String.join(BackupCheckerFileManager.COLUMN_SEPARATOR,
        "datetime",
        "blob_state_match_status",
        "server_blob_state",
        "azure_blob_state",
        "\n");
    fileManager.truncateAndWriteToFile(getFilePath(rinfo, BLOB_STATE_MISMATCHES_FILE), msg);
    super.addRemoteReplicaInfo(rinfo);
  }

  /**
   * Prints keys absent in server, but present in cloud. This may happen if the blob is compacted or somehow went
   * missing from the server but is still present in azure backups.
   * @param rinfo
   */
  public void printKeysAbsentInServer(RemoteReplicaInfo rinfo) {
    String output = getFilePath(rinfo, BLOB_STATE_MISMATCHES_FILE);
    azureBlobMap.values().forEach(azureBlob -> {
      String msg = String.join(BackupCheckerFileManager.COLUMN_SEPARATOR,
          azureBlob.isEqual(null).stream().map(s -> s.name()).collect(Collectors.joining(",")),
          MessageInfo.toText(null), MessageInfo.toText(azureBlob),
          "\n");
      fileManager.appendToFile(output, msg);
    });
    azureBlobMap.clear();
  }

  /**
   * Prints a log if local store has caught up with remote store
   * @param rinfo Info about remote replica
   * @param mdResponse Metadata response object
   */
  @Override
  protected void logReplicationStatus(RemoteReplicaInfo rinfo,
      ExchangeMetadataResponse mdResponse) {
    JSONObject json = new JSONObject();
    try {
      // Ensures json fields are printed exactly in the order they are inserted
      Field changeMap = json.getClass().getDeclaredField("map");
      changeMap.setAccessible(true);
      changeMap.set(json, new LinkedHashMap<>());
      changeMap.setAccessible(false);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      logger.error(e.getMessage());
      json = new JSONObject();
    }
    // Maintain alphabetical order of fields for string match
    json.put("replica_token", rinfo.getToken().toString());
    json.put("replicated_until", rinfo.getReplicatedUntilUTC());
    // Pretty print with indent for easy viewing
    fileManager.truncateAndWriteToFile(getFilePath(rinfo, REPLICA_STATUS_FILE), json.toString(4));
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
        remoteReplicaInfo.getReplicaId().getDataNodeId().getHostname(), fileName);
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
