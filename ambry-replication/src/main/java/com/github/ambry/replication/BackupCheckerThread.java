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
import com.github.ambry.messageformat.BlobAll;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.BlobMatchStatus;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Time;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
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
  private final ReplicationMetrics metrics;
  private final StoreKeyFactory storeKeyFactory;
  protected HashMap<String, MessageInfo> azureBlobMap;
  public static final String DR_Verifier_Keyword = "dr";
  public static final String BLOB_STATE_MISMATCHES_FILE = "blob_state_mismatches";
  public static final String REPLICA_STATUS_FILE = "server_replica_token";
  protected AtomicInteger numBlobScanned;
  protected long partitionBackedUpUntil = -1;

  public BackupCheckerThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, StoreKeyFactory storeKeyFactory, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    super(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient, replicationConfig,
        replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry, replicatingOverSsl,
        datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin);
    fileManager = new BackupCheckerFileManager(replicationConfig, metricRegistry);
    this.replicationConfig = replicationConfig;
    this.storeKeyFactory = storeKeyFactory;
    azureBlobMap = new HashMap<>();
    metrics = new ReplicationMetrics(clusterMap.getMetricRegistry(), Collections.emptyList());
    // Reset these counters if re-using the same thread object
    numBlobScanned = new AtomicInteger(0);
    logger.info("Created BackupCheckerThread {}", threadName);
  }

  /**
   * Return the {@link BackupCheckerFileManager}. This is only for test
   * @return
   */
  BackupCheckerFileManager getFileManager() {
    return this.fileManager;
  }

  public void setAzureBlobInfo(HashMap<String, MessageInfo> azureBlobMap, long partitionBackedUpUntil) {
    if (azureBlobMap == null) {
      logger.error("Azure blob map cannot be null");
      return;
    }
    this.azureBlobMap = azureBlobMap;
    this.partitionBackedUpUntil = partitionBackedUpUntil;
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

  protected MessageInfo mapBlob(MessageInfo blob) {
    StoreKey keyConvert = null;
    try {
      // Don't do batch-convert, if one replica in batch fails, then it affects handling others
      keyConvert = storeKeyConverter.convert(Collections.singleton(blob.getStoreKey()))
          .get(blob.getStoreKey());
    } catch (Throwable e) {
      metrics.backupIntegrityError.inc();
      logger.error("Failed to convert blobID {} due to ", blob.getStoreKey().getID(), e.toString());
    }
    return new MessageInfo(keyConvert, blob);
  }

  /**
   * Re-check by fetching the local blob
   * @param replica
   * @param serverBlob
   * @param azureBlob
   * @return
   */
  Set<BlobMatchStatus> recheck(RemoteReplicaInfo replica, MessageInfo serverBlob, MessageInfo azureBlob,
      Set<BlobMatchStatus> status) {
    BlobStore store = (BlobStore) replica.getLocalStore();
    EnumSet<StoreGetOptions> storeGetOptions = EnumSet.of(StoreGetOptions.Store_Include_Deleted,
        StoreGetOptions.Store_Include_Expired);
    MessageReadSet rdset = null;
    try {
      StoreInfo stinfo = store.get(Collections.singletonList(azureBlob.getStoreKey()), storeGetOptions);
      rdset = stinfo.getMessageReadSet();
      rdset.doPrefetch(0, 0, rdset.sizeInBytes(0));
      ByteBuf bytebuf = rdset.getPrefetchedData(0);
      MessageFormatRecord.deserializeBlobAll(new NettyByteBufDataInputStream(bytebuf), storeKeyFactory);
      return Collections.singleton(BLOB_INTACT_IN_AZURE);
    } catch (Throwable e) {
      logger.error("Failed to deserialize blob due to ", e);
      status.add(BLOB_CORRUPT_IN_AZURE);
    } finally {
      if (rdset != null && rdset.count() > 0 && rdset.getPrefetchedData(0) != null) {
        rdset.getPrefetchedData(0).release();
      }
    }
    return status;
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
    IntStream.range(0, response.getReplicaMetadataResponseInfoList().size())
        .filter(i -> response.getReplicaMetadataResponseInfoList().get(i).getError() == ServerErrorCode.No_Error)
        .forEach(i -> {
          ReplicaMetadataResponseInfo metadata = response.getReplicaMetadataResponseInfoList().get(i);
          RemoteReplicaInfo replica = replicas.get(i);
          String output = getFilePath(replica, BLOB_STATE_MISMATCHES_FILE);
          metadata.getMessageInfoList().stream()
              .map(serverBlob -> {
                numBlobScanned.incrementAndGet();
                replica.setReplicatedUntilTime(Math.max(replica.getReplicatedUntilTime(),
                    serverBlob.getOperationTimeMs()));
                return mapBlob(serverBlob);
              })
              .filter(serverBlob -> serverBlob.getStoreKey() != null)
              .map(serverBlob -> {
                MessageInfo azureBlob = azureBlobMap.remove(serverBlob.getStoreKey().getID());
                Set<BlobMatchStatus> status = azureBlob == null ? Collections.singleton(BLOB_ABSENT_IN_AZURE)
                    : serverBlob.isEqual(azureBlob);
                // if size mismatch, then re-check
                // this is the only workaround due to a field reservedMetadataBlobId
                if (status.contains(BLOB_STATE_SIZE_MISMATCH)) {
                  status = recheck(replica, serverBlob, azureBlob, status);
                }
                return new ImmutableTriple(serverBlob, azureBlob, status);
              })
              .filter(tuple -> {
                Set<BlobMatchStatus> status = (Set<BlobMatchStatus>) tuple.getRight();
                // ignore blobs that are intact in Azure, despite a state mismatch between server and cloud
                return !status.contains(BLOB_INTACT_IN_AZURE);
              })
              .filter(tuple -> {
                Set<BlobMatchStatus> status = (Set<BlobMatchStatus>) tuple.getRight();
                // ignore blobs that match between server and azure
                return !status.contains(BLOB_STATE_MATCH);
              })
              .filter(tuple -> {
                MessageInfo serverBlob = (MessageInfo) tuple.getLeft();
                Set<BlobMatchStatus> status = (Set<BlobMatchStatus>) tuple.getRight();
                // ignore blobs that are deleted or expired on server and absent in azure
                // Blob can be absent in azure for 3 reasons:
                // 1. backups haven't caught up
                // 2. cloud compaction executed before server compaction
                // 3. replication ignored such blobs and did not upload them to azure
                return !((serverBlob.isDeleted() || serverBlob.isExpired()) && status.contains(BLOB_ABSENT_IN_AZURE));
              })
              .filter(tuple -> {
                MessageInfo serverBlob = (MessageInfo) tuple.getLeft();
                Set<BlobMatchStatus> status = (Set<BlobMatchStatus>) tuple.getRight();
                // ignore blobs that are yet to be backed up
                return !(serverBlob.getOperationTimeMs() > partitionBackedUpUntil && status.contains(BLOB_ABSENT_IN_AZURE));
              })
              .forEach(tuple -> {
                MessageInfo serverBlob = (MessageInfo) tuple.getLeft();
                MessageInfo azureBlob = (MessageInfo) tuple.getMiddle();
                Set<BlobMatchStatus> status = (Set<BlobMatchStatus>) tuple.getRight();
                String msg = String.join(BackupCheckerFileManager.COLUMN_SEPARATOR,
                    status.stream().map(s -> s.name()).collect(Collectors.joining(",")),
                    MessageInfo.toText(serverBlob), MessageInfo.toText(azureBlob), "\n");
                fileManager.appendToFile(output, msg);
                metrics.backupIntegrityError.inc();
              }); // For each message from replica
          // TODO: To compare data, treat all keys as missing, do not mark rgroup as "done" and compare blob-crc.
          advanceToken(replica, new ExchangeMetadataResponse(Collections.emptySet(), metadata.getFindToken(),
              metadata.getRemoteReplicaLagInBytes(), Collections.emptyMap(), time));
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
    azureBlobMap.values().stream()
        .filter(azureBlob -> {
          // ignore blobs absent in server and have expired/obsolete in azure
          return !(azureBlob.isExpired() || azureBlob.isDeleted());
        })
        .forEach(azureBlob -> {
          String msg = String.join(BackupCheckerFileManager.COLUMN_SEPARATOR,
          BLOB_ABSENT_IN_SERVER.name(),
          MessageInfo.toText(null), MessageInfo.toText(azureBlob),
          "\n");
          fileManager.appendToFile(output, msg);
          metrics.backupIntegrityError.inc();
    });
    azureBlobMap.clear();
  }

  public int getNumBlobScanned() {
    return numBlobScanned.get();
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
    json.put("replicated_until", rinfo.getReplicatedUntilTime());
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
