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
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Recovery thread restores backup from cloud and reports its status
 */
public class RecoveryThread extends ReplicaThread {
  private final Logger logger = LoggerFactory.getLogger(RecoveryThread.class);
  protected final BackupCheckerFileManager fileManager;
  protected final ReplicationConfig _replicationConfig;
  public static final String RECOVERY_STATUS_FILE = "cloudReplicaRecoveryStatusFile";

  public RecoveryThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
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
      fileManager = Utils.getObj(replicationConfig.backupCheckFileManagerType,
          "RecoveryThread" + threadName, replicationConfig, metricRegistry);
    } catch (ReflectiveOperationException e) {
      logger.error("Failed to create file manager. ", e.toString());
      throw new RuntimeException(e);
    }
    this._replicationConfig = replicationConfig;
    logger.info("Created RecoveryThread {}", threadName);
  }

  /**
   * Prints recovery progress when recovering from cloud
   * @param remoteReplicaInfo Info about remote replica
   * @param exchangeMetadataResponse Metadata response object
   */
  @Override
  protected void logReplicationStatus(RemoteReplicaInfo remoteReplicaInfo,
      ExchangeMetadataResponse exchangeMetadataResponse) {
    // This will help us know when to stop recovery process
    String text = String.format("%s | ReplicaType = %s | Token = %s | localLagFromRemoteInBytes = %s \n",
        remoteReplicaInfo, remoteReplicaInfo.getLocalReplicaId().getReplicaType(),
        remoteReplicaInfo.getToken().toString(),
        exchangeMetadataResponse.localLagFromRemoteInBytes);
    fileManager.truncateAndWriteToFile(getFilePath(remoteReplicaInfo, RECOVERY_STATUS_FILE), text);
  }

  /**
   * Returns a concatenated file path
   * @param remoteReplicaInfo Info about remote replica
   * @param fileName Name of file to write text to
   * @return Returns a concatenated file path
   */
  protected String getFilePath(RemoteReplicaInfo remoteReplicaInfo, String fileName) {
    return String.join(File.separator, this._replicationConfig.backupCheckerReportDir,
        Long.toString(remoteReplicaInfo.getReplicaId().getPartitionId().getId()),
        fileName);
  }
}
