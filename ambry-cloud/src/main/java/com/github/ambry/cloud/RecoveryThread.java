/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.commons.AmbryCache;
import com.github.ambry.commons.AmbryCacheEntry;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Time;
import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RecoveryThread extends ReplicaThread {
  private final Logger logger = LoggerFactory.getLogger(RecoveryThread.class);
  protected final AmbryCache fileDescriptorCache;
  protected final RecoveryManager recoveryManager;
  protected class FileDescriptor implements AmbryCacheEntry {

    SeekableByteChannel _seekableByteChannel;

    public FileDescriptor(SeekableByteChannel seekableByteChannel) {
      this._seekableByteChannel = seekableByteChannel;
    }

    public SeekableByteChannel getSeekableByteChannel() {
      return _seekableByteChannel;
    }
  }

  public RecoveryThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin, RecoveryManager recoveryManager) {
    super(threadName, findTokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient, replicationConfig,
        replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry, replicatingOverSsl,
        datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin);
    this.fileDescriptorCache = new AmbryCache(String.format("%s-FDCache", threadName), true,
        replicationConfig.maxBackupCheckerReportFd, metricRegistry);
    this.recoveryManager = recoveryManager;
    setMaxIterationsPerGroupPerCycle(1);
  }

  /**
   * Persists token to disk in each replication cycle
   * @param remoteReplicaInfo Remote replica info object
   * @param exchangeMetadataResponse Metadata object exchanged between replicas
   */
  @Override
  public void advanceToken(RemoteReplicaInfo remoteReplicaInfo, ExchangeMetadataResponse exchangeMetadataResponse) {
    // Advance in-memory token
    RecoveryToken oldToken = (RecoveryToken) remoteReplicaInfo.getToken();
    super.advanceToken(remoteReplicaInfo, exchangeMetadataResponse);
    // truncate previous token in-place and persist in-place
    RecoveryToken token = (RecoveryToken) remoteReplicaInfo.getToken();
    if (token == null) {
      logger.error("Null token for replica {}", remoteReplicaInfo);
      return;
    }
    if (token.equals(oldToken)) {
      logger.trace("Not persisting recovery token as it is unchanged, oldToken = {}, newToken = {}", oldToken, token);
      return;
    }
    if (token.isEndOfPartition()) {
      logger.info("Completed recovering partition {}", remoteReplicaInfo.getReplicaId().getPartitionId().getId());
      token.endRecovery();
    }
    logger.trace("replica = {}, token = {}", remoteReplicaInfo, token);
    String tokenFilename = recoveryManager.getRecoveryTokenFilename(remoteReplicaInfo);
    logger.trace("Writing recovery token to disk at {}", tokenFilename);
    truncateAndWriteToFile(tokenFilename, token.toString());
  }

  /**
   * Returns a cached file-desc or creates a new one
   * @param filePath File system path
   * @param options Options to use when opening or creating a file
   * @return File descriptor
   */
  protected SeekableByteChannel getFd(String filePath, EnumSet<StandardOpenOption> options) {
    FileDescriptor fileDescriptor = (FileDescriptor) fileDescriptorCache.getObject(filePath);
    if (fileDescriptor == null) {
      // Create parent folders
      Path directories = Paths.get(filePath.substring(0, filePath.lastIndexOf(File.separator)));
      try {
        Files.createDirectories(directories);
      } catch (IOException e) {
        logger.error("Path = {}, Error creating folders = {}", directories, e.toString());
        return null;
      }
      // Create file
      try {
        fileDescriptor = new FileDescriptor(Files.newByteChannel(Paths.get(filePath), options));
      } catch (IOException e) {
        logger.error("Path = {}, Options = {}, Error creating file = {}", filePath, options, e.toString());
        return null;
      }
      // insert into cache
      fileDescriptorCache.putObject(filePath, fileDescriptor);
    }
    return fileDescriptor.getSeekableByteChannel();
  }

  /**
   * Truncates a file and then writes to it.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  protected boolean truncateAndWriteToFile(String filePath, String text) {
    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING);
    SeekableByteChannel seekableByteChannel = getFd(filePath, options);
    try {
      seekableByteChannel.truncate(0);
      seekableByteChannel.write(ByteBuffer.wrap(text.getBytes()));
      return true;
    } catch (Throwable e) {
      logger.error("Failed to write token {} to file {} due to {}", text, filePath, e.toString());
    }
    return false;
  }
}
