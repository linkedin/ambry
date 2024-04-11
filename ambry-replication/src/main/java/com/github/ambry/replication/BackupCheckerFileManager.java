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
import com.github.ambry.commons.AmbryCache;
import com.github.ambry.commons.AmbryCacheEntry;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.store.MessageInfoType;
import com.github.ambry.store.StoreKey;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.EnumSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * File manager for BackupChecker, although it can be used for other purposes
 */
public class BackupCheckerFileManager {
  private final Logger logger = LoggerFactory.getLogger(BackupCheckerFileManager.class);
  // 2024/04/11 00:00:05.481
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MMM/dd HH:mm:ss.SSS");
  protected final ReplicationConfig replicationConfig;
  protected final AmbryCache fileDescriptorCache;
  public static final String COLUMN_SEPARATOR = " | ";

  protected class FileDescriptor implements AmbryCacheEntry {
    SeekableByteChannel _seekableByteChannel;
    public FileDescriptor(SeekableByteChannel seekableByteChannel) {
      this._seekableByteChannel = seekableByteChannel;
    }
    public SeekableByteChannel getSeekableByteChannel() {
      return _seekableByteChannel;
    }
  }

  public BackupCheckerFileManager(ReplicationConfig replicationConfig, MetricRegistry metricRegistry) {
    this.replicationConfig = replicationConfig;
    this.fileDescriptorCache = new AmbryCache("BackupCheckerFileManagerCache", true,
        replicationConfig.maxBackupCheckerReportFd, metricRegistry);
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
   * Write to a given file
   * @param seekableByteChannel File descriptor
   * @param text Text to append
   * @return True if write was successful, false otherwise
   */
  protected boolean writeToFile(SeekableByteChannel seekableByteChannel, String text) {
    try {
      seekableByteChannel.write(ByteBuffer.wrap(
          String.join(COLUMN_SEPARATOR, DATE_FORMAT.format(System.currentTimeMillis()), text).getBytes(StandardCharsets.UTF_8)));
      return true;
    } catch (IOException e) {
      logger.error(e.toString());
      return false;
    }
  }

  /**
   * Append to a given file.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  protected boolean appendToFile(String filePath, String text) {
    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.APPEND);
    if (!Files.exists(Paths.get(filePath))) {
      options.add(StandardOpenOption.CREATE);
    }
    SeekableByteChannel seekableByteChannel = getFd(filePath, options);
    return writeToFile(seekableByteChannel, text);
  }


  /**
   * Append these information to the give file.
   * @param filePath The file path to append
   * @param remoteReplicaInfo The {@link RemoteReplicaInfo}.
   * @param acceptableLocalBlobStates The set of acceptable local blob states
   * @param storeKey The {@link StoreKey}.
   * @param operationTime The operation time in ms
   * @param remoteBlobState The blob state from remote
   * @param localBlobState The blob state from local, could also be error codes
   * @return True if append was successful, false otherwise
   */
  protected boolean appendToFile(String filePath, RemoteReplicaInfo remoteReplicaInfo,
      EnumSet<MessageInfoType> acceptableLocalBlobStates, StoreKey storeKey, long operationTime,
      EnumSet<MessageInfoType> remoteBlobState, Object localBlobState) {
    String messageFormat = "%s | Missing %s | %s | Optime = %s | RemoteBlobState = %s | LocalBlobState = %s \n";
    return appendToFile(filePath, String.format(messageFormat, remoteReplicaInfo, acceptableLocalBlobStates, storeKey,
        DATE_FORMAT.format(operationTime), remoteBlobState, localBlobState));
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

  /**
   * Truncates a fie and then writes these information to it.
   * @param filePath The file to truncate
   * @param remoteReplicaInfo The {@link RemoteReplicaInfo}.
   * @param localLagFromRemoteInBytes The lag from remote in bytes
   * @return True if write was successful, false otherwise
   */
  protected boolean truncateAndWriteToFile(String filePath, RemoteReplicaInfo remoteReplicaInfo,
      long localLagFromRemoteInBytes) {
    String text =
        String.format("%s | isSealed = %s | Token = %s | localLagFromRemoteInBytes = %s \n", remoteReplicaInfo,
            remoteReplicaInfo.getReplicaId().isSealed(), remoteReplicaInfo.getToken().toString(),
            localLagFromRemoteInBytes);
    return truncateAndWriteToFile(filePath, text);
  }
}
