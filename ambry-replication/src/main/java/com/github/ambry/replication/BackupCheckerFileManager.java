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
  protected final AmbryCache fileChannelCache;
  public static final String COLUMN_SEPARATOR = " | ";

  protected static class FileChannel implements AmbryCacheEntry {
    SeekableByteChannel _seekableByteChannel;
    public FileChannel(SeekableByteChannel seekableByteChannel) {
      this._seekableByteChannel = seekableByteChannel;
    }
    public SeekableByteChannel getSeekableByteChannel() {
      return _seekableByteChannel;
    }
  }

  public BackupCheckerFileManager(ReplicationConfig replicationConfig, MetricRegistry metricRegistry) {
    this.replicationConfig = replicationConfig;
    this.fileChannelCache = new AmbryCache("BackupCheckerFileManagerCache", true,
        replicationConfig.maxBackupCheckerReportFd, metricRegistry);
  }

  /**
   * Returns a cached file-desc or creates a new one
   * @param filePath File system path
   * @param options Options to use when opening or creating a file
   * @return File descriptor
   */
  protected SeekableByteChannel getFd(String filePath, EnumSet<StandardOpenOption> options) {
    FileChannel fchannel = (FileChannel) fileChannelCache.getObject(filePath);
    if (fchannel == null) {
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
        fchannel = new FileChannel(Files.newByteChannel(Paths.get(filePath), options));
      } catch (IOException e) {
        logger.error("Path = {}, Options = {}, Error creating file = {}", filePath, options, e.toString());
        return null;
      }
      // insert into cache
      fileChannelCache.putObject(filePath, fchannel);
    }
    return fchannel.getSeekableByteChannel();
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
   * Truncates a file and then writes to it.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  protected boolean truncateAndWriteToFile(String filePath, String text) {
    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE);
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
