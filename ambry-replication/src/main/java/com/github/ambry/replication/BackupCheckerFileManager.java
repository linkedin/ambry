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
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.store.MessageInfoType;
import com.github.ambry.store.StoreKey;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.EnumSet;


/**
 * File manager for BackupChecker, although it can be used for other purposes
 */
public class BackupCheckerFileManager {
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS");

  public BackupCheckerFileManager(ReplicationConfig replicationConfig, MetricRegistry metricRegistry) {
  }

  /**
   * Append to a given file.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  protected boolean appendToFile(String filePath, String text) {
    // open-source impl
    return true;
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
    // open-source impl
    return true;
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
