/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Configuration parameters required by {@link DumpDataTool}
 */
public class DumpIndexConfig {
  /**
   * "The file that needs to be dumped. Index file incase of "DumpIndexTool", "CompareIndexToLog",
   * log file incase of "DumpLogTool" and replicatoken file in case of "DumpReplicatoken"
   */
  @Config("file.to.read")
  public final String fileToRead;

  /**
   * File path referring to the hardware layout
   */
  @Config("hardware.layout.file.path")
  public final String hardwareLayoutFilePath;

  /**
   * File path referring to the partition layout
   */
  @Config("partition.layout.file.path")
  public final String partitionLayoutFilePath;

  /**
   * The type of operation to perform
   */
  @Config("type.of.operation")
  public final String typeOfOperation;

  /**
   * List of blobIds (comma separated values) to filter
   */
  @Config("blobId.list")
  public final String blobIdList;

  /**
   * Path referring to replica root directory
   */
  @Config("replica.root.directory")
  public final String replicaRootDirecotry;

  /**
   * Count of active blobs
   */
  @Config("active.blobs.count")
  @Default("-1")
  public final long activeBlobsCount;

  /**
   * True if active blobs onlhy needs to be dumped, false otherwise
   */
  @Config("active.blobs.only")
  @Default("false")
  public final boolean activeBlobsOnly;

  /**
   * True if blob stats needs to be logged, false otherwise
   */
  @Config("log.blob.stats")
  @Default("false")
  public final boolean logBlobStats;

  /**
   * Create a {@link DumpIndexConfig} instance.
   * @param verifiableProperties the properties map to refer to.
   */
  public DumpIndexConfig(VerifiableProperties verifiableProperties) {
    fileToRead = verifiableProperties.getString("file.to.read");
    hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
    partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
    typeOfOperation = verifiableProperties.getString("type.of.operation");
    blobIdList = verifiableProperties.getString("blobId.list", "");
    replicaRootDirecotry = verifiableProperties.getString("replica.root.directory");
    activeBlobsCount = verifiableProperties.getInt("active.blobs.count", -1);
    activeBlobsOnly = verifiableProperties.getBoolean("active.blobs.only");
    logBlobStats = verifiableProperties.getBoolean("log.blob.stats");
  }
}
