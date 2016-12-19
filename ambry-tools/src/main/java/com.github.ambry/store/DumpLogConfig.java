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
public class DumpLogConfig {
  /**
   * Refers to log file that needs to be dumped
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
   * List of blobIds (comma separated values) to filter
   */
  @Config("blobId.list")
  public final String blobIdList;

  /**
   * The offset in the log to start dumping from
   */
  @Config("log.start.offset")
  @Default("0")
  public final long logStartOffset;

  /**
   * The offset in the log until which to dump data
   */
  @Config("log.end.offset")
  @Default("-1")
  public final long logEndOffset;

  /**
   * Create a {@link DumpLogConfig} instance.
   * @param verifiableProperties the properties map to refer to.
   */
  public DumpLogConfig(VerifiableProperties verifiableProperties) {
    fileToRead = verifiableProperties.getString("file.to.read");
    hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
    partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
    blobIdList = verifiableProperties.getString("blobId.list", "");
    logStartOffset = verifiableProperties.getInt("log.start.offset", -1);
    logEndOffset = verifiableProperties.getInt("log.end.offset", -1);
  }
}
