/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;


/**
 * The tool to print out the compaction log details from a given compaction log file.
 */
public class DumpCompactionLogTool {

  private static class DumpCompactionLogConfig {
    final String hardwareLayoutFilePath;
    final String partitionLayoutFilePath;
    final String compactionLogFilePath;

    DumpCompactionLogConfig(VerifiableProperties verifiableProperties) {
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
      compactionLogFilePath = verifiableProperties.getString("compaction.log.file.path");
    }
  }

  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    DumpCompactionLogConfig config = new DumpCompactionLogConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    try (ClusterMap clusterMap = ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory,
        clusterMapConfig, config.hardwareLayoutFilePath, config.partitionLayoutFilePath)).getClusterMap()) {
      File file = new File(config.compactionLogFilePath);
      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      StoreConfig storeConfig = new StoreConfig(verifiableProperties);
      Time time = SystemTime.getInstance();
      CompactionLog compactionLog = new CompactionLog(file, blobIdFactory, time, storeConfig);
      System.out.println(compactionLog.toString());
    }
  }
}
