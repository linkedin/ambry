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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DumpDataTool tool to assist in dumping data from log file in Ambry
 */
public class DumpLogTool {

  private DumpDataHelper dumpDataHelper;
  private static final Logger logger = LoggerFactory.getLogger(DumpLogTool.class);

  public DumpLogTool(ClusterMap map) {
    dumpDataHelper = new DumpDataHelper(map);
  }

  public static void main(String args[]) {
    try {

      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> propsFileOpt = parser.accepts("propsFile", "Properties file path")
          .withRequiredArg()
          .describedAs("propsFile")
          .ofType(String.class);

      OptionSet options = parser.parse(args);
      String propsFilePath = options.valueOf(propsFileOpt);
      Properties properties = Utils.loadProps(propsFilePath);
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      DumpLogConfig config = new DumpLogConfig(verifiableProperties);

      if (!new File(config.hardwareLayoutFilePath).exists() || !new File(config.partitionLayoutFilePath).exists()) {
        throw new IllegalArgumentException("Hardware or Partition Layout file does not exist");
      }

      ClusterMap map = new ClusterMapManager(config.hardwareLayoutFilePath, config.partitionLayoutFilePath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));
      DumpLogTool dumpLogTool = new DumpLogTool(map);

      boolean filter = !config.blobIdList.equals("");
      ArrayList<String> blobs = new ArrayList<String>();
      String[] blobArray;
      if (filter) {
        blobArray = config.blobIdList.split(",");
        blobs.addAll(Arrays.asList(blobArray));
        logger.info("Blobs to look out for :: " + blobs);
      }

      if (config.fileToRead != null) {
        logger.info("File to read " + config.fileToRead);
      }
      dumpLogTool.dumpLog(new File(config.fileToRead), config.logStartOffset, config.logEndOffset, blobs, filter);
    } catch (Exception e) {
      logger.error("Closed with exception ", e);
    }
  }

  /**
   * Dumps all records in a given log file
   * @param logFile the log file that needs to be parsed for
   * @param startOffset the starting offset from which records needs to be dumped from. Can be {@code null}
   * @param endOffset the end offset until which records need to be dumped to. Can be {@code null}
   * @param blobs List of blobIds to be filtered for
   * @param filter {@code true} if filtering has to be done, {@code false} otherwise
   * @throws IOException
   */
  public void dumpLog(File logFile, long startOffset, long endOffset, ArrayList<String> blobs, boolean filter)
      throws IOException {
    Map<String, DumpDataHelper.LogBlobRecord> blobIdToLogRecord = new HashMap<>();
    dumpDataHelper.dumpLog(logFile, startOffset, endOffset, blobs, filter, blobIdToLogRecord, true);
    long totalInConsistentBlobs = 0;
    for (String blobId : blobIdToLogRecord.keySet()) {
      DumpDataHelper.LogBlobRecord logBlobRecord = blobIdToLogRecord.get(blobId);
      if (!logBlobRecord.isConsistent) {
        totalInConsistentBlobs++;
        logger.error("Inconsistent blob " + blobId + " " + logBlobRecord);
      }
    }
    logger.info("Total inconsistent blob count " + totalInConsistentBlobs);
  }
}
