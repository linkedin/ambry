/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudStorageCompactor;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.cloud.azure.AzureCloudDestinationFactory;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to purge dead blobs for an Ambry partition from Azure storage.
 * Usage: java -cp /path/to/ambry.jar com.github.ambry.tools.admin.AzureCompactionTool <-propsFile <property-file-path> [-purge] partitionPath
 */
public class AzureCompactionTool {

  private static final Logger logger = LoggerFactory.getLogger(AzureCompactionTool.class);
  private static final String PURGE_OPTION = "purge";
  private static final String PROPS_FILE = "propsFile";

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();
    ArgumentAcceptingOptionSpec<String> propsFileOpt = parser.accepts(PROPS_FILE, "Properties file path")
        .withRequiredArg()
        .describedAs(PROPS_FILE)
        .ofType(String.class);
    String commandName = AzureCompactionTool.class.getSimpleName();
    parser.accepts(PURGE_OPTION, "Flag to purge dead blobs from the partition");
    parser.nonOptions("The partitions to compact").ofType(String.class);
    OptionSet optionSet = parser.parse(args);
    String propsFilePath = optionSet.valueOf(propsFileOpt);
    if (propsFilePath == null) {
      printHelpAndExit(parser);
    }
    Properties properties = Utils.loadProps(propsFilePath);
    ToolUtils.addClusterMapProperties(properties);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    List<String> partitions = (List<String>) optionSet.nonOptionArguments();
    if (partitions.isEmpty()) {
      printHelpAndExit(parser);
    }

    String partitionPath = partitions.get(0);
    // User needs to specify this option to actually delete blobs
    boolean testMode = !optionSet.has(PURGE_OPTION);
    long now = System.currentTimeMillis();

    CloudDestination azureDest = null;
    try {
      azureDest = new AzureCloudDestinationFactory(verifiableProperties, new MetricRegistry()).getCloudDestination();
      CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
      CloudStorageCompactor compactor = new CloudStorageCompactor(azureDest, cloudConfig, Collections.emptySet(),
          new VcrMetrics(new MetricRegistry()));
      if (testMode) {
        CloudBlobMetadata blobMetadata = compactor.getOldestDeletedBlob(partitionPath);
        if (blobMetadata == null) {
          logger.info("No deleted blobs need purging");
        } else {
          int daysOld = (int) ((now - blobMetadata.getDeletionTime()) / TimeUnit.DAYS.toMillis(1));
          logger.info("Oldest deleted blob was deleted about {} days ago: {}", daysOld, blobMetadata.toMap());
        }
        blobMetadata = compactor.getOldestExpiredBlob(partitionPath);
        if (blobMetadata == null) {
          logger.info("No expired blobs need purging");
        } else {
          int daysOld = (int) ((now - blobMetadata.getExpirationTime()) / TimeUnit.DAYS.toMillis(1));
          logger.info("Oldest expired blob was expired about {} days ago: {}", daysOld, blobMetadata.toMap());
        }
        System.exit(0);
      }
      int result = compactor.compactPartition(partitionPath, CloudBlobMetadata.FIELD_DELETION_TIME, now);
      logger.info("In partition {}: {} deleted blobs purged", partitionPath, result);
      result = compactor.compactPartition(partitionPath, CloudBlobMetadata.FIELD_EXPIRATION_TIME, now);
      logger.info("In partition {}: {} expired blobs purged", partitionPath, result);
      System.exit(0);
    } catch (Exception ex) {
      logger.error("Command {} failed", commandName, ex);
      System.exit(1);
    } finally {
      if (azureDest != null) {
        azureDest.close();
      }
    }
  }

  private static void printHelpAndExit(OptionParser parser) throws IOException {
    parser.printHelpOn(System.err);
    System.exit(1);
  }
}
