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
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudStorageCompactor;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.cloud.azure.AzureCloudDestinationFactory;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import java.util.Collections;
import java.util.List;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to purge dead blobs for an Ambry partition from Azure storage.
 * Usage: java -cp /path/to/ambry.jar -propsFile <property-file-path> [-purge] partitionPath
 */
public class AzureCompactionTool {

  private static final Logger logger = LoggerFactory.getLogger(AzureCompactionTool.class);
  private static final String PURGE_OPTION = "purge";

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();
    parser.accepts("propsFile", "Properties file path").withRequiredArg().ofType(String.class);
    String commandName = AzureCompactionTool.class.getSimpleName();
    parser.nonOptions("The partitions to compact").ofType(String.class);
    parser.accepts(PURGE_OPTION, "Flag to purge dead blobs from the partition");
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    OptionSet optionSet = parser.parse(args);
    List<String> partitions = (List<String>) optionSet.nonOptionArguments();
    if (partitions.isEmpty()) {
      parser.printHelpOn(System.err);
      return;
    }

    String partitionPath = partitions.get(0);
    // User needs to specify this option to actually delete blobs
    boolean testMode = !optionSet.has(PURGE_OPTION);

    try {
      CloudDestination azureDest =
          new AzureCloudDestinationFactory(verifiableProperties, new MetricRegistry()).getCloudDestination();
      CloudStorageCompactor compactor =
          new CloudStorageCompactor(azureDest, Collections.emptySet(), new VcrMetrics(new MetricRegistry()), testMode);
      int result = compactor.compactPartition(partitionPath);
      String resultMessage =
          String.format("In partition %s: %d blobs %s", partitionPath, result, testMode ? "ready to purge" : "purged");
      System.out.println(resultMessage);
    } catch (Exception ex) {
      System.err.println("Command " + commandName + " failed with " + ex);
      logger.error("Command {} failed", commandName, ex);
      System.exit(1);
    }
  }
}
