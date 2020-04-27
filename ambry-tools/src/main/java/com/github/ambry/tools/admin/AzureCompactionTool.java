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
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to purge dead blobs for an Ambry partition from Azure storage.
 * Usage: java -cp /path/to/ambry.jar AzureCompactionTool <-propsFile <property-file-path> [-purge] partitionPath
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

    Set<PartitionId> partitionIdSet =
        partitions.stream().map(path -> new PartitionPathId(path)).collect(Collectors.toSet());

    // User needs to specify this option to actually delete blobs
    boolean testMode = !optionSet.has(PURGE_OPTION);

    CloudDestination azureDest = null;
    try {
      azureDest = new AzureCloudDestinationFactory(verifiableProperties, new MetricRegistry()).getCloudDestination();
      CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
      CloudStorageCompactor compactor =
          new CloudStorageCompactor(azureDest, cloudConfig, partitionIdSet, new VcrMetrics(new MetricRegistry()));

      // Attempt clean shutdown if someone Ctrl-C's us.
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.info("Received shutdown signal. Shutting down compactor.");
        compactor.shutdown();
      }));

      if (testMode) {
        String partitionPath = partitions.get(0);
        long now = System.currentTimeMillis();
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
      } else {
        compactor.compactPartitions();
      }
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

  /**
   * PartitionId implementation that returns only its path.
   */
  private static class PartitionPathId implements PartitionId {
    private final String path;

    private PartitionPathId(String path) {
      this.path = path;
    }

    @Override
    public byte[] getBytes() {
      return new byte[0];
    }

    @Override
    public List<? extends ReplicaId> getReplicaIds() {
      return null;
    }

    @Override
    public List<? extends ReplicaId> getReplicaIdsByState(ReplicaState state, String dcName) {
      return null;
    }

    @Override
    public PartitionState getPartitionState() {
      return null;
    }

    @Override
    public boolean isEqual(String partitionId) {
      return false;
    }

    @Override
    public String toPathString() {
      return path;
    }

    @Override
    public String getPartitionClass() {
      return null;
    }

    @Override
    public JSONObject getSnapshot() {
      return null;
    }

    @Override
    public int compareTo(PartitionId o) {
      return 0;
    }
  }

  private static void printHelpAndExit(OptionParser parser) throws IOException {
    parser.printHelpOn(System.err);
    System.exit(1);
  }
}
