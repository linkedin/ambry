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
package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.json.JSONObject;


/**
 * Helps to perform all operations related to a partition
 */
public class PartitionManager {
  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> operationTypeOpt = parser.accepts("operationType",
          " REQUIRED: The type of operation to perform on the partition. Currently supported"
              + " operations are 'AddPartition', 'AddReplicas'")
          .withRequiredArg()
          .describedAs("operation_type")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> hardwareLayoutPathOpt =
          parser.accepts("hardwareLayoutPath", " REQUIRED: The path to the hardware layout map")
              .withRequiredArg()
              .describedAs("hardware_layout_path")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutPathOpt = parser.accepts("partitionLayoutPath",
          "REQUIRED: The path to the partition layout map. If outputPartitionLayoutPath is not defined,"
              + "this file is updated with the new partitions")
          .withRequiredArg()
          .describedAs("partition_layout_path")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> serverPropsFilePathOpt =
          parser.accepts("serverPropsFilePath", "REQUIRED: The path to server properties file")
              .withRequiredArg()
              .describedAs("server_props_file_path")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> outputPartitionLayoutPathOpt = parser.accepts("outputPartitionLayoutPath",
          "The path to the output partition layout map. The file is updated with the new partitions")
          .withOptionalArg()
          .describedAs("output_partition_layout_path")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<Integer> numberOfPartitionsOpt =
          parser.accepts("numberOfPartitionsToAdd", "The number of partitions to add")
              .withOptionalArg()
              .ofType(Integer.class);

      ArgumentAcceptingOptionSpec<Integer> numberOfReplicasPerDatacenterOpt =
          parser.accepts("numberOfReplicasPerDatacenter",
              "The number of replicas for the partition per datacenter when adding partitions")
              .withOptionalArg()
              .ofType(Integer.class);

      ArgumentAcceptingOptionSpec<Long> replicaCapacityInBytesOpt =
          parser.accepts("replicaCapacityInBytes", "The capacity of each replica in bytes for the partitions to add")
              .withOptionalArg()
              .ofType(Long.class);

      ArgumentAcceptingOptionSpec<String> partitionIdsToAddReplicasToOpt = parser.accepts("partitionIdToAddReplicasTo",
          "The partitionIds to add replicas to. This can either take a "
              + "comma separated list of partitions to add replicas to or '.' to add replicas to all partitions in "
              + "the partitionLayout ").withOptionalArg().ofType(String.class);

      ArgumentAcceptingOptionSpec<String> datacenterToAddReplicasToOpt =
          parser.accepts("datacenterToAddReplicasTo", "The data center to which replicas need to be added to")
              .withOptionalArg()
              .ofType(String.class);

      String attemptNonRackAwareOnFailureFlag = "attemptNonRackAwareOnFailure";
      parser.accepts(attemptNonRackAwareOnFailureFlag,
          "If a rack-aware partition allocation cannot be found, attempt a non rack-aware one");

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec> listOpt = new ArrayList<OptionSpec>();
      listOpt.add(hardwareLayoutPathOpt);
      listOpt.add(operationTypeOpt);
      listOpt.add(serverPropsFilePathOpt);

      ToolUtils.ensureOrExit(listOpt, options, parser);

      String hardwareLayoutPath = options.valueOf(hardwareLayoutPathOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutPathOpt);
      String serverPropsFilePath = options.valueOf(serverPropsFilePathOpt);
      String outputPartitionLayoutPath =
          options.has(outputPartitionLayoutPathOpt) ? options.valueOf(outputPartitionLayoutPathOpt)
              : partitionLayoutPath;
      String operationType = options.valueOf(operationTypeOpt);
      boolean attemptNonRackAwareOnFailure = options.has(attemptNonRackAwareOnFailureFlag);

      String fileString = null;
      try {
        fileString = Utils.readStringFromFile(partitionLayoutPath);
      } catch (FileNotFoundException e) {
        System.out.println("Partition layout path not found. Creating new file");
      }
      StaticClusterManager manager = null;
      Properties properties = Utils.loadProps(serverPropsFilePath);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
      if (fileString == null) {
        manager = (new StaticClusterAgentsFactory(clusterMapConfig, new PartitionLayout(
            new HardwareLayout(new JSONObject(Utils.readStringFromFile(hardwareLayoutPath)), clusterMapConfig),
            clusterMapConfig))).getClusterMap();
      } else {
        manager =
            (new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutPath, partitionLayoutPath)).getClusterMap();
      }
      if (operationType.compareToIgnoreCase("AddPartition") == 0) {
        listOpt.add(numberOfPartitionsOpt);
        listOpt.add(numberOfReplicasPerDatacenterOpt);
        listOpt.add(replicaCapacityInBytesOpt);
        ToolUtils.ensureOrExit(listOpt, options, parser);
        int numberOfPartitions = options.valueOf(numberOfPartitionsOpt);
        int numberOfReplicas = options.valueOf(numberOfReplicasPerDatacenterOpt);
        long replicaCapacityInBytes = options.valueOf(replicaCapacityInBytesOpt);
        manager.allocatePartitions(numberOfPartitions, clusterMapConfig.clusterMapDefaultPartitionClass,
            numberOfReplicas, replicaCapacityInBytes, attemptNonRackAwareOnFailure);
      } else if (operationType.compareToIgnoreCase("AddReplicas") == 0) {
        listOpt.add(partitionIdsToAddReplicasToOpt);
        listOpt.add(datacenterToAddReplicasToOpt);
        listOpt.add(partitionLayoutPathOpt);
        ToolUtils.ensureOrExit(listOpt, options, parser);
        String partitionIdsToAddReplicas = options.valueOf(partitionIdsToAddReplicasToOpt);
        String datacenterToAddReplicasTo = options.valueOf(datacenterToAddReplicasToOpt);
        if (partitionIdsToAddReplicas.compareToIgnoreCase(".") == 0) {
          for (PartitionId partitionId : manager.getAllPartitionIds(null)) {
            manager.addReplicas(partitionId, datacenterToAddReplicasTo, attemptNonRackAwareOnFailure);
          }
        } else {
          String[] partitionIds = partitionIdsToAddReplicas.split(",");
          for (String partitionId : partitionIds) {
            for (PartitionId partitionInCluster : manager.getAllPartitionIds(null)) {
              if (partitionInCluster.isEqual(partitionId)) {
                manager.addReplicas(partitionInCluster, datacenterToAddReplicasTo, attemptNonRackAwareOnFailure);
              }
            }
          }
        }
      }
      manager.persist(hardwareLayoutPath, outputPartitionLayoutPath);
    } catch (Exception e) {
      System.out.println("Error while executing partition command " + e);
    }
  }
}
