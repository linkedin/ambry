package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.HardwareLayout;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionLayout;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.util.ArrayList;


/**
 * Helps to perform all operations related to a partition
 */
public class PartitionManager {
  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> hardwareLayoutPathOpt =
          parser.accepts("hardwareLayoutPath", "The path to the hardware layout map").withRequiredArg()
              .describedAs("hardware_layout_path").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutPathOpt = parser.accepts("partitionLayoutPath",
          "The path to the partition layout map. The file is updated with the new partitions").withRequiredArg()
          .describedAs("partition_layout_path").ofType(String.class).defaultsTo("");

      ArgumentAcceptingOptionSpec<String> operationTypeOpt = parser.accepts("operationType",
          "The type of operation to perform on the partition. Currently supported" + "operations are 'AddPartition', 'AddReplicas'")
          .withRequiredArg().describedAs("operation_type").ofType(String.class);

      ArgumentAcceptingOptionSpec<Integer> numberOfPartitionsOpt =
          parser.accepts("numberOfPartitionsToAdd", "The number of partitions to add").withRequiredArg()
              .describedAs("number_of_partitions_to_add").ofType(Integer.class);

      ArgumentAcceptingOptionSpec<Integer> numberOfReplicasPerDatacenterOpt =
          parser.accepts("numberOfReplicasPerDatacenter", "The number of replicas for the partition per datacenter")
              .withRequiredArg().describedAs("number_of_replicas_per_datacenter").ofType(Integer.class);

      ArgumentAcceptingOptionSpec<Long> replicaCapacityInBytesOpt =
          parser.accepts("replicaCapacityInBytes", "The capacity of each replica in bytes").withRequiredArg()
              .describedAs("replica_capacity_in_bytes").ofType(Long.class);

      ArgumentAcceptingOptionSpec<String> partitionIdsToAddReplicasToOpt =
          parser.accepts("partitionIdToAddReplicasTo", "The partitionIds to add replicas to").withRequiredArg()
              .describedAs("partition_id_to_add_replicas_to").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> datacenterToAddReplicasToOpt =
          parser.accepts("datacenterToAddReplicasTo", "The data center to which replicas need to be added to").withRequiredArg()
              .describedAs("datacenter_to_add_replicas_to").ofType(String.class);


      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(hardwareLayoutPathOpt);
      listOpt.add(operationTypeOpt);

      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      String hardwareLayoutPath = options.valueOf(hardwareLayoutPathOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutPathOpt);
      String operationType = options.valueOf(operationTypeOpt);

      String fileString = null;
      try {
        fileString = Utils.readStringFromFile(partitionLayoutPath);
      } catch (FileNotFoundException e) {
        System.out.println("Partition layout path not found. Creating new file");
      }
      ClusterMapManager manager = null;
      if (fileString == null) {
        manager = new ClusterMapManager(
            new PartitionLayout(new HardwareLayout(new JSONObject(Utils.readStringFromFile(hardwareLayoutPath)))));
      } else {
        manager = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath);
      }
      if (operationType.compareToIgnoreCase("AddPartition") == 0) {
        listOpt.add(numberOfPartitionsOpt);
        listOpt.add(numberOfReplicasPerDatacenterOpt);
        listOpt.add(replicaCapacityInBytesOpt);
        for (OptionSpec opt : listOpt) {
          if (!options.has(opt)) {
            System.err.println("Missing required argument \"" + opt + "\"");
            parser.printHelpOn(System.err);
            System.exit(1);
          }
        }
        int numberOfPartitions = options.valueOf(numberOfPartitionsOpt);
        int numberOfReplicas = options.valueOf(numberOfReplicasPerDatacenterOpt);
        long replicaCapacityInBytes = options.valueOf(replicaCapacityInBytesOpt);
        manager.allocatePartitions(numberOfPartitions, numberOfReplicas, replicaCapacityInBytes);
      } else if (operationType.compareToIgnoreCase("AddReplicas") == 0) {
        listOpt.add(partitionIdsToAddReplicasToOpt);
        listOpt.add(datacenterToAddReplicasToOpt);
        listOpt.add(partitionLayoutPathOpt);
        for (OptionSpec opt : listOpt) {
          if (!options.has(opt)) {
            System.err.println("Missing required argument \"" + opt + "\"");
            parser.printHelpOn(System.err);
            System.exit(1);
          }
        }
        String partitionIdsToAddReplicas = options.valueOf(partitionIdsToAddReplicasToOpt);
        String datacenterToAddReplicasTo = options.valueOf(datacenterToAddReplicasToOpt);
        if (partitionIdsToAddReplicas.compareToIgnoreCase("*") == 0) {
          for (PartitionId partitionId : manager.getAllPartitions()) {
            manager.addReplicas(partitionId, datacenterToAddReplicasTo);
          }
        } else {
          String[] partitionIds = partitionIdsToAddReplicas.split(",");
          for (String partitionId : partitionIds) {
            for (PartitionId partitionInCluster : manager.getAllPartitions()) {
              if (new String(partitionInCluster.getBytes()).compareToIgnoreCase(partitionId) == 0) {
                manager.addReplicas(partitionInCluster, datacenterToAddReplicasTo);
              }
            }
          }
        }
      }
      manager.persist(hardwareLayoutPath, partitionLayoutPath);
    } catch (Exception e) {
      System.out.println("Error while executing partition command " + e);
    }
  }
}
