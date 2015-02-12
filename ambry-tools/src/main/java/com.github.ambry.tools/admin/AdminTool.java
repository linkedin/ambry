package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * Tool to support admin related operations
 * Operations supported so far:
 * List Replicas for a given blobid
 */
public class AdminTool {

  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> typeOfOperationOpt =
          parser.accepts("typeOfOperation", "The type of operation to execute - LIST_REPLICAS").withRequiredArg()
              .describedAs("The type of file").ofType(String.class).defaultsTo("GET");

      ArgumentAcceptingOptionSpec<String> ambryBlobIdOpt =
          parser.accepts("ambryBlobId", "The blob id to execute get on").withRequiredArg().describedAs("The blob id")
              .ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(typeOfOperationOpt);
      listOpt.add(ambryBlobIdOpt);
      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.out.println("AdminTool --hardwareLayout hl --partitionLayout pl --typeOfOperation "
              + "LIST_REPLICAS -- ambryBlobId blobId");
          System.exit(1);
        }
      }

      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));

      String blobIdStr = options.valueOf(ambryBlobIdOpt);
      AdminTool adminTool = new AdminTool();
      BlobId blobId = new BlobId(blobIdStr, map);
      String typeOfOperation = options.valueOf(typeOfOperationOpt);
      if (typeOfOperation.equalsIgnoreCase("LIST_REPLICAS")) {
        List<ReplicaId> replicaIdList = adminTool.getReplicas(blobId);
        for (ReplicaId replicaId : replicaIdList) {
          System.out.println(replicaId);
        }
      } else {
        System.out.println("Invalid Type of Operation ");
        System.exit(1);
      }
    } catch (Exception e) {
      System.out.println("Closed with error " + e);
    }
  }

  public List<ReplicaId> getReplicas(BlobId blobId) {
    return blobId.getPartition().getReplicaIds();
  }
}
