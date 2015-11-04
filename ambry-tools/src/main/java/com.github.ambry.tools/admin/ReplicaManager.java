package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Helps to perform all operations related to a replica
 */
public class ReplicaManager {

  private static final short Crc_Size = 8;

  public static void main(String args[]) {
    ConnectionPool connectionPool = null;
    try {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> typeOfOperationOpt =
          parser.accepts("typeOfOperation", "The type of operation to execute - DUMP_REPLICA_TOKEN_FILE")
              .withRequiredArg().describedAs("The type of operation to be performed").ofType(String.class)
              .defaultsTo("DUMP_REPLICA_TOKEN_FILE");

      ArgumentAcceptingOptionSpec<String> replicaTokenFilePathOpt =
          parser.accepts("replicaTokenFilePath", "Replica token file path to be dumped").withOptionalArg()
              .describedAs("The file path of replica token file").defaultsTo("").ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(typeOfOperationOpt);
      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));
      String typeOfOperation = options.valueOf(typeOfOperationOpt);
      String replicaTokenFilePath = options.valueOf(replicaTokenFilePathOpt);

      if (typeOfOperation.equalsIgnoreCase("DUMP_REPLICA_TOKEN_FILE")) {
        readFromFileAndPersistIfNecessary(replicaTokenFilePath, map,
            new ReplicationConfig(new VerifiableProperties(new Properties())),
            new StoreConfig(new VerifiableProperties(new Properties())));
      } else {
        System.out.println("Invalid Type of Operation ");
        System.exit(1);
      }
    } catch (Exception e) {
      System.out.println("Closed with error " + e);
    } finally {
      if (connectionPool != null) {
        connectionPool.shutdown();
      }
    }
  }

  /**
   * Dumps replica token file for its contents.
   * @param replicaTokenFileName File to be dumped
   * @param clusterMap ClusterMap to read the partition from the stream
   * @param replicationConfig To get replication token factory
   * @param storeConfig to get store key factory
   * @throws Exception
   */
  public static void readFromFileAndPersistIfNecessary(String replicaTokenFileName, ClusterMap clusterMap,
      ReplicationConfig replicationConfig, StoreConfig storeConfig)
      throws Exception {
    System.out.println("Reading replica token from file " + replicaTokenFileName);
    // read replica token
    StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
    FindTokenFactory factory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);
    System.out.println(
        "ReplicationTokenFactory " + replicationConfig.replicationTokenFactory.toString() + ", storeKeyFactory "
            + storeConfig.storeKeyFactory.toString());
    File replicaTokenFile = new File(replicaTokenFileName);
    if (replicaTokenFile.exists()) {
      CrcInputStream crcStream = new CrcInputStream(new FileInputStream(replicaTokenFile));
      DataInputStream stream = new DataInputStream(crcStream);
      try {
        short version = stream.readShort();
        switch (version) {
          case 0:
            int count = 0;
            while (stream.available() > Crc_Size) {
              // read partition id
              PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
              // read remote node host name
              String hostname = Utils.readIntString(stream);
              // read remote replica path
              String replicaPath = Utils.readIntString(stream);
              // read remote port
              int port = stream.readInt();
              // read total bytes read from local store
              long totalBytesReadFromLocalStore = stream.readLong();
              FindToken token = factory.getFindToken(stream);
              System.out.println((count++) + " PartitionId " + partitionId + ", hostName " + hostname + ", port " + port
                  + ", replicaPath " + replicaPath + "," +
                  " totalBytesReadFromLocalStore " + totalBytesReadFromLocalStore + ", token " + token);
            }
            System.out.println("Total entries " + count);
            long crc = crcStream.getValue();
            if (crc != stream.readLong()) {
              System.out
                  .println("Crc check does not match for replica token file for mount path " + replicaTokenFileName);
            }
            break;
          default:
            throw new IllegalStateException(
                "Invalid version in replica token file for mount path " + replicaTokenFileName);
        }
      } finally {
        stream.close();
      }
    }
  }
}
