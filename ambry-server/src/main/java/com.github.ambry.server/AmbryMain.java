package com.github.ambry.server;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.util.Properties;


/**
 * Ambry main
 */
public class AmbryMain {
  public static void main(String args[]) {
    if (args.length != 3) {
      System.out.println("USAGE: java [options] " + AmbryMain.class.getCanonicalName() +
          " server.properties hardwarelayout partitionlayout");
      System.exit(1);
    }

    try {
      Properties props = Utils.loadProps(args[0]);
      VerifiableProperties vprops = new VerifiableProperties(props);

      ClusterMap clusterMap = new ClusterMapManager(args[1], args[2], new ClusterMapConfig(vprops));

      final AmbryServer server = new AmbryServer(vprops, clusterMap);

      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          server.shutdown();
        }
      });

      server.startup();
      server.awaitShutdown();
    } catch (Exception e) {
      System.out.println("error " + e);
    }
    System.exit(0);
  }
}
