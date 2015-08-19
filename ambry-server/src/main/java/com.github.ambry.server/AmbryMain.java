package com.github.ambry.server;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.util.Properties;
import org.apache.log4j.PropertyConfigurator;


/**
 * Ambry main
 */
public class AmbryMain {
  public static void main(String args[]) {
    if (args.length != 4) {
      System.out.println("USAGE: java [options] " + AmbryServer.class.getSimpleName()
          + " log4j.properties server.properties hardwarelayout partitionlayout");
      System.exit(1);
    }

    try {
      PropertyConfigurator.configure(args[0]);
      Properties props = Utils.loadProps(args[1]);
      VerifiableProperties vprops = new VerifiableProperties(props);

      ClusterMap clusterMap = new ClusterMapManager(args[2], args[3], new ClusterMapConfig(vprops));

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
