package com.github.ambry.server;


import java.util.Properties;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ambry main
 */
public class AmbryMain {
  public static void main(String args[]) {
    Logger logger = LoggerFactory.getLogger("AmbryMain");
    if (args.length != 3) {
      System.out.println("USAGE: java [options] %s server.properties hardwarelayout partitionlayout"
                         .format(AmbryServer.class.getSimpleName()));
      System.exit(1);
    }

    try {
      // need to create config
      Properties props = Utils.loadProps(args[0]);
      VerifiableProperties vprops = new VerifiableProperties(props);
      ClusterMap clusterMap = new ClusterMapManager(args[1], args[2]);

      final AmbryServer server = new AmbryServer(vprops, clusterMap);

      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          server.shutdown();
        }
      });

      server.startup();
      server.awaitShutdown();
    }
    catch (Exception e) {
      System.out.println("error " + e);
    }
    System.exit(0);
  }
}
