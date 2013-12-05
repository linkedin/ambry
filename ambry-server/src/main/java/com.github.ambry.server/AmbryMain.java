package com.github.ambry.server;


import java.util.Properties;

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
    if (args.length != 1) {
      System.out.println("USAGE: java [options] %s server.properties".format(AmbryServer.class.getSimpleName()));
      System.exit(1);
    }

    try {
      // need to create config
      Properties props = Utils.loadProps(args[0]);
      VerifiableProperties vprops = new VerifiableProperties(props);
      final AmbryServer server = new AmbryServer(vprops);

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
