package com.github.ambry;


/**
 * Ambry main
 */
public class AmbryMain {
  public static void main(String args[]) {
    if (args.length != 1) {
      System.out.println("USAGE: java [options] %s server.properties".format(AmbryServer.class.getSimpleName()));
      System.exit(1);
    }

    try {
      // need to create config
      final AmbryServer server = new AmbryServer();

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
      // fatal logging
    }
    System.exit(0);
  }
}
