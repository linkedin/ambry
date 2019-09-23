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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.InvocationOptions;
import com.github.ambry.utils.Utils;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Start point for creating an instance of {@link VcrServer} and starting/shutting it down.
 */
public class VcrMain {
  private static Logger logger = LoggerFactory.getLogger(VcrMain.class);

  public static void main(String[] args) {
    final VcrServer vcrServer;
    int exitCode = 0;
    try {
      InvocationOptions options = new InvocationOptions(args);
      Properties properties = Utils.loadProps(options.serverPropsFilePath);
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
      ClusterAgentsFactory clusterAgentsFactory =
          Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
              options.hardwareLayoutFilePath, options.partitionLayoutFilePath);
      logger.info("Bootstrapping VcrServer");
      vcrServer = new VcrServer(verifiableProperties, clusterAgentsFactory, new LoggingNotificationSystem());
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          logger.info("Received shutdown signal. Shutting down VcrServer");
          vcrServer.shutdown();
        }
      });
      vcrServer.startup();
      vcrServer.awaitShutdown(Integer.MAX_VALUE);
    } catch (Exception e) {
      logger.error("Exception during bootstrap of VcrServer", e);
      exitCode = 1;
    }
    logger.info("Exiting VcrMain");
    System.exit(exitCode);
  }
}
