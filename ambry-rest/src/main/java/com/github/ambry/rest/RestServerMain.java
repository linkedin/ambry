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
package com.github.ambry.rest;

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.InvocationOptions;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Start point for creating an instance of {@link RestServer} and starting/shutting it down.
 */
public class RestServerMain {
  private static Logger logger = LoggerFactory.getLogger(RestServerMain.class);

  public static void main(String[] args) {
    final RestServer restServer;
    int exitCode = 0;
    ClusterMap clusterMap = null;
    try {
      InvocationOptions options = new InvocationOptions(args);
      Properties properties = Utils.loadProps(options.serverPropsFilePath);
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
      ClusterAgentsFactory clusterAgentsFactory =
          Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
              options.hardwareLayoutFilePath, options.partitionLayoutFilePath);
      clusterMap = clusterAgentsFactory.getClusterMap();
      SSLFactory sslFactory = getSSLFactoryIfRequired(verifiableProperties);
      logger.info("Bootstrapping RestServer");
      restServer = new RestServer(verifiableProperties, clusterMap, new LoggingNotificationSystem(), sslFactory);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.info("Received shutdown signal. Shutting down RestServer");
        restServer.shutdown();
      }));
      restServer.start();
      restServer.awaitShutdown();
    } catch (Exception e) {
      logger.error("Exception during bootstrap of RestServer", e);
      exitCode = 1;
    } finally {
      if (clusterMap != null) {
        clusterMap.close();
      }
    }
    logger.info("Exiting RestServerMain");
    System.exit(exitCode);
  }

  /**
   * Instantiate an {@link SSLFactory} if any components require it.
   * @param verifiableProperties The {@link VerifiableProperties} to check if any components require it.
   * @return the {@link SSLFactory}, or {@code null} if no components require it.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private static SSLFactory getSSLFactoryIfRequired(VerifiableProperties verifiableProperties) throws Exception {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    boolean sslRequired = new NettyConfig(verifiableProperties).nettyServerEnableSSL
        || clusterMapConfig.clusterMapSslEnabledDatacenters.length() > 0;
    return sslRequired ? SSLFactory.getNewInstance(new SSLConfig(verifiableProperties)) : null;
  }
}


