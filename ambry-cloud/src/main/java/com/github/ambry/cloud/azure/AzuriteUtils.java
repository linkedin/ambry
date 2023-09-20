/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides methods to interact with Azurite - Azure Storage Emulator.
 */
public class AzuriteUtils {
  public static final Logger logger = LoggerFactory.getLogger(AzuriteUtils.class);
  public static final String AZURITE_CONNECTION_STRING =  "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
      "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
      "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

  /**
   * Checks connection to Azurite
   * @return True if successful, else false
   */
  public boolean connectToAzurite() {
    /**
     * To run the azurite azure storage desktop emulator:
     * $ npm install -g azurite
     * $ azurite
     *
     * Run the tests on your desktop.
     * Run unit-tests using azurite on each PR is not an option because
     * installing azurite via github action takes 45m and still fails.
     */
    try {
      Properties properties = new Properties();
      properties.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, "localhost");
      properties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "localhost");
      properties.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, "localhost");
      properties.setProperty("clustermap.resolve.hostnames", "false");
      properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, AZURITE_CONNECTION_STRING);
      getAzuriteClient(new VerifiableProperties(properties), new MetricRegistry(), null);
      return true;
    } catch (Exception e) {
      logger.error("Failed to connect to AzuriteUtils due to {}", e.toString());
      return false;
    }
  }

  /**
   * Returns a client to connect to Azurite
   * @param verifiableProperties Properties
   * @param metricRegistry Metrics
   * @param clusterMap Cluster map
   * @return Sync client to connect to Azurite
   */
  public AzureCloudDestinationSync getAzuriteClient(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry, ClusterMap clusterMap) {
    return new AzureCloudDestinationSync(verifiableProperties, metricRegistry,  clusterMap);
  }
}
