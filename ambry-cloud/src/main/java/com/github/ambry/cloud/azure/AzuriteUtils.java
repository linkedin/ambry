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
import com.github.ambry.account.AccountService;
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
  public static final String AZURITE_TABLE_CONNECTION_STRING =  "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
      "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
      "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
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
     */
    try {
      getAzuriteClient(getAzuriteConnectionProperties(), new MetricRegistry(), null, null);
      return true;
    } catch (Exception e) {
      logger.error("Failed to connect to Azurite due to {}", e.toString());
      return false;
    }
  }

  /**
   * Returns a client to connect to Azurite
   * @param metricRegistry Metrics
   * @param clusterMap Cluster map
   * @return Sync client to connect to Azurite
   */
  public AzureCloudDestinationSync getAzuriteClient(Properties properties, MetricRegistry metricRegistry,
      ClusterMap clusterMap, AccountService accountService)
      throws ReflectiveOperationException {
    // V2 does not use cosmos
    properties.setProperty(AzureCloudConfig.COSMOS_ENDPOINT, "does_not_matter");
    properties.setProperty(AzureCloudConfig.COSMOS_DATABASE, "does_not_matter");
    properties.setProperty(AzureCloudConfig.COSMOS_COLLECTION, "does_not_matter");
    properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, AzuriteUtils.AZURITE_CONNECTION_STRING);
    properties.setProperty(AzureCloudConfig.AZURE_TABLE_CONNECTION_STRING, AzuriteUtils.AZURITE_TABLE_CONNECTION_STRING);
    return new AzureCloudDestinationSync(new VerifiableProperties(properties), metricRegistry,  clusterMap, accountService);
  }

  /**
   * Returns a client to connect to Azurite
   * @param metricRegistry Metrics
   * @param clusterMap Cluster map
   * @return Sync client to connect to Azurite
   */
  public AzureCloudDestinationSync getAzuriteClient(Properties properties, MetricRegistry metricRegistry, ClusterMap clusterMap)
      throws ReflectiveOperationException {
    return getAzuriteClient(properties, metricRegistry, clusterMap, null);
  }

  public Properties getAzuriteConnectionProperties() {
    Properties properties = new Properties();
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, "localhost");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "localhost");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, "localhost");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_RESOLVE_HOSTNAMES, String.valueOf(false));
    properties.setProperty(AzureCloudConfig.COSMOS_ENDPOINT, "does_not_matter");
    properties.setProperty(AzureCloudConfig.COSMOS_DATABASE, "does_not_matter");
    properties.setProperty(AzureCloudConfig.COSMOS_COLLECTION, "does_not_matter");
    properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, AzuriteUtils.AZURITE_CONNECTION_STRING);
    properties.setProperty(AzureCloudConfig.AZURE_TABLE_CONNECTION_STRING,
        AzuriteUtils.AZURITE_TABLE_CONNECTION_STRING);
    return properties;
  }
}
