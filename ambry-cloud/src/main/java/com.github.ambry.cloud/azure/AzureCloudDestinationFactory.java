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
package com.github.ambry.cloud.azure;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for constructing {@link AzureCloudDestination} instances.
 */
public class AzureCloudDestinationFactory implements CloudDestinationFactory {

  private static final Logger logger = LoggerFactory.getLogger(AzureCloudDestinationFactory.class);
  private final CloudConfig cloudConfig;
  private final AzureCloudConfig azureCloudConfig;
  private final String clusterName;
  private final AzureMetrics azureMetrics;
  private final AzureReplicationFeed.FeedType azureReplicationFeedType;

  /**
   * Constructor for {@link AzureCloudDestinationFactory}
   * @param verifiableProperties properties containing configs.
   * @param metricRegistry metric registry.
   */
  public AzureCloudDestinationFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    this.cloudConfig = new CloudConfig(verifiableProperties);
    this.azureCloudConfig = new AzureCloudConfig(verifiableProperties);
    this.clusterName = new ClusterMapConfig(verifiableProperties).clusterMapClusterName;
    azureMetrics = new AzureMetrics(metricRegistry);
    azureReplicationFeedType = getReplicationFeedType(verifiableProperties);
  }

  @Override
  public CloudDestination getCloudDestination() throws IllegalStateException {
    try {
      AzureCloudDestination dest =
          new AzureCloudDestination(cloudConfig, azureCloudConfig, clusterName, azureMetrics, azureReplicationFeedType);
      dest.testAzureConnectivity();
      return dest;
    } catch (Exception e) {
      logger.error("Error initializing Azure destination: {}", e.getMessage());
      throw (e instanceof IllegalStateException) ? (IllegalStateException) e : new IllegalStateException(e);
    }
  }

  /**
   * Derive the replication feed type to use from the type of token factory passed in the config.
   * @param verifiableProperties properties containing configs.
   * @return {@link AzureReplicationFeed.FeedType} object.
   */
  public static AzureReplicationFeed.FeedType getReplicationFeedType(VerifiableProperties verifiableProperties) {
    ReplicationConfig replicationConfig = new ReplicationConfig(verifiableProperties);
    if (replicationConfig.replicationCloudTokenFactory.equals(
        CosmosChangeFeedFindTokenFactory.class.getCanonicalName())) {
      return AzureReplicationFeed.FeedType.COSMOS_CHANGE_FEED;
    } else if (replicationConfig.replicationCloudTokenFactory.equals(
        CosmosUpdateTimeFindTokenFactory.class.getCanonicalName())) {
      return AzureReplicationFeed.FeedType.COSMOS_UPDATE_TIME;
    }
    throw new IllegalArgumentException(String.format(
        "Unable to get azure replication feed type due to unknown replicationCloudFindTokenFactory config %s",
        replicationConfig.replicationCloudTokenFactory));
  }
}
