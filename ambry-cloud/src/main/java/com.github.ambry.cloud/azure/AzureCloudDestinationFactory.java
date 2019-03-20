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
import com.github.ambry.config.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for constructing {@link AzureCloudDestination} instances.
 */
public class AzureCloudDestinationFactory implements CloudDestinationFactory {

  private static final Logger logger = LoggerFactory.getLogger(AzureCloudDestinationFactory.class);
  private final VerifiableProperties destinationFactoryProperties;
  private final MetricRegistry metricRegistry;

  public AzureCloudDestinationFactory(VerifiableProperties destinationFactoryProperties,
      MetricRegistry metricRegistry) {
    this.destinationFactoryProperties = destinationFactoryProperties;
    this.metricRegistry = metricRegistry;
  }

  @Override
  public CloudDestination getCloudDestination(VerifiableProperties destinationProperties) throws IllegalStateException {
    try {
      return new AzureCloudDestination(destinationProperties, metricRegistry);
    } catch (Exception e) {
      logger.error("Initializing destination with properties {}", e.getMessage());
      throw new IllegalStateException(e);
    }
  }
}
