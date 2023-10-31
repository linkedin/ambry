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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import java.util.Collections;


/**
 * An implementation of {@link CloudDestinationFactory} to produce {@link LatchBasedInMemoryCloudDestination}
 */
public class LatchBasedInMemoryCloudDestinationFactory implements CloudDestinationFactory {
  LatchBasedInMemoryCloudDestination destination;

  /**
   * Instantiate {@link LatchBasedInMemoryCloudDestinationFactory}.
   * @param destination the instance of {@link LatchBasedInMemoryCloudDestination}.
   */
  public LatchBasedInMemoryCloudDestinationFactory(LatchBasedInMemoryCloudDestination destination) {
    this.destination = destination;
  }

  /**
   * Constructor for {@link LatchBasedInMemoryCloudDestinationFactory}.
   * @param verifiableProperties {@link VerifiableProperties} object.
   * @param metricRegistry  {@link MetricRegistry} object.
   * @param clusterMap {@link ClusterMap} object.
   * @param accountService {@link AccountService} object.
   */
  public LatchBasedInMemoryCloudDestinationFactory(VerifiableProperties verifiableProperties,
      MetricRegistry metricRegistry, ClusterMap clusterMap, AccountService accountService) {
    destination = new LatchBasedInMemoryCloudDestination(Collections.emptyList(), clusterMap);
  }

  @Override
  public CloudDestination getCloudDestination() throws IllegalStateException {
    return destination;
  }
}

