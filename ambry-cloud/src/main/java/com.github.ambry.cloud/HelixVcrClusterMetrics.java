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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;


public class HelixVcrClusterMetrics {

  public final Counter partitionIdNotInClusterMapOnRemove;
  public final Counter partitionIdNotInClusterMapOnAdd;

  public HelixVcrClusterMetrics(MetricRegistry registry) {
    partitionIdNotInClusterMapOnRemove =
        registry.counter(MetricRegistry.name(HelixVcrCluster.class, "PartitionIdNotInClusterMapOnRemove"));
    partitionIdNotInClusterMapOnAdd =
        registry.counter(MetricRegistry.name(VcrServer.class, "PartitionIdNotInClusterMapOnAdd"));
  }
}
