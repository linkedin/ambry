/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.clustermap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;


/**
 * Metrics for {@link DataNodeConfigSource} implementations
 */
class DataNodeConfigSourceMetrics {
  final Counter setInconsistentCount;
  final Counter getInconsistentCount;
  final Counter listenerInconsistentCount;
  final Counter listenerTrendingInconsistentCount;
  final Counter listenerConsistentCount;

  DataNodeConfigSourceMetrics(MetricRegistry registry) {
    setInconsistentCount =
        registry.counter(MetricRegistry.name(CompositeDataNodeConfigSource.class, "SetInconsistentCount"));
    getInconsistentCount =
        registry.counter(MetricRegistry.name(CompositeDataNodeConfigSource.class, "GetInconsistentCount"));
    listenerInconsistentCount =
        registry.counter(MetricRegistry.name(CompositeDataNodeConfigSource.class, "ListenerInconsistentCount"));
    listenerTrendingInconsistentCount =
        registry.counter(MetricRegistry.name(CompositeDataNodeConfigSource.class, "ListenerTrendingInconsistentCount"));
    listenerConsistentCount =
        registry.counter(MetricRegistry.name(CompositeDataNodeConfigSource.class, "ListenerConsistentCount"));
  }
}
