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
  final Counter setSwallowedErrorCount;
  final Counter getSwallowedErrorCount;
  final Counter addListenerSwallowedErrorCount;
  final Counter listenerInconsistencyCount;
  final Counter listenerTransientInconsistencyCount;

  DataNodeConfigSourceMetrics(MetricRegistry registry) {
    setSwallowedErrorCount =
        registry.counter(MetricRegistry.name(CompositeDataNodeConfigSource.class, "SetSwallowedErrorCount"));
    getSwallowedErrorCount =
        registry.counter(MetricRegistry.name(CompositeDataNodeConfigSource.class, "GetSwallowedErrorCount"));
    addListenerSwallowedErrorCount =
        registry.counter(MetricRegistry.name(CompositeDataNodeConfigSource.class, "AddListenerSwallowedErrorCount"));
    listenerInconsistencyCount =
        registry.counter(MetricRegistry.name(CompositeDataNodeConfigSource.class, "ListenerInconsistencyCount"));
    listenerTransientInconsistencyCount = registry.counter(
        MetricRegistry.name(CompositeDataNodeConfigSource.class, "ListenerTransientInconsistencyCount"));
  }
}
