/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.commons;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * {@link HelixNotifier} specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the {@link HelixNotifier} to the provided {@link MetricRegistry}.
 */
public class HelixNotifierMetrics {
  // Histogram
  public final Histogram startupTimeInMs;

  // Counter
  public final Counter publishMessageErrorCount;
  public final Counter unexpectedDeleteEventErrorCount;
  public final Counter sendMessageToLocalListenerErrorCount;

  public HelixNotifierMetrics(MetricRegistry metricRegistry) {
    // Histogram
    startupTimeInMs = metricRegistry.histogram(MetricRegistry.name(HelixNotifier.class, "startupTimeInMs"));

    // Counter
    publishMessageErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixNotifier.class, "publishMessageErrorCount"));
    unexpectedDeleteEventErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixNotifier.class, "unexpectedDeleteEventErrorCount"));
    sendMessageToLocalListenerErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixNotifier.class, "sendMessageToLocalListenerErrorCount"));
  }
}
