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
package com.github.ambry.metrics;

/**
 * A metric visitor visits a metric, before metrics are flushed to a metrics stream.
 * The metric visitor visits the metric and registers it to its list of metrics that
 * it has visited. A visitor should only visit a metric once for the lifetime of the
 * metric.
 */
public abstract class MetricsVisitor {
  public abstract void counter(Counter counter);

  public abstract <T> void gauge(Gauge<T> gauge);

  /**
   * Visits the specified metric.
   * @param metric The metric to visit
   */
  public void visit(Metric metric) {
    if (metric instanceof Counter) {
      counter((Counter) metric);
    } else if (metric instanceof Gauge<?>) {
      gauge((Gauge<?>) metric);
    }
  }
}
