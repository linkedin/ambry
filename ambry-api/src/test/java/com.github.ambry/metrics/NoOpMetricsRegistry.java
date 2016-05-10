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

public class NoOpMetricsRegistry implements MetricsRegistry {
  @Override
  public Counter newCounter(String group, String name) {
    return new Counter(name);
  }

  @Override
  public Counter newCounter(String group, Counter counter) {
    return counter;
  }

  @Override
  public <T> Gauge<T> newGauge(String group, String name, T value) {
    return new Gauge<T>(name, value);
  }

  @Override
  public <T> Gauge<T> newGauge(String group, Gauge<T> gauge) {
    return gauge;
  }
}

