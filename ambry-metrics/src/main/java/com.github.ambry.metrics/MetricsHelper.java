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

import java.util.concurrent.Callable;


/**
 * MetricsHelper is a little helper class to make it easy to register and
 * manage counters and gauges.
 */
public abstract class MetricsHelper {
  private final String group;
  private final MetricsRegistry registry;

  public MetricsHelper(MetricsRegistry registry) {
    this.group = this.getClass().getName();
    this.registry = registry;
  }

  public Counter newCounter(String name) {
    return registry.newCounter(group, (getPrefix() + name).toLowerCase());
  }

  public <T> Gauge<T> newGauge(String name, T value) {
    return registry.newGauge(group, new Gauge((getPrefix() + name).toLowerCase(), value));
  }

  public <T> Gauge<T> newGauge(String name, final Callable<T> value)
      throws Exception {
    return registry.newGauge(group, new Gauge((getPrefix() + name).toLowerCase(), value.call()) {
      public T getValue() {
        try {
          return value.call();
        } catch (Exception e) {
          return null;
        }
      }
    });
  }

  /**
   * Returns a prefix for metric names.
   */
  public String getPrefix() {
    return "";
  }
}
