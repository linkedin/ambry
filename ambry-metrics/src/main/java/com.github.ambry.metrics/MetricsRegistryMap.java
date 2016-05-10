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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.Map;


/**
 * A class that holds all metrics registered with it. It can be registered
 * with one or more MetricReporters to flush metrics.
 */
public class MetricsRegistryMap implements ReadableMetricsRegistry {

  protected final Set<ReadableMetricsRegistryListener> listeners;

  protected final ConcurrentHashMap<String, ConcurrentHashMap<String, Metric>> metrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  private final String name;

  public MetricsRegistryMap(String name) {
    this.name = name;
    listeners = new HashSet<ReadableMetricsRegistryListener>();
    metrics = new ConcurrentHashMap<String, ConcurrentHashMap<String, Metric>>();
  }

  public MetricsRegistryMap() {
    this("unknown");
  }

  @Override
  public Counter newCounter(String group, Counter counter) {
    logger.debug("Add new counter {} {} {}.", group, counter.getName(), counter);
    putAndGetGroup(group).putIfAbsent(counter.getName(), counter);
    Counter realCounter = (Counter) metrics.get(group).get(counter.getName());
    for (ReadableMetricsRegistryListener listener : listeners) {
      listener.onCounter(group, realCounter);
    }
    return realCounter;
  }

  public Counter newCounter(String group, String name) {
    logger.debug("Creating new counter {} {}.", group, name);
    return newCounter(group, new Counter(name));
  }

  @Override
  public <T> Gauge<T> newGauge(String group, String name, T value) {
    logger.debug("Creating new gauge {} {} {}.", group, name, value);
    return newGauge(group, new Gauge<T>(name, value));
  }

  @Override
  public <T> Gauge<T> newGauge(String group, Gauge<T> gauge) {
    logger.debug("Adding new gauge {} {} {}.", group, gauge.getName(), gauge);
    putAndGetGroup(group).putIfAbsent(gauge.getName(), gauge);
    Gauge<T> realGauge = (Gauge<T>) metrics.get(group).get(gauge.getName());
    for (ReadableMetricsRegistryListener listener : listeners) {
      listener.onGauge(group, realGauge);
    }
    return realGauge;
  }

  private ConcurrentHashMap<String, Metric> putAndGetGroup(String group) {
    metrics.putIfAbsent(group, new ConcurrentHashMap<String, Metric>());
    return metrics.get(group);
  }

  public String getName() {
    return name;
  }

  @Override
  public Set<String> getGroups() {
    return metrics.keySet();
  }

  @Override
  public Map<String, Metric> getGroup(String group) {
    return metrics.get(group);
  }

  @Override
  public String toString() {
    return metrics.toString();
  }

  @Override
  public void listen(ReadableMetricsRegistryListener listener) {
    listeners.add(listener);
  }

  @Override
  public void unlisten(ReadableMetricsRegistryListener listener) {
    listeners.remove(listener);
  }
}
