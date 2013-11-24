package com.github.ambry.metrics;

import java.util.Map;
import java.util.Set;

public interface ReadableMetricsRegistry extends MetricsRegistry {
  Set<String> getGroups();

  Map<String, Metric> getGroup(String group);

  void listen(ReadableMetricsRegistryListener listener);

  void unlisten(ReadableMetricsRegistryListener listener);
}

