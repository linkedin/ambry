package com.github.ambry.account.mysql;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


public class MySqlAccountStoreMetrics {

  public final Histogram writeTimeInMs;
  public final Histogram readTimeInMs;
  public final Counter writeSuccessCount;
  public final Counter writeFailureCount;
  public final Counter readSuccessCount;
  public final Counter readFailureCount;

  public MySqlAccountStoreMetrics(MetricRegistry metricRegistry) {
    writeTimeInMs = metricRegistry.histogram(MetricRegistry.name(MySqlAccountStore.class, "WriteTimeInMs"));
    readTimeInMs = metricRegistry.histogram(MetricRegistry.name(MySqlAccountStore.class, "ReadTimeInMs"));
    writeSuccessCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, "WriteSuccessCount"));
    writeFailureCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, "WriteFailureCount"));
    readSuccessCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, "ReadSuccessCount"));
    readFailureCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, "ReadFailureCount"));
  }
}
