package com.github.ambry.named;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


public class Metrics {
  public final Counter namedDataNotFoundGetCount;
  public final Counter namedDataErrorGetCount;
  public final Counter namedDataInconsistentGetCount;

  public final Counter namedDataInconsistentListCount;

  public final Counter namedDataNotFoundDeleteCount;
  public final Counter namedDataErrorDeleteCount;
  public final Counter namedDataInconsistentDeleteCount;

  public final Counter namedDataErrorPutCount;

  public final Counter namedTtlupdateErrorCount;

  public final Histogram namedBlobGetTimeInMs;
  public final Histogram namedBlobListTimeInMs;
  public final Histogram namedBlobPutTimeInMs;
  public final Histogram namedBlobDeleteTimeInMs;

  public final Histogram namedBlobPullStaleTimeInMs;
  public final Histogram namedBlobCleanupTimeInMs;

  public final Histogram namedTtlupdateTimeInMs;

  public final Meter namedBlobGetRate;
  public final Meter namedBlobListRate;
  public final Meter namedBlobDeleteRate;
  public final Meter namedBlobInsertRate;
  public final Meter namedBlobUpdateRate;

  /**
   * Constructor to create the Metrics.
   * @param metricRegistry The {@link MetricRegistry}.
   */
  public Metrics(MetricRegistry metricRegistry, String prefix) {
    namedDataNotFoundGetCount =
        metricRegistry.counter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedDataNotFoundGetCount"));
    namedDataErrorGetCount =
        metricRegistry.counter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedDataErrorGetCount"));
    namedDataInconsistentGetCount =
        metricRegistry.counter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedDataInconsistentGetCount"));

    namedDataInconsistentListCount =
        metricRegistry.counter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedDataInconsistentListCount"));

    namedDataNotFoundDeleteCount =
        metricRegistry.counter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedDataNotFoundDeleteCount"));
    namedDataErrorDeleteCount =
        metricRegistry.counter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedDataErrorDeleteCount"));
    namedDataInconsistentDeleteCount = metricRegistry.counter(
        MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedDataInconsistentDeleteCount"));

    namedDataErrorPutCount =
        metricRegistry.counter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedDataErrorPutCount"));

    namedTtlupdateErrorCount =
        metricRegistry.counter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedTtlupdateErrorCount"));

    namedBlobGetTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobGetTimeInMs"));
    namedBlobListTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobListTimeInMs"));
    namedBlobPutTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobPutTimeInMs"));
    namedBlobDeleteTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobDeleteTimeInMs"));

    namedBlobPullStaleTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobPullStaleTimeInMs"));
    namedBlobCleanupTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobCleanupTimeInMs"));

    namedTtlupdateTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedTtlupdateTimeInMs"));

    namedBlobGetRate = metricRegistry.meter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobGetRate"));
    namedBlobListRate = metricRegistry.meter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobListRate"));
    namedBlobDeleteRate =
        metricRegistry.meter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobDeleteRate"));
    namedBlobInsertRate =
        metricRegistry.meter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobPutRate"));
    namedBlobUpdateRate =
        metricRegistry.meter(MetricRegistry.name(MySqlNamedBlobDb.class, prefix + "NamedBlobUpdateRate"));
  }
}
