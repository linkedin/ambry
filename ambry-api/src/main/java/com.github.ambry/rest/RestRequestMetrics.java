package com.github.ambry.rest;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * TODO: write description
 */
public class RestRequestMetrics {
  private static RequestMetricsTracker defaultMetricsTracker;

  public final NioLayerMetrics nioLayerMetrics = new NioLayerMetrics();
  public final ScalingLayerMetrics scalingLayerMetrics = new ScalingLayerMetrics();

  private final AtomicLong totalTime = new AtomicLong(0);
  private final AtomicBoolean metricsRecorded = new AtomicBoolean(false);
  private RequestMetricsTracker metricsTracker = defaultMetricsTracker;

  public static class RequestMetricsTracker {
    public final Histogram nioRequestProcessingTime;
    public final Histogram nioResponseProcessingTime;
    public final Histogram nioResponseWriteTime;

    public final Histogram scRequestProcessingTime;
    public final Histogram scRequestQueueingTime;
    public final Histogram scResponseProcessingTime;
    public final Histogram scResponseQueueingTime;

    public final Histogram totalTime;

    public RequestMetricsTracker(Class ownerClass, String requestType, MetricRegistry metricRegistry) {
      if (ownerClass == null || metricRegistry == null || requestType == null) {
        StringBuilder errorMessage = new StringBuilder("Null arg(s) received during instantiation of MetricsTracker -");
        if (ownerClass == null) {
          errorMessage.append(" [Owner class] ");
        }
        if (metricRegistry == null) {
          errorMessage.append(" [MetricRegistry] ");
        }
        if (requestType == null) {
          errorMessage.append(" [Request type] ");
        }
        throw new IllegalArgumentException(errorMessage.toString());
      }

      nioRequestProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "NioRequestProcessingTime"));
      nioResponseProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "NioResponseProcessingTime"));
      nioResponseWriteTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "NioResponseWriteTime"));

      scRequestProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "ScRequestProcessingTime"));
      scRequestQueueingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "ScRequestQueueingTime"));
      scResponseProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "ScResponseProcessingTime"));
      scResponseQueueingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "ScResponseQueueingTime"));

      totalTime = metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "TotalTime"));
    }
  }

  public class NioLayerMetrics {
    private final AtomicLong requestProcessingTime = new AtomicLong(0);
    private final AtomicLong responseProcessingTime = new AtomicLong(0);
    private final AtomicLong responseWriteTime = new AtomicLong(0);

    public long addToRequestProcessingTime(long delta) {
      addToTotalTime(delta);
      return requestProcessingTime.addAndGet(delta);
    }

    public long addToResponseProcessingTime(long delta) {
      addToTotalTime(delta);
      return responseProcessingTime.addAndGet(delta);
    }

    public long addToResponseWriteTime(long delta) {
      addToTotalTime(delta);
      return responseWriteTime.addAndGet(delta);
    }
  }

  public class ScalingLayerMetrics {
    private final AtomicLong requestProcessingTime = new AtomicLong(0);
    private final AtomicLong requestQueueingTime = new AtomicLong(0);
    private final AtomicLong responseProcessingTime = new AtomicLong(0);
    private final AtomicLong responseQueueingTime = new AtomicLong(0);

    public long addToRequestProcessingTime(long delta) {
      addToTotalTime(delta);
      return requestProcessingTime.addAndGet(delta);
    }

    public long addToRequestQueueingTime(long delta) {
      addToTotalTime(delta);
      return requestQueueingTime.addAndGet(delta);
    }

    public long addToResponseProcessingTime(long delta) {
      addToTotalTime(delta);
      return responseProcessingTime.addAndGet(delta);
    }

    public long addToResponseQueueingTime(long delta) {
      addToTotalTime(delta);
      return responseQueueingTime.addAndGet(delta);
    }
  }

  public long addToTotalTime(long delta) {
    return totalTime.addAndGet(delta);
  }

  public void injectTracker(RequestMetricsTracker requestMetricsTracker) {
    metricsTracker = requestMetricsTracker;
  }

  public void recordMetrics() {
    if (metricsTracker != null) {
      if (metricsRecorded.compareAndSet(false, true)) {
        metricsTracker.nioRequestProcessingTime.update(nioLayerMetrics.requestProcessingTime.get());
        metricsTracker.nioResponseProcessingTime.update(nioLayerMetrics.responseProcessingTime.get());
        metricsTracker.nioResponseWriteTime.update(nioLayerMetrics.responseWriteTime.get());

        metricsTracker.scRequestProcessingTime.update(scalingLayerMetrics.requestProcessingTime.get());
        metricsTracker.scRequestQueueingTime.update(scalingLayerMetrics.requestQueueingTime.get());
        metricsTracker.scResponseProcessingTime.update(scalingLayerMetrics.responseProcessingTime.get());
        metricsTracker.scResponseQueueingTime.update(scalingLayerMetrics.responseQueueingTime.get());

        metricsTracker.totalTime.update(totalTime.get());
      }
    } else {
      throw new IllegalStateException("Could not record metrics because there is no metrics tracker");
    }
  }

  public static void setDefaults(MetricRegistry metricRegistry) {
    defaultMetricsTracker = new RequestMetricsTracker(RestRequestMetrics.class, "Unknown", metricRegistry);
  }
}


