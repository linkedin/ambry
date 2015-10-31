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

    public final Histogram scRequestProcessingTime;
    public final Histogram scRequestQueuingTime;
    public final Histogram scResponseProcessingTime;
    public final Histogram scResponseQueuingTime;

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

      scRequestProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "ScRequestProcessingTime"));
      scRequestQueuingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "ScRequestQueuingTime"));
      scResponseProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "ScResponseProcessingTime"));
      scResponseQueuingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "ScResponseQueuingTime"));

      totalTime = metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + "TotalTime"));
    }
  }

  public class NioLayerMetrics {
    private final AtomicLong requestProcessingTime = new AtomicLong(0);
    private final AtomicLong responseProcessingTime = new AtomicLong(0);

    public long addToRequestProcessingTime(long delta) {
      addToTotalTime(delta);
      return requestProcessingTime.addAndGet(delta);
    }

    public long addToResponseProcessingTime(long delta) {
      addToTotalTime(delta);
      return responseProcessingTime.addAndGet(delta);
    }
  }

  public class ScalingLayerMetrics {
    private final AtomicLong requestProcessingTime = new AtomicLong(0);
    private final AtomicLong requestQueuingTime = new AtomicLong(0);
    private final AtomicLong responseProcessingTime = new AtomicLong(0);
    private final AtomicLong responseQueuingTime = new AtomicLong(0);

    public long addToRequestProcessingTime(long delta) {
      addToTotalTime(delta);
      return requestProcessingTime.addAndGet(delta);
    }

    public long addToRequestQueuingTime(long delta) {
      addToTotalTime(delta);
      return requestQueuingTime.addAndGet(delta);
    }

    public long addToResponseProcessingTime(long delta) {
      addToTotalTime(delta);
      return responseProcessingTime.addAndGet(delta);
    }

    public long addToResponseQueuingTime(long delta) {
      addToTotalTime(delta);
      return responseQueuingTime.addAndGet(delta);
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

        metricsTracker.scRequestProcessingTime.update(scalingLayerMetrics.requestProcessingTime.get());
        metricsTracker.scRequestQueuingTime.update(scalingLayerMetrics.requestQueuingTime.get());
        metricsTracker.scResponseProcessingTime.update(scalingLayerMetrics.responseProcessingTime.get());
        metricsTracker.scResponseQueuingTime.update(scalingLayerMetrics.responseQueuingTime.get());

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


