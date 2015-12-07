package com.github.ambry.rest;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * A metrics object that is provided as input to {@link RestRequestMetricsTracker#injectMetrics(RestRequestMetrics)}.
 * </p>
 * It is expected that each request type will have it's own instance of RestRequestMetrics and the same instance is
 * used to track all requests of that type.
 */
public class RestRequestMetrics {
  protected static final String NIO_REQUEST_PROCESSING_TIME_SUFFIX = "NioRequestProcessingTime";
  protected static final String NIO_RESPONSE_PROCESSING_TIME_SUFFIX = "NioResponseProcessingTime";

  protected static final String SC_REQUEST_PROCESSING_TIME_SUFFIX = "ScRequestProcessingTime";
  protected static final String SC_REQUEST_QUEUING_TIME_SUFFIX = "ScRequestQueuingTime";
  protected static final String SC_RESPONSE_PROCESSING_TIME_SUFFIX = "ScResponseProcessingTime";
  protected static final String SC_RESPONSE_QUEUING_TIME_SUFFIX = "ScResponseQueuingTime";

  protected static final String TOTAL_TIME_SUFFIX = "TotalTime";

  protected final Histogram nioRequestProcessingTime;
  protected final Histogram nioResponseProcessingTime;

  protected final Histogram scRequestProcessingTime;
  protected final Histogram scRequestQueuingTime;
  protected final Histogram scResponseProcessingTime;
  protected final Histogram scResponseQueuingTime;

  protected final Histogram totalTime;

  /**
   * Creates an instance of RestRequestMetrics for {@code requestType} and attaches all the metrics related to the
   * request to the given {@code ownerClass}. The metrics are also registered in the provided {@code metricRegistry}.
   * @param ownerClass the {@link Class} that is supposed to own the metrics created by this tracker.
   * @param requestType the type of request for which a tracker is being created.
   * @param metricRegistry the {@link MetricRegistry} to use to register the created metrics.
   */
  public RestRequestMetrics(Class ownerClass, String requestType, MetricRegistry metricRegistry) {
    if (ownerClass == null || requestType == null || metricRegistry == null) {
      throw new IllegalArgumentException(
          "Null arg(s) during instantiation. Owner class - [" + ownerClass + "]. Request type - [" + requestType
              + "]. Metric registry - [" + metricRegistry + "]");
    }

    nioRequestProcessingTime =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + NIO_REQUEST_PROCESSING_TIME_SUFFIX));
    nioResponseProcessingTime =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + NIO_RESPONSE_PROCESSING_TIME_SUFFIX));

    scRequestProcessingTime =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_REQUEST_PROCESSING_TIME_SUFFIX));
    scRequestQueuingTime =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_REQUEST_QUEUING_TIME_SUFFIX));
    scResponseProcessingTime =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_RESPONSE_PROCESSING_TIME_SUFFIX));
    scResponseQueuingTime =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_RESPONSE_QUEUING_TIME_SUFFIX));

    totalTime = metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + TOTAL_TIME_SUFFIX));
  }
}
