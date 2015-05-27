package com.github.ambry.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles common metrics required by all implementations of servers
 */
public abstract class ServerMetrics {
  // RestMessageHandler
  // errors
  public final Counter handlerQueueOfferTookTooLongErrorCount;
  public final Counter handlerQueueOfferInterruptedErrorCount;
  public final Counter handlerQueueTakeInterruptedErrorCount;
  public final Counter handlerMessageProcessingFailureErrorCount;
  public final Counter handlerResponseHandlerMissingErrorCount;
  public final Counter handlerRestObjectMissingErrorCount;
  public final Counter handlerRestRequestMissingErrorCount;
  public final Counter handlerUnknownHttpMethodErrorCount;

  protected Logger logger = LoggerFactory.getLogger(getClass());

  protected ServerMetrics(MetricRegistry metricRegistry) {
    handlerQueueOfferTookTooLongErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "offerTookTooLongErrorCount"));
    handlerQueueOfferInterruptedErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "offerInterruptedErrorCount"));
    handlerQueueTakeInterruptedErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "queueTakeInterruptedErrorCount"));
    handlerMessageProcessingFailureErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "messageProcessingFailureErrorCount"));
    handlerResponseHandlerMissingErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "responseHandlerMissingErrorCount"));
    handlerRestObjectMissingErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "restObjectMissingErrorCount"));
    handlerRestRequestMissingErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "restRequestMissingErrorCount"));
    handlerUnknownHttpMethodErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "unknownHttpMethodErrorCount"));
  }
}
