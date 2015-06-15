package com.github.ambry.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles  metrics required by RestServer
 */
public class RestServerMetrics {
  // RestMessageHandler
  // errors
  // rest request delegator
  public final Counter delegatorHandlerSelectionErrorCount;

  // rest message handler
  public final Counter handlerQueueOfferTookTooLongErrorCount;
  public final Counter handlerQueueOfferInterruptedErrorCount;
  public final Counter handlerQueueTakeInterruptedErrorCount;
  public final Counter handlerMessageProcessingFailureErrorCount;
  public final Counter handlerResponseHandlerMissingErrorCount;
  public final Counter handlerRestObjectMissingErrorCount;
  public final Counter handlerRestRequestMissingErrorCount;
  public final Counter handlerUnknownRestMethodErrorCount;

  protected Logger logger = LoggerFactory.getLogger(getClass());

  public RestServerMetrics(MetricRegistry metricRegistry) {
    delegatorHandlerSelectionErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestRequestDelegator.class, "handlerSelectionErrorCount"));

    handlerQueueOfferTookTooLongErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "handlerQueueOfferTookTooLongErrorCount"));
    handlerQueueOfferInterruptedErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "handlerQueueOfferInterruptedErrorCount"));
    handlerQueueTakeInterruptedErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "handlerQueueTakeInterruptedErrorCount"));
    handlerMessageProcessingFailureErrorCount = metricRegistry
        .counter(MetricRegistry.name(RestMessageHandler.class, "handlerMessageProcessingFailureErrorCount"));
    handlerResponseHandlerMissingErrorCount = metricRegistry
        .counter(MetricRegistry.name(RestMessageHandler.class, "handlerResponseHandlerMissingErrorCount"));
    handlerRestObjectMissingErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "handlerRestObjectMissingErrorCount"));
    handlerRestRequestMissingErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "handlerRestRequestMissingErrorCount"));
    handlerUnknownRestMethodErrorCount =
        metricRegistry.counter(MetricRegistry.name(RestMessageHandler.class, "handlerUnknownRestMethodErrorCount"));
  }
}
