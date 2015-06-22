package com.github.ambry.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;


/**
 * {@link RestServer} and Rest infrastructure ({@link com.github.ambry.restservice.RestRequestHandlerController},
 * {@link com.github.ambry.restservice.RestRequestHandler}) specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by Rest infrastructure to the provided {@link MetricRegistry}.
 */
class RestServerMetrics {
  // TODO: expansion and documentation in another commit
  // errors
  // RequestHandlerController
  public final Counter handlerControllerHandlerSelectionErrorCount;

  // AsyncRequestHandler
  public final Counter handlerQueueOfferTookTooLongErrorCount;
  public final Counter handlerQueueOfferInterruptedErrorCount;
  public final Counter handlerQueueTakeInterruptedErrorCount;
  public final Counter handlerRequestInfoProcessingFailureErrorCount;
  public final Counter handlerRestRequestInfoMissingErrorCount;
  public final Counter handlerResponseHandlerMissingErrorCount;
  public final Counter handlerRestObjectMissingErrorCount;
  public final Counter handlerRestRequestMetadataMissingErrorCount;
  public final Counter handlerUnknownRestMethodErrorCount;

  public RestServerMetrics(MetricRegistry metricRegistry) {
    handlerControllerHandlerSelectionErrorCount =
        metricRegistry.counter(MetricRegistry.name(RequestHandlerController.class, "handlerSelectionErrorCount"));

    handlerQueueOfferTookTooLongErrorCount = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "handlerQueueOfferTookTooLongErrorCount"));
    handlerQueueOfferInterruptedErrorCount = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "handlerQueueOfferInterruptedErrorCount"));
    handlerQueueTakeInterruptedErrorCount =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "handlerQueueTakeInterruptedErrorCount"));
    handlerRequestInfoProcessingFailureErrorCount = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "handlerRequestInfoProcessingFailureErrorCount"));
    handlerRestRequestInfoMissingErrorCount = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "handlerRestRequestInfoMissingErrorCount"));
    handlerResponseHandlerMissingErrorCount = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "handlerResponseHandlerMissingErrorCount"));
    handlerRestObjectMissingErrorCount =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "handlerRestObjectMissingErrorCount"));
    handlerRestRequestMetadataMissingErrorCount = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "handlerRestRequestMetadataMissingErrorCount"));
    handlerUnknownRestMethodErrorCount =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "handlerUnknownRestMethodErrorCount"));
  }
}
