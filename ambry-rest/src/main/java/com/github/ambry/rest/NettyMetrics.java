package com.github.ambry.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;


/**
 * Netty specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by Netty to the provided {@link MetricRegistry}.
 */
class NettyMetrics {
  //errors
  public final Counter badRequestErrorCount;
  public final Counter channelActiveTasksFailureCount;
  public final Counter onRequestCompleteTasksFailure;
  public final Counter duplicateRequestErrorCount;
  public final Counter errorStateCount;
  public final Counter handleRequestFailureCount;
  public final Counter httpObjectConversionFailureCount;
  public final Counter internalServerErrorCount;
  public final Counter malformedRequestErrorCount;
  public final Counter noRequestErrorCount;
  public final Counter unknownExceptionCount;
  public final Counter unknownHttpObjectErrorCount;
  public final Counter unknownRestExceptionCount;
  public final Counter channelOperationAfterCloseErrorCount;
  public final Counter deadResponseAccess;

  public NettyMetrics(MetricRegistry metricRegistry) {
    //errors
    badRequestErrorCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "badRequestErrorCount"));
    channelActiveTasksFailureCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "channelActiveTasksFailureCount"));
    onRequestCompleteTasksFailure =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "onRequestCompleteTasksFailure"));
    duplicateRequestErrorCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "duplicateRequestErrorCount"));
    errorStateCount = metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "errorStateCount"));
    handleRequestFailureCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "handleRequestFailureCount"));
    httpObjectConversionFailureCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "httpObjectConversionFailureCount"));
    internalServerErrorCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "internalServerErrorCount"));
    malformedRequestErrorCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "malformedRequestErrorCount"));
    noRequestErrorCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "noRequestErrorCount"));
    unknownExceptionCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "unknownExceptionCount"));
    unknownHttpObjectErrorCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "unknownHttpObjectErrorCount"));
    unknownRestExceptionCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "unknownRestExceptionCount"));
    channelOperationAfterCloseErrorCount =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "channelOperationAfterCloseErrorCount"));
    deadResponseAccess = metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "deadResponseAccess"));
  }
}
