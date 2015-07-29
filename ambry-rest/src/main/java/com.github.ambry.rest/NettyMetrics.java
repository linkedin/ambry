package com.github.ambry.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/**
 * Netty specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by Netty to the provided {@link MetricRegistry}.
 */
class NettyMetrics {

  // Rates
  // NettyMessageProcessor
  public final Meter requestArrivalRate;
  public final Meter processorCreationRate;
  public final Meter channelCreationRate;
  public final Meter channelDestructionRate;
  public final Meter httpObjectArrivalRate;
  public final Meter requestCompletionRate;

  // Latencies
  public final Histogram channelWriteLatencyInMs;

  // Errors
  public final Counter channelActiveTasksError;
  public final Counter missingResponseHandlerError;
  public final Counter idleConnectionClose;
  public final Counter unknownHttpObjectError;
  public final Counter malformedRequestError;
  public final Counter duplicateRequestError;
  public final Counter noRequestError;
  public final Counter processorRequestCompleteTasksError;
  public final Counter nettyMessageProcessorExceptionCaught;
  public final Counter fallbackErrorSendingError;
  public final Counter requestHandlingError;
  public final Counter responseSendingError;
  public final Counter responseHandlerRequestCompleteTasksError;
  public final Counter channelWriteLockInterruptedError;
  public final Counter responseMetadataWriteLockInterruptedError;
  public final Counter channelCloseLockInterruptedError;
  public final Counter badRequestError;
  public final Counter internalServerError;
  public final Counter unknownExceptionError;
  public final Counter unknownRestServiceExceptionError;
  public final Counter channelWriteAfterCloseError;
  public final Counter deadResponseAccessError;
  public final Counter channelWriteFutureAlreadyExistsError;
  public final Counter channelWriteFutureNotFoundError;
  public final Counter channelWriteError;
  public final Counter nettyServerStartError;
  public final Counter nettyServerShutdownError;

  // Other
  public final Histogram nettyServerStartTimeInMs;
  public final Histogram nettyServerShutdownTimeInMs;

  public NettyMetrics(MetricRegistry metricRegistry) {

    requestArrivalRate = metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "RequestArrivalRate"));
    processorCreationRate =
        metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "ProcessorCreationRate"));
    channelCreationRate = metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "ChannelCreationRate"));
    channelDestructionRate =
        metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "ChannelDestructionRate"));
    httpObjectArrivalRate =
        metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "HttpObjectArrivalRate"));
    requestCompletionRate =
        metricRegistry.meter(MetricRegistry.name(NettyResponseHandler.class, "RequestCompletionRate"));

    channelWriteLatencyInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyResponseHandler.class, "ChannelWriteLatencyInMs"));

    channelActiveTasksError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "ChannelActiveTasksError"));
    missingResponseHandlerError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "MissingResponseHandlerError"));
    idleConnectionClose =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "IdleConnectionClose"));
    unknownHttpObjectError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "UnknownHttpObjectError"));
    malformedRequestError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "MalformedRequestError"));
    duplicateRequestError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "DuplicateRequestError"));
    noRequestError = metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "NoRequestError"));
    nettyMessageProcessorExceptionCaught = metricRegistry
        .counter(MetricRegistry.name(NettyMessageProcessor.class, "NettyMessageProcessorExceptionCaught"));
    processorRequestCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "OnRequestCompleteTasksError"));
    fallbackErrorSendingError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "FallbackErrorSendingError"));
    requestHandlingError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "RequestHandlingError"));
    responseSendingError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ErrorResponseSendingError"));
    responseHandlerRequestCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "OnRequestCompleteTasksError"));
    channelWriteLockInterruptedError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ChannelWriteLockInterruptedError"));
    responseMetadataWriteLockInterruptedError = metricRegistry
        .counter(MetricRegistry.name(NettyResponseHandler.class, "ResponseMetadataWriteLockInterruptedError"));
    channelCloseLockInterruptedError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ChannelCloseLockInterruptedError"));
    badRequestError = metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "BadRequestError"));
    internalServerError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "InternalServerError"));
    unknownExceptionError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "UnknownExceptionError"));
    unknownRestServiceExceptionError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "UnknownRestServiceExceptionError"));
    channelWriteAfterCloseError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ChannelWriteAfterClose"));
    deadResponseAccessError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "DeadResponseAccessError"));
    channelWriteFutureAlreadyExistsError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "WriteFutureAlreadyExistsError"));
    channelWriteFutureNotFoundError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "WriteFutureNotFoundError"));
    channelWriteError = metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ChannelWriteError"));
    nettyServerStartError = metricRegistry.counter(MetricRegistry.name(NettyServer.class, "StartError"));
    nettyServerShutdownError = metricRegistry.counter(MetricRegistry.name(NettyServer.class, "ShutdownError"));

    nettyServerStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "StartTimeInMs"));
    nettyServerShutdownTimeInMs = metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "ShutdownTimeInMs"));
  }
}
