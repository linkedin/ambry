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
  public final Counter channelActiveTasksFailure;
  public final Counter missingResponseHandler;
  public final Counter idleConnectionClose;
  public final Counter unknownHttpObject;
  public final Counter malformedRequest;
  public final Counter duplicateRequest;
  public final Counter noRequest;
  public final Counter processorRequestCompleteTasksFailure;
  public final Counter nettyMessageProcessorExceptionCaught;
  public final Counter fallbackErrorSendingFailure;
  public final Counter requestFailure;
  public final Counter errorSendingFailure;
  public final Counter responseHandlerRequestCompleteTasksFailure;
  public final Counter channelWriteLockInterrupted;
  public final Counter responseMetadataWriteLockInterrupted;
  public final Counter channelCloseLockInterrupted;
  public final Counter badRequest;
  public final Counter internalServerError;
  public final Counter unknownException;
  public final Counter unknownRestServiceException;
  public final Counter channelWriteAfterClose;
  public final Counter deadResponseAccess;
  public final Counter channelWriteFutureAlreadyExists;
  public final Counter channelWriteFutureNotFound;
  public final Counter channelWriteFailure;
  public final Counter nettyServerStartupFailure;
  public final Counter nettyServerShutdownFailure;

  // Other
  public final Histogram nettyServerStartupTimeInMs;
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

    channelActiveTasksFailure =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "ChannelActiveTasksFailure"));
    missingResponseHandler =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "MissingResponseHandler"));
    idleConnectionClose =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "IdleConnectionClose"));
    unknownHttpObject = metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "UnknownHttpObject"));
    malformedRequest = metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "MalformedRequest"));
    duplicateRequest = metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "DuplicateRequest"));
    noRequest = metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "NoRequest"));
    nettyMessageProcessorExceptionCaught = metricRegistry
        .counter(MetricRegistry.name(NettyMessageProcessor.class, "NettyMessageProcessorExceptionCaught"));
    processorRequestCompleteTasksFailure = metricRegistry
        .counter(MetricRegistry.name(NettyMessageProcessor.class, "ProcessorRequestCompleteTasksFailure"));
    fallbackErrorSendingFailure =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "FallbackErrorSendingFailure"));
    requestFailure = metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "RequestFailure"));
    errorSendingFailure =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ErrorSendingFailure"));
    responseHandlerRequestCompleteTasksFailure = metricRegistry
        .counter(MetricRegistry.name(NettyResponseHandler.class, "ResponseHandlerRequestCompleteTasksFailure"));
    channelWriteLockInterrupted =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ChannelWriteLockInterrupted"));
    responseMetadataWriteLockInterrupted =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ResponseMetadataWriteLockInterrupted"));
    channelCloseLockInterrupted =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ChannelCloseLockInterrupted"));
    badRequest = metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "BadRequest"));
    internalServerError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "InternalServerError"));
    unknownException = metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "UnknownException"));
    unknownRestServiceException =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "UnknownRestServiceException"));
    channelWriteAfterClose =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ChannelWriteAfterClose"));
    deadResponseAccess = metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "DeadResponseAccess"));
    channelWriteFutureAlreadyExists =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "WriteFutureAlreadyExists"));
    channelWriteFutureNotFound =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "WriteFutureNotFound"));
    channelWriteFailure =
        metricRegistry.counter(MetricRegistry.name(NettyResponseHandler.class, "ChannelWriteFailure"));
    nettyServerStartupFailure =
        metricRegistry.counter(MetricRegistry.name(NettyServer.class, "NettyServerStartupFailure"));
    nettyServerShutdownFailure =
        metricRegistry.counter(MetricRegistry.name(NettyServer.class, "NettyServerShutdownFailure"));

    nettyServerStartupTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "NettyServerStartupTimeInMs"));
    nettyServerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "NettyServerShutdownTimeInMs"));
  }
}
