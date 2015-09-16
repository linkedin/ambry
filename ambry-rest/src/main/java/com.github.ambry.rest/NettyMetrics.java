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
  // NettyResponseChannel
  public final Meter requestCompletionRate;

  // Latencies
  // NettyResponseChannel
  public final Histogram channelWriteLatencyInMs;

  // Errors
  // NettyMessageProcessor
  public final Counter channelActiveTasksError;
  public final Counter missingResponseChannelError;
  public final Counter idleConnectionClose;
  public final Counter unknownHttpObjectError;
  public final Counter malformedRequestError;
  public final Counter duplicateRequestError;
  public final Counter noRequestError;
  public final Counter processorRequestCompleteTasksError;
  public final Counter processorExceptionCaught;
  public final Counter fallbackErrorSendingError;
  // NettyResponseChannel
  public final Counter requestHandlingError;
  public final Counter responseSendingError;
  public final Counter responseChannelRequestCompleteTasksError;
  public final Counter responseMetadataBuildingFailure;
  public final Counter badRequestError;
  public final Counter internalServerError;
  public final Counter unknownExceptionError;
  public final Counter unknownRestServiceExceptionError;
  public final Counter channelWriteAfterCloseError;
  public final Counter deadResponseAccessError;
  public final Counter channelWriteFutureAlreadyExistsError;
  public final Counter channelWriteFutureNotFoundError;
  public final Counter channelWriteError;
  // NettyServer
  public final Counter nettyServerStartError;
  public final Counter nettyServerShutdownError;

  // Other
  // NettyServer
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
        metricRegistry.meter(MetricRegistry.name(NettyResponseChannel.class, "RequestCompletionRate"));

    channelWriteLatencyInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyResponseChannel.class, "ChannelWriteLatencyInMs"));

    channelActiveTasksError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "ChannelActiveTasksError"));
    missingResponseChannelError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "MissingResponseChannelError"));
    idleConnectionClose =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "IdleConnectionClose"));
    unknownHttpObjectError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "UnknownHttpObjectError"));
    malformedRequestError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "MalformedRequestError"));
    duplicateRequestError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "DuplicateRequestError"));
    noRequestError = metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "NoRequestError"));
    processorExceptionCaught =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "ExceptionCaught"));
    processorRequestCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "OnRequestCompleteTasksError"));
    fallbackErrorSendingError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "FallbackErrorSendingError"));
    requestHandlingError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "RequestHandlingError"));
    responseSendingError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ErrorResponseSendingError"));
    responseChannelRequestCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "OnRequestCompleteTasksError"));
    responseMetadataBuildingFailure =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ResponseMetadataBuildingFailure"));
    badRequestError = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "BadRequestError"));
    internalServerError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "InternalServerError"));
    unknownExceptionError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "UnknownExceptionError"));
    unknownRestServiceExceptionError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "UnknownRestServiceExceptionError"));
    channelWriteAfterCloseError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ChannelWriteAfterClose"));
    deadResponseAccessError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "DeadResponseAccessError"));
    channelWriteFutureAlreadyExistsError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "WriteFutureAlreadyExistsError"));
    channelWriteFutureNotFoundError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "WriteFutureNotFoundError"));
    channelWriteError = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ChannelWriteError"));
    nettyServerStartError = metricRegistry.counter(MetricRegistry.name(NettyServer.class, "StartError"));
    nettyServerShutdownError = metricRegistry.counter(MetricRegistry.name(NettyServer.class, "ShutdownError"));

    nettyServerStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "StartTimeInMs"));
    nettyServerShutdownTimeInMs = metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "ShutdownTimeInMs"));
  }
}
