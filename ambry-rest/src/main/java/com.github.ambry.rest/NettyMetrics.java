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
  public final Meter bytesReadRate;
  public final Meter channelCreationRate;
  public final Meter channelDestructionRate;
  public final Meter requestArrivalRate;
  // NettyResponseChannel
  public final Meter bytesWriteRate;
  public final Meter requestCompletionRate;

  // Latencies
  // NettyMessageProcessor
  public final Histogram requestChunkProcessingTimeInMs;
  // NettyResponseChannel
  public final Histogram channelWriteFailureProcessingTimeInMs;
  public final Histogram chunkDispenseTimeInMs;
  public final Histogram channelWriteTimeInMs;
  public final Histogram chunkResolutionProcessingTimeInMs;
  public final Histogram errorResponseProcessingTimeInMs;
  public final Histogram headerSetTimeInMs;
  public final Histogram responseFinishProcessingTimeInMs;
  public final Histogram responseMetadataProcessingTimeInMs;
  public final Histogram writeProcessingTimeInMs;

  // Errors
  // NettyMessageProcessor
  public final Counter contentAdditionError;
  public final Counter duplicateRequestError;
  public final Counter exceptionCaughtTasksError;
  public final Counter malformedRequestError;
  public final Counter missingResponseChannelError;
  public final Counter noRequestError;
  public final Counter unknownHttpObjectError;
  // NettyMultipartRequest
  public final Counter multipartRequestAlreadyClosedError;
  public final Counter multipartRequestDecodeError;
  public final Counter multipartRequestSizeMismatchError;
  public final Counter repeatedPartsError;
  public final Counter unsupportedPartError;
  // NettyRequest
  public final Counter requestAlreadyClosedError;
  public final Counter unsupportedHttpMethodError;
  // NettyResponseChannel
  public final Counter channelWriteError;
  public final Counter deadResponseAccessError;
  public final Counter responseCompleteTasksError;
  // NettyServer
  public final Counter nettyServerShutdownError;
  public final Counter nettyServerStartError;

  // Other
  // NettyRequest
  public final Counter contentCopyCount;
  // NettyMessageProcessor
  public final Histogram channelReadIntervalInMs;
  public final Counter idleConnectionCloseCount;
  public final Counter processorExceptionCaughtCount;
  // NettyResponseChannel
  public final Counter badRequestCount;
  public final Counter goneCount;
  public final Counter internalServerErrorCount;
  public final Counter notFoundCount;
  public final Counter throwableCount;
  public final Counter unknownResponseStatusCount;
  // NettyServer
  public final Histogram nettyServerShutdownTimeInMs;
  public final Histogram nettyServerStartTimeInMs;

  /**
   * Creates an instance of NettyMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public NettyMetrics(MetricRegistry metricRegistry) {
    // Rates
    // NettyMessageProcessor
    bytesReadRate = metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "BytesReadRate"));
    channelCreationRate = metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "ChannelCreationRate"));
    channelDestructionRate =
        metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "ChannelDestructionRate"));
    requestArrivalRate = metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "RequestArrivalRate"));
    // NettyResponseChannel
    bytesWriteRate = metricRegistry.meter(MetricRegistry.name(NettyResponseChannel.class, "BytesWriteRate"));
    requestCompletionRate =
        metricRegistry.meter(MetricRegistry.name(NettyResponseChannel.class, "RequestCompletionRate"));

    // Latencies
    // NettyMessageProcessor
    requestChunkProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyMessageProcessor.class, "RequestChunkProcessingTimeInMs"));
    // NettyResponseChannel
    channelWriteFailureProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(NettyResponseChannel.class, "ChannelWriteFailureProcessingTimeInMs"));
    channelWriteTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyResponseChannel.class, "ChannelWriteTimeInMs"));
    chunkDispenseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyResponseChannel.class, "ChunkDispenseTimeInMs"));
    chunkResolutionProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyResponseChannel.class, "ChunkResolutionProcessingTimeInMs"));
    errorResponseProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyResponseChannel.class, "ErrorResponseProcessingTimeInMs"));
    headerSetTimeInMs = metricRegistry.histogram(MetricRegistry.name(NettyResponseChannel.class, "HeaderSetTimeInMs"));
    responseFinishProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyResponseChannel.class, "ResponseFinishProcessingTimeInMs"));
    responseMetadataProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyResponseChannel.class, "ResponseMetadataProcessingTimeInMs"));
    writeProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyResponseChannel.class, "WriteProcessingTimeInMs"));

    // Errors
    // NettyMessageProcessor
    contentAdditionError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "ContentAdditionError"));
    duplicateRequestError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "DuplicateRequestError"));
    exceptionCaughtTasksError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "ExceptionCaughtTasksError"));
    malformedRequestError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "MalformedRequestError"));
    missingResponseChannelError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "MissingResponseChannelError"));
    noRequestError = metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "NoRequestError"));
    unknownHttpObjectError =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "UnknownHttpObjectError"));
    // NettyMultipartRequest
    multipartRequestAlreadyClosedError =
        metricRegistry.counter(MetricRegistry.name(NettyMultipartRequest.class, "AlreadyClosedError"));
    multipartRequestDecodeError =
        metricRegistry.counter(MetricRegistry.name(NettyMultipartRequest.class, "DecodeError"));
    multipartRequestSizeMismatchError =
        metricRegistry.counter(MetricRegistry.name(NettyMultipartRequest.class, "SizeMismatchError"));
    repeatedPartsError = metricRegistry.counter(MetricRegistry.name(NettyMultipartRequest.class, "RepeatedPartsError"));
    unsupportedPartError =
        metricRegistry.counter(MetricRegistry.name(NettyMultipartRequest.class, "UnsupportedPartError"));
    // NettyRequest
    requestAlreadyClosedError = metricRegistry.counter(MetricRegistry.name(NettyRequest.class, "AlreadyClosedError"));
    unsupportedHttpMethodError =
        metricRegistry.counter(MetricRegistry.name(NettyRequest.class, "UnsupportedHttpMethodError"));
    // NettyResponseChannel
    channelWriteError = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ChannelWriteError"));
    deadResponseAccessError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "DeadResponseAccessError"));
    responseCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ResponseCompleteTasksError"));
    // NettyServer
    nettyServerShutdownError = metricRegistry.counter(MetricRegistry.name(NettyServer.class, "ShutdownError"));
    nettyServerStartError = metricRegistry.counter(MetricRegistry.name(NettyServer.class, "StartError"));

    // Other
    // NettyMessageProcessor
    channelReadIntervalInMs =
        metricRegistry.histogram(MetricRegistry.name(NettyMessageProcessor.class, "ChannelReadIntervalInMs"));
    idleConnectionCloseCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "IdleConnectionCloseCount"));
    processorExceptionCaughtCount =
        metricRegistry.counter(MetricRegistry.name(NettyMessageProcessor.class, "ExceptionCaughtCount"));
    // NettyRequest
    contentCopyCount = metricRegistry.counter(MetricRegistry.name(NettyRequest.class, "ContentCopyCount"));
    // NettyResponseChannel
    badRequestCount = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "BadRequestCount"));
    goneCount = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "GoneCount"));
    internalServerErrorCount =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "InternalServerErrorCount"));
    notFoundCount = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "NotFoundCount"));
    throwableCount = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ThrowableCount"));
    unknownResponseStatusCount =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "UnknownResponseStatusCount"));
    // NettyServer
    nettyServerShutdownTimeInMs = metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "ShutdownTimeInMs"));
    nettyServerStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "StartTimeInMs"));
  }
}
