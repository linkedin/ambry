/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
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
  public final Meter multipartPostRequestRate;
  // NettyResponseChannel
  public final Meter bytesWriteRate;
  public final Meter requestCompletionRate;
  // PublicAccessLogRequestHandler
  public final Meter publicAccessLogRequestRate;
  // HealthCheckRequestHandler
  public final Meter healthCheckRequestRate;

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
  // PublicAccessLogRequestHandler
  public final Histogram publicAccessLogRequestProcessingTimeInMs;
  public final Histogram publicAccessLogResponseProcessingTimeInMs;
  // HealthCheckRequestHandler
  public final Histogram healthCheckRequestProcessingTimeInMs;
  public final Histogram healthCheckRequestRoundTripTimeInMs;

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
  public final Counter unauthorizedCount;
  public final Counter goneCount;
  public final Counter internalServerErrorCount;
  public final Counter notFoundCount;
  public final Counter forbiddenCount;
  public final Counter proxyAuthRequiredCount;
  public final Counter throwableCount;
  public final Counter unknownResponseStatusCount;
  // NettyServer
  public final Histogram nettyServerShutdownTimeInMs;
  public final Histogram nettyServerStartTimeInMs;
  // PublicAccessLogRequestHandler
  public final Counter publicAccessLogRequestDisconnectWhileInProgressCount;
  public final Counter publicAccessLogRequestCloseWhileRequestInProgressCount;
  // HealthCheckRequestHandler
  public final Counter healthCheckHandlerChannelCloseOnWriteCount;

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
    multipartPostRequestRate =
        metricRegistry.meter(MetricRegistry.name(NettyMessageProcessor.class, "MultipartPostRequestRate"));
    // NettyResponseChannel
    bytesWriteRate = metricRegistry.meter(MetricRegistry.name(NettyResponseChannel.class, "BytesWriteRate"));
    requestCompletionRate =
        metricRegistry.meter(MetricRegistry.name(NettyResponseChannel.class, "RequestCompletionRate"));
    publicAccessLogRequestRate =
        metricRegistry.meter(MetricRegistry.name(PublicAccessLogRequestHandler.class, "RequestArrivalRate"));
    healthCheckRequestRate = metricRegistry.meter(MetricRegistry.name(HealthCheckHandler.class, "RequestArrivalRate"));

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
    publicAccessLogRequestProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(PublicAccessLogRequestHandler.class, "RequestProcessingTimeInMs"));
    publicAccessLogResponseProcessingTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(PublicAccessLogRequestHandler.class, "ResponseProcessingTimeInMs"));
    healthCheckRequestProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HealthCheckHandler.class, "RequestProcessingTimeInMs"));
    healthCheckRequestRoundTripTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HealthCheckHandler.class, "RequestRoundTripTimeInMs"));

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
    publicAccessLogRequestDisconnectWhileInProgressCount = metricRegistry
        .counter(MetricRegistry.name(HealthCheckHandler.class, "ChannelDisconnectWhileRequestInProgressCount"));
    publicAccessLogRequestCloseWhileRequestInProgressCount = metricRegistry
        .counter(MetricRegistry.name(HealthCheckHandler.class, "ChannelCloseWhileRequestInProgressCount"));
    healthCheckHandlerChannelCloseOnWriteCount =
        metricRegistry.counter(MetricRegistry.name(HealthCheckHandler.class, "ChannelCloseOnWriteCount"));

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
    unauthorizedCount = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "UnauthorizedCount"));
    goneCount = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "GoneCount"));
    internalServerErrorCount =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "InternalServerErrorCount"));
    notFoundCount = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "NotFoundCount"));
    forbiddenCount = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ForbiddenCount"));
    proxyAuthRequiredCount =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ProxyAuthenticationRequiredCount"));
    throwableCount = metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "ThrowableCount"));
    unknownResponseStatusCount =
        metricRegistry.counter(MetricRegistry.name(NettyResponseChannel.class, "UnknownResponseStatusCount"));
    // NettyServer
    nettyServerShutdownTimeInMs = metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "ShutdownTimeInMs"));
    nettyServerStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(NettyServer.class, "StartTimeInMs"));
  }
}
