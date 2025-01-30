/*
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
 *
 */
package com.github.ambry.frontend.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.frontend.DeleteBlobHandler;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.rest.NoOpResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.WrappedRestRequest;
import com.github.ambry.router.ReadableStreamChannel;
import io.netty.buffer.ByteBuf;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.frontend.s3.S3Constants.*;
import static com.github.ambry.rest.RestUtils.*;

public class S3BatchDeleteHandler extends S3BaseHandler<ReadableStreamChannel> {
  private static DeleteBlobHandler deleteBlobHandler;
  private final XmlMapper xmlMapper;
  private FrontendMetrics metrics;
  private final Logger LOGGER = LoggerFactory.getLogger(S3BatchDeleteHandler.class);

  // Constructor
  public S3BatchDeleteHandler(DeleteBlobHandler deleteBlobHandler, FrontendMetrics frontendMetrics) {
    this.deleteBlobHandler = deleteBlobHandler;
    this.metrics = frontendMetrics;
    this.xmlMapper = new XmlMapper();
  }

  /**
   * Handles the S3 request and constructs the response.
   *
   * @param restRequest         the {@link RestRequest} that contains the request headers and body.
   * @param restResponseChannel the {@link RestResponseChannel} that contains the response headers and body.
   * @param callback            the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {

    // Create the channel to read the request body
    RetainingAsyncWritableChannel channel = new RetainingAsyncWritableChannel();

    // inject metrics for batch delete
    RestRequestMetrics requestMetrics =
        metrics.batchDeleteMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);

    // Pass the callback to handle the response
    restRequest.readInto(channel,
        parseRequestBodyAndDeleteCallback(channel, restRequest, deleteBlobHandler, restResponseChannel, callback));
  }

  /**
   * This method will read the request body, deserialize it, and trigger the batch delete process.
   */
  private Callback<Long> parseRequestBodyAndDeleteCallback(RetainingAsyncWritableChannel channel,
      RestRequest restRequest, DeleteBlobHandler deleteBlobHandler, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> finalCallback) {

    return buildCallback(metrics.s3BatchDeleteHandleMetrics, bytesRead -> {
      if (bytesRead == 0) {
        LOGGER.error("failed to read request into channel");
        throw new RestServiceException("bytesRead is empty", RestServiceErrorCode.BadRequest);
      }

      S3MessagePayload.DeleteResult response = new S3MessagePayload.DeleteResult();
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ReadableStreamChannel readableStreamChannel =
          new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
      S3MessagePayload.S3BatchDeleteObjects deleteRequest = deserializeRequest(channel);
      ConcurrentLinkedQueue<S3MessagePayload.S3ErrorObject> errors = new ConcurrentLinkedQueue<>();
      ConcurrentLinkedQueue<S3MessagePayload.S3DeletedObject> deleted = new ConcurrentLinkedQueue<>();
      RequestPath requestPath = (RequestPath) restRequest.getArgs().get(InternalKeys.REQUEST_PATH);
      if (deleteRequest.getObjects().size() > MAX_BATCH_DELETE_SIZE) {
        String batchSizeErrorMessage = "Exceeded Maximum Batch Size of ";
        LOGGER.error(batchSizeErrorMessage, MAX_BATCH_DELETE_SIZE);
        throw new RestServiceException(batchSizeErrorMessage, RestServiceErrorCode.BadRequest);
      }

      List<CompletableFuture<Void>> deleteFutures = new ArrayList<>();
      for (S3MessagePayload.S3BatchDeleteKey object : deleteRequest.getObjects()) {
        String singleDeletePath = requestPath.getPathAfterPrefixes() + "/" + object.getKey();
        RequestPath newRequestPath =
            RequestPath.parse(singleDeletePath, restRequest.getArgs(), new ArrayList<>(), requestPath.getClusterName());
        WrappedRestRequest singleDeleteRequest = new WrappedRestRequest(restRequest);
        singleDeleteRequest.setArg(InternalKeys.REQUEST_PATH, newRequestPath);
        NoOpResponseChannel noOpResponseChannel = new NoOpResponseChannel();
        CompletableFuture<Void> future = new CompletableFuture<>();

        // Handle the delete operation using the deleteBlobHandler
        deleteBlobHandler.handle(singleDeleteRequest, noOpResponseChannel, (result, exception) -> {
          // Call our custom onDeleteCompletion to track success/failure
          if (exception == null) {
            deleted.add(new S3MessagePayload.S3DeletedObject(object.getKey()));
          }
          else if (exception instanceof RestServiceException) {
            RestServiceException restServiceException = (RestServiceException) exception;
            errors.add((new S3MessagePayload.S3ErrorObject(object.getKey(), restServiceException.getErrorCode().toString())));
          }
          else {
            errors.add((new S3MessagePayload.S3ErrorObject(object.getKey(), RestServiceErrorCode.InternalServerError.toString())));
          }
          future.complete(null);
        });
        deleteFutures.add(future);
      }

      CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
          .whenComplete((result, exception) -> {
            try {
              // Add XML declaration at the top
              xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true);
              response.setDeleted(new ArrayList<>(deleted));
              response.setErrors(new ArrayList<>(errors));
              xmlMapper.writeValue(outputStream, response);
              ReadableStreamChannel finalreadableStreamChannel =
                  new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
              restResponseChannel.setHeader(Headers.CONTENT_LENGTH, readableStreamChannel.getSize());
              restResponseChannel.setHeader(Headers.CONTENT_TYPE, XML_CONTENT_TYPE);
              restResponseChannel.setStatus(ResponseStatus.Ok);
              finalCallback.onCompletion(finalreadableStreamChannel, null);
            } catch (IOException | RestServiceException e) {
              LOGGER.error("Failed to complete ", e);
              finalCallback.onCompletion(null, e);
            }
          });
    }, restRequest.getUri(), LOGGER, finalCallback);
  }

  /**
   * Deserialize the request body into an S3BatchDeleteObjects.
   */
  public S3MessagePayload.S3BatchDeleteObjects deserializeRequest(RetainingAsyncWritableChannel channel)
      throws RestServiceException {
    try {
      ByteBuf byteBuffer = channel.consumeContentAsByteBuf();
      byte[] byteArray = new byte[byteBuffer.readableBytes()];
      byteBuffer.readBytes(byteArray);
      return new XmlMapper().readValue(byteArray, S3MessagePayload.S3BatchDeleteObjects.class);
    } catch (Exception e) {
      throw new RestServiceException("failed to deserialize", e, RestServiceErrorCode.BadRequest);
    }
  }
}
