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
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.frontend.s3.S3Constants.*;
import static com.github.ambry.rest.RestUtils.*;

public class S3BatchDeleteHandler extends S3BaseHandler<ReadableStreamChannel> {
  private static DeleteBlobHandler deleteBlobHandler;
  private final XmlMapper xmlMapper;
  private FrontendMetrics metrics;
  private static final Logger logger = LoggerFactory.getLogger(S3BatchDeleteHandler.class);

  // Constructor
  public S3BatchDeleteHandler(DeleteBlobHandler deleteBlobHandler, FrontendMetrics frontendMetrics) {
    this.deleteBlobHandler = deleteBlobHandler;
    this.metrics = frontendMetrics;
    this.xmlMapper = new XmlMapper();
    this.xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true);
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
      S3MessagePayload.S3BatchDeleteObjects deleteRequest = null;
      try {
        // check for empty message
        if (bytesRead == 0) {
          logger.info("bytesRead is empty");
          throw new RestServiceException("bytesRead is empty", RestServiceErrorCode.BadRequest);
        }

        // ensure request body format is correct
        deleteRequest = deserializeRequest(channel);
        if (deleteRequest.getObjects() == null) {
          logger.info("s3batchdelete request size needs to be at least 1");
          String batchSizeEqualsZero = "Request size needs to be at least 1";
          throw new RestServiceException(batchSizeEqualsZero, RestServiceErrorCode.BadRequest);
        }

        // validate the request for size
        if (deleteRequest.getObjects().size() > MAX_BATCH_DELETE_SIZE) {
          logger.info("exceeded max batch delete size " + MAX_BATCH_DELETE_SIZE);
          String batchSizeErrorMessage = "Exceeded Maximum Batch Size of " + MAX_BATCH_DELETE_SIZE;
          throw new RestServiceException(batchSizeErrorMessage, RestServiceErrorCode.BadRequest);
        }
      } catch (Exception e) {
        S3MessagePayload.Error response = new S3MessagePayload.Error();
        response.setCode(ERR_MALFORMED_REQUEST_BODY_CODE);
        response.setMessage(ERR_MALFORMED_REQUEST_BODY_MESSAGE);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        xmlMapper.writeValue(outputStream, response);
        ReadableStreamChannel readableStreamChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
        restResponseChannel.setHeader(Headers.CONTENT_LENGTH, readableStreamChannel.getSize());
        restResponseChannel.setHeader(Headers.CONTENT_TYPE, XML_CONTENT_TYPE);
        restResponseChannel.setStatus(ResponseStatus.Ok);
        finalCallback.onCompletion(readableStreamChannel, null);
        return;
      }

      RequestPath requestPath = (RequestPath) restRequest.getArgs().get(InternalKeys.REQUEST_PATH);

      // create objects for processing the request
      ConcurrentLinkedQueue<S3MessagePayload.S3ErrorObject> errors = new ConcurrentLinkedQueue<>();
      ConcurrentLinkedQueue<S3MessagePayload.S3DeletedObject> deleted = new ConcurrentLinkedQueue<>();
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
          } else if (exception instanceof RestServiceException) {
            RestServiceException restServiceException = (RestServiceException) exception;
            errors.add((new S3MessagePayload.S3ErrorObject(object.getKey(), restServiceException.getErrorCode().toString())));
          } else {
            errors.add((new S3MessagePayload.S3ErrorObject(object.getKey(), RestServiceErrorCode.InternalServerError.toString())));
          }
          future.complete(null);
        });
        deleteFutures.add(future);
      }

      CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0])).whenComplete((result, exception) -> {
        try {
          // construct and serialize response
          S3MessagePayload.DeleteResult response = new S3MessagePayload.DeleteResult();
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
          response.setDeleted(new ArrayList<>(deleted));
          response.setErrors(new ArrayList<>(errors));
          xmlMapper.writeValue(outputStream, response);
          ReadableStreamChannel readableStreamChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
          restResponseChannel.setHeader(Headers.CONTENT_LENGTH, readableStreamChannel.getSize());
          restResponseChannel.setHeader(Headers.CONTENT_TYPE, XML_CONTENT_TYPE);
          restResponseChannel.setStatus(ResponseStatus.Ok);
          finalCallback.onCompletion(readableStreamChannel, null);
        } catch (IOException | RestServiceException e) {
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
      logger.info("s3batchdelete failed to deserialize request");
      throw new RestServiceException("failed to deserialize", e, RestServiceErrorCode.BadRequest);
    }
  }
}
