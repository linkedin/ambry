package com.github.ambry.frontend.s3;

import com.azure.core.models.ResponseError;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;

public class S3BatchDeleteHandler extends S3BaseHandler<ReadableStreamChannel> {
  private static DeleteBlobHandler deleteBlobHandler;
  private final XmlMapper xmlMapper;
  private ArrayList<String> deleted = new ArrayList<>();
  private ArrayList<S3MessagePayload.S3DeleteError> errors = new ArrayList<>();
  private FrontendMetrics metrics;
  private final int maxBatchSize = 1000;
  private S3MessagePayload.S3BatchDeleteObjects deleteRequest;
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
        metrics.deleteBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
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

      S3MessagePayload.S3BatchDeleteResponse resp = new S3MessagePayload.S3BatchDeleteResponse();
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ReadableStreamChannel readableStreamChannel =
          new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));

      // Deserialize the request body into a S3BatchDeleteObjects
      deleteRequest = deserializeRequest(channel);

      // Extract request path
      RequestPath requestPath = (RequestPath) restRequest.getArgs().get(InternalKeys.REQUEST_PATH);
      List<CompletableFuture<Void>> deleteFutures = new ArrayList<>();

      if (deleteRequest.getObjects().size() > maxBatchSize) {
        LOGGER.error("Exceeded batch size");
        throw new RestServiceException("Batch Size Exceeded", RestServiceErrorCode.BadRequest);
      }
      for (S3MessagePayload.S3BatchDeleteKey object : deleteRequest.getObjects()) {

        // Construct the delete path for each object
        String singleDeletePath = requestPath.getPathAfterPrefixes() + "/" + object.getKey();

        // Create a new RequestPath for the delete operation
        List<String> emptyList = new ArrayList<>();
        RequestPath newRequestPath =
            RequestPath.parse(singleDeletePath, restRequest.getArgs(), emptyList, requestPath.getClusterName());
        WrappedRestRequest singleDeleteRequest = new WrappedRestRequest(restRequest);
        singleDeleteRequest.setArg(InternalKeys.REQUEST_PATH, newRequestPath);

        NoOpResponseChannel noOpResponseChannel = new NoOpResponseChannel();
        CompletableFuture<Void> future = new CompletableFuture<>();

        // Handle the delete operation using the deleteBlobHandler
        deleteBlobHandler.handle(singleDeleteRequest, noOpResponseChannel, (result, exception) -> {
          // Call our custom onDeleteCompletion to track success/failure
          onDeleteCompletion(exception, object.getKey());
          future.complete(null);
        });
        deleteFutures.add(future);
      }

      CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
          .whenComplete((result, exception) -> {
            try {
              resp.setDeletedKeys(deleted);
              resp.setErrors(errors);
              xmlMapper.writeValue(outputStream, resp);
              ReadableStreamChannel finalreadableStreamChannel =
                  new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
              restResponseChannel.setHeader(Headers.CONTENT_LENGTH, readableStreamChannel.getSize());
              restResponseChannel.setHeader(Headers.CONTENT_TYPE, XML_CONTENT_TYPE);
              restResponseChannel.setStatus(ResponseStatus.Ok);
              finalCallback.onCompletion(finalreadableStreamChannel, null);
            } catch (IOException | RestServiceException e) {
              LOGGER.error("Failed to complete", e);
              finalCallback.onCompletion(null, e);
            }
          });
    }, restRequest.getUri(), LOGGER, finalCallback);
  }

  /**
   * This method is used to update the lists of deleted and errored objects.
   * @param exception whether the delete was successful or not
   * @param key the object key
   */
  public void onDeleteCompletion(Exception exception, String key) {
    // TODO: add error reasoning
    if (exception == null) {
      deleted.add(key);
    }
    else {
      if (exception instanceof RestServiceException) {
        RestServiceException restServiceException = (RestServiceException) exception;
        errors.add((new S3MessagePayload.S3DeleteError(key, ResponseStatus.getResponseStatus(restServiceException.getErrorCode()).getStatusCode())));
      } else {
        errors.add((new S3MessagePayload.S3DeleteError(key, ResponseStatus.getResponseStatus(RestServiceErrorCode.InternalServerError).getStatusCode())));
      }
    }
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
      throw new RestServiceException("failed to deserialize",e, RestServiceErrorCode.BadRequest);
    }
  }
}