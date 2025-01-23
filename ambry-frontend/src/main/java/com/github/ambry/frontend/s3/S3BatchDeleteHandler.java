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
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.WrappedRestRequest;
import com.github.ambry.router.ReadableStreamChannel;
import io.netty.buffer.ByteBuf;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;

public class S3BatchDeleteHandler extends S3BaseHandler<ReadableStreamChannel> {
  private static DeleteBlobHandler deleteBlobHandler;
  private ArrayList<String> deleted = new ArrayList<>();
  private ArrayList<String> errors = new ArrayList<>();
  private boolean failedRequest;
  private FrontendMetrics frontendMetrics;

  // Constructor
  public S3BatchDeleteHandler(DeleteBlobHandler deleteBlobHandler, FrontendMetrics frontendMetrics) {
    this.deleteBlobHandler = deleteBlobHandler;
    this.failedRequest = false;
    this.frontendMetrics = frontendMetrics;
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

    // Pass the callback to handle the response
    restRequest.readInto(channel, parseRequestBodyAndDeleteCallback(channel, restRequest, deleteBlobHandler, restResponseChannel, callback));
  }

  /**
   * This method will read the request body, deserialize it, and trigger the batch delete process.
   */
  private Callback<Long> parseRequestBodyAndDeleteCallback(RetainingAsyncWritableChannel channel,
      RestRequest restRequest, DeleteBlobHandler deleteBlobHandler, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {

    return buildCallback(frontendMetrics.deleteBlobRouterMetrics, bytesRead -> {
      if (bytesRead == null) {
        // Handle the case where bytesRead is null
        throw new IllegalStateException("bytesRead is null");
      }
      // Deserialize the request body into a S3BatchDeleteObjects
      S3MessagePayload.S3BatchDeleteObjects deleteRequest = deserializeRequest(channel);

      if (deleteRequest != null) {
        // Extract request path
        RequestPath requestPath = (RequestPath) restRequest.getArgs().get(RestUtils.InternalKeys.REQUEST_PATH);
        Map<String, CompletableFuture<Void>> deleteFutures = new HashMap<>();

        // Process each delete key in the batch
        for (S3MessagePayload.S3BatchDeleteKeys object : deleteRequest.getObjects()) {

          // Construct the delete path for each object
          String singleDeletePath = requestPath.getPathAfterPrefixes() + "/" + object.getKey();

          // Create a new RequestPath for the delete operation
          List<String> emptyList = new ArrayList<>();
          RequestPath newRequestPath =
              RequestPath.parse(singleDeletePath, restRequest.getArgs(), emptyList, requestPath.getClusterName());
          WrappedRestRequest singleDeleteRequest = new WrappedRestRequest(restRequest);
          singleDeleteRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH, newRequestPath);


          NoOpResponseChannel noOpResponseChannel = new NoOpResponseChannel();
          CompletableFuture<Void> future = new CompletableFuture<>();

          // Handle the delete operation using the deleteBlobHandler
          deleteBlobHandler.handle(singleDeleteRequest, noOpResponseChannel, new Callback<Void>() {
            @Override
            public void onCompletion(Void result, Exception exception) {
              // Call our custom onDeleteCompletion to track success/failure
              Map<String, Object> args = restRequest.getArgs();
              restRequest.getArgs().remove("ambry-internal-key-target-account");
              restRequest.getArgs().remove("ambry-internal-key-keep-alive-on-error-hint");
              restRequest.getArgs().remove("ambry-internal-key-target-container");
              boolean success = exception == null;
              onDeleteCompletion(success, object.getKey());
              future.complete(null);
            }
          });
          deleteFutures.put(object.getKey(), future);
        }

        // Wait for all delete operations to complete
        CompletableFuture<Void> allDeletes = CompletableFuture.allOf(deleteFutures.values().toArray(new CompletableFuture[0]));
        try {
          // Block until all futures complete
          allDeletes.get();  // This will throw an exception if any future fails
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }

        // Check if any delete request failed
        if (failedRequest) {
          restResponseChannel.setStatus(ResponseStatus.BadRequest);
          callback.onCompletion(null, new RestServiceException("Failed to execute S3BatchDelete", RestServiceErrorCode.BadRequest));
        } else {
          // Successful response handling
          restResponseChannel.setStatus(ResponseStatus.Ok);
          try {
            XmlMapper xmlMapper = new XmlMapper();
            S3MessagePayload.S3BatchDeleteResponse resp = new S3MessagePayload.S3BatchDeleteResponse();
            resp.setDeleted(deleted);
            resp.setErrors(errors);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            xmlMapper.writeValue(outputStream, resp);
            ReadableStreamChannel readableStreamChannel =
                new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
            restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, readableStreamChannel.getSize());
            restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, XML_CONTENT_TYPE);
            callback.onCompletion(readableStreamChannel, null);
          } catch (IOException e) {
            callback.onCompletion(null, new RestServiceException("Failed to serialize response", RestServiceErrorCode.InternalServerError));
          }
        }
    }, restRequest.getUri(), LOGGER, callback);
  }

  /**
   * This method is used to update the lists of deleted and errored objects.
   * @param success whether the delete was successful or not
   * @param key the object key
   */
  public void onDeleteCompletion(boolean success, String key) {
    if (success) {
      deleted.add(key);
    } else {
      errors.add(key);
    }
  }

  /**
   * Deserialize the request body into an S3BatchDeleteObjects.
   */
  public S3MessagePayload.S3BatchDeleteObjects deserializeRequest(RetainingAsyncWritableChannel channel) {
    S3MessagePayload.S3BatchDeleteObjects deleteRequest = null;
    ByteBuf byteBuffer = channel.consumeContentAsByteBuf();
    byte[] byteArray = new byte[byteBuffer.readableBytes()];
    byteBuffer.readBytes(byteArray);

    XmlMapper xmlMapper = new XmlMapper();
    try {
      deleteRequest = xmlMapper.readValue(byteArray, S3MessagePayload.S3BatchDeleteObjects.class);
    } catch (IOException e) {
      failedRequest = true;
    }
    return deleteRequest;
  }
}
