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
import java.util.concurrent.CountDownLatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.github.ambry.rest.RestUtils.*;


public class S3BatchDeleteHandler extends S3BaseHandler<ReadableStreamChannel> {
  private static DeleteBlobHandler deleteBlobHandler;
  private ArrayList<String> deleted = new ArrayList<>();
  private ArrayList<String> errors = new ArrayList<>();
  private boolean failedRequest;

  // S3PostHandler -> S3BatchDeleteHandler -> S3DeleteHandler -> S3DeleteObjectHandler -> DeleteBlobHandler
  public S3BatchDeleteHandler(DeleteBlobHandler deleteBlobHandler, S3BatchDeleteHandler s3DeleteHandler, FrontendMetrics frontendMetrics) {
    this.deleteBlobHandler = deleteBlobHandler;
    this.failedRequest = false;
  }

  /**
   * Callback for processing batch delete requests.
   */
  private class BatchDeleteCallback implements Callback<Long> {
    private final RetainingAsyncWritableChannel channel;
    private final RestRequest restRequest;
    private final DeleteBlobHandler deleteBlobHandler;

    public BatchDeleteCallback(RetainingAsyncWritableChannel channel, RestRequest restRequest, DeleteBlobHandler deleteBlobHandler) {
      this.channel = channel;
      this.restRequest = restRequest;
      this.deleteBlobHandler = deleteBlobHandler;
    }

    public void onDeleteCompletion(boolean success, String key) {
      if (success) {
        deleted.add(key);
      }
      else {
        errors.add(key);
      }
    }

    @Override
    public void onCompletion(Long respLong, Exception exception) {
      if (exception == null) {
        // Data read successfully
        try {
          // Get the retained content from the channel
          ByteBuf byteBuffer = channel.consumeContentAsByteBuf();
          // TODO byte array?
          // Convert ByteBuf to byte array
          byte[] byteArray = new byte[byteBuffer.readableBytes()];
          byteBuffer.readBytes(byteArray);

          // Deserialize into S3BatchDeleteObjects
          XmlMapper xmlMapper = new XmlMapper();
          S3MessagePayload.S3BatchDeleteObjects deleteRequest = xmlMapper.readValue(byteArray, S3MessagePayload.S3BatchDeleteObjects.class);

          // Process each delete key in the batch
          for (S3MessagePayload.S3BatchDeleteKeys object : deleteRequest.getObjects()) {
            RequestPath requestPath = (RequestPath) restRequest.getArgs().get(RestUtils.InternalKeys.REQUEST_PATH);

            // Construct the delete path
            // TODO: confirm that getPathAfterPrefixes is indeed "/named/application/container"
            String singleDeletePath = requestPath.getPathAfterPrefixes() + "/" + object.getKey();

            // Create a new RequestPath for the delete operation
            List<String> emptyList = new ArrayList<>();
            RequestPath newRequestPath = RequestPath.parse(singleDeletePath, restRequest.getArgs(), emptyList, requestPath.getClusterName());
            WrappedRestRequest singleDeleteRequest = new WrappedRestRequest(restRequest);
            singleDeleteRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH, newRequestPath);

            CountDownLatch latch = new CountDownLatch(1);
            NoOpResponseChannel noOpResponseChannel = new NoOpResponseChannel();

            deleteBlobHandler.handle(singleDeleteRequest, noOpResponseChannel, new Callback<Void>() {
              @Override
              public void onCompletion(Void result, Exception exception) {
                  // Call our custom onDeleteCompletion to track success/failure
                  boolean success = exception == null;
                  onDeleteCompletion(success, object.getKey());
                  latch.countDown();
                }
              });

            try {
              latch.await();  // This will block until latch count reaches zero
            } catch (InterruptedException e) {
              // TODO: what to do here
              Thread.currentThread().interrupt();
            }
          }
        } catch (IOException e) {
          // Handle exceptions during deserialization
          failedRequest = true;
        } catch (RestServiceException e) {
          failedRequest = true;
        }
      } else {
        // failed to read data
        failedRequest = true;
      }
    }
  }

  /**
   * Handles the S3 request and construct the response.
   *
   * @param restRequest         the {@link RestRequest} that contains the request headers and body.
   * @param restResponseChannel the {@link RestResponseChannel} that contains the response headers and body.
   * @param callback            the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<ReadableStreamChannel> callback)
      throws RestServiceException {

          // TODO determine if we need to define max size of chanel
        // Create the channel to read the request body
        RetainingAsyncWritableChannel channel = new RetainingAsyncWritableChannel();

        // Create and pass the BatchDeleteCallback to handle the response
        restRequest.readInto(channel, new BatchDeleteCallback(channel, restRequest, deleteBlobHandler));

        if (failedRequest) {
          restResponseChannel.setStatus(ResponseStatus.BadRequest);
          throw new RestServiceException("Failed to execute S3BatchDelete", RestServiceErrorCode.BadRequest);
        } else {
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
            throw new RestServiceException("Failed to serialize response", RestServiceErrorCode.InternalServerError);
          }
        }
  }
}



