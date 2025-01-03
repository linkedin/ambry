package com.github.ambry.frontend.s3;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.ambry.commons.Callback;
import com.github.ambry.frontend.DeleteBlobHandler;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.WrappedRestRequest;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.bouncycastle.cert.ocsp.Req;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


public class S3BatchDeleteHandler extends S3BaseHandler {
  //private static final ObjectMapper xmlMapper = new XmlMapper();
  private static DeleteBlobHandler deleteBlobHandler;


  // S3PostHandler -> S3BatchDeleteHandler -> S3DeleteHandler -> S3DeleteObjectHandler -> DeleteBlobHandler
  public S3BatchDeleteHandler(DeleteBlobHandler deleteBlobHandler, S3BatchDeleteHandler s3DeleteHandler, FrontendMetrics frontendMetrics) {
    this.deleteBlobHandler = deleteBlobHandler;
  }

  /**
   * Callback for processing batch delete requests.
   */
  private static class BatchDeleteCallback implements Callback<Long> {
    private final RetainingAsyncWritableChannel channel;
    private final RestRequest restRequest;
    private final DeleteBlobHandler deleteBlobHandler;

    public BatchDeleteCallback(RetainingAsyncWritableChannel channel, RestRequest restRequest, DeleteBlobHandler deleteBlobHandler) {
      this.channel = channel;
      this.restRequest = restRequest;
      this.deleteBlobHandler = deleteBlobHandler;
    }

    // TODO: determine if we need oncompletion or just a method like in other code examples
    @Override
    public void onCompletion(Long respLong, Exception exception) {
      if (exception == null) {
        // Data read successfully
        try {
          // Get the retained content from the channel
          ByteBuf byteBuffer = channel.consumeContentAsByteBuf();

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

            // Invoke the delete handler for each object
            deleteBlobHandler.handle(singleDeleteRequest, null, null);
          }
        } catch (IOException e) {
          // Handle exceptions during deserialization
          // TODO: address error in all the throws
          throw new RestServiceException("Error reading batch delete request", e, null);
        } catch (RestServiceException e) {
          // Handle service-specific exceptions
          throw new RuntimeException("Error handling batch delete", e);
        }
      } else {
        // Handle the exception that occurred during the read operation
        throw new RestServiceException("Error reading data from channel", exception, null);
      }
    }
  }


//   throw new RestServiceException(String.format(
//      "Path must have format '/named/<account_name>/<container_name>%s.  Received path='%s', arg='%s'",
//      isListRequest || isGetObjectLockRequest ? "" : "/<blob_name>'", path, args), RestServiceErrorCode.BadRequest);
//}

  /**
   * Handles the S3 request and construct the response.
   *
   * @param restRequest         the {@link RestRequest} that contains the request headers and body.
   * @param restResponseChannel the {@link RestResponseChannel} that contains the response headers and body.
   * @param callback            the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback callback)
      throws RestServiceException {

          // TODO determine if we need to define max size of chanel
        // Create the channel to read the request body
        RetainingAsyncWritableChannel channel = new RetainingAsyncWritableChannel();

        // Create and pass the BatchDeleteCallback to handle the response
        restRequest.readInto(channel, new BatchDeleteCallback(channel, restRequest, deleteBlobHandler));

  }
}


// finish one key, do next key
// futures
// fake response to collect .. construct overall callback
// 404



