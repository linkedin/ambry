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
    RetainingAsyncWritableChannel channel = new RetainingAsyncWritableChannel();

    // remove callback from func
    // give good name for callback ,, define in same class .. batchdeletecallback

    restRequest.readInto(channel, (respLong, exception) -> {
      if (exception == null) {
        // Data read successfully
        try {

          // Get the retained content from the channel
          ByteBuf byteBuffer = channel.consumeContentAsByteBuf();

              // TODO unit test to verify the following
              // create bytearray ... and read from bytebuffer from line 63 into array
              // cause an error potentially if bytebuffer.array

          XmlMapper xmlMapper = new XmlMapper();
          S3MessagePayload.S3BatchDeleteObjects deleteRequest = xmlMapper.readValue(byteBuffer.array(),
              S3MessagePayload.S3BatchDeleteObjects.class);

          for (S3MessagePayload.S3BatchDeleteKeys object : deleteRequest.getObjects()) {
            RequestPath requestPath =  (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
            // TODO: confirm that getPathAfterPrefixes is indeed "/named/application/container"
            String singleDeletePath = requestPath.getPathAfterPrefixes() + "/" + object.getKey();
            List<String> emptyList = new ArrayList<>();
            RequestPath newRequestPath = RequestPath.parse(singleDeletePath, restRequest.getArgs(), emptyList, requestPath.getClusterName());
            WrappedRestRequest singleDeleteRequest = new WrappedRestRequest(restRequest);
            singleDeleteRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH, newRequestPath);
            // TODO: fill in the null values
            deleteBlobHandler.handle(singleDeleteRequest, null, null);
          }

        } catch (IOException e) {
          // Handle exceptions during deserialization
        } catch (RestServiceException e) {
          throw new RuntimeException(e);
        }
      } else {
        // Handle the exception

      }
    });
  }
}


// finish one key, do next key
// futures
// fake response to collect .. construct overall callback
// 404



