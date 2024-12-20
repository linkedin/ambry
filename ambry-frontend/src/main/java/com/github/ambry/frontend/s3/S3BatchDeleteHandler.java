package com.github.ambry.frontend.s3;

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

      // define the size ...

    RetainingAsyncWritableChannel channel = new RetainingAsyncWritableChannel();

    // confirm what post request are doing

    // Read the request content into the channel

    // remove callback from func
    // give good name for callback ,, define in same class .. batchdeletecallback

    restRequest.readInto(channel, (respLong, exception) -> {
      if (exception == null) {
        // Data read successfully
        try {

          // Get the retained content from the channel
          ByteBuf byteBuffer = channel.consumeContentAsByteBuf();

          // Deserialize the XML content
          XmlMapper xmlMapper = new XmlMapper();
          S3MessagePayload.ListBucketResultV2 listBucketResultV2 = xmlMapper.readValue(byteBuffer.array(), S3MessagePayload.ListBucketResultV2.class);
          List<S3MessagePayload.Contents> contents = listBucketResultV2.getContents();

          for (int i = 0; i < contents.size(); i++) {
            S3MessagePayload.Contents content = contents.get(i);
            String key = content.getKey();
            String requestPath = (String) restRequest.getArgs().get(REQUEST_PATH);
            String[] pathParts = requestPath.split("/");
            pathParts[pathParts.length - 1] = key;
            String newRequestPath = String.join("/", pathParts);

            // S3 path is "/s3/my-account/my-container/key

            WrappedRestRequest singleDeleteRequest = new WrappedRestRequest(restRequest);
            singleDeleteRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH, newRequestPath);


            deleteBlobHandler.handle(singleDeleteRequest, null, null);




            // do operations
          }



        // Add logs ?

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



   // Implement the loop for deleting keys using the s3DeleteHandler.doHandle method


