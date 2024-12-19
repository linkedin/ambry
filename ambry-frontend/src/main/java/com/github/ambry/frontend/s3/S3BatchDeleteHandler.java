package com.github.ambry.frontend.s3;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.ambry.account.MySqlAccountService;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.rest.RestServiceException;
import io.netty.buffer.ByteBuf;
import java.io.ByteArrayOutputStream;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;


public class S3BatchDeleteHandler extends S3BaseHandler {
  //private static final ObjectMapper xmlMapper = new XmlMapper();


  // S3PostHandler -> S3BatchDeleteHandler -> S3DeleteHandler -> S3DeleteObjectHandler -> DeleteBlobHandler
  public S3BatchDeleteHandler(S3BatchDeleteHandler s3DeleteHandler, FrontendMetrics frontendMetrics) {
    super();
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

    // Read the request content into the channel
    restRequest.readInto(channel, (exception) -> {
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

            // do operations
          }


        // Add logs ?

        } catch (IOException e) {
          // Handle exceptions during deserialization
        }
      } else {
        // Handle the exception

      }
    });
  }



    // edit the callback null
    }



   // Implement the loop for deleting keys using the s3DeleteHandler.doHandle method


