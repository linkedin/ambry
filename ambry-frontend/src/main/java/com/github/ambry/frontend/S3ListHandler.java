package com.github.ambry.frontend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;


/**
 * Handles S3 requests for listing blobs that start with a provided prefix.
 */
public class S3ListHandler {
  private final NamedBlobListHandler namedBlobListHandler;
  private RestRequest restRequest;

  /**
   * Constructs a handler for handling s3 requests for listing blobs.
   * @param namedBlobListHandler named blob list handler
   */
  S3ListHandler(NamedBlobListHandler namedBlobListHandler) {
    this.namedBlobListHandler = namedBlobListHandler;
  }

  /**
   * Asynchronously get account metadata.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    this.restRequest = restRequest;
    namedBlobListHandler.handle(restRequest, restResponseChannel, ((result, exception) -> {
      if (exception != null) {
        callback.onCompletion(result, exception);
        return;
      }

      try {
        // Convert from json response to S3 xml response as defined in
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_ResponseSyntax.
        RetainingAsyncWritableChannel writableChannel = new RetainingAsyncWritableChannel();
        result.readInto(writableChannel, null).get(1, TimeUnit.SECONDS);
        JSONObject jsonObject = FrontendUtils.readJsonFromChannel(writableChannel);
        Page<NamedBlobListEntry> page = Page.fromJson(jsonObject, NamedBlobListEntry::new);

        ReadableStreamChannel readableStreamChannel = serializeAsXml(page);
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "application/xml");
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, readableStreamChannel.getSize());
        callback.onCompletion(readableStreamChannel, null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }));
  }

  private ReadableStreamChannel serializeAsXml(Page<NamedBlobListEntry> namedBlobRecordPage) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    ListBucketResult listBucketResult = new ListBucketResult();
    listBucketResult.setName(restRequest.getPath());
    listBucketResult.setPrefix(restRequest.getArgs().get("prefix").toString());

    List<Contents> contentsList = new ArrayList<>();
    List<NamedBlobListEntry> namedBlobRecords = namedBlobRecordPage.getEntries();
    for (NamedBlobListEntry namedBlobRecord : namedBlobRecords) {
      Contents contents = new Contents();
      contents.setKey(namedBlobRecord.getBlobName());
      String todayDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(Calendar.getInstance().getTime());
      contents.setLastModified(todayDate);
      contentsList.add(contents);
    }

    listBucketResult.setMaxKeys(namedBlobRecords.size());
    listBucketResult.setDelimiter("/");
    listBucketResult.setEncodingType("url");

    listBucketResult.setContents(contentsList);

    ObjectMapper objectMapper = new XmlMapper();
    objectMapper.writeValue(outputStream, listBucketResult);

    return new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
  }
}
