/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.NamedBlobListEntry;
import com.github.ambry.frontend.NamedBlobListHandler;
import com.github.ambry.frontend.NamedBlobPath;
import com.github.ambry.frontend.Operations;
import com.github.ambry.frontend.Page;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.ByteBufferDataInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.frontend.s3.S3MessagePayload.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Handles S3 requests for listing blobs that start with a provided prefix.
 * API reference: <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">...</a>
 */
public class S3ListHandler extends S3BaseHandler<ReadableStreamChannel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3ListHandler.class);
  private static final ObjectMapper xmlMapper = new XmlMapper();
  public static final String PREFIX_PARAM_NAME = "prefix";
  public static final String MAXKEYS_PARAM_NAME = "max-keys";
  public static final String DELIMITER_PARAM_NAME = "delimiter";
  public static final String ENCODING_TYPE_PARAM_NAME = "encoding-type";
  private final NamedBlobListHandler namedBlobListHandler;
  private final FrontendMetrics metrics;
  private final S3MultipartUploadHandler multipartUploadHandler;

  /**
   * Constructs a handler for handling s3 requests for listing blobs.
   * @param namedBlobListHandler  the {@link NamedBlobListHandler} to use.
   * @param multipartUploadHandler the {@link S3MultipartUploadHandler} to use.
   */
  public S3ListHandler(NamedBlobListHandler namedBlobListHandler, FrontendMetrics metrics,
      S3MultipartUploadHandler multipartUploadHandler) {
    this.namedBlobListHandler = namedBlobListHandler;
    this.metrics = metrics;
    this.multipartUploadHandler = multipartUploadHandler;
  }

  /**
   * Asynchronously get account metadata.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {

    if (S3MultipartUploadHandler.isListPartRequest(restRequest)) {
      multipartUploadHandler.handle(restRequest, restResponseChannel, callback);
      return;
    }

    namedBlobListHandler.handle(restRequest, restResponseChannel,
        buildCallback(metrics.s3ListHandleMetrics, (result) -> {
          // Convert from json response to S3 xml response as defined in
          // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_ResponseSyntax.
          ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) result).getContent();
          JSONObject jsonObject = new JSONObject(new JSONTokener(new ByteBufferDataInputStream(byteBuffer)));
          Page<NamedBlobListEntry> page = Page.fromJson(jsonObject, NamedBlobListEntry::new);
          ReadableStreamChannel readableStreamChannel = serializeAsXml(restRequest, page);
          restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "application/xml");
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, readableStreamChannel.getSize());
          callback.onCompletion(readableStreamChannel, null);
        }, restRequest.getUri(), LOGGER, callback));
  }

  private ReadableStreamChannel serializeAsXml(RestRequest restRequest, Page<NamedBlobListEntry> namedBlobRecordPage)
      throws IOException, RestServiceException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    String prefix = RestUtils.getHeader(restRequest.getArgs(), PREFIX_PARAM_NAME, false);
    String delimiter = RestUtils.getHeader(restRequest.getArgs(), DELIMITER_PARAM_NAME, false);
    String encodingType = RestUtils.getHeader(restRequest.getArgs(), ENCODING_TYPE_PARAM_NAME, false);
    String maxKeys = RestUtils.getHeader(restRequest.getArgs(), MAXKEYS_PARAM_NAME, false);
    int maxKeysValue = maxKeys == null ? Integer.MAX_VALUE : Integer.parseInt(maxKeys);
    // Iterate through list of blob names.
    List<Contents> contentsList = new ArrayList<>();
    int keyCount = 0;
    for (NamedBlobListEntry namedBlobRecord : namedBlobRecordPage.getEntries()) {
      String blobName = namedBlobRecord.getBlobName();
      String todayDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(Calendar.getInstance().getTime());
      contentsList.add(new Contents(blobName, todayDate));
      if (++keyCount == maxKeysValue) {
        break;
      }
    }
    ListBucketResult result =
        new ListBucketResult(restRequest.getPath(), prefix, maxKeysValue, keyCount, delimiter, contentsList,
            encodingType);
    LOGGER.debug("Sending response for S3 ListObjects {}", result);
    // Serialize xml
    xmlMapper.writeValue(outputStream, result);
    return new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is a request to list parts of a completed multipart upload request.
   */
  public static boolean isListBucketRequest(RestRequest restRequest) throws RestServiceException {
    RequestPath requestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
    return restRequest.getRestMethod() == RestMethod.GET && restRequest.getArgs().containsKey(S3_REQUEST)
        && requestPath.matchesOperation(Operations.NAMED_BLOB)
        && NamedBlobPath.parse(requestPath, restRequest.getArgs()).getBlobName() == null;
  }
}
