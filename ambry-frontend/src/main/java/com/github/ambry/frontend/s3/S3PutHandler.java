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

import com.github.ambry.commons.Callback;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.NamedBlobPath;
import com.github.ambry.frontend.NamedBlobPutHandler;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.Headers.*;


/**
 * Handles S3 requests for uploading blobs.
 * API reference: <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">...</a>
 */
public class S3PutHandler extends S3BaseHandler<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3PutHandler.class);
  private final NamedBlobPutHandler namedBlobPutHandler;
  private final S3MultipartUploadHandler multipartUploadHandler;
  private final FrontendMetrics metrics;

  /**
   * Constructs a handler for uploading s3 requests.
   *
   * @param namedBlobPutHandler  the {@link NamedBlobPutHandler} to use.
   * @param multipartUploadHandler the {@link S3MultipartUploadHandler} to use.
   */
  public S3PutHandler(NamedBlobPutHandler namedBlobPutHandler, S3MultipartUploadHandler multipartUploadHandler,
      FrontendMetrics metrics) {
    this.namedBlobPutHandler = namedBlobPutHandler;
    this.multipartUploadHandler = multipartUploadHandler;
    this.metrics = metrics;
  }

  /**
   * Handles a request for putting a blob.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback)
      throws RestServiceException {
    if (S3MultipartUploadHandler.isMultipartUploadPartRequest(restRequest)) {
      multipartUploadHandler.handle(restRequest, restResponseChannel,
          (result, exception) -> callback.onCompletion(null, exception));
      return;
    }

    // 1. Add headers required by Ambry. These become the blob properties.
    NamedBlobPath namedBlobPath = NamedBlobPath.parse(getRequestPath(restRequest), restRequest.getArgs());
    String accountName = namedBlobPath.getAccountName();
    restRequest.setArg(Headers.SERVICE_ID, accountName);

    // https://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html#sec7.2.1
    // Any HTTP/1.1 message containing an entity-body SHOULD include a Content-Type header field defining the media type of that body.
    // If and only if the media type is not given by a Content-Type field,
    // the recipient MAY attempt to guess the media type via inspection of its content and/or the name extension(s).
    // If the media type remains unknown, the recipient SHOULD treat it as type "application/octet-stream".
    // TiKV S3 client doesn't specify the Content-Type for the PutObject
    if (restRequest.getArgs().get(Headers.CONTENT_TYPE) == null) {
      restRequest.setArg(Headers.AMBRY_CONTENT_TYPE, RestUtils.OCTET_STREAM_CONTENT_TYPE);
    } else {
      restRequest.setArg(Headers.AMBRY_CONTENT_TYPE, restRequest.getArgs().get(Headers.CONTENT_TYPE));
    }
    if (restRequest.getArgs().get(CONTENT_ENCODING) != null) {
      restRequest.setArg(Headers.AMBRY_CONTENT_ENCODING, restRequest.getArgs().get(Headers.CONTENT_ENCODING));
    }

    // Overwrites are allowed by default for S3 APIs https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
    restRequest.setArg(NAMED_UPSERT, true);

    // Calculate checksum of the file content. This will be used in ETag header. Default algorithm seems to be MD5
    try {
      restRequest.setDigestAlgorithm("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RestServiceException("MD5 Algorithm is not supported", e, RestServiceErrorCode.InternalServerError);
    }

    // 2. Upload the blob by following named blob PUT path
    namedBlobPutHandler.handle(restRequest, restResponseChannel, buildCallback(metrics.s3PutHandleMetrics, (r) -> {
      if (restResponseChannel.getStatus() == ResponseStatus.Created) {
        // Set the response status to 200 since Ambry named blob PUT has response as 201.
        restResponseChannel.setStatus(ResponseStatus.Ok);
        // Set S3 ETag header
        // TODO: For Multipart uploads, ETag is calculated as md5(part1) + md5(part2) + ... + md5(partN) + "-" + N.
        //  This needs some external db to store md5 of individual parts.
        String eTag = calculateETag(restRequest);
        restResponseChannel.setHeader("ETag", eTag);
      }
      callback.onCompletion(null, null);
    }, restRequest.getUri(), LOGGER, (result, exception) -> {
      LOGGER.info("Request {} failed in s3 put object API", restRequest.getUri(), exception);
      callback.onCompletion(null, exception);
    }));
  }

  private String calculateETag(RestRequest restRequest) {
    byte[] digest = restRequest.getDigest();
    StringBuilder hexString = new StringBuilder();
    for (byte b : digest) {
      String hex = Integer.toHexString(0xff & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    LOGGER.debug("ETag calculated for RestRequest {} is {}", restRequest, hexString);
    return hexString.toString();
  }
}
