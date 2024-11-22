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
import com.github.ambry.frontend.AccountAndContainerInjector;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.NamedBlobPath;
import com.github.ambry.frontend.SecurityService;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.frontend.s3.S3MessagePayload.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;
import static com.github.ambry.rest.RestUtils.*;


/**
 * Handles a request for listing parts that have been uploaded for a specific multipart upload ID.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html">...</a>
 * TODO [S3] For now, this API returns empty list. Flink only uses it before starting a new multipart upload. So,
 *  sending an empty list unblocks them. In future, we may need to fetch the actual part IDs.
 */
public class S3MultipartListPartsHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3MultipartListPartsHandler.class);
  private static final ObjectMapper objectMapper = new XmlMapper();
  private final SecurityService securityService;
  private final FrontendMetrics frontendMetrics;
  private final AccountAndContainerInjector accountAndContainerInjector;

  /**
   * Construct a handler for handling S3 ListParts requests during multipart uploads.
   *
   * @param securityService             the {@link SecurityService} to use.
   * @param frontendMetrics             {@link FrontendMetrics} instance where metrics should be recorded.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   */
  public S3MultipartListPartsHandler(SecurityService securityService, FrontendMetrics frontendMetrics,
      AccountAndContainerInjector accountAndContainerInjector) {
    this.securityService = securityService;
    this.frontendMetrics = frontendMetrics;
    this.accountAndContainerInjector = accountAndContainerInjector;
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    new S3MultipartListPartsHandler.CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */
  private class CallbackChain {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> finalCallback;
    private final String uri;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    private void start() {
      try {
        accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest,
            frontendMetrics.getBlobMetricsGroup);
        securityService.processRequest(restRequest, securityProcessRequestCallback());
      } catch (Exception e) {
        finalCallback.onCompletion(null, e);
      }
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(frontendMetrics.putSecurityProcessRequestMetrics, securityCheckResult -> {
        securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback());
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, return response for the request.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(frontendMetrics.putSecurityPostProcessRequestMetrics, securityCheckResult -> {
        try {
          RequestPath requestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
          NamedBlobPath namedBlobPath = NamedBlobPath.parse(requestPath, restRequest.getArgs());
          String bucket = namedBlobPath.getContainerName();
          String key = namedBlobPath.getBlobName();
          String uploadId = (String) restRequest.getArgs().get(UPLOAD_ID_QUERY_PARAM);
          LOGGER.debug(
              "Sending response for listing parts of a multipart upload. Bucket = {}, Key = {}, Upload Id = {}", bucket,
              key, uploadId);
          ListPartsResult listPartsResult = new ListPartsResult(bucket, key, uploadId);
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
          objectMapper.writeValue(outputStream, listPartsResult);
          ReadableStreamChannel channel =
              new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
          restResponseChannel.setHeader(Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setHeader(Headers.CONTENT_TYPE, XML_CONTENT_TYPE);
          restResponseChannel.setHeader(Headers.CONTENT_LENGTH, channel.getSize());
          finalCallback.onCompletion(channel, null);
        } catch (RestServiceException | IOException e) {
          finalCallback.onCompletion(null, e);
        }
      }, uri, LOGGER, finalCallback);
    }
  }
}
