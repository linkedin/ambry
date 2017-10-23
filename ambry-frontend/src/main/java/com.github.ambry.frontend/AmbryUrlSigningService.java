/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
 */
package com.github.ambry.frontend;

import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Time;
import java.util.Map;


/**
 * Default implementation of {@link UrlSigningService} that currently only converts all headers that start with
 * "x-ambry-" into query parameters and does not actually sign the URL.
 * <p/>
 * A real implementation needs to encode enough information for manipulations to be discovered.
 * See https://github.com/linkedin/ambry/issues/788
 */
public class AmbryUrlSigningService implements UrlSigningService {
  static final String AMBRY_PARAMETERS_PREFIX = "x-ambry-";
  private static final String LINK_EXPIRY_TIME = "et";
  private static final String ENDPOINT_SUFFIX = "/";
  private static final String QUERY_STRING_START = "?";
  private static final String PARAMETER_SEPARATOR = "&";
  private static final String PARAMETER_ASSIGN = "=";

  private final String uploadEndpoint;
  private final String downloadEndpoint;
  private final long defaultUrlTtlSecs;
  private final long defaultMaxUploadSize;
  private final long maxUrlTtlSecs;
  private final Time time;

  /**
   * Constructor
   * @param uploadEndpoint the endpoint in all signed POST URLs.
   * @param downloadEndpoint the endpoint in all signed GET URLs.
   * @param defaultUrlTtlSecs the default ttl of signed URLs if the request does not customize it.
   * @param defaultMaxUploadSize the default max size of upload from signed POST URLs if the request does not customize
   *                             it.
   * @param maxUrlTtlSecs the maximum ttl of signed URLs. If the request specifies a higher TTL, it will be lowered
   *                      to this number.
   * @param time the {@link Time} instance to use.
   */
  AmbryUrlSigningService(String uploadEndpoint, String downloadEndpoint, long defaultUrlTtlSecs,
      long defaultMaxUploadSize, long maxUrlTtlSecs, Time time) {
    if (!uploadEndpoint.endsWith(ENDPOINT_SUFFIX)) {
      uploadEndpoint = uploadEndpoint + ENDPOINT_SUFFIX;
    }
    if (!downloadEndpoint.endsWith(ENDPOINT_SUFFIX)) {
      downloadEndpoint = downloadEndpoint + ENDPOINT_SUFFIX;
    }
    this.uploadEndpoint = uploadEndpoint;
    this.downloadEndpoint = downloadEndpoint;
    this.defaultUrlTtlSecs = defaultUrlTtlSecs;
    this.defaultMaxUploadSize = defaultMaxUploadSize;
    this.maxUrlTtlSecs = maxUrlTtlSecs;
    this.time = time;
  }

  @Override
  public String getSignedUrl(RestRequest restRequest) throws RestServiceException {
    Map<String, Object> args = restRequest.getArgs();
    String restMethodInSignedUrlStr = RestUtils.getHeader(args, RestUtils.Headers.URL_TYPE, true);
    RestMethod restMethodInSignedUrl;
    try {
      restMethodInSignedUrl = RestMethod.valueOf(restMethodInSignedUrlStr);
    } catch (IllegalArgumentException e) {
      throw new RestServiceException("Unrecognized RestMethod: " + restMethodInSignedUrlStr,
          RestServiceErrorCode.InvalidArgs);
    }
    StringBuilder urlBuilder = new StringBuilder();
    switch (restMethodInSignedUrl) {
      case GET:
        urlBuilder.append(downloadEndpoint).append(QUERY_STRING_START);
        break;
      case POST:
        urlBuilder.append(uploadEndpoint).append(QUERY_STRING_START);
        break;
      default:
        throw new RestServiceException("Signing request for " + restMethodInSignedUrl + " is not supported",
            RestServiceErrorCode.InvalidArgs);
    }
    long urlTtlSecs = defaultUrlTtlSecs;
    long maxUploadSize = defaultMaxUploadSize;
    for (Map.Entry<String, Object> entry : args.entrySet()) {
      String name = entry.getKey();
      Object value = entry.getValue();
      if (name.regionMatches(true, 0, AMBRY_PARAMETERS_PREFIX, 0, AMBRY_PARAMETERS_PREFIX.length())
          && value instanceof String) {
        switch (name) {
          case RestUtils.Headers.URL_TTL:
            urlTtlSecs = Math.min(maxUrlTtlSecs, RestUtils.getLongHeader(args, RestUtils.Headers.URL_TTL, true));
            break;
          case RestUtils.Headers.MAX_UPLOAD_SIZE:
            maxUploadSize = RestUtils.getLongHeader(args, RestUtils.Headers.MAX_UPLOAD_SIZE, true);
            break;
          default:
            urlBuilder.append(name).append(PARAMETER_ASSIGN).append(value).append(PARAMETER_SEPARATOR);
            break;
        }
      }
    }
    if (RestMethod.POST.equals(restMethodInSignedUrl)) {
      urlBuilder.append(RestUtils.Headers.MAX_UPLOAD_SIZE)
          .append(PARAMETER_ASSIGN)
          .append(maxUploadSize)
          .append(PARAMETER_SEPARATOR);
    }
    urlBuilder.append(LINK_EXPIRY_TIME).append(PARAMETER_ASSIGN).append(time.seconds() + urlTtlSecs);
    // since all strings came from the URL, they are assumed to be url encodable.
    return urlBuilder.toString();
  }

  @Override
  public boolean isRequestSigned(RestRequest restRequest) {
    Map<String, Object> args = restRequest.getArgs();
    return args.containsKey(RestUtils.Headers.URL_TYPE) && args.containsKey(LINK_EXPIRY_TIME);
  }

  @Override
  public void verifySignedRequest(RestRequest restRequest) throws RestServiceException {
    if (!isRequestSigned(restRequest)) {
      throw new RestServiceException("Request is not signed - method should not have been called",
          RestServiceErrorCode.InternalServerError);
    }
    Map<String, Object> args = restRequest.getArgs();
    long expiryTimeSecs = RestUtils.getLongHeader(args, LINK_EXPIRY_TIME, true);
    if (time.seconds() > expiryTimeSecs) {
      throw new RestServiceException("Signed URL has expired", RestServiceErrorCode.Unauthorized);
    }
    RestMethod restMethodInUrl = RestMethod.valueOf(RestUtils.getHeader(args, RestUtils.Headers.URL_TYPE, true));
    if (!restRequest.getRestMethod().equals(restMethodInUrl)) {
      throw new RestServiceException("Type of request being made not compatible with signed URL",
          RestServiceErrorCode.Unauthorized);
    }
  }
}
