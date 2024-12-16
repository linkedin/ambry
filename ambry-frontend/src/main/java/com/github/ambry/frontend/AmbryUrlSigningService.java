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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Time;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.github.ambry.rest.RestUtils.*;


/**
 * Default implementation of {@link UrlSigningService} that currently only converts all headers that start with
 * "x-ambry-" into query parameters and does not actually sign the URL.
 * <p/>
 * A real implementation needs to encode enough information for manipulations to be discovered.
 * See https://github.com/linkedin/ambry/issues/788
 */
public class AmbryUrlSigningService implements UrlSigningService {
  private static final String LINK_EXPIRY_TIME = "et";
  private static final String ENDPOINT_SUFFIX = "/";
  private static final String QUERY_STRING_START = "?";
  private static final String PARAMETER_SEPARATOR = "&";
  private static final String PARAMETER_ASSIGN = "=";

  private final String uploadEndpoint;
  private final String downloadEndpoint;

  private final long defaultUrlTtlSecs;
  private final long defaultMaxUploadSize;
  private final long chunkUploadInitialChunkTtlSecs;
  private final long chunkUploadMaxChunkSize;
  private final long maxUrlTtlSecs;
  private final Time time;
  private final ClusterMap clusterMap;
  private final ClusterMapConfig clusterMapConfig;
  private final boolean isReservedMetadataEnabled;
  private final RouterConfig routerConfig;

  /**
   * Constructor
   * @param uploadEndpoint the endpoint in all signed POST URLs.
   * @param downloadEndpoint the endpoint in all signed GET URLs.
   * @param defaultUrlTtlSecs the default ttl of signed URLs if the request does not customize it.
   * @param defaultMaxUploadSize the default max size of upload from signed POST URLs if the request does not customize
   *                             it.
   * @param maxUrlTtlSecs the maximum ttl of signed URLs. If the request specifies a higher TTL, it will be lowered
   * @param chunkUploadInitialChunkTtlSecs the preconfigured blob TTL for chunks of a stitched upload. These chunks
   *                                       will be made permanent once the blob is stitched together.
   * @param chunkUploadMaxChunkSize the preconfigured max size for chunks of a stitched upload.
   * @param time the {@link Time} instance to use.
   * @param clusterMap the {@link ClusterMap} object.
   * @param clusterMapConfig the {@link ClusterMapConfig} object.
   * @param routerConfig the {@link RouterConfig} object.
   */
  AmbryUrlSigningService(String uploadEndpoint, String downloadEndpoint, long defaultUrlTtlSecs,
      long defaultMaxUploadSize, long maxUrlTtlSecs, long chunkUploadInitialChunkTtlSecs, long chunkUploadMaxChunkSize,
      Time time, ClusterMap clusterMap, ClusterMapConfig clusterMapConfig, RouterConfig routerConfig) {
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
    this.chunkUploadInitialChunkTtlSecs = chunkUploadInitialChunkTtlSecs;
    this.chunkUploadMaxChunkSize = chunkUploadMaxChunkSize;
    this.maxUrlTtlSecs = maxUrlTtlSecs;
    this.time = time;
    this.clusterMap = clusterMap;
    this.clusterMapConfig = clusterMapConfig;
    this.isReservedMetadataEnabled = routerConfig.routerReservedMetadataEnabled;
    this.routerConfig = routerConfig;
  }

  @Override
  public String getSignedUrl(RestRequest restRequest) throws RestServiceException {
    Map<String, Object> args = restRequest.getArgs();
    String restMethodInSignedUrlStr = RestUtils.getHeader(args, RestUtils.Headers.URL_TYPE, true);
    Account account = RestUtils.getAccountFromArgs(args);
    Container container = RestUtils.getContainerFromArgs(args);
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
        urlBuilder.append(downloadEndpoint);
        break;
      case POST:
        urlBuilder.append(uploadEndpoint);
        break;
      default:
        throw new RestServiceException("Signing request for " + restMethodInSignedUrl + " is not supported",
            RestServiceErrorCode.InvalidArgs);
    }

    long urlTtlSecs = defaultUrlTtlSecs;
    long maxUploadSize = defaultMaxUploadSize;
    boolean chunkUpload = false;
    Map<String, Object> argsForUrl = new HashMap<>();
    for (Map.Entry<String, Object> entry : args.entrySet()) {
      String name = entry.getKey();
      Object value = entry.getValue();
      if (name.regionMatches(true, 0, AMBRY_HEADER_PREFIX, 0, AMBRY_HEADER_PREFIX.length())
          && value instanceof String) {
        switch (name) {
          case RestUtils.Headers.URL_TTL:
            urlTtlSecs = Math.min(maxUrlTtlSecs, RestUtils.getLongHeader(args, RestUtils.Headers.URL_TTL, true));
            break;
          case RestUtils.Headers.MAX_UPLOAD_SIZE:
            maxUploadSize = RestUtils.getLongHeader(args, RestUtils.Headers.MAX_UPLOAD_SIZE, true);
            break;
          case RestUtils.Headers.CHUNK_UPLOAD:
            chunkUpload = RestUtils.getBooleanHeader(args, RestUtils.Headers.CHUNK_UPLOAD, true);
            break;
          default:
            argsForUrl.put(name, value);
            break;
        }
      }
    }
    if (RestMethod.POST.equals(restMethodInSignedUrl)) {
      if (chunkUpload) {
        // Chunks of a stitched blob have a fixed max size to ensure that the router does not do further chunking as
        // this is not supported by the current metadata format.
        maxUploadSize = chunkUploadMaxChunkSize;
        // They also have a non-optional blob TTL to ensure that chunks that were not stitched within a reasonable time
        // span are cleaned up.
        if (RestUtils.getLongHeader(args, RestUtils.Headers.TTL, false) == null) {
          argsForUrl.put(RestUtils.Headers.TTL, chunkUploadInitialChunkTtlSecs);
        }
        argsForUrl.put(RestUtils.Headers.CHUNK_UPLOAD, true);
        argsForUrl.put(RestUtils.Headers.SESSION, UUID.randomUUID().toString());
        BlobId reservedMetadataBlobId = ClusterMapUtils.reserveMetadataBlobId(
            ClusterMapUtils.getPartitionClass(account, container, clusterMapConfig.clusterMapDefaultPartitionClass),
            null, ReservedMetadataIdMetrics.getReservedMetadataIdMetrics(clusterMap.getMetricRegistry()), clusterMap,
            account.getId(), container.getId(), container.isEncrypted(), routerConfig);
        if (reservedMetadataBlobId == null) {
          ReservedMetadataIdMetrics.getReservedMetadataIdMetrics(
              clusterMap.getMetricRegistry()).reserveMetadataIdFailedForPostSignedUrlCount.inc();
        }
        if (isReservedMetadataEnabled) {
          if (reservedMetadataBlobId == null) {
            throw new RestServiceException("Reserving metadata id failed", RestServiceErrorCode.InternalServerError);
          }
          argsForUrl.put(RestUtils.Headers.RESERVED_METADATA_ID, reservedMetadataBlobId);
        }
      }
      argsForUrl.put(RestUtils.Headers.MAX_UPLOAD_SIZE, maxUploadSize);
    }
    argsForUrl.put(LINK_EXPIRY_TIME, time.seconds() + urlTtlSecs);

    String nextSeparator = QUERY_STRING_START;
    for (Map.Entry<String, Object> entry : argsForUrl.entrySet()) {
      String value = urlEncode(entry.getValue());
      urlBuilder.append(nextSeparator).append(entry.getKey()).append(PARAMETER_ASSIGN).append(value);
      nextSeparator = PARAMETER_SEPARATOR;
    }
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
      // If the signed url is for download, we also want to support HEAD request against that signed url.
      if (!(restRequest.getRestMethod().equals(RestMethod.HEAD) && restMethodInUrl.equals(RestMethod.GET))) {
        throw new RestServiceException("Type of request being made not compatible with signed URL",
            RestServiceErrorCode.Unauthorized);
      }
    }
  }

  // Mainly for test verification
  public String getUploadEndpoint() {
    return uploadEndpoint;
  }

  public String getDownloadEndpoint() {
    return downloadEndpoint;
  }

  /**
   * @param obj the object to encode.
   * @return a UTF-8 URL encoded version of {@code obj.toString()}
   *
   */
  private static String urlEncode(Object obj) throws RestServiceException {
    try {
      return URLEncoder.encode(obj.toString(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RestServiceException("Unsupported URL encoding", e, RestServiceErrorCode.InternalServerError);
    }
  }
}
