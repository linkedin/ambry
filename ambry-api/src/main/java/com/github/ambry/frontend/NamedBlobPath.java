/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.util.Map;
import java.util.Objects;


/**
 * Represents the blob url parsing results for named blob.
 */
public class NamedBlobPath {
  private final String accountName;
  private final String containerName;
  private final String blobName;
  private final String blobNamePrefix;
  private final String pageToken;

  static final String PREFIX_PARAM = "prefix";
  static final String PAGE_PARAM = "page";

  /**
   * Parse the input path if it's a named blob request.
   * @param path the URI path that needs to be parsed. This path should already be URL decoded and have query paramaters
   *             removed. In the request handling path, this would be done by the {@link RestRequest} implementation.
   * @param args a map containing any query parameters from the request.
   * @return the {@link NamedBlobPath} that indicates the parsing result from blobUrl.
   * @throws RestServiceException on parsing errors.
   */
  public static NamedBlobPath parse(String path, Map<String, Object> args) throws RestServiceException {
    Objects.requireNonNull(path, "path should not be null");
    Objects.requireNonNull(args, "args should not be null");
    path = path.startsWith("/") ? path.substring(1) : path;
    String[] splitPath = path.split("/", 4);
    String blobNamePrefix = RestUtils.getHeader(args, PREFIX_PARAM, false);
    boolean isListRequest = blobNamePrefix != null;
    int expectedSegments = isListRequest ? 3 : 4;
    if (splitPath.length != expectedSegments || !Operations.NAMED_BLOB.equalsIgnoreCase(splitPath[0])) {
      throw new RestServiceException(String.format(
          "Path must have format '/named/<account_name>/<container_name>%s.  Received path='%s', blobNamePrefix='%s'",
          isListRequest ? "" : "/<blob_name>'", path, blobNamePrefix), RestServiceErrorCode.BadRequest);
    }
    String accountName = splitPath[1];
    String containerName = splitPath[2];
    if (isListRequest) {
      String pageToken = RestUtils.getHeader(args, PAGE_PARAM, false);
      return new NamedBlobPath(accountName, containerName, null, blobNamePrefix, pageToken);
    } else {
      String blobName = splitPath[3];
      return new NamedBlobPath(accountName, containerName, blobName, null, null);
    }
  }

  /**
   * Parse the input path if it's a named blob request.
   * @param requestPath the {@link RequestPath} to be parsed.
   * @param args a map containing any query parameters from the request.
   * @return the {@link NamedBlobPath} that indicates the parsing result from blobUrl.
   * @throws RestServiceException on parsing errors.
   */
  public static NamedBlobPath parse(RequestPath requestPath, Map<String, Object> args) throws RestServiceException {
    return parse(requestPath.getOperationOrBlobId(true), args);
  }

  /**
   * Constructs a {@link NamedBlobPath}
   * @param accountName name of the account for named blob.
   * @param containerName name of the container for named blob.
   * @param blobName name of the blob. This will be null for list requests.
   * @param blobNamePrefix the blob name prefix to search for in a list request.
   *                       This will be null for non-list requests.
   * @param pageToken the token representing the response page to start the search from in a list request.
   *                  This will be null for non-list requests or list requests seeking the first response page.
   */
  private NamedBlobPath(String accountName, String containerName, String blobName, String blobNamePrefix,
      String pageToken) {
    this.accountName = accountName;
    this.containerName = containerName;
    this.blobName = blobName;
    this.blobNamePrefix = blobNamePrefix;
    this.pageToken = pageToken;
  }

  /**
   * Get the account name.
   * @return account name of the named blob.
   */
  public String getAccountName() {
    return accountName;
  }

  /**
   * Get the container name.
   * @return container name of the named blob.
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * Get the blob name.
   * @return blob name of named blob. This will be null for list requests.
   */
  public String getBlobName() {
    return blobName;
  }

  /**
   * @return the blob name prefix to search for in a list request. This will be null for non-list requests.
   */
  public String getBlobNamePrefix() {
    return blobNamePrefix;
  }

  /**
   * @return the token representing the response page to start the search from in a list request. This will be null for
   *         non-list requests or list requests seeking the first response page.
   */
  public String getPageToken() {
    return pageToken;
  }
}
