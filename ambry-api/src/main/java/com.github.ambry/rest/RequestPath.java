/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.rest;

import java.util.List;


public class RequestPath {
  private final String prefix;
  private final String clusterName;
  private final String operationOrBlobId;
  private final RestUtils.SubResource subResource;
  private String operationOrBlobIdWithoutLeadingSlash = null;

  /**
   * Parse the request path (and additional headers in some cases). The path will match the following regex-like
   * description:
   * <pre>
   * {@code {prefixToRemove}?{clusterNameSegment}?({operationOrBlobId}|{operationOrBlobId}/{subResource})}
   * </pre>
   * For example, something like {@code /prefixOne/clusterA/parts/of/blobId/BlobInfo}, {@code /clusterA/operation},
   * or {@code /blobId}, among others.
   * @param restRequest {@link RestRequest} containing metadata about the request.
   * @param prefixesToRemove the list of prefixes that could precede the other parts of the URL. Removal of
   *                         prefixes earlier in the list will be preferred to removal of the ones later in the list.
   * @param clusterName the cluster name to recognize and handle when parsing the URL. Case is ignored when matching
   *                    this path segment.
   * @return a {@link RequestPath} object.
   */
  public static RequestPath parse(RestRequest restRequest, List<String> prefixesToRemove, String clusterName) {
    String path = restRequest.getPath();
    int offset = 0;

    // remove prefix.
    String prefixFound = "";
    if (prefixesToRemove != null) {
      for (String prefix : prefixesToRemove) {
        int nextSegmentOffset = matchPathSegments(path, offset, prefix, false);
        if (nextSegmentOffset >= 0) {
          prefixFound = prefix;
          offset = nextSegmentOffset;
          break;
        }
      }
    }

    // check if the next path segment matches the cluster name.
    String clusterNameFound = "";
    if (clusterName != null) {
      int nextSegmentOffset = matchPathSegments(path, offset, clusterName, true);
      if (nextSegmentOffset >= 0) {
        clusterNameFound = clusterName;
        offset = nextSegmentOffset;
      }
    }

    // if there are at least 2 path segments (*/*) after the current position,
    // test if the last segment is a sub-resource
    RestUtils.SubResource subResource = null;
    int lastSlashOffset = path.lastIndexOf('/');
    if (lastSlashOffset > offset) {
      try {
        subResource = RestUtils.SubResource.valueOf(path.substring(lastSlashOffset + 1));
      } catch (IllegalArgumentException e) {
        // nothing to do.
      }
    }

    // the operationOrBlobId is the part in between the prefix/cluster and sub-resource,
    // if these optional path segments exist.
    String operationOrBlobId = path.substring(offset, subResource == null ? path.length() : lastSlashOffset);
    if ((operationOrBlobId.isEmpty() || operationOrBlobId.equals("/")) && restRequest.getArgs()
        .containsKey(RestUtils.Headers.BLOB_ID)) {
      operationOrBlobId = restRequest.getArgs().get(RestUtils.Headers.BLOB_ID).toString();
    }

    return new RequestPath(prefixFound, clusterNameFound, operationOrBlobId, subResource);
  }

  RequestPath(String prefix, String clusterName, String operationOrBlobId, RestUtils.SubResource subResource) {
    this.prefix = prefix;
    this.clusterName = clusterName;
    this.operationOrBlobId = operationOrBlobId;
    this.subResource = subResource;
  }

  /**
   * @return the path prefix, or an empty string if the path did not start with a recognized prefix.
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * @returno the cluster name, or an empty string if the path did not include the recognized cluster name.
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * @return the extracted operation type or blob ID from the request.
   * @param stripLeadingSlash {@code true} to remove the leading slash, if present.
   */
  public String getOperationOrBlobId(boolean stripLeadingSlash) {
    if (stripLeadingSlash) {
      if (operationOrBlobIdWithoutLeadingSlash == null) {
        operationOrBlobIdWithoutLeadingSlash =
            operationOrBlobId.startsWith("/") ? operationOrBlobId.substring(1) : operationOrBlobId;
      }
      return operationOrBlobIdWithoutLeadingSlash;
    } else {
      return operationOrBlobId;
    }
  }

  /**
   * @return a {@link RestUtils.SubResource}, or {@code null} if no sub-resource was found in the request path.
   */
  public RestUtils.SubResource getSubResource() {
    return subResource;
  }

  /**
   * This will check if the request path matches the specified operation. This will check that that the first one or
   * more path segments in {@link #getOperationOrBlobId(boolean)} match the path segments in {@code operation}. For example,
   * {@code /op} or {@code /op/sub} will match the operation {@code op}.
   * @param operation the operation name to check the path against.
   * @return {@code true} if this request path matches the specified operation.
   */
  public boolean matchesOperation(String operation) {
    return matchPathSegments(operationOrBlobId, 0, operation, true) >= 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RequestPath that = (RequestPath) o;
    return prefix.equals(that.prefix) && clusterName.equals(that.clusterName) && operationOrBlobId.equals(
        that.operationOrBlobId) && subResource == that.subResource;
  }

  @Override
  public String toString() {
    return "RequestPath{" + "prefix='" + prefix + '\'' + ", clusterName='" + clusterName + '\''
        + ", operationOrBlobId='" + operationOrBlobId + '\'' + ", subResource=" + subResource + '}';
  }

  /**
   * A helper method to search for segments of a request path. This method checks if the region of {@code path} starting
   * at {@code pathOffset} matches the path segments in {@code segments}. This will only consider it a match if the
   * a new segment (following slash) or the end of the path immediately follows {@code segments}.
   * @param path the path to match the segments against.
   * @param pathOffset the start offset in {@code path} to match against.
   * @param segments the segments to search for. This method will ignore any leading or trailing slashes in this string.
   * @param ignoreCase if {@code true}, ignore case when comparing characters.
   * @return the offset of the character following {@code segments} in {@code path},
   *         or {@code -1} if the segments to search for were not found.
   */
  private static int matchPathSegments(String path, int pathOffset, String segments, boolean ignoreCase) {
    // start the search past the leading slash, if one exists
    pathOffset += path.startsWith("/", pathOffset) ? 1 : 0;
    // for search purposes we strip off leading and trailing slashes from the segments to search for.
    int segmentsStartOffset = segments.startsWith("/") ? 1 : 0;
    int segmentsLength = Math.max(segments.length() - segmentsStartOffset - (segments.endsWith("/") ? 1 : 0), 0);
    int nextSegmentOffset = -1;
    if (path.regionMatches(ignoreCase, pathOffset, segments, segmentsStartOffset, segmentsLength)) {
      int nextCharOffset = pathOffset + segmentsLength;
      // this is only a match if the end of the path was reached or the next character is a slash (starts new segment).
      if (nextCharOffset == path.length() || path.charAt(nextCharOffset) == '/') {
        nextSegmentOffset = nextCharOffset;
      }
    }
    return nextSegmentOffset;
  }
}
