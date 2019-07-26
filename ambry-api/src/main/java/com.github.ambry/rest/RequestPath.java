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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.router.GetBlobOptions.*;


public class RequestPath {
  private final String prefix;
  private final String clusterName;
  private final String operationOrBlobId;
  private final SubResource subResource;
  private final int blobSegmentIdx;
  private final String pathAfterPrefixes;
  private String operationOrBlobIdWithoutLeadingSlash = null;
  private static char PATH_SEPARATOR_CHAR = '/';
  private static String PATH_SEPARATOR_STRING = String.valueOf(PATH_SEPARATOR_CHAR);
  private static String SEGMENT = SubResource.Segment.toString();

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
  public static RequestPath parse(RestRequest restRequest, List<String> prefixesToRemove, String clusterName)
      throws RestServiceException {
    String blobIdHeader = RestUtils.getHeader(restRequest.getArgs(), Headers.BLOB_ID, false);
    if (blobIdHeader != null) {
      // If blob id is specified in the rest request header and it has cluster name prefix, we remove its prefix, subResource
      // and use this id (pure blob id with no prefix or subResource) to update "x-ambry-blob-id" header. The purpose is
      // to ensure IdConverter and IdSigningService can receive a valid blob id and correctly identify it.
      String blobIdStr =
          parse(blobIdHeader, Collections.emptyMap(), prefixesToRemove, clusterName).getOperationOrBlobId(false);
      restRequest.setArg(Headers.BLOB_ID, blobIdStr);
    }
    return parse(restRequest.getPath(), restRequest.getArgs(), prefixesToRemove, clusterName);
  }

  /**
   * This is similar to {@link #parse(RestRequest, List, String)} but allows usage with arbitrary paths that are not
   * part of a {@link RestRequest}.
   *
   * @param path the path
   * @param args a map containing any request arguments. This might be used to look for blob IDs supplied via query
   *             parameters or headers.
   * @param prefixesToRemove the list of prefixes that could precede the other parts of the URL. Removal of
   *                         prefixes earlier in the list will be preferred to removal of the ones later in the list.
   * @param clusterName the cluster name to recognize and handle when parsing the URL. Case is ignored when matching
   *                    this path segment.
   * @return a {@link RequestPath} object.
   */
  public static RequestPath parse(String path, Map<String, Object> args, List<String> prefixesToRemove,
      String clusterName) throws RestServiceException {
    int offset = 0;

    // remove prefix.
    String prefixFound = "";
    if (prefixesToRemove != null) {
      for (String prefix : prefixesToRemove) {
        int nextPathSegmentOffset = matchPathSegments(path, offset, prefix, false);
        if (nextPathSegmentOffset >= 0) {
          prefixFound = prefix;
          offset = nextPathSegmentOffset;
          break;
        }
      }
    }

    // check if the next path segment matches the cluster name.
    String clusterNameFound = "";
    if (clusterName != null) {
      int nextPathSegmentOffset = matchPathSegments(path, offset, clusterName, true);
      if (nextPathSegmentOffset >= 0) {
        clusterNameFound = clusterName;
        offset = nextPathSegmentOffset;
      }
    }

    // some use cases just require the path with the prefix and cluster name removed.
    String pathAfterPrefixes = path.substring(offset);

    // if there are at least 2 path segments (*/*) after the current position,
    // test if the last segment is a sub-resource
    SubResource subResource = null;
    int lastSlashOffset = path.lastIndexOf(PATH_SEPARATOR_CHAR);
    int blobSegmentIdx = NO_BLOB_SEGMENT_IDX_SPECIFIED;
    boolean isSegment = false;
    if (lastSlashOffset > offset) {
      try {
        String[] fields = path.split(PATH_SEPARATOR_STRING);
        //if subresource is a Segment subresource, it is expected that 'Segment' is followed
        //by a path separator and an integer number indicating the desired blob segment by
        //segment index, e.g. "Segment/32"
        if (SEGMENT.equals(fields[fields.length - 2])) {
          isSegment = true;
          subResource = SubResource.Segment;
          blobSegmentIdx = Integer.valueOf(fields[fields.length - 1]);
          lastSlashOffset = path.lastIndexOf(PATH_SEPARATOR_CHAR, lastSlashOffset - 1);
        } else {
          subResource = SubResource.valueOf(path.substring(lastSlashOffset + 1));
        }
      } catch (IllegalArgumentException e) {
        if (isSegment) {
          throw new RestServiceException("Segment index given is not an integer", RestServiceErrorCode.BadRequest);
        }
        //otherwise, nothing to do
      }
    }

    // the operationOrBlobId is the part in between the prefix/cluster and sub-resource,
    // if these optional path segments exist.
    String operationOrBlobId = path.substring(offset, subResource == null ? path.length() : lastSlashOffset);
    if ((operationOrBlobId.isEmpty() || operationOrBlobId.equals(PATH_SEPARATOR_STRING)) && args.containsKey(
        RestUtils.Headers.BLOB_ID)) {
      operationOrBlobId = args.get(RestUtils.Headers.BLOB_ID).toString();
    }

    return new RequestPath(prefixFound, clusterNameFound, pathAfterPrefixes, operationOrBlobId, subResource,
        blobSegmentIdx);
  }

  RequestPath(String prefix, String clusterName, String pathAfterPrefixes, String operationOrBlobId,
      SubResource subResource, int blobSegmentIdx) {
    this.prefix = prefix;
    this.clusterName = clusterName;
    this.pathAfterPrefixes = pathAfterPrefixes;
    this.operationOrBlobId = operationOrBlobId;
    this.subResource = subResource;
    this.blobSegmentIdx = blobSegmentIdx;
  }

  /**
   * @return the path prefix, or an empty string if the path did not start with a recognized prefix.
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * @return the cluster name, or an empty string if the path did not include the recognized cluster name.
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * For use cases that only require prefixes to be removed from a request path but do not need special handling for
   * blob IDs and sub-resources.
   * @return the path segments starting after the prefix and cluster name, if either is present.
   */
  public String getPathAfterPrefixes() {
    return pathAfterPrefixes;
  }

  /**
   * @return the extracted operation type or blob ID from the request.
   * @param stripLeadingSlash {@code true} to remove the leading slash, if present.
   */
  public String getOperationOrBlobId(boolean stripLeadingSlash) {
    if (stripLeadingSlash) {
      if (operationOrBlobIdWithoutLeadingSlash == null) {
        operationOrBlobIdWithoutLeadingSlash =
            operationOrBlobId.startsWith(PATH_SEPARATOR_STRING) ? operationOrBlobId.substring(1) : operationOrBlobId;
      }
      return operationOrBlobIdWithoutLeadingSlash;
    } else {
      return operationOrBlobId;
    }
  }

  /**
   * @return a {@link RestUtils.SubResource}, or {@code null} if no sub-resource was found in the request path.
   */
  public SubResource getSubResource() {
    return subResource;
  }

  /**
   * @return blob segment index for segmented blobs, or NO_BLOB_SEGMENT_IDX_SPECIFIED for non-segmented blobs
   */
  public int getBlobSegmentIdx() {
    return blobSegmentIdx;
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
        that.operationOrBlobId) && subResource == that.subResource && blobSegmentIdx == that.blobSegmentIdx;
  }

  @Override
  public String toString() {
    return "RequestPath{" + "prefix='" + prefix + '\'' + ", clusterName='" + clusterName + '\''
        + ", operationOrBlobId='" + operationOrBlobId + '\'' + ", subResource=" + subResource + ", blobSegmentIdx="
        + blobSegmentIdx + '}';
  }

  /**
   * A helper method to search for pathSegments of a request path. This method checks if the region of {@code path} starting
   * at {@code pathOffset} matches the path pathSegments in {@code pathSegments}. This will only consider it a match if the
   * a new segment (following slash) or the end of the path immediately follows {@code pathSegments}.
   * @param path the path to match the pathSegments against.
   * @param pathOffset the start offset in {@code path} to match against.
   * @param pathSegments the pathSegments to search for. This method will ignore any leading or trailing slashes in this string.
   * @param ignoreCase if {@code true}, ignore case when comparing characters.
   * @return the offset of the character following {@code pathSegments} in {@code path},
   *         or {@code -1} if the pathSegments to search for were not found.
   */
  private static int matchPathSegments(String path, int pathOffset, String pathSegments, boolean ignoreCase) {
    // start the search past the leading slash, if one exists
    pathOffset += path.startsWith("/", pathOffset) ? 1 : 0;
    // for search purposes we strip off leading and trailing slashes from the pathSegments to search for.
    int pathSegmentsStartOffset = pathSegments.startsWith(PATH_SEPARATOR_STRING) ? 1 : 0;
    int pathSegmentsLength = Math.max(
        pathSegments.length() - pathSegmentsStartOffset - (pathSegments.endsWith(PATH_SEPARATOR_STRING) ? 1 : 0), 0);
    int nextPathSegmentOffset = -1;
    if (path.regionMatches(ignoreCase, pathOffset, pathSegments, pathSegmentsStartOffset, pathSegmentsLength)) {
      int nextCharOffset = pathOffset + pathSegmentsLength;
      // this is only a match if the end of the path was reached or the next character is a slash (starts new segment).
      if (nextCharOffset == path.length() || path.charAt(nextCharOffset) == PATH_SEPARATOR_CHAR) {
        nextPathSegmentOffset = nextCharOffset;
      }
    }
    return nextPathSegmentOffset;
  }
}
