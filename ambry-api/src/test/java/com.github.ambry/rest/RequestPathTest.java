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

import com.github.ambry.frontend.Operations;
import com.github.ambry.utils.TestUtils;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.json.JSONObject;
import org.junit.Test;

import static com.github.ambry.router.GetBlobOptions.*;
import static org.junit.Assert.*;


/**
 * Test {@link RequestPath}.
 */
public class RequestPathTest {

  private static final String CLUSTER_NAME = "Ambry-TesT";

  /**
   * Tests {@link RequestPath#getOperationOrBlobId(boolean)}.
   */
  @Test
  public void testRequestPathParsing() {
    String baseId = "expectedOpOrId";
    String queryString = "?queryParam1=queryValue1&queryParam2=queryParam2=queryValue2";
    String securePath = "secure-path";
    List<String> prefixesToRemove = Arrays.asList("media", "toRemove/multipart", securePath);
    String blobId = TestUtils.getRandomString(10);
    String blobIdQuery = RestUtils.Headers.BLOB_ID + "=" + blobId;

    Map<String, String> prefixesToTest = new HashMap<>();
    prefixesToTest.put("", "");
    prefixesToRemove.forEach(prefix -> prefixesToTest.put("/" + prefix, prefix));
    Map<String, String> clusterNameSegmentsToTest = new HashMap<>();
    clusterNameSegmentsToTest.put("", "");
    clusterNameSegmentsToTest.put("/" + CLUSTER_NAME.toLowerCase(), CLUSTER_NAME);
    clusterNameSegmentsToTest.put("/" + CLUSTER_NAME.toUpperCase(), CLUSTER_NAME);
    List<String> opsOrIdsToTest = Arrays.asList("/", "/" + baseId, "/" + baseId + "/random/extra",
        "/" + RestUtils.SIGNED_ID_PREFIX + "/" + baseId, "/media" + baseId, "/" + CLUSTER_NAME + baseId,
        "/" + RestUtils.SubResource.BlobInfo);
    // construct test cases
    prefixesToTest.forEach((prefixToUse, expectedPrefix) -> {
      clusterNameSegmentsToTest.forEach((clusterNameToUse, expectedClusterName) -> {
        opsOrIdsToTest.forEach(opOrId -> {
          String path = prefixToUse + clusterNameToUse + opOrId;
          // the uri as is (e.g. "/expectedOp).
          parseRequestPathAndVerify(path, prefixesToRemove, CLUSTER_NAME,
              new RequestPath(expectedPrefix, expectedClusterName, opOrId, opOrId, null, NO_BLOB_SEGMENT_IDX_SPECIFIED),
              null);

          // the uri with a query string (e.g. "/expectedOp?param=value").
          parseRequestPathAndVerify(path + queryString, prefixesToRemove, CLUSTER_NAME,
              new RequestPath(expectedPrefix, expectedClusterName, opOrId, opOrId, null, NO_BLOB_SEGMENT_IDX_SPECIFIED),
              null);
          if (opOrId.length() > 1) {
            for (RestUtils.SubResource subResource : RestUtils.SubResource.values()) {
              String subResourceStr = "/" + subResource.name();
              //if subResource is "Segment", generate a random integer segment to add to the end of the uri
              int segment = NO_BLOB_SEGMENT_IDX_SPECIFIED;
              if (subResource.name().equals("Segment")) {
                segment = new Random().nextInt(Integer.MAX_VALUE);
                subResourceStr = subResourceStr + "/" + segment;
              }
              parseRequestPathAndVerify(path + subResourceStr, prefixesToRemove, CLUSTER_NAME,
                  new RequestPath(expectedPrefix, expectedClusterName, opOrId + subResourceStr, opOrId, subResource,
                      segment), null);
              parseRequestPathAndVerify(path + subResourceStr + queryString, prefixesToRemove, CLUSTER_NAME,
                  new RequestPath(expectedPrefix, expectedClusterName, opOrId + subResourceStr, opOrId, subResource,
                      segment), null);
            }
          } else {
            parseRequestPathAndVerify(path + "?" + blobIdQuery, prefixesToRemove, CLUSTER_NAME,
                new RequestPath(expectedPrefix, expectedClusterName, opOrId, blobId, null,
                    NO_BLOB_SEGMENT_IDX_SPECIFIED), null);
            parseRequestPathAndVerify(path + queryString + "&" + blobIdQuery, prefixesToRemove, CLUSTER_NAME,
                new RequestPath(expectedPrefix, expectedClusterName, opOrId, blobId, null,
                    NO_BLOB_SEGMENT_IDX_SPECIFIED), null);
          }
        });
      });
    });
  }

  /**
   * Test the behavior of {@link RequestPath#matchesOperation(String)}.
   */
  @Test
  public void testOperationMatching() {
    String operation = "opToMatch";
    Stream.of(new RequestPath("", "", "", "/" + operation + "/abc/def", null, NO_BLOB_SEGMENT_IDX_SPECIFIED),
        new RequestPath("", "", "", "/" + operation, null, NO_BLOB_SEGMENT_IDX_SPECIFIED),
        new RequestPath("", "", "", "/" + operation + "/", null, NO_BLOB_SEGMENT_IDX_SPECIFIED))
        .forEach(requestPath -> {
          assertTrue("Operation should match", requestPath.matchesOperation(operation));
          assertTrue("Operation should match", requestPath.matchesOperation("/" + operation));
          assertTrue("Operation should match", requestPath.matchesOperation("/" + operation + "/"));
          assertFalse("Operation should not match", requestPath.matchesOperation("notOpToMatch"));
        });
  }

  /**
   * Test the behavior of {@link RequestPath#getOperationOrBlobId(boolean)}.
   */
  @Test
  public void testStripLeadingSlash() {
    RequestPath requestPath = new RequestPath("", "", "", "/opToMatch/abc", null, NO_BLOB_SEGMENT_IDX_SPECIFIED);
    assertEquals("Should strip leading slash", "opToMatch/abc", requestPath.getOperationOrBlobId(true));
    assertEquals("Should not strip leading slash", "/opToMatch/abc", requestPath.getOperationOrBlobId(false));
  }

  /**
   * Tests that the expected exception is thrown when a Segment request comes in without a valid
   * integer index
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void testBadSegmentSubResourceNumber() throws UnsupportedEncodingException, URISyntaxException {
    String requestPath = "/mediaExpectedOpOrId/Segment/notAnInteger";
    List<String> prefixesToRemove = new ArrayList<>();
    RestRequest restRequest = RestUtilsTest.createRestRequest(RestMethod.GET, requestPath, null);
    try {
      RequestPath.parse(restRequest, prefixesToRemove, CLUSTER_NAME);
      fail();
    } catch (RestServiceException e) {
      assertEquals(RestServiceErrorCode.BadRequest, e.getErrorCode());
    }
  }

  /**
   * Test that blob id string (with prefix and sub-resource) is specified in request header. The {@link RequestPath#parse(RestRequest, List, String)}
   * should correctly remove prefix, sub-resource and use pure blob id to update request header.
   */
  @Test
  public void testBlobIdInRequestHeader() {
    String prefixToRemove = "media";
    JSONObject headers = new JSONObject();
    String blobId = TestUtils.getRandomString(10);
    // we purposely add prefix and sub resource into blob id string in request header
    headers.putOpt(RestUtils.Headers.BLOB_ID,
        prefixToRemove + "/" + CLUSTER_NAME + "/" + blobId + "/" + RestUtils.SubResource.Segment);

    // test when operationOrBlobId is specified in url, then this operationOrBlobId should show up in expected RequestPath
    String path = prefixToRemove + "/" + CLUSTER_NAME + "/" + Operations.GET_SIGNED_URL;
    RequestPath expectedRequestPath =
        new RequestPath(prefixToRemove, CLUSTER_NAME, "/" + Operations.GET_SIGNED_URL, "/" + Operations.GET_SIGNED_URL,
            null, NO_BLOB_SEGMENT_IDX_SPECIFIED);
    parseRequestPathAndVerify(path, Collections.singletonList(prefixToRemove), CLUSTER_NAME, expectedRequestPath,
        headers);

    // test when operationOrBlobId is not specified in url and blob id is provided in request header, we verify that
    // RequestPath will take the updated "x-ambry-blob-id" header as operationOrBlobId.
    path = prefixToRemove + "/" + CLUSTER_NAME;
    // the operationOrBlobId in expected request path should be pure blob id without any prefix or SubResource.
    expectedRequestPath =
        new RequestPath(prefixToRemove, CLUSTER_NAME, "", "/" + blobId, null, NO_BLOB_SEGMENT_IDX_SPECIFIED);
    parseRequestPathAndVerify(path, Collections.singletonList(prefixToRemove), CLUSTER_NAME, expectedRequestPath,
        headers);
  }

  /**
   * Form the request, call {@link RequestPath#parse}, and check the result.
   * @param requestPath the request path to supply
   * @param prefixesToRemove the prefix list to supply
   * @param clusterName the cluster name to supply
   * @param expected the expected {@link RequestPath}.
   * @param headers any associated headers as a {@link org.json.JSONObject}
   */
  private void parseRequestPathAndVerify(String requestPath, List<String> prefixesToRemove, String clusterName,
      RequestPath expected, JSONObject headers) {
    try {
      RestRequest restRequest = RestUtilsTest.createRestRequest(RestMethod.GET, requestPath, headers);
      RequestPath actual = RequestPath.parse(restRequest, prefixesToRemove, clusterName);
      assertEquals(
          "Unexpected result for testRequestPath(" + requestPath + ", " + prefixesToRemove + ", " + clusterName + ")",
          expected, actual);
    } catch (UnsupportedEncodingException | URISyntaxException | RestServiceException e) {
      throw new RuntimeException(e);
    }
  }
}