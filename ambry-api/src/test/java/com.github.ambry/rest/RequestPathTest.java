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

import com.github.ambry.utils.UtilsTest;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test {@link RequestPath}.
 */
public class RequestPathTest {

  /**
   * Tests {@link RequestPath#getOperationOrBlobId(boolean)}.
   */
  @Test
  public void testRequestPathParsing() {
    String baseId = "expectedOpOrId";
    String queryString = "?queryParam1=queryValue1&queryParam2=queryParam2=queryValue2";
    String securePath = "secure-path";
    List<String> prefixesToRemove = Arrays.asList("media", "toRemove/multipart", securePath);
    String clusterName = "Ambry-TesT";
    String blobId = UtilsTest.getRandomString(10);
    String blobIdQuery = RestUtils.Headers.BLOB_ID + "=" + blobId;

    Map<String, String> prefixesToTest = new HashMap<>();
    prefixesToTest.put("", "");
    prefixesToRemove.forEach(prefix -> prefixesToTest.put("/" + prefix, prefix));
    Map<String, String> clusterNameSegmentsToTest = new HashMap<>();
    clusterNameSegmentsToTest.put("", "");
    clusterNameSegmentsToTest.put("/" + clusterName.toLowerCase(), clusterName);
    clusterNameSegmentsToTest.put("/" + clusterName.toUpperCase(), clusterName);
    List<String> opsOrIdsToTest = Arrays.asList("/", "/" + baseId, "/" + baseId + "/random/extra",
        "/" + RestUtils.SIGNED_ID_PREFIX + "/" + baseId, "/media" + baseId, "/" + clusterName + baseId,
        "/" + RestUtils.SubResource.BlobInfo);
    // construct test cases
    prefixesToTest.forEach((prefixToUse, expectedPrefix) -> {
      clusterNameSegmentsToTest.forEach((clusterNameToUse, expectedClusterName) -> {
        opsOrIdsToTest.forEach(opOrId -> {
          String path = prefixToUse + clusterNameToUse + opOrId;
          // the uri as is (e.g. "/expectedOp).
          parseRequestPathAndVerify(path, prefixesToRemove, clusterName,
              new RequestPath(expectedPrefix, expectedClusterName, opOrId, opOrId, null));

          // the uri with a query string (e.g. "/expectedOp?param=value").
          parseRequestPathAndVerify(path + queryString, prefixesToRemove, clusterName,
              new RequestPath(expectedPrefix, expectedClusterName, opOrId, opOrId, null));
          if (opOrId.length() > 1) {
            for (RestUtils.SubResource subResource : RestUtils.SubResource.values()) {
              String subResourceStr = "/" + subResource.name();
              parseRequestPathAndVerify(path + subResourceStr, prefixesToRemove, clusterName,
                  new RequestPath(expectedPrefix, expectedClusterName, opOrId + subResourceStr, opOrId, subResource));
              parseRequestPathAndVerify(path + subResourceStr + queryString, prefixesToRemove, clusterName,
                  new RequestPath(expectedPrefix, expectedClusterName, opOrId + subResourceStr, opOrId, subResource));
            }
          } else {
            parseRequestPathAndVerify(path + "?" + blobIdQuery, prefixesToRemove, clusterName,
                new RequestPath(expectedPrefix, expectedClusterName, opOrId, blobId, null));
            parseRequestPathAndVerify(path + queryString + "&" + blobIdQuery, prefixesToRemove, clusterName,
                new RequestPath(expectedPrefix, expectedClusterName, opOrId, blobId, null));
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
    Stream.of(new RequestPath("", "", "", "/" + operation + "/abc/def", null),
        new RequestPath("", "", "", "/" + operation, null), new RequestPath("", "", "", "/" + operation + "/", null))
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
    RequestPath requestPath = new RequestPath("", "", "", "/opToMatch/abc", null);
    assertEquals("Should strip leading slash", "opToMatch/abc", requestPath.getOperationOrBlobId(true));
    assertEquals("Should not strip leading slash", "/opToMatch/abc", requestPath.getOperationOrBlobId(false));
  }

  /**
   * Form the request, call {@link RequestPath#parse}, and check the result.
   * @param requestPath the request path to supply
   * @param prefixesToRemove the prefix list to supply
   * @param clusterName the cluster name to supply
   * @param expected the expected {@link RequestPath}.
   */
  private void parseRequestPathAndVerify(String requestPath, List<String> prefixesToRemove, String clusterName,
      RequestPath expected) {
    try {
      RestRequest restRequest = RestUtilsTest.createRestRequest(RestMethod.GET, requestPath, null);
      RequestPath actual = RequestPath.parse(restRequest, prefixesToRemove, clusterName);
      assertEquals(
          "Unexpected result for testRequestPath(" + requestPath + ", " + prefixesToRemove + ", " + clusterName + ")",
          expected, actual);
    } catch (UnsupportedEncodingException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}