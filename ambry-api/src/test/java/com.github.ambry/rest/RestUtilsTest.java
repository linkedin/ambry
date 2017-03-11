/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link RestUtils}.
 */
public class RestUtilsTest {
  private static final Random RANDOM = new Random();
  private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

  /**
   * Tests building of {@link BlobProperties} given good input (all arguments in the number and format expected).
   * @throws Exception
   */
  @Test
  public void getBlobPropertiesGoodInputTest() throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeaders(headers, Long.toString(RANDOM.nextInt(10000)), Long.toString(RANDOM.nextInt(10000)),
        Boolean.toString(RANDOM.nextBoolean()), generateRandomString(10), "image/gif", generateRandomString(10));
    verifyBlobPropertiesConstructionSuccess(headers);
  }

  /**
   * Tests building of {@link BlobProperties} given varied input. Some input fail (tests check the correct error code),
   * some should succeed (check for default values expected).
   * @throws Exception
   */
  @Test
  public void getBlobPropertiesVariedInputTest() throws Exception {
    String contentLength = Long.toString(RANDOM.nextInt(10000));
    String ttl = Long.toString(RANDOM.nextInt(10000));
    String isPrivate = Boolean.toString(RANDOM.nextBoolean());
    String serviceId = generateRandomString(10);
    String contentType = "image/gif";
    String ownerId = generateRandomString(10);

    JSONObject headers;
    // failure required.
    // content length missing.
    headers = new JSONObject();
    setAmbryHeaders(headers, null, ttl, isPrivate, serviceId, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.MissingArgs);
    // content length null.
    headers = new JSONObject();
    setAmbryHeaders(headers, null, ttl, isPrivate, serviceId, contentType, ownerId);
    headers.put(RestUtils.Headers.BLOB_SIZE, JSONObject.NULL);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // content length not a number.
    headers = new JSONObject();
    setAmbryHeaders(headers, "NaN", ttl, isPrivate, serviceId, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // content length < 0.
    headers = new JSONObject();
    setAmbryHeaders(headers, "-1", ttl, isPrivate, serviceId, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // ttl not a number.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, "NaN", isPrivate, serviceId, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // ttl < -1.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, "-2", isPrivate, serviceId, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // isPrivate not true or false.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, "!(true||false)", serviceId, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // serviceId missing.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, null, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.MissingArgs);
    // serviceId null.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, null, contentType, ownerId);
    headers.put(RestUtils.Headers.SERVICE_ID, JSONObject.NULL);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // contentType missing.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, null, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.MissingArgs);
    // contentType null.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, null, ownerId);
    headers.put(RestUtils.Headers.AMBRY_CONTENT_TYPE, JSONObject.NULL);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // too many values for some headers.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, contentType, ownerId);
    tooManyValuesTest(headers, RestUtils.Headers.BLOB_SIZE);
    tooManyValuesTest(headers, RestUtils.Headers.TTL);
    tooManyValuesTest(headers, RestUtils.Headers.PRIVATE);

    // no failures.
    // ttl missing. Should be infinite time by default.
    // isPrivate missing. Should be false by default.
    // ownerId missing.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, null, null, serviceId, contentType, null);
    verifyBlobPropertiesConstructionSuccess(headers);

    // ttl null.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, null, isPrivate, serviceId, contentType, ownerId);
    headers.put(RestUtils.Headers.TTL, JSONObject.NULL);
    verifyBlobPropertiesConstructionSuccess(headers);

    // isPrivate null.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, contentType, ownerId);
    headers.put(RestUtils.Headers.PRIVATE, JSONObject.NULL);
    verifyBlobPropertiesConstructionSuccess(headers);

    // ownerId null.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, contentType, null);
    headers.put(RestUtils.Headers.OWNER_ID, JSONObject.NULL);
    verifyBlobPropertiesConstructionSuccess(headers);
  }

  /**
   * Tests building of user metadata.
   * @throws Exception
   */
  @Test
  public void getUserMetadataTest() throws Exception {
    byte[] usermetadata = RestUtils.buildUsermetadata(new HashMap<String, Object>());
    assertArrayEquals("Unexpected user metadata", new byte[0], usermetadata);
  }

  /**
   * Tests building of User Metadata with good input
   * @throws Exception
   */
  @Test
  public void getUserMetadataGoodInputTest() throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeaders(headers, Long.toString(RANDOM.nextInt(10000)), Long.toString(RANDOM.nextInt(10000)),
        Boolean.toString(RANDOM.nextBoolean()), generateRandomString(10), "image/gif", generateRandomString(10));
    Map<String, String> userMetadata = new HashMap<String, String>();
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
    setUserMetadataHeaders(headers, userMetadata);
    verifyUserMetadataConstructionSuccess(headers, userMetadata);
  }

  /**
   * Tests building of User Metadata when the {@link RestRequest} contains an arg with name
   * {@link RestUtils.MultipartPost#USER_METADATA_PART}.
   * @throws Exception
   */
  @Test
  public void getUserMetadataWithUserMetadataArgTest() throws Exception {
    byte[] original = new byte[100];
    RANDOM.nextBytes(original);
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.MultipartPost.USER_METADATA_PART, ByteBuffer.wrap(original));
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    byte[] rcvd = RestUtils.buildUsermetadata(restRequest.getArgs());
    assertArrayEquals("Received user metadata does not match with original", original, rcvd);
  }

  /**
   * Tests building of User Metadata with unusual input
   * @throws Exception
   */
  @Test
  public void getUserMetadataUnusualInputTest() throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeaders(headers, Long.toString(RANDOM.nextInt(10000)), Long.toString(RANDOM.nextInt(10000)),
        Boolean.toString(RANDOM.nextBoolean()), generateRandomString(10), "image/gif", generateRandomString(10));
    Map<String, String> userMetadata = new HashMap<String, String>();
    String key1 = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1";
    userMetadata.put(key1, "value1");
    // no valid prefix
    userMetadata.put("key2", "value2_1");
    // valid prefix as suffix
    userMetadata.put("key3" + RestUtils.Headers.USER_META_DATA_HEADER_PREFIX, "value3");
    // empty value
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key4", "");
    setUserMetadataHeaders(headers, userMetadata);

    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    byte[] userMetadataByteArray = RestUtils.buildUsermetadata(restRequest.getArgs());
    Map<String, String> userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);

    // key1, output should be same as input
    String key = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1";
    assertTrue(key + " not found in user metadata map ", userMetadataMap.containsKey(key));
    assertEquals("Value for " + key + " didnt match input value ", userMetadata.get(key), userMetadataMap.get(key));

    // key4 should match
    key = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key4";
    assertTrue(key + " not found in user metadata map ", userMetadataMap.containsKey(key));
    assertEquals("Value for " + key + " didnt match input value ", userMetadata.get(key), userMetadataMap.get(key));

    assertEquals("Size of map unexpected ", 2, userMetadataMap.size());
  }

  /**
   * Tests building of User Metadata with empty input
   * @throws Exception
   */
  @Test
  public void getEmptyUserMetadataInputTest() throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeaders(headers, Long.toString(RANDOM.nextInt(10000)), Long.toString(RANDOM.nextInt(10000)),
        Boolean.toString(RANDOM.nextBoolean()), generateRandomString(10), "image/gif", generateRandomString(10));
    Map<String, String> userMetadata = new HashMap<String, String>();
    setUserMetadataHeaders(headers, userMetadata);

    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    byte[] userMetadataByteArray = RestUtils.buildUsermetadata(restRequest.getArgs());
    Map<String, String> userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);
  }

  /**
   * Tests deserializing user metadata from byte array
   * @throws Exception
   */
  @Test
  public void getUserMetadataFromByteArrayComplexTest() throws Exception {

    Map<String, String> userMetadataMap = null;
    // user metadata of size 1 byte
    byte[] userMetadataByteArray = new byte[1];
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // user metadata with just the version
    userMetadataByteArray = new byte[4];
    ByteBuffer byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // user metadata with wrong version
    userMetadataByteArray = new byte[4];
    byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 3);
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // 0 sized user metadata
    userMetadataByteArray = new byte[12];
    byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    byteBuffer.putInt(4);
    byteBuffer.putInt(0);
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // wrong size
    userMetadataByteArray = new byte[36];
    byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    String key = "key1";
    byte[] keyInBytes = key.getBytes(StandardCharsets.US_ASCII);
    int keyLength = keyInBytes.length;
    byteBuffer.putInt(21);
    byteBuffer.putInt(1);
    byteBuffer.putInt(keyLength);
    byteBuffer.put(keyInBytes);
    String value = "value1";
    byte[] valueInBytes = value.getBytes(StandardCharsets.US_ASCII);
    int valueLength = valueInBytes.length;
    byteBuffer.putInt(valueLength);
    byteBuffer.put(valueInBytes);
    Crc32 crc32 = new Crc32();
    crc32.update(userMetadataByteArray, 0, userMetadataByteArray.length - 8);
    byteBuffer.putLong(crc32.getValue());
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // wrong total number of entries
    userMetadataByteArray = new byte[36];
    byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    byteBuffer.putInt(22);
    byteBuffer.putInt(2);
    byteBuffer.putInt(keyLength);
    byteBuffer.put(keyInBytes);
    byteBuffer.putInt(valueLength);
    byteBuffer.put(valueInBytes);
    crc32 = new Crc32();
    crc32.update(userMetadataByteArray, 0, userMetadataByteArray.length - 8);
    byteBuffer.putLong(crc32.getValue());
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // diff key length
    userMetadataByteArray = new byte[36];
    byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    byteBuffer.putInt(22);
    byteBuffer.putInt(1);
    byteBuffer.putInt(keyLength + 1);
    byteBuffer.put(keyInBytes);
    byteBuffer.putInt(valueLength);
    byteBuffer.put(valueInBytes);
    crc32 = new Crc32();
    crc32.update(userMetadataByteArray, 0, userMetadataByteArray.length - 8);
    byteBuffer.putLong(crc32.getValue());
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // diff value length
    userMetadataByteArray = new byte[36];
    byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    byteBuffer.putInt(22);
    byteBuffer.putInt(1);
    byteBuffer.putInt(keyLength);
    byteBuffer.put(keyInBytes);
    byteBuffer.putInt(valueLength + 1);
    byteBuffer.put(valueInBytes);
    crc32 = new Crc32();
    crc32.update(userMetadataByteArray, 0, userMetadataByteArray.length - 8);
    byteBuffer.putLong(crc32.getValue());
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // no crc
    userMetadataByteArray = new byte[36];
    byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    byteBuffer.putInt(22);
    byteBuffer.putInt(1);
    byteBuffer.putInt(keyLength);
    byteBuffer.put(keyInBytes);
    byteBuffer.putInt(valueLength);
    byteBuffer.put(valueInBytes);
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // wrong crc
    userMetadataByteArray = new byte[36];
    byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    byteBuffer.putInt(22);
    byteBuffer.putInt(1);
    byteBuffer.putInt(keyLength);
    byteBuffer.put(keyInBytes);
    byteBuffer.putInt(valueLength);
    byteBuffer.put(valueInBytes);
    crc32 = new Crc32();
    crc32.update(userMetadataByteArray, 0, userMetadataByteArray.length - 8);
    byteBuffer.putLong(crc32.getValue() - 1);
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertNull("UserMetadata should have been null ", userMetadataMap);

    // correct crc
    userMetadataByteArray = new byte[36];
    byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    byteBuffer.putInt(22);
    byteBuffer.putInt(1);
    byteBuffer.putInt(keyLength);
    byteBuffer.put(keyInBytes);
    byteBuffer.putInt(valueLength);
    byteBuffer.put(valueInBytes);
    crc32 = new Crc32();
    crc32.update(userMetadataByteArray, 0, userMetadataByteArray.length - 8);
    byteBuffer.putLong(crc32.getValue());
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertEquals("Sizes don't match ", userMetadataMap.size(), 1);
    assertTrue("User metadata " + RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + key + " not found in user metadata ",
        userMetadataMap.containsKey(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + key));
    assertEquals("User metadata " + RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + key + " value don't match ", value,
        userMetadataMap.get(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + key));
  }

  /**
   * Tests {@link RestUtils#getOperationOrBlobIdFromUri(RestRequest, RestUtils.SubResource, List)}.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void getOperationOrBlobIdFromUriTest() throws JSONException, UnsupportedEncodingException, URISyntaxException {
    String baseId = "expectedOp";
    String queryString = "?queryParam1=queryValue1&queryParam2=queryParam2=queryValue2";
    String[] validIdUris = {"/" + baseId, "/" + baseId + "/random/extra", baseId, baseId + "/random/extra"};
    List<String> prefixesToTestOn = Arrays.asList("", "/media", "/toRemove", "/orNotToRemove");
    List<String> prefixesToRemove = Arrays.asList("/media", "/toRemove");

    // construct test cases
    Map<String, String> testCases = new HashMap<>();
    for (String validIdUri : validIdUris) {
      // the uri as is (e.g. "/expectedOp).
      testCases.put(validIdUri, validIdUri);
      // the uri with a query string (e.g. "/expectedOp?param=value").
      testCases.put(validIdUri + queryString, validIdUri);
      for (RestUtils.SubResource subResource : RestUtils.SubResource.values()) {
        String subResourceStr = "/" + subResource.name();
        // the uri with a sub-resource (e.g. "/expectedOp/BlobInfo").
        testCases.put(validIdUri + subResourceStr, validIdUri);
        // the uri with a sub-resource and query string (e.g. "/expectedOp/BlobInfo?param=value").
        testCases.put(validIdUri + subResourceStr + queryString, validIdUri);
      }
    }

    // test each case on each prefix.
    for (String prefixToTestOn : prefixesToTestOn) {
      for (Map.Entry<String, String> testCase : testCases.entrySet()) {
        String testPath = testCase.getKey();
        // skip the ones with no leading slash if prefix is not "". Otherwise they become -> "/prefixexpectedOp".
        if (prefixToTestOn.isEmpty() || testPath.startsWith("/")) {
          String realTestPath = prefixToTestOn + testPath;
          String expectedOutput = testCase.getValue();
          expectedOutput = prefixesToRemove.contains(prefixToTestOn) ? expectedOutput : prefixToTestOn + expectedOutput;
          RestRequest restRequest = createRestRequest(RestMethod.GET, realTestPath, null);
          assertEquals("Unexpected operation/blob id for: " + realTestPath, expectedOutput,
              RestUtils.getOperationOrBlobIdFromUri(restRequest, RestUtils.getBlobSubResource(restRequest),
                  prefixesToRemove));
        }
      }
    }
  }

  /**
   * Tests {@link RestUtils#getBlobSubResource(RestRequest)}.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void getBlobSubResourceTest() throws JSONException, UnsupportedEncodingException, URISyntaxException {
    // sub resource null
    String queryString = "?queryParam1=queryValue1&queryParam2=queryParam2=queryValue2";
    String[] nullUris = {"/op", "/op/", "/op/invalid", "/op/invalid/", "op", "op/", "op/invalid", "op/invalid/"};
    for (String uri : nullUris) {
      RestRequest restRequest = createRestRequest(RestMethod.GET, uri, null);
      assertNull("There was no sub-resource expected in: " + uri, RestUtils.getBlobSubResource(restRequest));
      // add a sub-resource at the end as part of the query string.
      for (RestUtils.SubResource subResource : RestUtils.SubResource.values()) {
        String fullUri = uri + queryString + subResource;
        restRequest = createRestRequest(RestMethod.GET, fullUri, null);
        assertNull("There was no sub-resource expected in: " + fullUri, RestUtils.getBlobSubResource(restRequest));
      }
    }

    // valid sub resource
    String[] nonNullUris = {"/op/", "/op/random/", "op/", "op/random/"};
    for (String uri : nonNullUris) {
      for (RestUtils.SubResource subResource : RestUtils.SubResource.values()) {
        String fullUri = uri + subResource;
        RestRequest restRequest = createRestRequest(RestMethod.GET, fullUri, null);
        assertEquals("Unexpected sub resource in uri: " + fullUri, subResource,
            RestUtils.getBlobSubResource(restRequest));
        // add a query-string.
        fullUri = uri + subResource + queryString;
        restRequest = createRestRequest(RestMethod.GET, fullUri, null);
        assertEquals("Unexpected sub resource in uri: " + fullUri, subResource,
            RestUtils.getBlobSubResource(restRequest));
      }
    }
  }

  /**
   * Tests {@link RestUtils#toSecondsPrecisionInMs(long)}.
   */
  @Test
  public void toSecondsPrecisionInMsTest() {
    assertEquals(0, RestUtils.toSecondsPrecisionInMs(999));
    assertEquals(1000, RestUtils.toSecondsPrecisionInMs(1000));
    assertEquals(1000, RestUtils.toSecondsPrecisionInMs(1001));
  }

  /**
   * Tests {@link RestUtils#getTimeFromDateString(String)}.
   */
  @Test
  public void getTimeFromDateStringTest() {
    SimpleDateFormat dateFormatter = new SimpleDateFormat(RestUtils.HTTP_DATE_FORMAT, Locale.ENGLISH);
    dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    long curTime = System.currentTimeMillis();
    Date curDate = new Date(curTime);
    String dateStr = dateFormatter.format(curDate);
    long epochTime = RestUtils.getTimeFromDateString(dateStr);
    long actualExpectedTime = (curTime / 1000L) * 1000;
    // Note http time is kept in Seconds so last three digits will be 000
    assertEquals("Time mismatch ", actualExpectedTime, epochTime);

    dateFormatter = new SimpleDateFormat(RestUtils.HTTP_DATE_FORMAT, Locale.CHINA);
    curTime = System.currentTimeMillis();
    curDate = new Date(curTime);
    dateStr = dateFormatter.format(curDate);
    // any other locale is not accepted
    assertEquals("Should have returned null", null, RestUtils.getTimeFromDateString(dateStr));

    assertEquals("Should have returned null", null, RestUtils.getTimeFromDateString("abc"));
  }

  /**
   * This tests the construction of {@link GetBlobOptions} objects with various range and sub-resource settings using
   * {@link RestUtils#buildGetBlobOptions(Map, RestUtils.SubResource, GetOption)} and
   * {@link RestUtils#buildByteRange(String)}.
   * @throws RestServiceException
   */
  @Test
  public void buildGetBlobOptionsTest() throws RestServiceException {
    // no range
    doBuildGetBlobOptionsTest(null, null, true, true);
    // valid ranges
    doBuildGetBlobOptionsTest("bytes=0-7", ByteRange.fromOffsetRange(0, 7), true, false);
    doBuildGetBlobOptionsTest("bytes=234-56679090", ByteRange.fromOffsetRange(234, 56679090), true, false);
    doBuildGetBlobOptionsTest("bytes=1-", ByteRange.fromStartOffset(1), true, false);
    doBuildGetBlobOptionsTest("bytes=12345678-", ByteRange.fromStartOffset(12345678), true, false);
    doBuildGetBlobOptionsTest("bytes=-8", ByteRange.fromLastNBytes(8), true, false);
    doBuildGetBlobOptionsTest("bytes=-123456789", ByteRange.fromLastNBytes(123456789), true, false);
    // bad ranges
    String[] badRanges =
        {"bytes=0-abcd", "bytes=0as23-44444444", "bytes=22-7777777777777777777777777777777777777777777", "bytes=22--53", "bytes=223-34", "bytes=-34ab", "bytes=--12", "bytes=-12-", "bytes=12ab-", "bytes=---", "btes=3-5", "bytes=345", "bytes=3.14-22", "bytes=3-6.2", "bytes=", "bytes=-", "bytes= -"};
    for (String badRange : badRanges) {
      doBuildGetBlobOptionsTest(badRange, null, false, false);
    }
  }

  /**
   * Test {@link RestUtils#buildContentRangeAndLength(ByteRange, long)}.
   */
  @Test
  public void buildContentRangeAndLengthTest() throws RestServiceException {
    // good cases
    doBuildContentRangeAndLengthTest(ByteRange.fromOffsetRange(4, 8), 12, "bytes 4-8/12", 5, true);
    doBuildContentRangeAndLengthTest(ByteRange.fromStartOffset(14), 17, "bytes 14-16/17", 3, true);
    doBuildContentRangeAndLengthTest(ByteRange.fromLastNBytes(12), 17, "bytes 5-16/17", 12, true);
    doBuildContentRangeAndLengthTest(ByteRange.fromLastNBytes(17), 17, "bytes 0-16/17", 17, true);
    // bad cases
    doBuildContentRangeAndLengthTest(ByteRange.fromOffsetRange(4, 12), 12, null, -1, false);
    doBuildContentRangeAndLengthTest(ByteRange.fromOffsetRange(4, 15), 12, null, -1, false);
    doBuildContentRangeAndLengthTest(ByteRange.fromStartOffset(12), 12, null, -1, false);
    doBuildContentRangeAndLengthTest(ByteRange.fromStartOffset(15), 12, null, -1, false);
    doBuildContentRangeAndLengthTest(ByteRange.fromLastNBytes(13), 12, null, -1, false);
  }

  /**
   * Tests {@link RestUtils#getGetOption(RestRequest)}.
   * @throws Exception
   */
  @Test
  public void getGetOptionTest() throws Exception {
    for (GetOption option : GetOption.values()) {
      JSONObject headers = new JSONObject();
      headers.put(RestUtils.Headers.GET_OPTION, option.toString().toLowerCase());
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/", headers);
      assertEquals("Option returned not as expected", option, RestUtils.getGetOption(restRequest));
    }
    // no value defined
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null);
    assertEquals("Option returned not as expected", GetOption.None, RestUtils.getGetOption(restRequest));
    // bad value
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.Headers.GET_OPTION, "non_existent_option");
    restRequest = createRestRequest(RestMethod.GET, "/", headers);
    try {
      RestUtils.getGetOption(restRequest);
      fail("Should have failed to get GetOption because value of header is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
  }

  /**
   * Tests {@link RestUtils#getServiceId(RestRequest)}
   * @throws Exception
   */
  @Test
  public void getServiceIdTest() throws Exception {
    String serviceId = "the-service-id";
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.Headers.SERVICE_ID, serviceId);
    RestRequest restRequest = createRestRequest(RestMethod.DELETE, "/", headers);
    assertEquals("Unexpected service id", serviceId, RestUtils.getServiceId(restRequest));
    restRequest = createRestRequest(RestMethod.DELETE, "/", new JSONObject());
    assertNull("Should not have found service ID", RestUtils.getServiceId(restRequest));
  }

  // helpers.
  // general.

  /**
   * Method to easily create {@link RestRequest} objects containing a specific request.
   * @param restMethod the {@link RestMethod} desired.
   * @param uri string representation of the desired URI.
   * @param headers any associated headers as a {@link org.json.JSONObject}.
   * @return A {@link RestRequest} object that defines the request required by the input.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  private RestRequest createRestRequest(RestMethod restMethod, String uri, JSONObject headers)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod);
    request.put(MockRestRequest.URI_KEY, uri);
    if (headers != null) {
      request.put(MockRestRequest.HEADERS_KEY, headers);
    }
    return new MockRestRequest(request, null);
  }

  /**
   * Generates a string of size {@code length} with random characters from {@link #ALPHABET}.
   * @param length the length of random string required.
   * @return a string of size {@code length} with random characters from {@link #ALPHABET}.
   */
  private String generateRandomString(int length) {
    char[] text = new char[length];
    for (int i = 0; i < length; i++) {
      text[i] = ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length()));
    }
    return new String(text);
  }

  /**
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param headers the {@link JSONObject} where the headers should be set.
   * @param contentLength sets the {@link RestUtils.Headers#BLOB_SIZE} header.
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header.
   * @param isPrivate sets the {@link RestUtils.Headers#PRIVATE} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestUtils.Headers#SERVICE_ID} header.
   * @param contentType sets the {@link RestUtils.Headers#AMBRY_CONTENT_TYPE} header.
   * @param ownerId sets the {@link RestUtils.Headers#OWNER_ID} header. Optional - if not required, send null.
   * @throws JSONException
   */
  private void setAmbryHeaders(JSONObject headers, String contentLength, String ttlInSecs, String isPrivate,
      String serviceId, String contentType, String ownerId) throws JSONException {
    headers.putOpt(RestUtils.Headers.BLOB_SIZE, contentLength);
    headers.putOpt(RestUtils.Headers.TTL, ttlInSecs);
    headers.putOpt(RestUtils.Headers.PRIVATE, isPrivate);
    headers.putOpt(RestUtils.Headers.SERVICE_ID, serviceId);
    headers.putOpt(RestUtils.Headers.AMBRY_CONTENT_TYPE, contentType);
    headers.putOpt(RestUtils.Headers.OWNER_ID, ownerId);
  }

  /**
   * Verifies that a request with headers defined by {@code headers} builds {@link BlobProperties} successfully and
   * matches the values of the properties with those in {@code headers}.
   * @param headers the headers that need to go with the request that is used to construct {@link BlobProperties}.
   * @throws Exception
   */
  private void verifyBlobPropertiesConstructionSuccess(JSONObject headers) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest.getArgs());
    assertEquals("Blob size does not match", headers.getLong(RestUtils.Headers.BLOB_SIZE),
        blobProperties.getBlobSize());
    long expectedTTL = Utils.Infinite_Time;
    if (headers.has(RestUtils.Headers.TTL) && !JSONObject.NULL.equals(headers.get(RestUtils.Headers.TTL))) {
      expectedTTL = headers.getLong(RestUtils.Headers.TTL);
    }
    assertEquals("Blob TTL does not match", expectedTTL, blobProperties.getTimeToLiveInSeconds());
    boolean expectedIsPrivate = false;
    if (headers.has(RestUtils.Headers.PRIVATE) && !JSONObject.NULL.equals(headers.get(RestUtils.Headers.PRIVATE))) {
      expectedIsPrivate = headers.getBoolean(RestUtils.Headers.PRIVATE);
    }
    assertEquals("Blob isPrivate does not match", expectedIsPrivate, blobProperties.isPrivate());
    assertEquals("Blob service ID does not match", headers.getString(RestUtils.Headers.SERVICE_ID),
        blobProperties.getServiceId());
    assertEquals("Blob content type does not match", headers.getString(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        blobProperties.getContentType());
    if (headers.has(RestUtils.Headers.OWNER_ID) && !JSONObject.NULL.equals(headers.get(RestUtils.Headers.OWNER_ID))) {
      assertEquals("Blob owner ID does not match", headers.getString(RestUtils.Headers.OWNER_ID),
          blobProperties.getOwnerId());
    }
  }

  /**
   * Verifies that a request with headers defined by {@code headers} builds UserMetadata successfully and
   * matches the values with those in {@code headers}.
   * @param headers the headers that need to go with the request that is used to construct the User Metadata
   * @throws Exception
   */
  private void verifyUserMetadataConstructionSuccess(JSONObject headers, Map<String, String> inputUserMetadata)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    byte[] userMetadata = RestUtils.buildUsermetadata(restRequest.getArgs());
    Map<String, String> userMetadataMap = RestUtils.buildUserMetadata(userMetadata);
    assertEquals("Total number of entries doesnt match ", inputUserMetadata.size(), userMetadataMap.size());
    for (String key : userMetadataMap.keySet()) {
      boolean keyFromInputMap = inputUserMetadata.containsKey(key);
      assertTrue("Key " + key + " not found in input user metadata", keyFromInputMap);
      assertTrue("Values didn't match for key " + key + ", value from input map value " + inputUserMetadata.get(key)
              + ", and output map value " + userMetadataMap.get(key),
          inputUserMetadata.get(key).equals(userMetadataMap.get(key)));
    }
  }

  // getBlobPropertiesVariedInputTest() helpers.

  /**
   * Verifies that {@link RestUtils#buildBlobProperties(Map<String,Object>)} fails if given a request with bad
   * arguments.
   * @param headers the headers that were provided to the request.
   * @param expectedCode the expected {@link RestServiceErrorCode} because of the failure.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  private void verifyBlobPropertiesConstructionFailure(JSONObject headers, RestServiceErrorCode expectedCode)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    try {
      RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
      RestUtils.buildBlobProperties(restRequest.getArgs());
      fail("An exception was expected but none were thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", expectedCode, e.getErrorCode());
    }
  }

  /**
   * Adds extra values for the header {@code extraValueHeader} and tests that the right exception is thrown.
   * @param headers the headers that need to go with the request that is used to construct {@link BlobProperties}.
   * @param extraValueHeader the header for which extra values will be added.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  private void tooManyValuesTest(JSONObject headers, String extraValueHeader)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    String uri = "?" + extraValueHeader + "=extraVal1&" + extraValueHeader + "=extraVal2";
    try {
      RestRequest restRequest = createRestRequest(RestMethod.POST, uri, headers);
      RestUtils.buildBlobProperties(restRequest.getArgs());
      fail("An exception was expected but none were thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
  }

  /**
   * Test that {@link RestUtils#buildGetBlobOptions(Map, RestUtils.SubResource, GetOption)} works correctly for a given
   * range with and without a specified sub-resource.
   * @param rangeHeader the Range header value to add to the {@code args} map.
   * @param expectedRange the {@link ByteRange} expected to be parsed if the call should succeed, or {@code null} if no
   *                      range is expected.
   * @param shouldSucceedWithoutSubResource {@code true} if the call should succeed with no specified sub-resource.
   * @param shouldSucceedWithSubResource {@code true} if the call should succeed with a specified sub-resource.
   * @throws RestServiceException
   */
  private void doBuildGetBlobOptionsTest(String rangeHeader, ByteRange expectedRange,
      boolean shouldSucceedWithoutSubResource, boolean shouldSucceedWithSubResource) throws RestServiceException {
    Map<String, Object> args = new HashMap<>();
    if (rangeHeader != null) {
      args.put(RestUtils.Headers.RANGE, rangeHeader);
    }
    doBuildGetBlobOptionsTestForSubResource(args, null, expectedRange, GetBlobOptions.OperationType.All,
        shouldSucceedWithoutSubResource);
    for (RestUtils.SubResource subResource : RestUtils.SubResource.values()) {
      doBuildGetBlobOptionsTestForSubResource(args, subResource, expectedRange, GetBlobOptions.OperationType.BlobInfo,
          shouldSucceedWithSubResource);
    }
  }

  /**
   * Test that {@link RestUtils#buildGetBlobOptions(Map, RestUtils.SubResource, GetOption)} works correctly with given args and a
   * specified sub-resource.
   * @param args the map of args for the method call.
   * @param subResource the sub-resource for the call.
   * @param expectedRange the {@link ByteRange} expected to be parsed if the call should succeed, or {@code null} if no
   *                      range is expected.
   * @param expectedOpType the {@link GetBlobOptions.OperationType} expected to be set in the {@link GetBlobOptions}
   *                       object.
   * @param shouldSucceed {@code true} if the call should succeed.
   * @throws RestServiceException
   */
  private void doBuildGetBlobOptionsTestForSubResource(Map<String, Object> args, RestUtils.SubResource subResource,
      ByteRange expectedRange, GetBlobOptions.OperationType expectedOpType, boolean shouldSucceed)
      throws RestServiceException {
    if (shouldSucceed) {
      GetBlobOptions options = RestUtils.buildGetBlobOptions(args, subResource, GetOption.None);
      assertEquals("Unexpected range for args=" + args + " and subResource=" + subResource, expectedRange,
          options.getRange());
      assertEquals("Unexpected operation type for args=" + args + " and subResource=" + subResource, expectedOpType,
          options.getOperationType());
      assertEquals("Unexpected get options type for args=" + args + " and subResource=" + subResource, GetOption.None,
          options.getGetOption());
    } else {
      try {
        RestUtils.buildGetBlobOptions(args, subResource, GetOption.None);
        fail("buildGetBlobOptions should not have succeeded with args=" + args + "and subResource=" + subResource);
      } catch (RestServiceException expected) {
        assertEquals("Unexpected error code.", RestServiceErrorCode.InvalidArgs, expected.getErrorCode());
      }
    }
  }

  /**
   * Test {@link RestUtils#buildContentRangeAndLength(ByteRange, long)} for a specific {@link ByteRange} and total blob
   * size.
   * @param range the {@link ByteRange} to test for.
   * @param blobSize the total blob size in bytes to test for.
   * @param expectedContentRange the expected Content-Range header string.
   * @param expectedContentLength the expected Content-Length in bytes.
   * @param shouldSucceed {@code true} if the call should succeed, {@code false} if an error is expected.
   * @throws RestServiceException
   */
  private void doBuildContentRangeAndLengthTest(ByteRange range, long blobSize, String expectedContentRange,
      long expectedContentLength, boolean shouldSucceed) throws RestServiceException {
    if (shouldSucceed) {
      Pair<String, Long> rangeAndLength = RestUtils.buildContentRangeAndLength(range, blobSize);
      assertEquals(expectedContentRange, rangeAndLength.getFirst());
      assertEquals(expectedContentLength, (long) rangeAndLength.getSecond());
    } else {
      try {
        RestUtils.buildContentRangeAndLength(range, blobSize);
        fail("Should have encountered exception when building Content-Range");
      } catch (RestServiceException e) {
        assertEquals("Unexpected error code.", RestServiceErrorCode.RangeNotSatisfiable, e.getErrorCode());
      }
    }
  }

  /**
   * Sets entries from the passed in HashMap to the @{link JSONObject} headers
   * @param headers  {@link JSONObject} to which the new headers are to be added
   * @param userMetadata {@link Map} which has the new entries that has to be added
   * @throws org.json.JSONException
   */
  public static void setUserMetadataHeaders(JSONObject headers, Map<String, String> userMetadata) throws JSONException {
    for (String key : userMetadata.keySet()) {
      headers.put(key, userMetadata.get(key));
    }
  }
}
