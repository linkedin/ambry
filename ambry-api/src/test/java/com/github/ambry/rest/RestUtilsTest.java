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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.ByteRanges;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static com.github.ambry.router.GetBlobOptions.*;
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
    Container[] containers = {Container.DEFAULT_PRIVATE_CONTAINER, Container.DEFAULT_PUBLIC_CONTAINER};
    for (Container container : containers) {
      setAmbryHeadersForPut(headers, Long.toString(RANDOM.nextInt(10000)), generateRandomString(10), container,
          "image/gif", generateRandomString(10));
      verifyBlobPropertiesConstructionSuccess(headers);
    }
  }

  /**
   * Tests building of {@link BlobProperties} given varied input. Some input fail (tests check the correct error code),
   * some should succeed (check for default values expected).
   * @throws Exception
   */
  @Test
  public void getBlobPropertiesVariedInputTest() throws Exception {
    String ttl = Long.toString(RANDOM.nextInt(10000));
    String serviceId = generateRandomString(10);
    String contentType = "image/gif";
    String ownerId = generateRandomString(10);

    JSONObject headers;
    // failure required.
    // ttl not a number.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, "NaN", serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // ttl < -1.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, "-2", serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // serviceId missing.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, null, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.MissingArgs);
    // serviceId null.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, null, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId);
    headers.put(RestUtils.Headers.SERVICE_ID, JSONObject.NULL);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // contentType missing.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, serviceId, Container.DEFAULT_PUBLIC_CONTAINER, null, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.MissingArgs);
    // contentType null.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, serviceId, Container.DEFAULT_PUBLIC_CONTAINER, null, ownerId);
    headers.put(RestUtils.Headers.AMBRY_CONTENT_TYPE, JSONObject.NULL);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // too many values for some headers.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId);
    tooManyValuesTest(headers, RestUtils.Headers.TTL);
    // no internal keys for account and container
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, serviceId, null, contentType, ownerId, false);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InternalServerError);
    // no internal keys for account
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId, false);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InternalServerError);
    // no internal keys for container
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, serviceId, null, contentType, ownerId, true);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InternalServerError);

    // no failures.
    // ttl missing. Should be infinite time by default
    // ownerId missing.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, null, serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, null);
    verifyBlobPropertiesConstructionSuccess(headers);

    // ttl null.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, null, serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId);
    headers.put(RestUtils.Headers.TTL, JSONObject.NULL);
    verifyBlobPropertiesConstructionSuccess(headers);

    // Post with valid ttl
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, "100", serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId);
    verifyBlobPropertiesConstructionSuccess(headers);

    // ownerId null.
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, null);
    headers.put(RestUtils.Headers.OWNER_ID, JSONObject.NULL);
    verifyBlobPropertiesConstructionSuccess(headers);

    // blobSize null (should be ignored)
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId);
    headers.put(RestUtils.Headers.BLOB_SIZE, JSONObject.NULL);
    verifyBlobPropertiesConstructionSuccess(headers);

    // blobSize negative (should succeed)
    headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, serviceId, Container.DEFAULT_PUBLIC_CONTAINER, contentType, ownerId);
    headers.put(RestUtils.Headers.BLOB_SIZE, -1);
    verifyBlobPropertiesConstructionSuccess(headers);
  }

  /**
   * Tests for {@link RestUtils#isPrivate(Map)}
   * @throws Exception
   */
  @Test
  public void isPrivateTest() throws Exception {
    String serviceId = generateRandomString(10);
    String contentType = "image/gif";
    JSONObject headers = new JSONObject();
    // using this just to help with setting the other necessary headers - doesn't matter what the container is, the
    // PRIVATE header won't have been set.
    setAmbryHeadersForPut(headers, null, serviceId, Container.DEFAULT_PRIVATE_CONTAINER, contentType, null);

    // isPrivate not true or false.
    headers.put(RestUtils.Headers.PRIVATE, "!(true || false)");
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    try {
      RestUtils.isPrivate(restRequest.getArgs());
      fail("An exception was expected but none were thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }

    // isPrivate null.
    headers.put(RestUtils.Headers.PRIVATE, JSONObject.NULL);
    restRequest = createRestRequest(RestMethod.POST, "/", headers);
    assertFalse("isPrivate should be false because it was not set", RestUtils.isPrivate(restRequest.getArgs()));

    // isPrivate false
    headers.put(RestUtils.Headers.PRIVATE, "false");
    restRequest = createRestRequest(RestMethod.POST, "/", headers);
    assertFalse("isPrivate should be false because it was set to false", RestUtils.isPrivate(restRequest.getArgs()));

    // isPrivate true
    headers.put(RestUtils.Headers.PRIVATE, "true");
    restRequest = createRestRequest(RestMethod.POST, "/", headers);
    assertTrue("isPrivate should be true because it was set to true", RestUtils.isPrivate(restRequest.getArgs()));
  }

  /**
   * Tests for {@link RestUtils#ensureRequiredHeadersOrThrow(RestRequest, Set)}.
   * @throws Exception
   */
  @Test
  public void ensureRequiredHeadersOrThrowTest() throws Exception {
    JSONObject headers = new JSONObject();
    Set<String> requiredHeaders = new HashSet<>(Arrays.asList("required_a", "required_b", "required_c"));
    for (String requiredHeader : requiredHeaders) {
      headers.put(requiredHeader, TestUtils.getRandomString(10));
    }
    headers.put(TestUtils.getRandomString(10), TestUtils.getRandomString(10));

    // success test
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    RestUtils.ensureRequiredHeadersOrThrow(restRequest, requiredHeaders);

    // failure test
    // null
    headers.put(requiredHeaders.iterator().next(), JSONObject.NULL);
    restRequest = createRestRequest(RestMethod.POST, "/", headers);
    try {
      RestUtils.ensureRequiredHeadersOrThrow(restRequest, requiredHeaders);
      fail("Should have failed because one of the required headers has a bad value");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
    // not present
    headers.remove(requiredHeaders.iterator().next());
    restRequest = createRestRequest(RestMethod.POST, "/", headers);
    try {
      RestUtils.ensureRequiredHeadersOrThrow(restRequest, requiredHeaders);
      fail("Should have failed because one of the required headers is not present");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.MissingArgs, e.getErrorCode());
    }
  }

  /**
   * Tests building of user metadata.
   * @throws Exception
   */
  @Test
  public void getUserMetadataTest() throws Exception {
    byte[] usermetadata = RestUtils.buildUserMetadata(new HashMap<>());
    assertArrayEquals("Unexpected user metadata", new byte[0], usermetadata);
  }

  /**
   * Tests building of User Metadata with good input
   * @throws Exception
   */
  @Test
  public void getUserMetadataGoodInputTest() throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeadersForPut(headers, Long.toString(RANDOM.nextInt(10000)), generateRandomString(10),
        Container.DEFAULT_PUBLIC_CONTAINER, "image/gif", generateRandomString(10));
    Map<String, String> userMetadata = new HashMap<String, String>();
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
    // changed cases
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX.toUpperCase() + "KeY3", "value3");
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX.toLowerCase() + "kEy4", "value4");
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
    byte[] rcvd = RestUtils.buildUserMetadata(restRequest.getArgs());
    assertArrayEquals("Received user metadata does not match with original", original, rcvd);
  }

  /**
   * Tests building of User Metadata with unusual input
   * @throws Exception
   */
  @Test
  public void getUserMetadataUnusualInputTest() throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeadersForPut(headers, Long.toString(RANDOM.nextInt(10000)), generateRandomString(10),
        Container.DEFAULT_PUBLIC_CONTAINER, "image/gif", generateRandomString(10));
    Map<String, String> userMetadataArgs = new HashMap<String, String>();
    Map<String, String> expectedUserMetadata = new HashMap<>();
    String key = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1";
    String value = "value1";
    userMetadataArgs.put(key, value);
    expectedUserMetadata.put(key, value);
    // no valid prefix
    userMetadataArgs.put("key2", "value2_1");
    // valid prefix as suffix
    userMetadataArgs.put("key3" + RestUtils.Headers.USER_META_DATA_HEADER_PREFIX, "value3");
    // empty value
    key = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key4";
    value = "";
    userMetadataArgs.put(key, value);
    expectedUserMetadata.put(key, value);
    // different casing
    key = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX.toUpperCase() + "KeY5";
    value = "value5";
    userMetadataArgs.put(key, value);
    expectedUserMetadata.put(key, value);
    key = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX.toLowerCase() + "kEy6";
    value = "value6";
    userMetadataArgs.put(key, value);
    expectedUserMetadata.put(key, value);
    // Unicode with multiple code point characters (i.e. emoji)
    key = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX.toLowerCase() + "kEy7";
    userMetadataArgs.put(key, "å∫ \uD83D\uDE1D\uD83D\uDE31abcd");
    // Non ascii characters should be replaced with question marks
    expectedUserMetadata.put(key, "?? ??abcd");
    setUserMetadataHeaders(headers, userMetadataArgs);

    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    byte[] userMetadataByteArray = RestUtils.buildUserMetadata(restRequest.getArgs());
    Map<String, String> userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);

    assertEquals("Size of map unexpected ", expectedUserMetadata.size(), userMetadataMap.size());
    expectedUserMetadata.forEach((keyToCheck, valueToCheck) -> {
      String keyInOutputMap = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + keyToCheck.substring(
          RestUtils.Headers.USER_META_DATA_HEADER_PREFIX.length());
      assertTrue(keyInOutputMap + " not found in user metadata map", userMetadataMap.containsKey(keyInOutputMap));
      assertEquals("Value for " + keyToCheck + " didnt match expected value", valueToCheck,
          userMetadataMap.get(keyInOutputMap));
    });
  }

  /**
   * Tests building of User Metadata with empty input
   * @throws Exception
   */
  @Test
  public void getEmptyUserMetadataInputTest() throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeadersForPut(headers, Long.toString(RANDOM.nextInt(10000)), generateRandomString(10),
        Container.DEFAULT_PUBLIC_CONTAINER, "image/gif", generateRandomString(10));
    Map<String, String> userMetadata = new HashMap<String, String>();
    setUserMetadataHeaders(headers, userMetadata);

    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    byte[] userMetadataByteArray = RestUtils.buildUserMetadata(restRequest.getArgs());
    Map<String, String> userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertEquals("UserMetadata should have no entries ", 0, userMetadataMap.size());
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
   * {@link RestUtils#buildGetBlobOptions}.
   * @throws RestServiceException
   */
  @Test
  public void buildGetBlobOptionsTest() throws RestServiceException {
    // no range
    doBuildGetBlobOptionsTest(null, null, true, true);
    // valid ranges
    doBuildGetBlobOptionsTest("bytes=0-7", ByteRanges.fromOffsetRange(0, 7), true, false, true);
    doBuildGetBlobOptionsTest("bytes=234-56679090", ByteRanges.fromOffsetRange(234, 56679090), true, false, true);
    doBuildGetBlobOptionsTest("bytes=1-", ByteRanges.fromStartOffset(1), true, false, true);
    doBuildGetBlobOptionsTest("bytes=12345678-", ByteRanges.fromStartOffset(12345678), true, false, true);
    doBuildGetBlobOptionsTest("bytes=-8", ByteRanges.fromLastNBytes(8), true, false, true);
    doBuildGetBlobOptionsTest("bytes=-123456789", ByteRanges.fromLastNBytes(123456789), true, false, true);
    // bad ranges
    String[] badRanges =
        {"bytes=0-abcd", "bytes=0as23-44444444", "bytes=22-7777777777777777777777777777777777777777777", "bytes=22--53",
            "bytes=223-34", "bytes=-34ab", "bytes=--12", "bytes=-12-", "bytes=12ab-", "bytes=---", "btes=3-5",
            "bytes=345", "bytes=3.14-22", "bytes=3-6.2", "bytes=", "bytes=-", "bytes= -"};
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
    doBuildContentRangeAndLengthTest(ByteRanges.fromOffsetRange(4, 8), 12, "bytes 4-8/12", 5, true);
    doBuildContentRangeAndLengthTest(ByteRanges.fromOffsetRange(4, 12), 12, "bytes 4-11/12", 8, true);
    doBuildContentRangeAndLengthTest(ByteRanges.fromOffsetRange(4, 15), 12, "bytes 4-11/12", 8, true);
    doBuildContentRangeAndLengthTest(ByteRanges.fromStartOffset(14), 17, "bytes 14-16/17", 3, true);
    doBuildContentRangeAndLengthTest(ByteRanges.fromLastNBytes(12), 17, "bytes 5-16/17", 12, true);
    doBuildContentRangeAndLengthTest(ByteRanges.fromLastNBytes(17), 17, "bytes 0-16/17", 17, true);
    doBuildContentRangeAndLengthTest(ByteRanges.fromLastNBytes(13), 12, "bytes 0-11/12", 12, true);
    // bad cases
    doBuildContentRangeAndLengthTest(ByteRanges.fromOffsetRange(4, 12), 4, null, -1, false);
    doBuildContentRangeAndLengthTest(ByteRanges.fromStartOffset(12), 12, null, -1, false);
    doBuildContentRangeAndLengthTest(ByteRanges.fromStartOffset(15), 12, null, -1, false);
  }

  /**
   * Tests {@link RestUtils#getGetOption(RestRequest, GetOption)}.
   * @throws Exception
   */
  @Test
  public void getGetOptionTest() throws Exception {
    for (GetOption option : GetOption.values()) {
      JSONObject headers = new JSONObject();
      headers.put(RestUtils.Headers.GET_OPTION, option.toString().toLowerCase());
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/", headers);
      assertEquals("Option returned not as expected", option, RestUtils.getGetOption(restRequest, null));
      assertEquals("Option returned not as expected", option, RestUtils.getGetOption(restRequest, option));
      assertEquals("Option returned not as expected", option, RestUtils.getGetOption(restRequest, GetOption.None));
    }
    // no value defined
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null);
    assertEquals("Option returned not as expected", GetOption.None, RestUtils.getGetOption(restRequest, null));
    for (GetOption option : GetOption.values()) {
      assertEquals("Option returned not as expected", option, RestUtils.getGetOption(restRequest, option));
    }
    // bad value
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.Headers.GET_OPTION, "non_existent_option");
    restRequest = createRestRequest(RestMethod.GET, "/", headers);
    try {
      RestUtils.getGetOption(restRequest, GetOption.None);
      fail("Should have failed to get GetOption because value of header is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
  }

  /**
   * Tests {@link RestUtils#getHeader(Map, String, boolean)}.
   * @throws RestServiceException
   */
  @Test
  public void getHeaderTest() throws RestServiceException {
    Map<String, Object> args = new HashMap<>();
    args.put("HeaderA", "ValueA");
    args.put("HeaderB", null);

    // get HeaderA
    assertEquals("Header value does not match", args.get("HeaderA"), RestUtils.getHeader(args, "HeaderA", true));
    assertEquals("Header value does not match", args.get("HeaderA"), RestUtils.getHeader(args, "HeaderA", false));
    // get HeaderB
    assertNull("There should be no value for HeaderB", RestUtils.getHeader(args, "HeaderB", false));
    try {
      RestUtils.getHeader(args, "HeaderB", true);
      fail("Getting HeaderB as required should have failed");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }

    // get HeaderC
    assertNull("There should be no value for HeaderC", RestUtils.getHeader(args, "HeaderB", false));
    try {
      RestUtils.getHeader(args, "Headerc", true);
      fail("Getting HeaderB as required should have failed");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.MissingArgs, e.getErrorCode());
    }
  }

  /**
   * Tests for {@link RestUtils#getLongHeader(Map, String, boolean)}.
   * @throws RestServiceException
   */
  @Test
  public void getLongHeaderTest() throws RestServiceException {
    Map<String, Object> args = new HashMap<>();
    args.put("HeaderA", 1000L);
    args.put("HeaderB", "NotLong");
    args.put("HeaderC", "10000");
    // getLongHeader() calls getHeader() and in the interest of keeping tests short, tests for that functionality
    // are not repeated here. If that changes, these tests need to change.
    assertEquals("Header value does not match", args.get("HeaderA"), RestUtils.getLongHeader(args, "HeaderA", true));
    assertEquals("Header value does not match", Long.parseLong(args.get("HeaderC").toString()),
        RestUtils.getLongHeader(args, "HeaderC", true).longValue());
    try {
      RestUtils.getLongHeader(args, "HeaderB", true);
      fail("Getting HeaderB as required should have failed");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
    assertNull("There should no value for HeaderD", RestUtils.getLongHeader(args, "HeaderD", false));
  }

  /**
   * Tests for {@link RestUtils#setUserMetadataHeaders(byte[], RestResponseChannel)}
   * @throws Exception
   */
  @Test
  public void setUserMetadataHeadersTest() throws Exception {
    // empty user metadata
    MockRestResponseChannel responseChannel = new MockRestResponseChannel();
    assertTrue("Should report that headers are set", RestUtils.setUserMetadataHeaders(new byte[0], responseChannel));
    assertEquals("No headers should have been set", 0, responseChannel.getResponseHeaders().size());

    JSONObject headers = new JSONObject();
    setUserMetadataHeaders(headers, Collections.emptyMap());
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    byte[] userMetadata = RestUtils.buildUserMetadata(restRequest.getArgs());
    responseChannel = new MockRestResponseChannel();
    assertTrue("Should report that headers are set", RestUtils.setUserMetadataHeaders(userMetadata, responseChannel));
    assertEquals("No headers should have been set", 0, responseChannel.getResponseHeaders().size());

    // user metadata that can be deserialized
    Map<String, String> usermetadataMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      usermetadataMap.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key" + i, "value" + i);
    }
    setUserMetadataHeaders(headers, usermetadataMap);
    restRequest = createRestRequest(RestMethod.POST, "/", headers);
    userMetadata = RestUtils.buildUserMetadata(restRequest.getArgs());
    responseChannel = new MockRestResponseChannel();
    assertTrue("Should report that headers are set", RestUtils.setUserMetadataHeaders(userMetadata, responseChannel));
    Map<String, Object> responseHeaders = responseChannel.getResponseHeaders();
    assertEquals("There is a mismatch in the numebr of headers", usermetadataMap.size(), responseHeaders.size());
    usermetadataMap.forEach((k, v) -> assertEquals("Value of " + k + " not as expected", v, responseHeaders.get(k)));

    // user metadata that cannot be deserialized
    responseChannel = new MockRestResponseChannel();
    userMetadata = TestUtils.getRandomBytes(100);
    assertFalse("Should report that headers are not set",
        RestUtils.setUserMetadataHeaders(userMetadata, responseChannel));
    assertEquals("No headers should have been set", 0, responseChannel.getResponseHeaders().size());
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
  static RestRequest createRestRequest(RestMethod restMethod, String uri, JSONObject headers)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
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
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header.
   * @param serviceId sets the {@link RestUtils.Headers#SERVICE_ID} header.
   * @param container used to set the container for {@link RestUtils.InternalKeys#TARGET_CONTAINER_KEY}.
   * @param contentType sets the {@link RestUtils.Headers#AMBRY_CONTENT_TYPE} header.
   * @param ownerId sets the {@link RestUtils.Headers#OWNER_ID} header. Optional - if not required, send null.
   * @param insertAccount {@code true} if {@link Account} info has to be injected into the headers.
   * @throws JSONException
   */
  private void setAmbryHeadersForPut(JSONObject headers, String ttlInSecs, String serviceId, Container container,
      String contentType, String ownerId, boolean insertAccount) throws JSONException {
    headers.putOpt(RestUtils.Headers.TTL, ttlInSecs);
    headers.putOpt(RestUtils.Headers.SERVICE_ID, serviceId);
    headers.putOpt(RestUtils.Headers.AMBRY_CONTENT_TYPE, contentType);
    headers.putOpt(RestUtils.Headers.OWNER_ID, ownerId);
    if (insertAccount) {
      headers.putOpt(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, InMemAccountService.UNKNOWN_ACCOUNT);
    }
    if (container != null) {
      headers.putOpt(RestUtils.InternalKeys.TARGET_CONTAINER_KEY, container);
    }
  }

  /**
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param headers the {@link JSONObject} where the headers should be set.
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header.
   * @param serviceId sets the {@link RestUtils.Headers#SERVICE_ID} header.
   * @param container used to set the container for {@link RestUtils.InternalKeys#TARGET_CONTAINER_KEY}.
   * @param contentType sets the {@link RestUtils.Headers#AMBRY_CONTENT_TYPE} header.
   * @param ownerId sets the {@link RestUtils.Headers#OWNER_ID} header. Optional - if not required, send null.
   * @throws JSONException
   */
  private void setAmbryHeadersForPut(JSONObject headers, String ttlInSecs, String serviceId, Container container,
      String contentType, String ownerId) throws JSONException {
    setAmbryHeadersForPut(headers, ttlInSecs, serviceId, container, contentType, ownerId, true);
  }

  /**
   * Verifies that a request with headers defined by {@code headers} builds {@link BlobProperties} successfully and
   * matches the values of the properties with those in {@code headers}.
   * @param headers the headers that need to go with the request that is used to construct {@link BlobProperties}.
   * @throws Exception
   */
  private void verifyBlobPropertiesConstructionSuccess(JSONObject headers) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    Account account = (Account) headers.get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY);
    Container container = (Container) headers.get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY);
    restRequest.setArg(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, account);
    restRequest.setArg(RestUtils.InternalKeys.TARGET_CONTAINER_KEY, container);
    BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest.getArgs());
    long expectedTTL = Utils.Infinite_Time;
    if (headers.has(RestUtils.Headers.TTL) && !JSONObject.NULL.equals(headers.get(RestUtils.Headers.TTL))) {
      expectedTTL = headers.getLong(RestUtils.Headers.TTL);
    }
    assertEquals("Blob TTL does not match", expectedTTL, blobProperties.getTimeToLiveInSeconds());
    assertEquals("Blob isPrivate does not match", !container.isCacheable(), blobProperties.isPrivate());
    assertEquals("Blob service ID does not match", headers.getString(RestUtils.Headers.SERVICE_ID),
        blobProperties.getServiceId());
    assertEquals("Blob content type does not match", headers.getString(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        blobProperties.getContentType());
    if (headers.has(RestUtils.Headers.OWNER_ID) && !JSONObject.NULL.equals(headers.get(RestUtils.Headers.OWNER_ID))) {
      assertEquals("Blob owner ID does not match", headers.getString(RestUtils.Headers.OWNER_ID),
          blobProperties.getOwnerId());
    }
    assertEquals("Target account id does not match", account.getId(), blobProperties.getAccountId());
    assertEquals("Target container id does not match", container.getId(), blobProperties.getContainerId());
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
    byte[] userMetadata = RestUtils.buildUserMetadata(restRequest.getArgs());
    Map<String, String> userMetadataMap = RestUtils.buildUserMetadata(userMetadata);
    assertEquals("Total number of entries doesnt match ", inputUserMetadata.size(), userMetadataMap.size());
    for (String key : inputUserMetadata.keySet()) {
      String keyInOutputMap = RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + key.substring(
          RestUtils.Headers.USER_META_DATA_HEADER_PREFIX.length());
      assertTrue("Key " + keyInOutputMap + " not found in input user metadata",
          userMetadataMap.containsKey(keyInOutputMap));
      assertEquals("Values didn't match for key " + key, inputUserMetadata.get(key),
          userMetadataMap.get(keyInOutputMap));
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
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    try {
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
      restRequest.setArg(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, InMemAccountService.UNKNOWN_ACCOUNT);
      restRequest.setArg(RestUtils.InternalKeys.TARGET_CONTAINER_KEY, Container.UNKNOWN_CONTAINER);
      RestUtils.buildBlobProperties(restRequest.getArgs());
      fail("An exception was expected but none were thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
  }

  /**
   * Test that {@link RestUtils#buildGetBlobOptions(Map, RestUtils.SubResource, GetOption, int)} works correctly for a given
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
    doBuildGetBlobOptionsTest(rangeHeader, expectedRange, shouldSucceedWithoutSubResource, shouldSucceedWithSubResource,
        shouldSucceedWithSubResource);
  }

  /**
   * Test that {@link RestUtils#buildGetBlobOptions(Map, RestUtils.SubResource, GetOption, int)} works correctly for a given
   * range with and without a specified sub-resource.
   * @param rangeHeader the Range header value to add to the {@code args} map.
   * @param expectedRange the {@link ByteRange} expected to be parsed if the call should succeed, or {@code null} if no
   *                      range is expected.
   * @param shouldSucceedWithoutSubResource {@code true} if the call should succeed with no specified sub-resource.
   * @param shouldSucceedWithNonSegmentSubResource {@code true} if the call should succeed with a specified non-Segment sub-resource.
   * @param shouldSucceedWithSegment {@code true} if the call should succeed with a specified Segment sub-resource
   *                                             (used if different than shouldSucceedWithSubResource).
   * @throws RestServiceException
   */
  private void doBuildGetBlobOptionsTest(String rangeHeader, ByteRange expectedRange,
      boolean shouldSucceedWithoutSubResource, boolean shouldSucceedWithNonSegmentSubResource, boolean shouldSucceedWithSegment)
      throws RestServiceException {
    Map<String, Object> args = new HashMap<>();
    if (rangeHeader != null) {
      args.put(RestUtils.Headers.RANGE, rangeHeader);
    }
    doBuildGetBlobOptionsTestForSubResource(args, null, expectedRange, GetBlobOptions.OperationType.All,
        shouldSucceedWithoutSubResource);
    for (RestUtils.SubResource subResource : RestUtils.SubResource.values()) {
      if (subResource.equals(RestUtils.SubResource.Segment)) {
        doBuildGetBlobOptionsTestForSubResource(args, subResource, expectedRange, OperationType.All,
            shouldSucceedWithSegment);
      } else {
        doBuildGetBlobOptionsTestForSubResource(args, subResource, expectedRange, GetBlobOptions.OperationType.BlobInfo,
            shouldSucceedWithNonSegmentSubResource);
      }
    }
  }

  /**
   * Test that {@link RestUtils#buildGetBlobOptions(Map, RestUtils.SubResource, GetOption, int)} works correctly with given args and a
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
      GetBlobOptions options =
          RestUtils.buildGetBlobOptions(args, subResource, GetOption.None, NO_BLOB_SEGMENT_IDX_SPECIFIED);
      assertEquals("Unexpected range for args=" + args + " and subResource=" + subResource, expectedRange,
          options.getRange());
      assertEquals("Unexpected operation type for args=" + args + " and subResource=" + subResource, expectedOpType,
          options.getOperationType());
      assertEquals("Unexpected get options type for args=" + args + " and subResource=" + subResource, GetOption.None,
          options.getGetOption());
    } else {
      try {
        RestUtils.buildGetBlobOptions(args, subResource, GetOption.None, NO_BLOB_SEGMENT_IDX_SPECIFIED);
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
    if (userMetadata != null) {
      for (String key : userMetadata.keySet()) {
        headers.put(key, userMetadata.get(key));
      }
    }
  }
}
