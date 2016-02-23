package com.github.ambry.rest;

import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit tests for {@link RestUtils}.
 */
public class RestUtilsTest {
  private static final Random RANDOM = new Random();
  private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

  /**
   * Tests building of {@link BlobProperties} given good input (all headers in the number and format expected).
   * @throws Exception
   */
  @Test
  public void getBlobPropertiesGoodInputTest()
      throws Exception {
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
  public void getBlobPropertiesVariedInputTest()
      throws Exception {
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
  public void getUserMetadataTest()
      throws Exception {
    byte[] usermetadata = RestUtils.buildUsermetadata(createRestRequest(RestMethod.POST, "/", null));
    assertArrayEquals("Unexpected user metadata", new byte[0], usermetadata);
  }

  /**
   * Tests building of User Metadata with good input
   * @throws Exception
   */
  @Test
  public void getUserMetadataGoodInputTest()
      throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeaders(headers, Long.toString(RANDOM.nextInt(10000)), Long.toString(RANDOM.nextInt(10000)),
        Boolean.toString(RANDOM.nextBoolean()), generateRandomString(10), "image/gif", generateRandomString(10));
    Map<String, String> userMetadata = new HashMap<String, String>();
    userMetadata.put(RestUtils.Headers.UserMetaData_Header_Prefix + "key1", "value1");
    userMetadata.put(RestUtils.Headers.UserMetaData_Header_Prefix + "key2", "value2");
    setUserMetadataHeaders(headers, userMetadata);
    verifyBlobPropertiesConstructionSuccess(headers);
    verifyUserMetadataConstructionSuccess(headers, userMetadata);
  }

  /**
   * Tests building of User Metadata with unusual input
   * @throws Exception
   */
  @Test
  public void getUserMetadataUnusualInputTest()
      throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeaders(headers, Long.toString(RANDOM.nextInt(10000)), Long.toString(RANDOM.nextInt(10000)),
        Boolean.toString(RANDOM.nextBoolean()), generateRandomString(10), "image/gif", generateRandomString(10));
    Map<String, String> userMetadata = new HashMap<String, String>();
    String key1 = RestUtils.Headers.UserMetaData_Header_Prefix + "key1";
    userMetadata.put(key1, "value1");
    // no valid prefix
    userMetadata.put("key2", "value2_1");
    // valid prefix as suffix
    userMetadata.put("key3" + RestUtils.Headers.UserMetaData_Header_Prefix, "value3");
    // empty value
    userMetadata.put(RestUtils.Headers.UserMetaData_Header_Prefix + "key4", "");
    setUserMetadataHeaders(headers, userMetadata);
    verifyBlobPropertiesConstructionSuccess(headers);

    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    byte[] userMetadataByteArray = RestUtils.buildUsermetadata(restRequest);
    Map<String, String> userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);

    // key1, output should be same as input
    String key = RestUtils.Headers.UserMetaData_Header_Prefix + "key1";
    assertTrue(key + " not found in user metadata map ", userMetadataMap.containsKey(key));
    assertEquals("Value for " + key + " didnt match input value ", userMetadata.get(key), userMetadataMap.get(key));

    // key4 should match
    key = RestUtils.Headers.UserMetaData_Header_Prefix + "key4";
    assertTrue(key + " not found in user metadata map ", userMetadataMap.containsKey(key));
    assertEquals("Value for " + key + " didnt match input value ", userMetadata.get(key), userMetadataMap.get(key));

    assertEquals("Size of map unexpected ", 2, userMetadataMap.size());
  }

  /**
   * Tests building of User Metadata with empty input
   * @throws Exception
   */
  @Test
  public void getEmptyUserMetadataInputTest()
      throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeaders(headers, Long.toString(RANDOM.nextInt(10000)), Long.toString(RANDOM.nextInt(10000)),
        Boolean.toString(RANDOM.nextBoolean()), generateRandomString(10), "image/gif", generateRandomString(10));
    Map<String, String> userMetadata = new HashMap<String, String>();
    setUserMetadataHeaders(headers, userMetadata);
    verifyBlobPropertiesConstructionSuccess(headers);

    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    byte[] userMetadataByteArray = RestUtils.buildUsermetadata(restRequest);
    Map<String, String> userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertTrue("UserMetadata should have been empty " + userMetadataMap, userMetadataMap.size() == 0);
  }

  /**
   * Tests getting back user metadata (old style) from byte array
   * @throws Exception
   */
  @Test
  public void getUserMetadataFromByteArrayComplexTest()
      throws Exception {

    Map<String, String> userMetadataMap = null;
    String key1 = RestUtils.Headers.UserMetaData_OldStyle_Prefix + "0";
    String key2 = RestUtils.Headers.UserMetaData_OldStyle_Prefix + "1";
    // user metadata of size 1 byte
    byte[] userMetadataByteArray = new byte[1];
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertEquals("User metadata size don't match ", userMetadataMap.size(), 1);
    assertTrue("User metadata " + key1 + " not found in user metadata ", userMetadataMap.containsKey(key1));
    assertEquals("User metadata " + key1 + " value don't match ",
        new String(userMetadataByteArray, StandardCharsets.US_ASCII), userMetadataMap.get(key1));

    // user metadata with just the version
    userMetadataByteArray = new byte[4];
    ByteBuffer byteBuffer = ByteBuffer.wrap(userMetadataByteArray);
    byteBuffer.putShort((short) 1);
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertEquals("Sizes don't match ", userMetadataMap.size(), 1);
    assertTrue("User metadata " + key1 + " not found in user metadata ", userMetadataMap.containsKey(key1));
    assertEquals("User metadata " + key1 + " value don't match ",
        new String(userMetadataByteArray, StandardCharsets.US_ASCII), userMetadataMap.get(key1));

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
    assertEquals("Sizes don't match ", userMetadataMap.size(), 1);
    assertTrue("User metadata " + key1 + " not found in user metadata ", userMetadataMap.containsKey(key1));
    assertEquals("User metadata " + key1 + " value don't match ",
        new String(userMetadataByteArray, StandardCharsets.US_ASCII), userMetadataMap.get(key1));

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
    assertEquals("Sizes don't match ", userMetadataMap.size(), 1);
    assertTrue("User metadata " + key1 + " not found in user metadata ", userMetadataMap.containsKey(key1));
    assertEquals("User metadata " + key1 + " value don't match ",
        new String(userMetadataByteArray, StandardCharsets.US_ASCII), userMetadataMap.get(key1));

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
    assertEquals("Sizes don't match ", userMetadataMap.size(), 1);
    assertTrue("User metadata " + key1 + " not found in user metadata ", userMetadataMap.containsKey(key1));
    assertEquals("User metadata " + key1 + " value don't match ",
        new String(userMetadataByteArray, StandardCharsets.US_ASCII), userMetadataMap.get(key1));

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
    assertEquals("Sizes don't match ", userMetadataMap.size(), 1);
    assertTrue("User metadata " + key1 + " not found in user metadata ", userMetadataMap.containsKey(key1));
    assertEquals("User metadata " + key1 + " value don't match ",
        new String(userMetadataByteArray, StandardCharsets.US_ASCII), userMetadataMap.get(key1));

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
    assertEquals("Sizes don't match ", userMetadataMap.size(), 1);
    assertTrue("User metadata " + key1 + " not found in user metadata ", userMetadataMap.containsKey(key1));
    assertEquals("User metadata " + key1 + " value don't match ",
        new String(userMetadataByteArray, StandardCharsets.US_ASCII), userMetadataMap.get(key1));

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
    assertEquals("Sizes don't match ", userMetadataMap.size(), 1);
    assertTrue("User metadata " + key1 + " not found in user metadata ", userMetadataMap.containsKey(key1));
    assertEquals("User metadata " + key1 + " value don't match ",
        new String(userMetadataByteArray, StandardCharsets.US_ASCII), userMetadataMap.get(key1));

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
    assertTrue("User metadata " + RestUtils.Headers.UserMetaData_Header_Prefix + "key1 not found in user metadata ",
        userMetadataMap.containsKey(RestUtils.Headers.UserMetaData_Header_Prefix + "key1"));
    assertEquals("User metadata " + RestUtils.Headers.UserMetaData_Header_Prefix + "key1 value don't match ", value,
        userMetadataMap.get(RestUtils.Headers.UserMetaData_Header_Prefix + "key1"));

    // mimicing old style user metadata which will result in more than one key value pairs
    userMetadataByteArray = UtilsTest.getRandomString(RestUtils.Max_UserMetadata_Value_Size + 2).getBytes();
    userMetadataMap = RestUtils.buildUserMetadata(userMetadataByteArray);
    assertEquals("User metadata size don't match ", userMetadataMap.size(), 2);
    assertTrue("User metadata key1 not found in user metadata ", userMetadataMap.containsKey(key1));
    assertTrue("User metadata key2 not found in user metadata ",
        userMetadataMap.containsKey(RestUtils.Headers.UserMetaData_OldStyle_Prefix + "1"));
    byte[] value1 = new byte[RestUtils.Max_UserMetadata_Value_Size];
    ByteBuffer byteBufferPart = ByteBuffer.wrap(userMetadataByteArray);
    byteBufferPart.get(value1);
    assertEquals("User metadata key1 value don't match ", new String(value1, StandardCharsets.US_ASCII),
        userMetadataMap.get(key1));
    byte[] value2 = new byte[2];
    byteBufferPart.get(value2);
    assertEquals("User metadata key1 value don't match ", new String(value2, StandardCharsets.US_ASCII),
        userMetadataMap.get(key2));
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
  public String generateRandomString(int length) {
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
      String serviceId, String contentType, String ownerId)
      throws JSONException {
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
  private void verifyBlobPropertiesConstructionSuccess(JSONObject headers)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers);
    BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest);
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
    byte[] userMetadata = RestUtils.buildUsermetadata(restRequest);
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
   * Verifies that {@link RestUtils#buildBlobProperties(RestRequest)} fails if given a request with bad headers.
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
      RestUtils.buildBlobProperties(restRequest);
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
      RestUtils.buildBlobProperties(restRequest);
      fail("An exception was expected but none were thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
  }

  /**
   * Sets entries from the passed in HashMap to the @{link JSONObject} headers
   * @param headers  {@link JSONObject} to which the new headers are to be added
   * @param userMetadata {@link Map} which has the new entries that has to be added
   * @throws org.json.JSONException
   */
  public static void setUserMetadataHeaders(JSONObject headers, Map<String, String> userMetadata)
      throws JSONException {
    for (String key : userMetadata.keySet()) {
      headers.put(key, userMetadata.get(key));
    }
  }
}
