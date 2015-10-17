package com.github.ambry.rest;

import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Utils;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Random;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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
    headers.put(RestConstants.Headers.Blob_Size, JSONObject.NULL);
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
    headers.put(RestConstants.Headers.Service_Id, JSONObject.NULL);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // contentType missing.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, null, ownerId);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.MissingArgs);
    // contentType null.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, null, ownerId);
    headers.put(RestConstants.Headers.Content_Type, JSONObject.NULL);
    verifyBlobPropertiesConstructionFailure(headers, RestServiceErrorCode.InvalidArgs);
    // too many values for all headers.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, contentType, ownerId);
    tooManyValuesTest(headers, RestConstants.Headers.Blob_Size);
    tooManyValuesTest(headers, RestConstants.Headers.TTL);
    tooManyValuesTest(headers, RestConstants.Headers.Private);
    tooManyValuesTest(headers, RestConstants.Headers.Service_Id);
    tooManyValuesTest(headers, RestConstants.Headers.Content_Type);
    tooManyValuesTest(headers, RestConstants.Headers.Owner_Id);

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
    headers.put(RestConstants.Headers.TTL, JSONObject.NULL);
    verifyBlobPropertiesConstructionSuccess(headers);

    // isPrivate null.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, contentType, ownerId);
    headers.put(RestConstants.Headers.Private, JSONObject.NULL);
    verifyBlobPropertiesConstructionSuccess(headers);

    // ownerId null.
    headers = new JSONObject();
    setAmbryHeaders(headers, contentLength, ttl, isPrivate, serviceId, contentType, null);
    headers.put(RestConstants.Headers.Owner_Id, JSONObject.NULL);
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
   * @param contentLength sets the {@link RestConstants.Headers#Blob_Size} header.
   * @param ttlInSecs sets the {@link RestConstants.Headers#TTL} header.
   * @param isPrivate sets the {@link RestConstants.Headers#Private} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestConstants.Headers#Service_Id} header.
   * @param contentType sets the {@link RestConstants.Headers#Content_Type} header.
   * @param ownerId sets the {@link RestConstants.Headers#Owner_Id} header. Optional - if not required, send null.
   * @throws JSONException
   */
  private void setAmbryHeaders(JSONObject headers, String contentLength, String ttlInSecs, String isPrivate,
      String serviceId, String contentType, String ownerId)
      throws JSONException {
    if (contentLength != null) {
      headers.put(RestConstants.Headers.Blob_Size, contentLength);
    }
    if (ttlInSecs != null) {
      headers.put(RestConstants.Headers.TTL, ttlInSecs);
    }
    if (isPrivate != null) {
      headers.put(RestConstants.Headers.Private, isPrivate);
    }
    if (serviceId != null) {
      headers.put(RestConstants.Headers.Service_Id, serviceId);
    }
    if (contentType != null) {
      headers.put(RestConstants.Headers.Content_Type, contentType);
    }
    if (ownerId != null) {
      headers.put(RestConstants.Headers.Owner_Id, ownerId);
    }
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
    assertEquals("Blob size does not match", headers.getLong(RestConstants.Headers.Blob_Size),
        blobProperties.getBlobSize());
    long expectedTTL = Utils.Infinite_Time;
    if (headers.has(RestConstants.Headers.TTL) && !JSONObject.NULL.equals(headers.get(RestConstants.Headers.TTL))) {
      expectedTTL = headers.getLong(RestConstants.Headers.TTL);
    }
    assertEquals("Blob TTL does not match", expectedTTL, blobProperties.getTimeToLiveInSeconds());
    boolean expectedIsPrivate = false;
    if (headers.has(RestConstants.Headers.Private) && !JSONObject.NULL
        .equals(headers.get(RestConstants.Headers.Private))) {
      expectedIsPrivate = headers.getBoolean(RestConstants.Headers.Private);
    }
    assertEquals("Blob isPrivate does not match", expectedIsPrivate, blobProperties.isPrivate());
    assertEquals("Blob service ID does not match", headers.getString(RestConstants.Headers.Service_Id),
        blobProperties.getServiceId());
    assertEquals("Blob content type does not match", headers.getString(RestConstants.Headers.Content_Type),
        blobProperties.getContentType());
    if (headers.has(RestConstants.Headers.Owner_Id) && !JSONObject.NULL
        .equals(headers.get(RestConstants.Headers.Owner_Id))) {
      assertEquals("Blob owner ID does not match", headers.getString(RestConstants.Headers.Owner_Id),
          blobProperties.getOwnerId());
    }
  }

  // getBlobPropertiesVariedInputTest() helpers.

  /**
   * Verifies that {@link RestUtils#buildBlobProperties(RestRequest)} fails if given a request with bad headers.
   * @param headers the headers that were provided to the request.
   * @param expectedCode the expected {@link RestServiceErrorCode} because of the failure.
   * @throws Exception
   */
  private void verifyBlobPropertiesConstructionFailure(JSONObject headers, RestServiceErrorCode expectedCode)
      throws Exception {
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
   * @throws Exception
   */
  private void tooManyValuesTest(JSONObject headers, String extraValueHeader)
      throws Exception {
    StringBuilder uriBuilder =
        new StringBuilder("?").append(extraValueHeader).append("=").append("extraVal1").append("&")
            .append(extraValueHeader).append("=").append("extraVal2");
    try {
      RestRequest restRequest = createRestRequest(RestMethod.POST, uriBuilder.toString(), headers);
      RestUtils.buildBlobProperties(restRequest);
      fail("An exception was expected but none were thrown");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
  }
}
