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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link AmbryUrlSigningService}.
 */
public class AmbryUrlSigningServiceTest {
  private static final String UPLOAD_ENDPOINT = "http://uploadUrl:15158";
  private static final String DOWNLOAD_ENDPOINT = "http://downloadUrl:15158";
  private static final long DEFAULT_URL_TTL_SECS = 5 * 60;
  private static final long DEFAULT_MAX_UPLOAD_SIZE = 100 * 1024 * 1024;
  private static final long MAX_URL_TTL_SECS = 60 * 60;
  private static final long CHUNK_UPLOAD_INITIAL_CHUNK_TTL_SECS = 24 * 1024 * 1024;
  private static final long CHUNK_UPLOAD_MAX_CHUNK_SIZE = 4 * 1024 * 1024;
  private static final String RANDOM_AMBRY_HEADER = AmbryUrlSigningService.AMBRY_PARAMETERS_PREFIX + "random";

  /**
   * Tests for {@link AmbryUrlSigningServiceFactory}.
   */
  @Test
  public void factoryTest() {
    Properties properties = new Properties();
    JSONObject jsonObject = new JSONObject().put("POST", UPLOAD_ENDPOINT).put("GET", DOWNLOAD_ENDPOINT);
    properties.setProperty(FrontendConfig.URL_SIGNER_ENDPOINTS, jsonObject.toString());
    properties.setProperty("frontend.url.signer.default.url.ttl.secs", Long.toString(DEFAULT_URL_TTL_SECS));
    properties.setProperty("frontend.url.signer.default.max.upload.size.bytes", Long.toString(DEFAULT_MAX_UPLOAD_SIZE));
    properties.setProperty("frontend.url.signer.max.url.ttl.secs", Long.toString(MAX_URL_TTL_SECS));
    properties.setProperty(FrontendConfig.CHUNK_UPLOAD_INITIAL_CHUNK_TTL_SECS_KEY,
        Long.toString(CHUNK_UPLOAD_INITIAL_CHUNK_TTL_SECS));
    CommonTestUtils.populateRequiredRouterProps(properties);
    properties.setProperty("router.max.put.chunk.size.bytes", Long.toString(CHUNK_UPLOAD_MAX_CHUNK_SIZE));
    UrlSigningService signer = new AmbryUrlSigningServiceFactory(new VerifiableProperties(properties),
        new MetricRegistry()).getUrlSigningService();
    assertNotNull("UrlSigningService is null", signer);
    assertTrue("UrlSigningService is AmbryUrlSigningService", signer instanceof AmbryUrlSigningService);
    assertTrue(((AmbryUrlSigningService) signer).getUploadEndpoint().contains(UPLOAD_ENDPOINT));
    assertTrue(((AmbryUrlSigningService) signer).getDownloadEndpoint().contains(DOWNLOAD_ENDPOINT));
  }

  /**
   * Tests for {@link AmbryUrlSigningServiceFactory}.
   */
  @Test
  public void factoryTestBadJson() {
    Properties properties = new Properties();
    CommonTestUtils.populateRequiredRouterProps(properties);
    // Missing GET
    JSONObject jsonObject = new JSONObject().put("POST", UPLOAD_ENDPOINT);
    properties.setProperty(FrontendConfig.URL_SIGNER_ENDPOINTS, jsonObject.toString());
    try {
      new AmbryUrlSigningServiceFactory(new VerifiableProperties(properties),
          new MetricRegistry()).getUrlSigningService();
      fail("Expected exception");
    } catch (IllegalStateException ex) {
    }
    // Missing POST
    jsonObject = new JSONObject().put("GET", DOWNLOAD_ENDPOINT);
    properties.setProperty(FrontendConfig.URL_SIGNER_ENDPOINTS, jsonObject.toString());
    try {
      new AmbryUrlSigningServiceFactory(new VerifiableProperties(properties),
          new MetricRegistry()).getUrlSigningService();
      fail("Expected exception");
    } catch (IllegalStateException ex) {
    }
    // Gibberish
    properties.setProperty(FrontendConfig.URL_SIGNER_ENDPOINTS, "[Garbage string &%#123");
    try {
      new AmbryUrlSigningServiceFactory(new VerifiableProperties(properties),
          new MetricRegistry()).getUrlSigningService();
      fail("Expected exception");
    } catch (IllegalStateException ex) {
    }
  }

  /**
   * Tests that generate and verify signed URLs.
   * @throws Exception
   */
  @Test
  public void signAndVerifyTest() throws Exception {
    Time time = new MockTime();
    AmbryUrlSigningService signer = getUrlSignerWithDefaults(time);
    doSignAndVerifyTest(signer, RestMethod.POST, time);
    doSignAndVerifyTest(signer, RestMethod.GET, time);
    signFailuresTest();
  }

  /**
   * Tests for some failure scenarios in verification.
   * @throws Exception
   */
  @Test
  public void notSignedRequestFailuresTest() throws Exception {
    // positive test done in signAndVerifyTest()
    AmbryUrlSigningService signer = getUrlSignerWithDefaults(new MockTime());
    RestRequest request = getRequestFromUrl(RestMethod.GET, "/");
    assertFalse("Request should not be declared signed", signer.isRequestSigned(request));
    ensureVerificationFailure(signer, request, RestServiceErrorCode.InternalServerError);
    request.setArg(RestUtils.Headers.URL_TYPE, RestMethod.POST.name());
    assertFalse("Request should not be declared signed", signer.isRequestSigned(request));
    ensureVerificationFailure(signer, request, RestServiceErrorCode.InternalServerError);
  }

  // helpers
  // general

  /**
   * Gets a {@link AmbryUrlSigningService} with some default construction parameters.
   * @param time the {@link Time} instance to use.
   * @return a {@link AmbryUrlSigningService} with some default construction parameters.
   */
  private AmbryUrlSigningService getUrlSignerWithDefaults(Time time) {
    return new AmbryUrlSigningService(UPLOAD_ENDPOINT, DOWNLOAD_ENDPOINT, DEFAULT_URL_TTL_SECS, DEFAULT_MAX_UPLOAD_SIZE,
        MAX_URL_TTL_SECS, CHUNK_UPLOAD_INITIAL_CHUNK_TTL_SECS, CHUNK_UPLOAD_MAX_CHUNK_SIZE, time);
  }

  /**
   * Gets a {@link RestRequest} that is a request to get a signed URL.
   * @param restMethod the {@link RestMethod} of signed URL required. Not added to request if {@code null}.
   * @param urlTtlSecs the ttl of the signed URL in secs. Ignored if == {@link Utils#Infinite_Time}.
   * @param randomHeaderVal the value of {@link #RANDOM_AMBRY_HEADER}.
   * @param maxUploadSize if {@code restMethod} is {@link RestMethod#POST}, the value of max upload size. Ignored if
   *                      {@code null}.
   * @param chunkUpload
   * @return a {@link RestRequest} that is a request to sign a URL.
   * @throws Exception
   */
  private RestRequest getUrlSignRequest(RestMethod restMethod, long urlTtlSecs, String randomHeaderVal,
      Long maxUploadSize, boolean chunkUpload) throws Exception {
    RestRequest request = getRequestFromUrl(RestMethod.GET, "signedUrl");
    if (restMethod != null) {
      request.setArg(RestUtils.Headers.URL_TYPE, restMethod.name());
    }
    if (urlTtlSecs != Utils.Infinite_Time) {
      request.setArg(RestUtils.Headers.URL_TTL, Long.toString(urlTtlSecs));
    }
    if (randomHeaderVal != null) {
      request.setArg(RANDOM_AMBRY_HEADER, randomHeaderVal);
    }
    if (RestMethod.POST.equals(restMethod) && maxUploadSize != null) {
      request.setArg(RestUtils.Headers.MAX_UPLOAD_SIZE, Long.toString(maxUploadSize));
    }
    if (chunkUpload) {
      request.setArg(RestUtils.Headers.CHUNK_UPLOAD, "true");
    }
    return request;
  }

  /**
   * Gets a {@link RestRequest} from {@code url}.
   * @param restMethod the {@link RestMethod} of the request.
   * @param url the url of the request.
   * @return a {@link RestRequest} from {@code url}.
   * @throws Exception
   */
  private RestRequest getRequestFromUrl(RestMethod restMethod, String url) throws Exception {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    request.put(MockRestRequest.URI_KEY, url);
    return new MockRestRequest(request, null);
  }

  // signAndVerifyTest() tests helpers

  /**
   * Generates and verifies signed URLs. Also tests some failure scenarios.
   * @param signer the {@link AmbryUrlSigningService} to use.
   * @param restMethod the {@link RestMethod} to verify.
   * @param time the {@link Time} instance to use.
   * @throws Exception
   */
  private void doSignAndVerifyTest(AmbryUrlSigningService signer, RestMethod restMethod, Time time) throws Exception {
    long urlTtl = Math.min(Utils.getRandomLong(TestUtils.RANDOM, 2000) + 2000, MAX_URL_TTL_SECS);
    String randomHeaderVal = TestUtils.getRandomString(10);
    long maxUploadSize = Utils.getRandomLong(TestUtils.RANDOM, 4001) + 2000;

    // all defaults overridden
    RestRequest request = getUrlSignRequest(restMethod, urlTtl, randomHeaderVal, maxUploadSize, false);
    String url = signer.getSignedUrl(request);
    verifySignedUrl(signer, url, restMethod, randomHeaderVal, maxUploadSize, Utils.Infinite_Time, false);
    time.sleep(TimeUnit.SECONDS.toMillis(urlTtl + 1));
    ensureVerificationFailure(signer, getRequestFromUrl(restMethod, url), RestServiceErrorCode.Unauthorized);
    // chunk upload case, should use special stitched chunk upload settings.
    request = getUrlSignRequest(restMethod, urlTtl, randomHeaderVal, maxUploadSize, true);
    url = signer.getSignedUrl(request);
    verifySignedUrl(signer, url, restMethod, randomHeaderVal, CHUNK_UPLOAD_MAX_CHUNK_SIZE,
        CHUNK_UPLOAD_INITIAL_CHUNK_TTL_SECS, true);
    time.sleep(TimeUnit.SECONDS.toMillis(urlTtl + 1));
    ensureVerificationFailure(signer, getRequestFromUrl(restMethod, url), RestServiceErrorCode.Unauthorized);
    // no defaults overridden
    request = getUrlSignRequest(restMethod, Utils.Infinite_Time, randomHeaderVal, null, false);
    url = signer.getSignedUrl(request);
    verifySignedUrl(signer, url, restMethod, randomHeaderVal, DEFAULT_MAX_UPLOAD_SIZE, Utils.Infinite_Time, false);
    // change RestMethod and ensure verification failure
    ensureVerificationFailure(signer, getRequestFromUrl(RestMethod.UNKNOWN, url), RestServiceErrorCode.Unauthorized);
    time.sleep(TimeUnit.SECONDS.toMillis(DEFAULT_URL_TTL_SECS + 1));
    ensureVerificationFailure(signer, getRequestFromUrl(restMethod, url), RestServiceErrorCode.Unauthorized);
  }

  /**
   * Verifies that a signed URL contains parameters as provided and passes verification.
   * @param signer the {@link AmbryUrlSigningService} to use.
   * @param url the signed URL.
   * @param restMethod the {@link RestMethod} intended by {@code url}.
   * @param randomHeaderVal the expected value of {@link #RANDOM_AMBRY_HEADER}.
   * @param maxUploadSize the expected value of {@link RestUtils.Headers#MAX_UPLOAD_SIZE}.
   * @param chunkUploadSet {@code true} if the signed URL should be a chunk upload URL.
   * @throws Exception
   */
  private void verifySignedUrl(AmbryUrlSigningService signer, String url, RestMethod restMethod, String randomHeaderVal,
      long maxUploadSize, long blobTtl, boolean chunkUploadSet) throws Exception {
    RestRequest signedRequest = getRequestFromUrl(restMethod, url);
    assertTrue("Request should be declared as signed", signer.isRequestSigned(signedRequest));
    signer.verifySignedRequest(signedRequest);
    Map<String, Object> args = signedRequest.getArgs();
    assertEquals("URL type not as expected", restMethod.name(), args.get(RestUtils.Headers.URL_TYPE).toString());
    assertEquals("Random header value is not as expected", randomHeaderVal, args.get(RANDOM_AMBRY_HEADER).toString());
    Object blobTtlVal = args.get(RestUtils.Headers.TTL);
    Object chunkUploadVal = args.get(RestUtils.Headers.CHUNK_UPLOAD);
    Object sessionVal = args.get(RestUtils.Headers.SESSION);
    if (restMethod.equals(RestMethod.POST)) {
      assertEquals("Max upload size not as expected", maxUploadSize,
          Long.parseLong(args.get(RestUtils.Headers.MAX_UPLOAD_SIZE).toString()));
      if (blobTtl != Utils.Infinite_Time) {
        assertEquals("Blob TTL not as expected", blobTtl, Long.parseLong(blobTtlVal.toString()));
      }
      if (chunkUploadSet) {
        assertTrue("Chunk upload should be set", Boolean.valueOf(chunkUploadVal.toString()));
        assertNotNull("Session should be set", sessionVal);
        // ensure that the x-ambry-chunk-upload-session value is a valid UUID.
        UUID.fromString(sessionVal.toString());
      } else {
        assertNull("Chunk upload should not be set", chunkUploadVal);
        assertNull("Session should not be set", sessionVal);
      }
    } else {
      assertNull("Blob TTL should not be set", blobTtlVal);
      assertNull("Chunk upload should not be set", chunkUploadVal);
      assertNull("Session should not be set", sessionVal);
    }
  }

  /**
   * Ensures that the verification of {@code url} fails.
   * @param signer the {@link AmbryUrlSigningService} to use.
   * @param request the {@link RestRequest} which contains the url to check
   * @param errorCode the {@link RestServiceErrorCode} expected on verification. If {@code null}, it is assumed that a
   *                  {@link IllegalArgumentException} is expected.
   * @throws Exception
   */
  private void ensureVerificationFailure(AmbryUrlSigningService signer, RestRequest request,
      RestServiceErrorCode errorCode) throws Exception {
    try {
      signer.verifySignedRequest(request);
      fail("Verification of request should have failed");
    } catch (IllegalArgumentException e) {
      assertNull("Did not encounter a RestServiceException with given error code", errorCode);
      assertFalse("URL should have been declared as not signed", signer.isRequestSigned(request));
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", errorCode, e.getErrorCode());
    }
  }

  /**
   * Tests for failure scenarios when trying to generate signed URLs.
   * @throws Exception
   */
  private void signFailuresTest() throws Exception {
    AmbryUrlSigningService signer = getUrlSignerWithDefaults(new MockTime());
    // RestMethod not present
    RestRequest request = getUrlSignRequest(null, Utils.Infinite_Time, null, -1L, false);
    ensureSignedUrlCreationFailure(signer, request, RestServiceErrorCode.MissingArgs);

    // unknown RestMethod
    request = getUrlSignRequest(null, Utils.Infinite_Time, null, -1L, false);
    request.setArg(RestUtils.Headers.URL_TYPE, "@@unknown@@");
    ensureSignedUrlCreationFailure(signer, request, RestServiceErrorCode.InvalidArgs);

    // RestMethod not supported
    request = getUrlSignRequest(RestMethod.DELETE, Utils.Infinite_Time, null, -1L, false);
    ensureSignedUrlCreationFailure(signer, request, RestServiceErrorCode.InvalidArgs);

    // url ttl not long
    request = getUrlSignRequest(RestMethod.POST, Utils.Infinite_Time, null, -1L, false);
    request.setArg(RestUtils.Headers.URL_TTL, "@@notlong@@");
    ensureSignedUrlCreationFailure(signer, request, RestServiceErrorCode.InvalidArgs);

    // max upload size not long
    request = getUrlSignRequest(RestMethod.POST, Utils.Infinite_Time, null, -1L, false);
    request.setArg(RestUtils.Headers.MAX_UPLOAD_SIZE, "@@notlong@@");
    ensureSignedUrlCreationFailure(signer, request, RestServiceErrorCode.InvalidArgs);
  }

  /**
   * Ensures that construction of a signed URL from {@code request} fails.
   * @param signer the {@link AmbryUrlSigningService} to use.
   * @param request the {@link RestRequest} containing all the parameters for signing.
   * @param errorCode the {@link RestServiceErrorCode} expected when an attempt is made to the construct a signed URL.
   */
  private void ensureSignedUrlCreationFailure(AmbryUrlSigningService signer, RestRequest request,
      RestServiceErrorCode errorCode) {
    try {
      signer.getSignedUrl(request);
      fail("Should have failed to create a signed URL");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", errorCode, e.getErrorCode());
    }
  }
}
