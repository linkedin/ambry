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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.Callback;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;


/**
 * Unit tests {@link AmbrySecurityService}
 */
public class AmbrySecurityServiceTest {

  private static final FrontendConfig FRONTEND_CONFIG = new FrontendConfig(new VerifiableProperties(new Properties()));
  private static final String SERVICE_ID = "AmbrySecurityService";
  private static final String OWNER_ID = SERVICE_ID;
  private static final BlobInfo DEFAULT_INFO =
      new BlobInfo(new BlobProperties(100, SERVICE_ID, OWNER_ID, "image/gif", true, Utils.Infinite_Time), null);

  private final SecurityService securityService =
      new AmbrySecurityService(FRONTEND_CONFIG, new FrontendMetrics(new MetricRegistry()));

  /**
   * Tests {@link AmbrySecurityService#processRequest(RestRequest, Callback)} for common as well as uncommon cases
   * @throws Exception
   */
  @Test
  public void processRequestTest() throws Exception {
    SecurityServiceCallback callback = new SecurityServiceCallback();
    //rest request being null
    try {
      securityService.processRequest(null, callback).get();
      Assert.fail("Should have thrown IllegalArgumentException ");
    } catch (IllegalArgumentException e) {
    }

    // without callbacks
    RestMethod[] methods = new RestMethod[]{RestMethod.POST, RestMethod.GET, RestMethod.DELETE, RestMethod.HEAD};
    for (RestMethod restMethod : methods) {
      RestRequest restRequest = createRestRequest(restMethod, "/", null);
      securityService.processRequest(restRequest, null).get();
    }

    // with callbacks
    callback = new SecurityServiceCallback();
    for (RestMethod restMethod : methods) {
      RestRequest restRequest = createRestRequest(restMethod, "/", null);
      securityService.processRequest(restRequest, callback).get();
      Assert.assertTrue("Callback should have been invoked", callback.callbackLatch.await(1, TimeUnit.SECONDS));
      Assert.assertNull("Exception should not have been thrown", callback.exception);
      callback.reset();
    }

    // with GET sub resources
    callback.reset();
    for (RestUtils.SubResource subResource : RestUtils.SubResource.values()) {
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/sampleId/" + subResource, null);
      switch (subResource) {
        case BlobInfo:
        case UserMetadata:
          securityService.processRequest(restRequest, callback).get();
          Assert.assertTrue("Callback should have been invoked", callback.callbackLatch.await(1, TimeUnit.SECONDS));
          Assert.assertNull("Exception should not have been thrown", callback.exception);
          break;
        default:
          testExceptionCasesProcessRequest(restRequest, RestServiceErrorCode.BadRequest);
          break;
      }

      callback.reset();
    }

    // security service closed
    securityService.close();
    for (RestMethod restMethod : methods) {
      testExceptionCasesProcessRequest(createRestRequest(restMethod, "/", null),
          RestServiceErrorCode.ServiceUnavailable);
    }
  }

  /**
   * Tests {@link AmbrySecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)}  for
   * common as well as uncommon cases
   * @throws Exception
   */
  @Test
  public void processResponseTest() throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null);
    //rest request being null
    try {
      securityService.processResponse(null, new MockRestResponseChannel(), DEFAULT_INFO, null).get();
      Assert.fail("Should have thrown IllegalArgumentException ");
    } catch (IllegalArgumentException e) {
    }

    //restResponseChannel being null
    try {
      securityService.processResponse(restRequest, null, DEFAULT_INFO, null).get();
      Assert.fail("Should have thrown IllegalArgumentException ");
    } catch (IllegalArgumentException e) {
    }

    //blob info being null
    try {
      securityService.processResponse(restRequest, new MockRestResponseChannel(), null, null).get();
      Assert.fail("Should have thrown IllegalArgumentException ");
    } catch (IllegalArgumentException e) {
    }

    // without callbacks
    RestMethod[] methods = new RestMethod[]{RestMethod.POST, RestMethod.GET, RestMethod.HEAD};
    for (RestMethod restMethod : methods) {
      restRequest = createRestRequest(restMethod, "/", null);
      securityService.processResponse(restRequest, new MockRestResponseChannel(), DEFAULT_INFO, null).get();
    }

    // with callbacks
    // for unsupported methods
    methods = new RestMethod[]{RestMethod.DELETE};
    for (RestMethod restMethod : methods) {
      testExceptionCasesProcessResponse(restMethod, new MockRestResponseChannel(), DEFAULT_INFO,
          RestServiceErrorCode.InternalServerError);
    }

    // HEAD
    // normal
    testHeadBlobWithVariousRanges(DEFAULT_INFO);
    // with no owner id
    BlobInfo blobInfo =
        new BlobInfo(new BlobProperties(100, SERVICE_ID, null, "image/gif", false, Utils.Infinite_Time), null);
    testHeadBlobWithVariousRanges(blobInfo);
    // with no content type
    blobInfo = new BlobInfo(new BlobProperties(100, SERVICE_ID, OWNER_ID, null, false, Utils.Infinite_Time), null);
    testHeadBlobWithVariousRanges(blobInfo);
    // with a TTL
    blobInfo = new BlobInfo(new BlobProperties(100, SERVICE_ID, OWNER_ID, "image/gif", false, 10000), null);
    testHeadBlobWithVariousRanges(blobInfo);

    // GET BlobInfo
    testGetSubResource(RestUtils.SubResource.BlobInfo);
    // GET UserMetadata
    testGetSubResource(RestUtils.SubResource.UserMetadata);

    // POST
    testPostBlob();

    // GET Blob
    // less than chunk threshold size
    blobInfo = new BlobInfo(
        new BlobProperties(FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes - 1, SERVICE_ID, OWNER_ID,
            "image/gif", false, 10000), null);
    testGetBlobWithVariousRanges(blobInfo);
    // == chunk threshold size
    blobInfo = new BlobInfo(
        new BlobProperties(FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes, SERVICE_ID, OWNER_ID,
            "image/gif", false, 10000), null);
    testGetBlobWithVariousRanges(blobInfo);
    // more than chunk threshold size
    blobInfo = new BlobInfo(
        new BlobProperties(FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes * 2, SERVICE_ID, OWNER_ID,
            "image/gif", false, 10000), null);
    testGetBlobWithVariousRanges(blobInfo);
    // Get blob with content type null
    blobInfo = new BlobInfo(new BlobProperties(100, SERVICE_ID, OWNER_ID, null, true, 10000), null);
    testGetBlobWithVariousRanges(blobInfo);
    // Get blob for a private blob
    blobInfo =
        new BlobInfo(new BlobProperties(100, SERVICE_ID, OWNER_ID, "image/gif", false, Utils.Infinite_Time), null);
    testGetBlobWithVariousRanges(blobInfo);
    // Get blob for a public blob with content type as "text/html"
    blobInfo = new BlobInfo(new BlobProperties(100, SERVICE_ID, OWNER_ID, "text/html", true, 10000), null);
    testGetBlobWithVariousRanges(blobInfo);
    // not modified response
    // > creation time (in secs).
    testGetNotModifiedBlob(DEFAULT_INFO, DEFAULT_INFO.getBlobProperties().getCreationTimeInMs() + 1000);
    // == creation time
    testGetNotModifiedBlob(DEFAULT_INFO, DEFAULT_INFO.getBlobProperties().getCreationTimeInMs());
    // < creation time (in secs)
    testGetNotModifiedBlob(DEFAULT_INFO, DEFAULT_INFO.getBlobProperties().getCreationTimeInMs() - 1000);

    // bad rest response channel
    testExceptionCasesProcessResponse(RestMethod.HEAD, new BadRestResponseChannel(), blobInfo,
        RestServiceErrorCode.InternalServerError);
    testExceptionCasesProcessResponse(RestMethod.GET, new BadRestResponseChannel(), blobInfo,
        RestServiceErrorCode.InternalServerError);
    testExceptionCasesProcessResponse(RestMethod.POST, new BadRestResponseChannel(), blobInfo,
        RestServiceErrorCode.InternalServerError);

    // security service closed
    securityService.close();
    methods = new RestMethod[]{RestMethod.POST, RestMethod.GET, RestMethod.DELETE, RestMethod.HEAD};
    for (RestMethod restMethod : methods) {
      testExceptionCasesProcessResponse(restMethod, new MockRestResponseChannel(), blobInfo,
          RestServiceErrorCode.ServiceUnavailable);
    }
  }

  /**
   * Method to easily create {@link RestRequest} objects containing a specific request.
   * @param restMethod the {@link RestMethod} desired.
   * @param uri string representation of the desired URI.
   * @param headers any associated headers as a {@link JSONObject}.
   * @return A {@link RestRequest} object that defines the request required by the input.
   * @throws org.json.JSONException
   * @throws java.io.UnsupportedEncodingException
   * @throws java.net.URISyntaxException
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
   * Tests exception cases for {@link SecurityService#processRequest(RestRequest, Callback)}
   * @param restRequest the {@link RestRequest} to provide as input.
   * @param expectedErrorCode the {@link RestServiceErrorCode} expected in the exception returned.
   * @throws Exception
   */
  private void testExceptionCasesProcessRequest(RestRequest restRequest, RestServiceErrorCode expectedErrorCode)
      throws Exception {
    SecurityServiceCallback callback = new SecurityServiceCallback();
    try {
      securityService.processRequest(restRequest, callback).get();
      Assert.fail("Should have thrown Exception");
    } catch (ExecutionException e) {
      Assert.assertTrue("Exception should have been an instance of RestServiceException",
          e.getCause() instanceof RestServiceException);
      RestServiceException re = (RestServiceException) e.getCause();
      Assert.assertEquals("Unexpected RestServerErrorCode (Future)", expectedErrorCode, re.getErrorCode());
      Assert.assertTrue("Callback should have been invoked", callback.callbackLatch.await(1, TimeUnit.SECONDS));
      Assert.assertNotNull("Exception should have been thrown", callback.exception);
      re = (RestServiceException) callback.exception;
      Assert.assertEquals("Unexpected RestServerErrorCode (Callback)", expectedErrorCode, re.getErrorCode());
    }
  }

  /**
   * Verifies that there are no values for all headers in {@code headers}.
   * @param restResponseChannel the {@link MockRestResponseChannel} over which response has been received.
   * @param headers the headers that must have no values.
   */
  private void verifyAbsenceOfHeaders(MockRestResponseChannel restResponseChannel, String... headers) {
    for (String header : headers) {
      Assert.assertNull("[" + header + "] should not have been present in the response",
          restResponseChannel.getHeader(header));
    }
  }

  /**
   * Tests {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)} for a Get blob
   * with the passed in {@link BlobInfo} and various range settings, including no set range (entire blob).
   * @param blobInfo the {@link BlobInfo} to be used for the {@link RestRequest}s
   * @throws Exception
   */
  private void testGetBlobWithVariousRanges(BlobInfo blobInfo) throws Exception {
    long blobSize = blobInfo.getBlobProperties().getBlobSize();
    testGetBlob(blobInfo, null);

    testGetBlob(blobInfo, ByteRange.fromLastNBytes(0));
    if (blobSize > 0) {
      testGetBlob(blobInfo, ByteRange.fromStartOffset(0));
      testGetBlob(blobInfo, ByteRange.fromStartOffset(ThreadLocalRandom.current().nextLong(1, blobSize - 1)));
      testGetBlob(blobInfo, ByteRange.fromStartOffset(blobSize - 1));

      long random1 = ThreadLocalRandom.current().nextLong(blobSize);
      long random2 = ThreadLocalRandom.current().nextLong(blobSize);
      testGetBlob(blobInfo, ByteRange.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2)));
      testGetBlob(blobInfo, ByteRange.fromLastNBytes(blobSize));
    }
    if (blobSize > 1) {
      testGetBlob(blobInfo, ByteRange.fromLastNBytes(ThreadLocalRandom.current().nextLong(1, blobSize)));
    }
  }

  /**
   * Tests {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)} for a Get blob
   * with the passed in {@link BlobInfo} and {@link ByteRange}
   * @param blobInfo the {@link BlobInfo} to be used for the {@link RestRequest}
   * @param range the {@link ByteRange} for the {@link RestRequest}
   * @throws Exception
   */
  private void testGetBlob(BlobInfo blobInfo, ByteRange range) throws Exception {
    SecurityServiceCallback callback = new SecurityServiceCallback();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    JSONObject headers =
        range != null ? new JSONObject().put(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range)) : null;
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", headers);
    securityService.processResponse(restRequest, restResponseChannel, blobInfo, callback).get();
    Assert.assertTrue("Callback should have been invoked", callback.callbackLatch.await(1, TimeUnit.SECONDS));
    Assert.assertNull("Exception should not have been thrown", callback.exception);
    Assert.assertEquals("Response status should have been set",
        range == null ? ResponseStatus.Ok : ResponseStatus.PartialContent, restResponseChannel.getStatus());
    verifyHeadersForGetBlob(blobInfo.getBlobProperties(), range, restResponseChannel);
  }

  /**
   * Tests {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)} for a Get blob
   * with the passed in {@link BlobInfo} for a not modified response
   * @param blobInfo the {@link BlobInfo} to be used for the {@link RestRequest}
   * @param ifModifiedSinceMs the value (as a date string) of the {@link RestUtils.Headers#IF_MODIFIED_SINCE} header.
   * @throws Exception
   */
  private void testGetNotModifiedBlob(BlobInfo blobInfo, long ifModifiedSinceMs) throws Exception {
    SecurityServiceCallback callback = new SecurityServiceCallback();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    JSONObject headers = new JSONObject();
    SimpleDateFormat dateFormat = new SimpleDateFormat(RestUtils.HTTP_DATE_FORMAT, Locale.ENGLISH);
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    Date date = new Date(ifModifiedSinceMs);
    String dateStr = dateFormat.format(date);
    headers.put(RestUtils.Headers.IF_MODIFIED_SINCE, dateStr);
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/abc", headers);
    securityService.processResponse(restRequest, restResponseChannel, blobInfo, callback).get();
    Assert.assertTrue("Callback should have been invoked", callback.callbackLatch.await(1, TimeUnit.SECONDS));
    Assert.assertNull("Exception should not have been thrown", callback.exception);
    if (ifModifiedSinceMs >= blobInfo.getBlobProperties().getCreationTimeInMs()) {
      Assert.assertEquals("Not modified response expected", ResponseStatus.NotModified,
          restResponseChannel.getStatus());
      verifyHeadersForGetBlobNotModified(restResponseChannel);
    } else {
      Assert.assertEquals("Not modified response should not be returned", ResponseStatus.Ok,
          restResponseChannel.getStatus());
      verifyHeadersForGetBlob(blobInfo.getBlobProperties(), null, restResponseChannel);
    }
  }

  /**
   * Tests {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)} for a Head
   * request with the passed in {@link BlobInfo} and various range settings, including no set range (entire blob).
   * @param blobInfo the {@link BlobInfo} to be used for the {@link RestRequest}s
   * @throws Exception
   */
  private void testHeadBlobWithVariousRanges(BlobInfo blobInfo) throws Exception {
    long blobSize = blobInfo.getBlobProperties().getBlobSize();
    testHeadBlob(blobInfo, null);

    testHeadBlob(blobInfo, ByteRange.fromLastNBytes(0));
    if (blobSize > 0) {
      testHeadBlob(blobInfo, ByteRange.fromStartOffset(0));
      testHeadBlob(blobInfo, ByteRange.fromStartOffset(ThreadLocalRandom.current().nextLong(1, blobSize - 1)));
      testHeadBlob(blobInfo, ByteRange.fromStartOffset(blobSize - 1));

      long random1 = ThreadLocalRandom.current().nextLong(blobSize);
      long random2 = ThreadLocalRandom.current().nextLong(blobSize);
      testHeadBlob(blobInfo, ByteRange.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2)));

      testHeadBlob(blobInfo, ByteRange.fromLastNBytes(blobSize));
    }
    if (blobSize > 1) {
      testHeadBlob(blobInfo, ByteRange.fromLastNBytes(ThreadLocalRandom.current().nextLong(1, blobSize)));
    }
  }

  /**
   * Tests {@link AmbrySecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)} for
   * {@link RestMethod#HEAD}.
   * @param blobInfo the {@link BlobInfo} of the blob for which {@link RestMethod#HEAD} is required.
   * @param range the {@link ByteRange} used for a range request, or {@code null} for non-ranged requests.
   * @throws Exception
   */
  private void testHeadBlob(BlobInfo blobInfo, ByteRange range) throws Exception {
    SecurityServiceCallback callback = new SecurityServiceCallback();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    JSONObject headers =
        range != null ? new JSONObject().put(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range)) : null;
    RestRequest restRequest = createRestRequest(RestMethod.HEAD, "/", headers);
    securityService.processResponse(restRequest, restResponseChannel, blobInfo, callback).get();
    Assert.assertTrue("Callback should have been invoked", callback.callbackLatch.await(1, TimeUnit.SECONDS));
    Assert.assertNull("Exception should not have been thrown", callback.exception);
    Assert.assertEquals("Response status should have been set",
        range == null ? ResponseStatus.Ok : ResponseStatus.PartialContent, restResponseChannel.getStatus());
    verifyHeadersForHead(blobInfo.getBlobProperties(), range, restResponseChannel);
  }

  /**
   * Tests GET of sub-resources.
   * @param subResource the {@link RestUtils.SubResource}  to test.
   * @throws Exception
   */
  private void testGetSubResource(RestUtils.SubResource subResource) throws Exception {
    SecurityServiceCallback callback = new SecurityServiceCallback();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/sampleId/" + subResource, null);
    securityService.processResponse(restRequest, restResponseChannel, DEFAULT_INFO, callback).get();
    Assert.assertTrue("Callback should have been invoked", callback.callbackLatch.await(1, TimeUnit.SECONDS));
    Assert.assertNull("Exception should not have been thrown", callback.exception);
    Assert.assertEquals("Response status should have been set ", ResponseStatus.Ok, restResponseChannel.getStatus());
    Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    Assert.assertEquals("Last Modified does not match creation time",
        RestUtils.toSecondsPrecisionInMs(DEFAULT_INFO.getBlobProperties().getCreationTimeInMs()),
        RestUtils.getTimeFromDateString(restResponseChannel.getHeader(RestUtils.Headers.LAST_MODIFIED)).longValue());
    if (subResource.equals(RestUtils.SubResource.BlobInfo)) {
      verifyBlobPropertiesHeaders(DEFAULT_INFO.getBlobProperties(), restResponseChannel);
    } else {
      verifyAbsenceOfHeaders(restResponseChannel, RestUtils.Headers.PRIVATE, RestUtils.Headers.TTL,
          RestUtils.Headers.SERVICE_ID, RestUtils.Headers.OWNER_ID, RestUtils.Headers.AMBRY_CONTENT_TYPE,
          RestUtils.Headers.CREATION_TIME, RestUtils.Headers.BLOB_SIZE, RestUtils.Headers.ACCEPT_RANGES,
          RestUtils.Headers.CONTENT_RANGE);
    }
  }

  /**
   * Tests {@link AmbrySecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)} for
   * {@link RestMethod#POST}.
   * @throws Exception
   */
  private void testPostBlob() throws Exception {
    SecurityServiceCallback callback = new SecurityServiceCallback();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", null);
    securityService.processResponse(restRequest, restResponseChannel, DEFAULT_INFO, callback).get();
    Assert.assertTrue("Callback should have been invoked", callback.callbackLatch.await(1, TimeUnit.SECONDS));
    Assert.assertNull("Exception should not have been thrown", callback.exception);
    Assert.assertEquals("Response status should have been set", ResponseStatus.Created,
        restResponseChannel.getStatus());
    Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    Assert.assertEquals("Creation time should have been set correctly",
        RestUtils.toSecondsPrecisionInMs(DEFAULT_INFO.getBlobProperties().getCreationTimeInMs()),
        RestUtils.getTimeFromDateString(restResponseChannel.getHeader(RestUtils.Headers.CREATION_TIME)).longValue());
    Assert.assertEquals("Content-Length should have been 0", "0",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
  }

  /**
   * Tests exception cases for
   * {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)}
   * @param restMethod the {@link RestMethod} of the request to be made
   * @param restResponseChannel the {@link RestResponseChannel} to write responses over.
   * @param blobInfo the {@link BlobInfo} to be used for the {@link RestRequest}
   * @param expectedErrorCode the {@link RestServiceErrorCode} expected in the exception returned.
   * @throws Exception
   */
  private void testExceptionCasesProcessResponse(RestMethod restMethod, RestResponseChannel restResponseChannel,
      BlobInfo blobInfo, RestServiceErrorCode expectedErrorCode) throws Exception {
    RestRequest restRequest = createRestRequest(restMethod, "/", null);
    SecurityServiceCallback callback = new SecurityServiceCallback();
    try {
      securityService.processResponse(restRequest, restResponseChannel, blobInfo, callback).get();
      Assert.fail("Should have thrown Exception");
    } catch (ExecutionException e) {
      Assert.assertTrue("Exception should have been an instance of RestServiceException",
          e.getCause() instanceof RestServiceException);
      RestServiceException re = (RestServiceException) e.getCause();
      Assert.assertEquals("Unexpected RestServerErrorCode (Future)", expectedErrorCode, re.getErrorCode());
      Assert.assertTrue("Callback should have been invoked", callback.callbackLatch.await(1, TimeUnit.SECONDS));
      Assert.assertNotNull("Exception should have been thrown", callback.exception);
      re = (RestServiceException) callback.exception;
      Assert.assertEquals("Unexpected RestServerErrorCode (Callback)", expectedErrorCode, re.getErrorCode());
    }
  }

  /**
   * Verify the headers from the response are as expected
   * @param blobProperties the {@link BlobProperties} to refer to while getting headers.
   * @param range the {@link ByteRange} used for a range request, or {@code null} for non-ranged requests.
   * @param restResponseChannel {@link MockRestResponseChannel} from which headers are to be verified
   * @throws RestServiceException if there was any problem getting the headers.
   */
  private void verifyHeadersForHead(BlobProperties blobProperties, ByteRange range,
      MockRestResponseChannel restResponseChannel) throws RestServiceException {
    Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    Assert.assertEquals("Last Modified does not match creation time",
        RestUtils.toSecondsPrecisionInMs(blobProperties.getCreationTimeInMs()),
        RestUtils.getTimeFromDateString(restResponseChannel.getHeader(RestUtils.Headers.LAST_MODIFIED)).longValue());
    Assert.assertEquals("Accept ranges header not set correctly", "bytes",
        restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    long contentLength = blobProperties.getBlobSize();
    if (range != null) {
      Pair<String, Long> rangeAndLength = RestUtils.buildContentRangeAndLength(range, contentLength);
      Assert.assertEquals("Content range header not set correctly for range " + range, rangeAndLength.getFirst(),
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
      contentLength = rangeAndLength.getSecond();
    } else {
      Assert.assertNull("Content range header should not be set",
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    }
    Assert.assertEquals("Content length mismatch", contentLength,
        Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
    if (blobProperties.getContentType() != null) {
      Assert.assertEquals("Content Type mismatch", blobProperties.getContentType(),
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    }
    verifyBlobPropertiesHeaders(blobProperties, restResponseChannel);
  }

  /**
   * Verify the headers from the response are as expected
   * @param blobProperties the {@link BlobProperties} to refer to while getting headers.
   * @param range the {@link ByteRange} used for a range request, or {@code null} for non-ranged requests.
   * @param restResponseChannel {@link MockRestResponseChannel} from which headers are to be verified
   * @throws RestServiceException if there was any problem getting the headers.
   */
  private void verifyHeadersForGetBlob(BlobProperties blobProperties, ByteRange range,
      MockRestResponseChannel restResponseChannel) throws RestServiceException {
    Assert.assertEquals("Blob size mismatch ", blobProperties.getBlobSize(),
        Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE)));
    verifyAbsenceOfHeaders(restResponseChannel, RestUtils.Headers.PRIVATE, RestUtils.Headers.TTL,
        RestUtils.Headers.SERVICE_ID, RestUtils.Headers.OWNER_ID, RestUtils.Headers.AMBRY_CONTENT_TYPE,
        RestUtils.Headers.CREATION_TIME);
    if (blobProperties.getContentType() != null) {
      Assert.assertEquals("Content Type mismatch", blobProperties.getContentType(),
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
      if (blobProperties.getContentType().equals("text/html")) {
        Assert.assertEquals("Content disposition not set for text/html Cotnent type", "attachment",
            restResponseChannel.getHeader("Content-Disposition"));
      } else {
        Assert.assertNull("Content disposition should not have been set",
            restResponseChannel.getHeader("Content-Disposition"));
      }
    }

    Assert.assertEquals("Accept ranges header not set correctly", "bytes",
        restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    long contentLength = blobProperties.getBlobSize();
    if (range != null) {
      Pair<String, Long> rangeAndLength = RestUtils.buildContentRangeAndLength(range, contentLength);
      Assert.assertEquals("Content range header not set correctly for range " + range, rangeAndLength.getFirst(),
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
      contentLength = rangeAndLength.getSecond();
    } else {
      Assert.assertNull("Content range header should not be set",
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    }

    if (contentLength < FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes) {
      Assert.assertEquals("Content length value mismatch", contentLength,
          Integer.parseInt(restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
    } else {
      Assert.assertNull("Content length value should not be set",
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    }

    if (blobProperties.isPrivate()) {
      Assert.assertEquals("Expires value is incorrect for private blob", 0,
          RestUtils.getTimeFromDateString(restResponseChannel.getHeader(RestUtils.Headers.EXPIRES)).longValue());
      Assert.assertEquals("Cache-Control value not as expected", "private, no-cache, no-store, proxy-revalidate",
          restResponseChannel.getHeader(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertEquals("Pragma value not as expected", "no-cache",
          restResponseChannel.getHeader(RestUtils.Headers.PRAGMA));
    } else {
      Assert.assertTrue("Expires value should be in the future",
          RestUtils.getTimeFromDateString(restResponseChannel.getHeader(RestUtils.Headers.EXPIRES)).longValue() > System
              .currentTimeMillis());
      Assert.assertEquals("Cache-Control value not as expected",
          "max-age=" + FRONTEND_CONFIG.frontendCacheValiditySeconds,
          restResponseChannel.getHeader(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertNull("Pragma value should not have been set",
          restResponseChannel.getHeader(RestUtils.Headers.PRAGMA));
    }
  }

  /**
   * Verify the headers from the response for a Not modified blob are as expected
   * @param restResponseChannel {@link MockRestResponseChannel} from which headers are to be verified
   * @throws RestServiceException if there was any problem getting the headers.
   */
  private void verifyHeadersForGetBlobNotModified(MockRestResponseChannel restResponseChannel)
      throws RestServiceException {
    Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    Assert.assertEquals("Content length should have been 0", "0",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    Assert.assertNull("Accept-Ranges should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    Assert.assertNull("Content-Range header should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    verifyAbsenceOfHeaders(restResponseChannel, RestUtils.Headers.LAST_MODIFIED, RestUtils.Headers.BLOB_SIZE,
        RestUtils.Headers.CONTENT_TYPE, RestUtils.Headers.EXPIRES, RestUtils.Headers.CACHE_CONTROL,
        RestUtils.Headers.PRAGMA);
  }

  /**
   * Verify the headers from the response are as expected
   * @param blobProperties the {@link BlobProperties} to refer to while getting headers.
   * @param restResponseChannel {@link MockRestResponseChannel} from which headers are to be verified
   * @throws RestServiceException if there was any problem getting the headers.
   */
  private void verifyBlobPropertiesHeaders(BlobProperties blobProperties, MockRestResponseChannel restResponseChannel)
      throws RestServiceException {
    if (blobProperties.getContentType() != null) {
      Assert.assertEquals("Ambry Content Type mismatch", blobProperties.getContentType(),
          restResponseChannel.getHeader(RestUtils.Headers.AMBRY_CONTENT_TYPE));
    }
    Assert.assertEquals("Blob Size mismatch", blobProperties.getBlobSize(),
        Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE)));
    Assert.assertEquals("Service Id mismatch", blobProperties.getServiceId(),
        restResponseChannel.getHeader(RestUtils.Headers.SERVICE_ID));
    Assert.assertEquals("Creation time should have been set correctly",
        RestUtils.toSecondsPrecisionInMs(blobProperties.getCreationTimeInMs()),
        RestUtils.getTimeFromDateString(restResponseChannel.getHeader(RestUtils.Headers.CREATION_TIME)).longValue());
    Assert.assertEquals("Private value mismatch", blobProperties.isPrivate(),
        Boolean.parseBoolean(restResponseChannel.getHeader(RestUtils.Headers.PRIVATE)));
    if (blobProperties.getTimeToLiveInSeconds() != Utils.Infinite_Time) {
      Assert.assertEquals("TTL mismatch", blobProperties.getTimeToLiveInSeconds(),
          Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.TTL)));
    }
    if (blobProperties.getOwnerId() != null) {
      Assert.assertEquals("OwnerId mismatch", blobProperties.getOwnerId(),
          restResponseChannel.getHeader(RestUtils.Headers.OWNER_ID));
    }
  }

  /**
   * Callback for all operations on {@link SecurityService}.
   */
  class SecurityServiceCallback implements Callback<Void> {
    public volatile Exception exception;
    public CountDownLatch callbackLatch = new CountDownLatch(1);

    private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

    @Override
    public void onCompletion(Void result, Exception exception) {
      if (callbackInvoked.compareAndSet(false, true)) {
        this.exception = exception;
        callbackLatch.countDown();
      } else {
        this.exception = new IllegalStateException("Callback invoked more than once");
      }
    }

    /**
     * Resets the state for using this instance again.
     */
    public void reset() {
      exception = null;
      callbackInvoked.set(false);
      callbackLatch = new CountDownLatch(1);
    }
  }

  /**
   * A bad implementation of {@link RestResponseChannel}. Just throws exceptions.
   */
  class BadRestResponseChannel implements RestResponseChannel {

    @Override
    public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
      return null;
    }

    @Override
    public boolean isOpen() {
      return false;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void onResponseComplete(Exception exception) {
    }

    @Override
    public void setStatus(ResponseStatus status) throws RestServiceException {
      throw new RestServiceException("Not Implemented", RestServiceErrorCode.InternalServerError);
    }

    @Override
    public ResponseStatus getStatus() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void setHeader(String headerName, Object headerValue) throws RestServiceException {
      throw new RestServiceException("Not Implemented", RestServiceErrorCode.InternalServerError);
    }

    @Override
    public Object getHeader(String headerName) {
      throw new IllegalStateException("Not implemented");
    }
  }
}
