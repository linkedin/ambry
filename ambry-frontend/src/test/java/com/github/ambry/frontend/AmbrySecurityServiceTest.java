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
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.ByteRanges;
import com.github.ambry.router.Callback;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingConsumer;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;


/**
 * Unit tests {@link AmbrySecurityService}
 */
public class AmbrySecurityServiceTest {

  private static final FrontendConfig FRONTEND_CONFIG = new FrontendConfig(new VerifiableProperties(new Properties()));
  private static final String SERVICE_ID = "AmbrySecurityService";
  private static final String OWNER_ID = SERVICE_ID;
  private static final String CLUSTER_NAME = "ambry-test";
  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();
  private static final QuotaManager quotaManager = new QuotaManager(FRONTEND_CONFIG);
  private static final Account REF_ACCOUNT;
  private static final Container REF_CONTAINER;
  private static final Map<String, Object> USER_METADATA = new HashMap<>();
  private static final BlobInfo DEFAULT_INFO;
  private static final BlobInfo UNKNOWN_INFO = new BlobInfo(
      new BlobProperties(100, SERVICE_ID, OWNER_ID, "image/gif", false, Utils.Infinite_Time, Account.UNKNOWN_ACCOUNT_ID,
          Container.UNKNOWN_CONTAINER_ID, false, null), new byte[0]);
  private static final BlobInfo UNKNOWN_INFO_ENC = new BlobInfo(
      new BlobProperties(100, SERVICE_ID, OWNER_ID, "image/gif", false, Utils.Infinite_Time, Account.UNKNOWN_ACCOUNT_ID,
          Container.UNKNOWN_CONTAINER_ID, true, null), new byte[0]);
  private static final FrontendTestUrlSigningServiceFactory URL_SIGNING_SERVICE_FACTORY =
      new FrontendTestUrlSigningServiceFactory();

  private final SecurityService securityService =
      new AmbrySecurityService(FRONTEND_CONFIG, new FrontendMetrics(new MetricRegistry()),
          URL_SIGNING_SERVICE_FACTORY.getUrlSigningService(), quotaManager);

  static {
    try {
      ACCOUNT_SERVICE.clear();
      REF_ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount();
      REF_CONTAINER = REF_ACCOUNT.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
      USER_METADATA.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + TestUtils.getRandomString(9),
          TestUtils.getRandomString(9));
      USER_METADATA.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + TestUtils.getRandomString(10),
          TestUtils.getRandomString(10));
      USER_METADATA.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + TestUtils.getRandomString(11),
          TestUtils.getRandomString(11));
      DEFAULT_INFO = new BlobInfo(
          new BlobProperties(Utils.getRandomLong(TestUtils.RANDOM, 1000) + 100, SERVICE_ID, OWNER_ID, "image/gif",
              false, Utils.Infinite_Time, REF_ACCOUNT.getId(), REF_CONTAINER.getId(), false, null),
          RestUtils.buildUserMetadata(USER_METADATA));
      ACCOUNT_SERVICE.updateAccounts(Collections.singletonList(InMemAccountService.UNKNOWN_ACCOUNT));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Tests for {@link AmbrySecurityService#preProcessRequest(RestRequest, Callback)}
   * @throws Exception
   */
  @Test
  public void preProcessRequestTest() throws Exception {
    RestMethod[] methods =
        new RestMethod[]{RestMethod.POST, RestMethod.GET, RestMethod.DELETE, RestMethod.HEAD, RestMethod.OPTIONS,
            RestMethod.PUT};
    for (RestMethod restMethod : methods) {
      // add a header that is prohibited
      JSONObject headers = new JSONObject();
      headers.put(RestUtils.InternalKeys.KEEP_ALIVE_ON_ERROR_HINT, true);
      RestRequest restRequest = createRestRequest(restMethod, "/", headers);
      try {
        securityService.preProcessRequest(restRequest).get(1, TimeUnit.SECONDS);
        Assert.fail("Should have failed because the request contains a prohibited header: "
            + RestUtils.InternalKeys.KEEP_ALIVE_ON_ERROR_HINT);
      } catch (ExecutionException e) {
        RestServiceException rse = (RestServiceException) Utils.getRootCause(e);
        Assert.assertEquals("Should be a bad request", RestServiceErrorCode.BadRequest, rse.getErrorCode());
      }
    }
    // verify request args regarding to tracking is set accordingly
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null);
    securityService.preProcessRequest(restRequest).get();
    Assert.assertTrue("The arg with key: ambry-internal-keys-send-tracking-info should be set to true",
        (Boolean) restRequest.getArgs().get(RestUtils.InternalKeys.SEND_TRACKING_INFO));
    Properties properties = new Properties();
    properties.setProperty("frontend.attach.tracking.info", "false");
    FrontendConfig frontendConfig = new FrontendConfig(new VerifiableProperties(properties));
    SecurityService securityServiceWithTrackingDisabled =
        new AmbrySecurityService(frontendConfig, new FrontendMetrics(new MetricRegistry()),
            URL_SIGNING_SERVICE_FACTORY.getUrlSigningService(), quotaManager);
    restRequest = createRestRequest(RestMethod.GET, "/", null);
    securityServiceWithTrackingDisabled.preProcessRequest(restRequest);
    Assert.assertFalse("The arg with key: ambry-internal-keys-send-tracking-info should be set to false",
        (Boolean) restRequest.getArgs().get(RestUtils.InternalKeys.SEND_TRACKING_INFO));
  }

  /**
   * Tests {@link AmbrySecurityService#processRequest(RestRequest, Callback)} for common as well as uncommon cases
   * @throws Exception
   */
  @Test
  public void processRequestTest() throws Exception {
    //rest request being null
    TestUtils.assertException(IllegalArgumentException.class, () -> securityService.preProcessRequest(null).get(),
        null);
    TestUtils.assertException(IllegalArgumentException.class, () -> securityService.processRequest(null).get(), null);
    TestUtils.assertException(IllegalArgumentException.class, () -> securityService.postProcessRequest(null).get(),
        null);

    // without callbacks
    RestMethod[] methods =
        new RestMethod[]{RestMethod.POST, RestMethod.GET, RestMethod.DELETE, RestMethod.HEAD, RestMethod.OPTIONS,
            RestMethod.PUT};
    for (RestMethod restMethod : methods) {
      RestRequest restRequest = createRestRequest(restMethod, "/", null);
      securityService.preProcessRequest(restRequest).get();
      securityService.processRequest(restRequest).get();
      securityService.postProcessRequest(restRequest).get();
    }

    // with GET sub resources
    for (RestUtils.SubResource subResource : RestUtils.SubResource.values()) {
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/sampleId/" + subResource, null);
      Account account = InMemAccountService.UNKNOWN_ACCOUNT;
      insertAccountAndContainer(restRequest, account, account.getContainerById(Container.UNKNOWN_CONTAINER_ID));
      securityService.preProcessRequest(restRequest).get();
      securityService.processRequest(restRequest).get();
      securityService.postProcessRequest(restRequest).get();
    }

    // with UrlSigningService denying the request
    URL_SIGNING_SERVICE_FACTORY.isRequestSigned = true;
    URL_SIGNING_SERVICE_FACTORY.verifySignedRequestException =
        new RestServiceException("Msg", RestServiceErrorCode.Unauthorized);
    testExceptionCasesProcessRequest(createRestRequest(RestMethod.GET, "/", null), RestServiceErrorCode.Unauthorized,
        false);

    URL_SIGNING_SERVICE_FACTORY.isRequestSigned = false;
    // security service closed
    securityService.close();
    for (RestMethod restMethod : methods) {
      testExceptionCasesProcessRequest(createRestRequest(restMethod, "/", null),
          RestServiceErrorCode.ServiceUnavailable, true);
    }
  }

  /**
   * {@link AmbrySecurityService#postProcessRequest(RestRequest, Callback)})} should throw RestServiceException if rate
   * is more than expected. RestServiceErrorCode.TooManyRequests is expected in this case.
   */
  @Test
  public void postProcessQuotaManagerTest() throws Exception {
    QuotaManager quotaManager = Mockito.mock(QuotaManager.class);
    AmbrySecurityService ambrySecurityService =
        new AmbrySecurityService(new FrontendConfig(new VerifiableProperties(new Properties())),
            new FrontendMetrics(new MetricRegistry()), URL_SIGNING_SERVICE_FACTORY.getUrlSigningService(),
            quotaManager);
    // Everything should be good.
    Mockito.when(quotaManager.shouldThrottle(any())).thenReturn(false);
    for (int i = 0; i < 100; i++) {
      for (RestMethod restMethod : RestMethod.values()) {
        RestRequest restRequest = createRestRequest(restMethod, "/", null);
        ambrySecurityService.postProcessRequest(restRequest).get();
      }
    }
    // Requests should be denied.
    Mockito.when(quotaManager.shouldThrottle(any())).thenReturn(true);
    for (RestMethod restMethod : RestMethod.values()) {
      RestRequest restRequest = createRestRequest(restMethod, "/", null);
      try {
        ambrySecurityService.postProcessRequest(restRequest).get();
        Assert.fail("Should have failed.");
      } catch (Exception e) {
        Assert.assertEquals("Exception should be TooManyRequests", RestServiceErrorCode.TooManyRequests,
            ((RestServiceException) e.getCause()).getErrorCode());
      }
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
    TestUtils.assertException(IllegalArgumentException.class,
        () -> securityService.processResponse(null, new MockRestResponseChannel(), DEFAULT_INFO).get(), null);

    //restResponseChannel being null
    TestUtils.assertException(IllegalArgumentException.class,
        () -> securityService.processResponse(restRequest, null, DEFAULT_INFO).get(), null);

    //blob info being null
    TestUtils.assertException(IllegalArgumentException.class,
        () -> securityService.processResponse(restRequest, new MockRestResponseChannel(), null).get(), null);

    // for unsupported methods
    RestMethod[] methods = {RestMethod.DELETE};
    for (RestMethod restMethod : methods) {
      testExceptionCasesProcessResponse(restMethod, new MockRestResponseChannel(), DEFAULT_INFO,
          RestServiceErrorCode.InternalServerError);
    }

    // OPTIONS (should be no errors)
    securityService.processResponse(createRestRequest(RestMethod.OPTIONS, "/", null), new MockRestResponseChannel(),
        null).get();

    // PUT (should be no errors)
    securityService.processResponse(createRestRequest(RestMethod.PUT, "/", null), new MockRestResponseChannel(), null)
        .get();

    // GET signed URL (should be no errors)
    securityService.processResponse(createRestRequest(RestMethod.GET, Operations.GET_SIGNED_URL, null),
        new MockRestResponseChannel(), null).get();

    // HEAD
    // normal
    testHeadBlobWithVariousRanges(DEFAULT_INFO);
    // unknown account
    testHeadBlobWithVariousRanges(UNKNOWN_INFO);
    // encrypted unknown account
    testHeadBlobWithVariousRanges(UNKNOWN_INFO_ENC);
    // with no owner id
    BlobInfo blobInfo = new BlobInfo(
        new BlobProperties(100, SERVICE_ID, null, "image/gif", false, Utils.Infinite_Time, REF_ACCOUNT.getId(),
            REF_CONTAINER.getId(), false, null), new byte[0]);
    testHeadBlobWithVariousRanges(blobInfo);
    // with no content type
    blobInfo = new BlobInfo(
        new BlobProperties(100, SERVICE_ID, OWNER_ID, null, false, Utils.Infinite_Time, REF_ACCOUNT.getId(),
            REF_CONTAINER.getId(), false, null), new byte[0]);
    testHeadBlobWithVariousRanges(blobInfo);
    // with a TTL
    blobInfo = new BlobInfo(
        new BlobProperties(100, SERVICE_ID, OWNER_ID, "image/gif", false, 10000, REF_ACCOUNT.getId(),
            REF_CONTAINER.getId(), false, null), new byte[0]);
    testHeadBlobWithVariousRanges(blobInfo);

    // GET BlobInfo
    testGetSubResource(DEFAULT_INFO, RestUtils.SubResource.BlobInfo);
    testGetSubResource(UNKNOWN_INFO, RestUtils.SubResource.BlobInfo);
    testGetSubResource(UNKNOWN_INFO, RestUtils.SubResource.BlobInfo);
    testGetSubResource(UNKNOWN_INFO_ENC, RestUtils.SubResource.BlobInfo);
    // GET UserMetadata
    testGetSubResource(DEFAULT_INFO, RestUtils.SubResource.UserMetadata);
    byte[] usermetadata = TestUtils.getRandomBytes(10);
    testGetSubResource(new BlobInfo(DEFAULT_INFO.getBlobProperties(), usermetadata),
        RestUtils.SubResource.UserMetadata);

    // POST
    testPostBlob();

    // GET Blob
    // less than chunk threshold size
    blobInfo = new BlobInfo(
        new BlobProperties(FRONTEND_CONFIG.chunkedGetResponseThresholdInBytes - 1, SERVICE_ID, OWNER_ID, "image/gif",
            false, 10000, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, false, null), new byte[0]);
    testGetBlobWithVariousRanges(blobInfo);
    // == chunk threshold size
    blobInfo = new BlobInfo(
        new BlobProperties(FRONTEND_CONFIG.chunkedGetResponseThresholdInBytes, SERVICE_ID, OWNER_ID, "image/gif", false,
            10000, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, false, null), new byte[0]);
    testGetBlobWithVariousRanges(blobInfo);
    // more than chunk threshold size
    blobInfo = new BlobInfo(
        new BlobProperties(FRONTEND_CONFIG.chunkedGetResponseThresholdInBytes * 2, SERVICE_ID, OWNER_ID, "image/gif",
            false, 10000, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, false, null), new byte[0]);
    testGetBlobWithVariousRanges(blobInfo);
    // Get blob with content type null
    blobInfo = new BlobInfo(new BlobProperties(100, SERVICE_ID, OWNER_ID, null, true, 10000, Account.UNKNOWN_ACCOUNT_ID,
        Container.UNKNOWN_CONTAINER_ID, false, null), new byte[0]);
    testGetBlobWithVariousRanges(blobInfo);
    // Get blob in a non-cacheable container. AmbrySecurityService should not care about the isPrivate setting.
    blobInfo = new BlobInfo(new BlobProperties(100, SERVICE_ID, OWNER_ID, "image/gif", false, Utils.Infinite_Time,
        Account.UNKNOWN_ACCOUNT_ID, Container.DEFAULT_PRIVATE_CONTAINER_ID, false, null), new byte[0]);
    testGetBlobWithVariousRanges(blobInfo);
    // Get blob in a cacheable container. AmbrySecurityService should not care about the isPrivate setting.
    blobInfo = new BlobInfo(new BlobProperties(100, SERVICE_ID, OWNER_ID, "image/gif", true, Utils.Infinite_Time,
        Account.UNKNOWN_ACCOUNT_ID, Container.DEFAULT_PUBLIC_CONTAINER_ID, false, null), new byte[0]);
    testGetBlobWithVariousRanges(blobInfo);
    // not modified response
    // > creation time (in secs).
    testGetNotModifiedBlob(blobInfo, blobInfo.getBlobProperties().getCreationTimeInMs() + 1000);
    // == creation time
    testGetNotModifiedBlob(blobInfo, blobInfo.getBlobProperties().getCreationTimeInMs());
    // < creation time (in secs)
    testGetNotModifiedBlob(blobInfo, blobInfo.getBlobProperties().getCreationTimeInMs() - 1000);

    // Get blob for a public blob with content type as "text/html"
    blobInfo = new BlobInfo(
        new BlobProperties(100, SERVICE_ID, OWNER_ID, "text/html", true, 10000, Account.UNKNOWN_ACCOUNT_ID,
            Container.UNKNOWN_CONTAINER_ID, false, null), new byte[0]);
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
   * Inserts the given {@code account} and {@code container} in {@code restRequest}.
   * @param restRequest the {@link RestRequest} to insert the objects into.
   * @param account the {@link Account} to insert.
   * @param container the {@link Container} to insert.
   */
  private void insertAccountAndContainer(RestRequest restRequest, Account account, Container container) {
    if (account != null && container != null) {
      restRequest.setArg(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, account);
      restRequest.setArg(RestUtils.InternalKeys.TARGET_CONTAINER_KEY, container);
    }
  }

  /**
   * Verify that the account and container headers in the response are correct.
   * @param restResponseChannel the {@link MockRestResponseChannel} to get headers from.
   * @param expectedAccount the expected {@link Account}. Can be {@code null} if response is not expected to have the
   *                        details.
   * @param expectedContainer the expected {@link Container}. Can be {@code null} if response is not expected to have
   *                          the details.
   */
  private void verifyAccountAndContainerHeaders(MockRestResponseChannel restResponseChannel, Account expectedAccount,
      Container expectedContainer) {
    if (expectedAccount != null && expectedAccount.getId() != Account.UNKNOWN_ACCOUNT_ID && expectedContainer != null
        && expectedContainer.getId() != Container.UNKNOWN_CONTAINER_ID) {
      Assert.assertEquals("Account name not as expected", expectedAccount.getName(),
          restResponseChannel.getHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME));
      Assert.assertEquals("Container name not as expected", expectedContainer.getName(),
          restResponseChannel.getHeader(RestUtils.Headers.TARGET_CONTAINER_NAME));
      Assert.assertEquals("Private value mismatch", !expectedContainer.isCacheable(),
          Boolean.parseBoolean(restResponseChannel.getHeader(RestUtils.Headers.PRIVATE)));
    } else {
      verifyAbsenceOfHeaders(restResponseChannel, RestUtils.Headers.TARGET_ACCOUNT_NAME,
          RestUtils.Headers.TARGET_CONTAINER_NAME);
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
      throws JSONException, UnsupportedEncodingException, URISyntaxException, RestServiceException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    request.put(MockRestRequest.URI_KEY, uri);
    if (headers != null) {
      request.put(MockRestRequest.HEADERS_KEY, headers);
    }
    RestRequest restRequest = new MockRestRequest(request, null);
    restRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(restRequest, FRONTEND_CONFIG.pathPrefixesToRemove, CLUSTER_NAME));
    return restRequest;
  }

  /**
   * Tests exception cases for {@link SecurityService#preProcessRequest(RestRequest, Callback)} and
   * {@link SecurityService#processRequest(RestRequest, Callback)} and
   * {@link SecurityService#postProcessRequest(RestRequest)}
   * @param restRequest the {@link RestRequest} to provide as input.
   * @param expectedErrorCode the {@link RestServiceErrorCode} expected in the exception returned.
   * @param testAllRequestProcessing {@code true} if all request processing functions need to be tested for the same
   *                                              behavior.
   * @throws Exception
   */
  private void testExceptionCasesProcessRequest(RestRequest restRequest, RestServiceErrorCode expectedErrorCode,
      boolean testAllRequestProcessing) throws Exception {
    ThrowingConsumer<ExecutionException> errorAction = e -> {
      Assert.assertTrue("Exception should have been an instance of RestServiceException",
          e.getCause() instanceof RestServiceException);
      RestServiceException re = (RestServiceException) e.getCause();
      Assert.assertEquals("Unexpected RestServerErrorCode (Future)", expectedErrorCode, re.getErrorCode());
    };

    TestUtils.assertException(ExecutionException.class, () -> securityService.preProcessRequest(restRequest).get(),
        errorAction);
    if (testAllRequestProcessing) {
      TestUtils.assertException(ExecutionException.class, () -> securityService.processRequest(restRequest).get(),
          errorAction);
      TestUtils.assertException(ExecutionException.class, () -> securityService.postProcessRequest(restRequest).get(),
          errorAction);
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

    testGetBlob(blobInfo, ByteRanges.fromLastNBytes(0));
    if (blobSize > 0) {
      testGetBlob(blobInfo, ByteRanges.fromStartOffset(0));
      testGetBlob(blobInfo, ByteRanges.fromStartOffset(ThreadLocalRandom.current().nextLong(1, blobSize - 1)));
      testGetBlob(blobInfo, ByteRanges.fromStartOffset(blobSize - 1));

      long random1 = ThreadLocalRandom.current().nextLong(blobSize);
      long random2 = ThreadLocalRandom.current().nextLong(blobSize);
      testGetBlob(blobInfo, ByteRanges.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2)));
      testGetBlob(blobInfo, ByteRanges.fromLastNBytes(blobSize));
    }
    if (blobSize > 1) {
      testGetBlob(blobInfo, ByteRanges.fromLastNBytes(ThreadLocalRandom.current().nextLong(1, blobSize)));
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
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    JSONObject headers =
        range != null ? new JSONObject().put(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range)) : null;
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", headers);
    Pair<Account, Container> accountAndContainer = getAccountAndContainer(blobInfo.getBlobProperties());
    insertAccountAndContainer(restRequest, accountAndContainer.getFirst(), accountAndContainer.getSecond());
    securityService.processResponse(restRequest, restResponseChannel, blobInfo).get();
    Assert.assertEquals("ProcessResponse status should have been set",
        range == null ? ResponseStatus.Ok : ResponseStatus.PartialContent, restResponseChannel.getStatus());
    verifyHeadersForGetBlob(restRequest, blobInfo, accountAndContainer, range, restResponseChannel);
  }

  /**
   * Tests {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)} for a Get blob
   * with the passed in {@link BlobInfo} for a not modified response
   * @param blobInfo the {@link BlobInfo} to be used for the {@link RestRequest}
   * @param ifModifiedSinceMs the value (as a date string) of the {@link RestUtils.Headers#IF_MODIFIED_SINCE} header.
   * @throws Exception
   */
  private void testGetNotModifiedBlob(BlobInfo blobInfo, long ifModifiedSinceMs) throws Exception {
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    JSONObject headers = new JSONObject();
    SimpleDateFormat dateFormat = new SimpleDateFormat(RestUtils.HTTP_DATE_FORMAT, Locale.ENGLISH);
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    Date date = new Date(ifModifiedSinceMs);
    String dateStr = dateFormat.format(date);
    headers.put(RestUtils.Headers.IF_MODIFIED_SINCE, dateStr);
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/abc", headers);
    Pair<Account, Container> accountAndContainer = getAccountAndContainer(blobInfo.getBlobProperties());
    insertAccountAndContainer(restRequest, accountAndContainer.getFirst(), accountAndContainer.getSecond());
    securityService.processResponse(restRequest, restResponseChannel, blobInfo).get();
    if (ifModifiedSinceMs >= blobInfo.getBlobProperties().getCreationTimeInMs()) {
      Assert.assertEquals("Not modified response expected", ResponseStatus.NotModified,
          restResponseChannel.getStatus());
      verifyHeadersForGetBlobNotModified(restResponseChannel, accountAndContainer.getSecond().isCacheable());
    } else {
      Assert.assertEquals("Not modified response should not be returned", ResponseStatus.Ok,
          restResponseChannel.getStatus());
      verifyHeadersForGetBlob(restRequest, blobInfo, accountAndContainer, null, restResponseChannel);
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

    testHeadBlob(blobInfo, ByteRanges.fromLastNBytes(0));
    if (blobSize > 0) {
      testHeadBlob(blobInfo, ByteRanges.fromStartOffset(0));
      testHeadBlob(blobInfo, ByteRanges.fromStartOffset(ThreadLocalRandom.current().nextLong(1, blobSize - 1)));
      testHeadBlob(blobInfo, ByteRanges.fromStartOffset(blobSize - 1));

      long random1 = ThreadLocalRandom.current().nextLong(blobSize);
      long random2 = ThreadLocalRandom.current().nextLong(blobSize);
      testHeadBlob(blobInfo, ByteRanges.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2)));

      testHeadBlob(blobInfo, ByteRanges.fromLastNBytes(blobSize));
    }
    if (blobSize > 1) {
      testHeadBlob(blobInfo, ByteRanges.fromLastNBytes(ThreadLocalRandom.current().nextLong(1, blobSize)));
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
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    JSONObject headers =
        range != null ? new JSONObject().put(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range)) : null;
    RestRequest restRequest = createRestRequest(RestMethod.HEAD, "/", headers);
    Pair<Account, Container> accountAndContainer = getAccountAndContainer(blobInfo.getBlobProperties());
    insertAccountAndContainer(restRequest, accountAndContainer.getFirst(), accountAndContainer.getSecond());
    securityService.processResponse(restRequest, restResponseChannel, blobInfo).get();
    Assert.assertEquals("ProcessResponse status should have been set",
        range == null ? ResponseStatus.Ok : ResponseStatus.PartialContent, restResponseChannel.getStatus());
    verifyHeadersForHead(blobInfo.getBlobProperties(), range, restResponseChannel);
    verifyAccountAndContainerHeaders(restResponseChannel, accountAndContainer.getFirst(),
        accountAndContainer.getSecond());
  }

  /**
   * Tests GET of sub-resources.
   * @param subResource the {@link RestUtils.SubResource}  to test.
   * @throws Exception
   */
  private void testGetSubResource(BlobInfo blobInfo, RestUtils.SubResource subResource) throws Exception {
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/sampleId/" + subResource, null);
    Pair<Account, Container> accountAndContainer = getAccountAndContainer(blobInfo.getBlobProperties());
    insertAccountAndContainer(restRequest, accountAndContainer.getFirst(), accountAndContainer.getSecond());
    securityService.processResponse(restRequest, restResponseChannel, blobInfo).get();
    Assert.assertEquals("ProcessResponse status should have been set ", ResponseStatus.Ok,
        restResponseChannel.getStatus());
    Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    Assert.assertEquals("Last Modified does not match creation time",
        RestUtils.toSecondsPrecisionInMs(blobInfo.getBlobProperties().getCreationTimeInMs()),
        RestUtils.getTimeFromDateString(restResponseChannel.getHeader(RestUtils.Headers.LAST_MODIFIED)).longValue());
    if (subResource.equals(RestUtils.SubResource.BlobInfo)) {
      verifyBlobPropertiesHeaders(blobInfo.getBlobProperties(), restResponseChannel);
      verifyAccountAndContainerHeaders(restResponseChannel, accountAndContainer.getFirst(),
          accountAndContainer.getSecond());
    } else {
      verifyAbsenceOfHeaders(restResponseChannel, RestUtils.Headers.PRIVATE, RestUtils.Headers.TTL,
          RestUtils.Headers.SERVICE_ID, RestUtils.Headers.OWNER_ID, RestUtils.Headers.AMBRY_CONTENT_TYPE,
          RestUtils.Headers.CREATION_TIME, RestUtils.Headers.BLOB_SIZE, RestUtils.Headers.ACCEPT_RANGES,
          RestUtils.Headers.CONTENT_RANGE);
    }
    Map<String, String> userMetadata =
        blobInfo.getUserMetadata() != null ? RestUtils.buildUserMetadata(blobInfo.getUserMetadata()) : null;
    if (userMetadata == null && !(blobInfo.getUserMetadata().length == 0)) {
      Assert.assertTrue("Internal key " + RestUtils.InternalKeys.SEND_USER_METADATA_AS_RESPONSE_BODY + " should be set",
          (Boolean) restRequest.getArgs().get(RestUtils.InternalKeys.SEND_USER_METADATA_AS_RESPONSE_BODY));
    } else if (!(blobInfo.getUserMetadata().length == 0)) {
      USER_METADATA.forEach((key, value) -> Assert.assertEquals("Value of " + key + " not as expected", value,
          restResponseChannel.getHeader(key)));
    }
  }

  /**
   * Tests {@link AmbrySecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)} for
   * {@link RestMethod#POST}.
   * @throws Exception
   */
  private void testPostBlob() throws Exception {
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", null);
    securityService.processResponse(restRequest, restResponseChannel, DEFAULT_INFO).get();
    Assert.assertEquals("ProcessResponse status should have been set", ResponseStatus.Created,
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
    ThrowingConsumer<ExecutionException> errorAction = e -> {
      Assert.assertTrue("Exception should have been an instance of RestServiceException",
          e.getCause() instanceof RestServiceException);
      RestServiceException re = (RestServiceException) e.getCause();
      Assert.assertEquals("Unexpected RestServerErrorCode (Future)", expectedErrorCode, re.getErrorCode());
    };
    RestRequest restRequest = createRestRequest(restMethod, "/", null);
    TestUtils.assertException(ExecutionException.class,
        () -> securityService.processResponse(restRequest, restResponseChannel, blobInfo).get(), errorAction);
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
   * @param restRequest the original request received.
   * @param blobInfo the {@link BlobInfo} to refer to while getting headers.
   * @param accountAndContainer the {@link Account} and {@link Container} of the blob being requested.
   * @param range the {@link ByteRange} used for a range request, or {@code null} for non-ranged requests.
   * @param restResponseChannel {@link MockRestResponseChannel} from which headers are to be verified
   * @throws RestServiceException if there was any problem getting the headers.
   */
  private void verifyHeadersForGetBlob(RestRequest restRequest, BlobInfo blobInfo,
      Pair<Account, Container> accountAndContainer, ByteRange range, MockRestResponseChannel restResponseChannel)
      throws RestServiceException {
    BlobProperties blobProperties = blobInfo.getBlobProperties();
    Assert.assertEquals("Blob size mismatch ", blobProperties.getBlobSize(),
        Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE)));
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

    if (contentLength < FRONTEND_CONFIG.chunkedGetResponseThresholdInBytes) {
      Assert.assertEquals("Content length value mismatch", contentLength,
          Integer.parseInt(restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
    } else {
      Assert.assertNull("Content length value should not be set",
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    }
    verifyBlobPropertiesHeaders(blobInfo.getBlobProperties(), restResponseChannel);
    verifyAccountAndContainerHeaders(restResponseChannel, accountAndContainer.getFirst(),
        accountAndContainer.getSecond());
    Map<String, String> userMetadata =
        blobInfo.getUserMetadata() != null ? RestUtils.buildUserMetadata(blobInfo.getUserMetadata()) : null;
    if (blobInfo.getUserMetadata().length == 0 || userMetadata == null) {
      Assert.assertNull(
          "Internal key " + RestUtils.InternalKeys.SEND_USER_METADATA_AS_RESPONSE_BODY + " should not be set",
          restRequest.getArgs().get(RestUtils.InternalKeys.SEND_USER_METADATA_AS_RESPONSE_BODY));
    } else {
      USER_METADATA.forEach((key, value) -> Assert.assertEquals("Value of " + key + " not as expected", value,
          restResponseChannel.getHeader(key)));
    }
    verifyCacheHeaders(getAccountAndContainer(blobProperties).getSecond().isCacheable(), restResponseChannel);
  }

  /**
   * Verify the headers from the response for a Not modified blob are as expected
   * @param restResponseChannel {@link MockRestResponseChannel} from which headers are to be verified
   * @param cacheable {@code true} if blob is cacheable, {@code false} otherwise.
   */
  private void verifyHeadersForGetBlobNotModified(MockRestResponseChannel restResponseChannel, boolean cacheable) {
    Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    Assert.assertNotNull("Last-Modified has not been set",
        restResponseChannel.getHeader(RestUtils.Headers.LAST_MODIFIED));
    Assert.assertNull("Content length should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    Assert.assertNull("Accept-Ranges should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    Assert.assertNull("Content-Range header should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    verifyCacheHeaders(cacheable, restResponseChannel);
    verifyAbsenceOfHeaders(restResponseChannel, RestUtils.Headers.BLOB_SIZE, RestUtils.Headers.CONTENT_TYPE);
  }

  /**
   * Verifies that the right cache headers are returned.
   * @param cacheable {@code true} if the blob is cacheable, {@code false} if not.
   * @param restResponseChannel the {@link RestResponseChannel} over which the response is sent.
   */
  private void verifyCacheHeaders(boolean cacheable, MockRestResponseChannel restResponseChannel) {
    if (cacheable) {
      Assert.assertTrue("Expires value should be in the future",
          RestUtils.getTimeFromDateString(restResponseChannel.getHeader(RestUtils.Headers.EXPIRES))
              > System.currentTimeMillis());
      Assert.assertEquals("Cache-Control value not as expected", "max-age=" + FRONTEND_CONFIG.cacheValiditySeconds,
          restResponseChannel.getHeader(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertNull("Pragma value should not have been set",
          restResponseChannel.getHeader(RestUtils.Headers.PRAGMA));
    } else {
      Assert.assertEquals("Expires value is incorrect for non-cacheable blob",
          restResponseChannel.getHeader(RestUtils.Headers.DATE),
          restResponseChannel.getHeader(RestUtils.Headers.EXPIRES));
      Assert.assertEquals("Cache-Control value not as expected", "private, no-cache, no-store, proxy-revalidate",
          restResponseChannel.getHeader(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertEquals("Pragma value not as expected", "no-cache",
          restResponseChannel.getHeader(RestUtils.Headers.PRAGMA));
    }
  }

  /**
   * Verify the headers from the response are as expected
   * @param blobProperties the {@link BlobProperties} to refer to while getting headers.
   * @param restResponseChannel {@link MockRestResponseChannel} from which headers are to be verified
   */
  private void verifyBlobPropertiesHeaders(BlobProperties blobProperties, MockRestResponseChannel restResponseChannel) {
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
    Assert.assertEquals("IsEncrypted value mismatch", blobProperties.isEncrypted(),
        Boolean.parseBoolean(restResponseChannel.getHeader(RestUtils.Headers.ENCRYPTED_IN_STORAGE)));
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
   * @param blobProperties the {@link BlobProperties} to read.
   * @return the {@link Account} and {@link Container} objects corresponding to the IDs in the {@link BlobProperties}.
   */
  private Pair<Account, Container> getAccountAndContainer(BlobProperties blobProperties) {
    Account account = ACCOUNT_SERVICE.getAccountById(blobProperties.getAccountId());
    Container container = account.getContainerById(blobProperties.getContainerId());
    return new Pair<>(account, container);
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
    public void setHeader(String headerName, Object headerValue) {
    }

    @Override
    public Object getHeader(String headerName) {
      throw new IllegalStateException("Not implemented");
    }
  }
}
