package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
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
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.router.Callback;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;


/**
 * Unit tests {@link AmbrySecurityService}
 */
public class AmbrySecurityServiceTest {

  private SecurityService securityService;
  private final long cacheValidityInSecs =
      new FrontendConfig(new VerifiableProperties(new Properties())).frontendCacheValiditySeconds;

  public AmbrySecurityServiceTest()
      throws InstantiationException {
    securityService = new AmbrySecurityServiceFactory(new VerifiableProperties(new Properties()), new MetricRegistry())
        .getSecurityService();
  }

  /**
   * Tests {@link AmbrySecurityService#processRequest(RestRequest, Callback)} for common as well as uncommon cases
   * @throws Exception
   */
  @Test
  public void testProcessRequest()
      throws Exception {
    SecurityServiceCallback callback = new SecurityServiceCallback();
    RestRequest restRequest = null;
    //rest request being null
    try {
      securityService.processRequest(restRequest, callback).get();
      Assert.fail("Should have thrown IllegalArgumentException ");
    } catch (IllegalArgumentException e) {
    }

    // no callbacks
    RestMethod[] methods = new RestMethod[]{RestMethod.POST, RestMethod.GET, RestMethod.DELETE, RestMethod.HEAD};
    for (RestMethod restMethod : methods) {
      restRequest = createRestRequest(restMethod, "/", null);
      securityService.processRequest(restRequest, null).get();
    }

    // w/ callbacks
    callback = new SecurityServiceCallback();
    for (RestMethod restMethod : methods) {
      restRequest = createRestRequest(restMethod, "/", null);
      securityService.processRequest(restRequest, callback).get();
      Assert.assertTrue("Call back should have been invoked", callback.callbackInvoked.get());
      Assert.assertNull("Exception should not have been thrown", callback.exception);
      callback.reset();
    }

    // security service closed
    callback = new SecurityServiceCallback();
    securityService.close();
    for (RestMethod restMethod : methods) {
      restRequest = createRestRequest(restMethod, "/", null);
      try {
        securityService.processRequest(restRequest, callback).get();
        Assert.fail("Process Request should have failed because Security Service is closed");
      } catch (ExecutionException e) {
        Assert.assertTrue("Exception should have been an instance of RestServiceException",
            e.getCause() instanceof RestServiceException);
        RestServiceException re = (RestServiceException) e.getCause();
        Assert.assertEquals("Unexpected RestServerErrorCode (Future)", RestServiceErrorCode.ServiceUnavailable,
            re.getErrorCode());
        re = (RestServiceException) callback.exception;
        Assert.assertEquals("Unexpected RestServerErrorCode (Callback)", RestServiceErrorCode.ServiceUnavailable,
            re.getErrorCode());
      }
      callback.reset();
    }
  }

  /**
   * Tests {@link AmbrySecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)}  for
   * common as well as uncommon cases
   * @throws Exception
   */
  @Test
  public void testProcessResponse()
      throws Exception {
    SecurityServiceCallback callback = new SecurityServiceCallback();
    RestRequest restRequest = null;
    BlobInfo blobInfo = new BlobInfo(getBlobProperties(100, true, Utils.Infinite_Time, "image/gif"), null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    //rest request being null
    try {
      securityService.processResponse(restRequest, restResponseChannel, blobInfo, null).get();
      Assert.fail("Should have thrown IllegalArgumentException ");
    } catch (IllegalArgumentException e) {
    }

    restRequest = createRestRequest(RestMethod.GET, "/", null);
    //restResponseChannel being null
    try {
      securityService.processResponse(restRequest, null, blobInfo, null).get();
      Assert.fail("Should have thrown IllegalArgumentException ");
    } catch (IllegalArgumentException e) {
    }

    //blob info being null
    restResponseChannel = new MockRestResponseChannel();
    try {
      securityService.processResponse(restRequest, restResponseChannel, null, null).get();
      Assert.fail("Should have thrown IllegalArgumentException ");
    } catch (IllegalArgumentException e) {
    }

    // no callbacks
    restResponseChannel = new MockRestResponseChannel();
    securityService.processResponse(restRequest, restResponseChannel, blobInfo, null).get();

    // w/ callbacks
    // for no-ops
    RestMethod[] methods = new RestMethod[]{RestMethod.POST, RestMethod.DELETE};
    for (RestMethod restMethod : methods) {
      callback.reset();
      restRequest = createRestRequest(restMethod, "/", null);
      restResponseChannel = new MockRestResponseChannel();
      securityService.processResponse(restRequest, restResponseChannel, blobInfo, callback).get();
      Assert.assertTrue("Call back should have been invoked", callback.callbackInvoked.get());
      Assert.assertNull("Exception should not have been thrown", callback.exception);
      Assert.assertNull("No exception is expected in the response", restResponseChannel.getException());
      Assert.assertEquals("No body is expected in the response", 0, restResponseChannel.getResponseBody().length);
    }

    // Head for a public blob with a diff TTL
    blobInfo = new BlobInfo(getBlobProperties(100, false, 10000, "image/gif"), null);
    callback.reset();
    restResponseChannel = new MockRestResponseChannel();
    restRequest = createRestRequest(RestMethod.HEAD, "/", null);
    securityService.processResponse(restRequest, restResponseChannel, blobInfo, callback).get();
    Assert.assertTrue("Call back should have been invoked", callback.callbackInvoked.get());
    Assert.assertNull("Exception should not have been thrown", callback.exception);
    Assert.assertEquals("No body is expected in the response", 0, restResponseChannel.getResponseBody().length);
    verifyHeadersForHead(blobInfo.getBlobProperties(), restResponseChannel);

    // Get Blobinfo
    callback.reset();
    restResponseChannel = new MockRestResponseChannel();
    restRequest = createRestRequest(RestMethod.GET, "/abc/" + RestUtils.SubResource.BlobInfo, null);
    securityService.processResponse(restRequest, restResponseChannel, blobInfo, callback).get();
    Assert.assertTrue("Call back should have been invoked", callback.callbackInvoked.get());
    Assert.assertNull("Exception should not have been thrown", callback.exception);
    Assert.assertEquals("Response should have been set ", ResponseStatus.Ok, restResponseChannel.getResponseStatus());
    Assert.assertEquals("No body is expected in the response", 0, restResponseChannel.getResponseBody().length);
    verifyBlobPropertiesHeaders(blobInfo.getBlobProperties(), restResponseChannel);

    // Get Blob
    testGetBlob(callback, blobInfo);
    // Get blob for a private blob
    blobInfo = new BlobInfo(getBlobProperties(100, false, Utils.Infinite_Time, "image/gif"), null);
    testGetBlob(callback, blobInfo);

    // Get blob for a public blob with content type as "text/html"
    blobInfo = new BlobInfo(getBlobProperties(100, true, 10000, "text/html"), null);
    testGetBlob(callback, blobInfo);

    // bad rest response channel
    // Head and Get
    testExceptionCasesProcessResponse(RestMethod.HEAD, callback, blobInfo);
    testExceptionCasesProcessResponse(RestMethod.GET, callback, blobInfo);

    // security service closed
    callback = new SecurityServiceCallback();
    securityService.close();
    methods = new RestMethod[]{RestMethod.POST, RestMethod.GET, RestMethod.DELETE, RestMethod.HEAD};
    for (RestMethod restMethod : methods) {
      restRequest = createRestRequest(restMethod, "/", null);
      try {
        securityService.processResponse(restRequest, restResponseChannel, blobInfo, callback).get();
        Assert.fail("Process Request should have failed because Security Service is closed");
      } catch (Exception e) {
        Assert.assertTrue("Exception should have been an instance of RestServiceException",
            e.getCause() instanceof RestServiceException);
        RestServiceException re = (RestServiceException) e.getCause();
        Assert.assertEquals("Unexpected RestServerErrorCode (Future)", RestServiceErrorCode.ServiceUnavailable,
            re.getErrorCode());
        re = (RestServiceException) callback.exception;
        Assert.assertEquals("Unexpected RestServerErrorCode (Callback)", RestServiceErrorCode.ServiceUnavailable,
            re.getErrorCode());
      }
      callback.reset();
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
   * Tests {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)} for a Get blob
   * with the passed in {@link BlobInfo}
   * @param callback {@link Callback} to be used with the security service
   * @param blobInfo the {@link BlobInfo} to be used for the {@link RestRequest}
   * @throws Exception
   */
  private void testGetBlob(SecurityServiceCallback callback, BlobInfo blobInfo)
      throws Exception {
    callback.reset();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/abc", null);
    securityService.processResponse(restRequest, restResponseChannel, blobInfo, callback).get();
    Assert.assertTrue("Call back should have been invoked", callback.callbackInvoked.get());
    Assert.assertNull("Exception should not have been thrown", callback.exception);
    Assert.assertEquals("No body is expected in the response", 0, restResponseChannel.getResponseBody().length);
    verifyHeadersForGetBlob(blobInfo.getBlobProperties(), restResponseChannel);
  }

  /**
   * Tests exception cases for {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)}
   * with a {@link BadRestResponseChannel}
   * @param restMethod the {@link RestMethod} of the request to be made
   * @param callback {@link Callback} to be used with the security service
   * @param blobInfo the {@link BlobInfo} to be used for the {@link RestRequest}
   * @throws Exception
   */
  private void testExceptionCasesProcessResponse(RestMethod restMethod, SecurityServiceCallback callback,
      BlobInfo blobInfo)
      throws Exception {
    RestRequest restRequest = createRestRequest(restMethod, "/", null);
    BadRestResponseChannel badRestResponseChannel = new BadRestResponseChannel();
    callback.reset();
    try {
      securityService.processResponse(restRequest, badRestResponseChannel, blobInfo, callback).get();
      Assert.fail("Should have thrown Exception");
    } catch (Exception e) {
    }
    Assert.assertTrue("Call back should have been invoked", callback.callbackInvoked.get());
    Assert.assertNotNull("Exception should have been thrown", callback.exception);
    Assert.assertTrue("Exception class mismatch", callback.exception instanceof RestServiceException);
    Assert.assertEquals("Exception msg mismatch", "Not Implemented", callback.exception.getMessage());
    Assert.assertEquals("Error code mismatch", RestServiceErrorCode.InternalServerError,
        ((RestServiceException) callback.exception).getErrorCode());
  }

  /**
   * Create {@link BlobProperties} with the passed in params
   * @param blobSize Size of the blob to be set in the blob properties
   * @param isPrivate {@code true}, if private. {@code false} otherwise
   * @param ttl time to live for the blob
   * @param contentType content type of the blob
   * @return {@link com.github.ambry.messageformat.BlobProperties} created with the passsed in params
   */
  private BlobProperties getBlobProperties(long blobSize, boolean isPrivate, long ttl, String contentType) {
    BlobProperties blobProperties = new BlobProperties(blobSize, "serviceId", "ownerId", contentType, isPrivate, ttl);
    return blobProperties;
  }

  /**
   * Verify the headers from the response are as expected
   * @param blobProperties the {@link BlobProperties} to refer to while getting headers.
   * @param restResponseChannel {@link MockRestResponseChannel} from which headers are to be verified
   * @throws RestServiceException if there was any problem getting the headers.
   */
  private void verifyHeadersForHead(BlobProperties blobProperties, MockRestResponseChannel restResponseChannel)
      throws RestServiceException {
    Assert.assertEquals("Last Modified not set", new Date(blobProperties.getCreationTimeInMs()).toString(),
        restResponseChannel.getHeader(RestUtils.Headers.LAST_MODIFIED));
    Assert.assertEquals("Content length mismatch", blobProperties.getBlobSize(),
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
   * @param restResponseChannel {@link MockRestResponseChannel} from which headers are to be verified
   * @throws RestServiceException if there was any problem getting the headers.
   */
  private void verifyHeadersForGetBlob(BlobProperties blobProperties, MockRestResponseChannel restResponseChannel)
      throws RestServiceException {
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

    Assert.assertNotNull("Expires value not set", restResponseChannel.getHeader(RestUtils.Headers.EXPIRES));
    Assert.assertNull("Private value should not be set", restResponseChannel.getHeader(RestUtils.Headers.PRIVATE));
    Assert.assertNull("TTL value should not be set", restResponseChannel.getHeader(RestUtils.Headers.TTL));
    Assert.assertNull("ServiceID value should not be set", restResponseChannel.getHeader(RestUtils.Headers.SERVICE_ID));
    Assert.assertNull("OwnerId value should not be set", restResponseChannel.getHeader(RestUtils.Headers.OWNER_ID));
    Assert.assertNull("Content length value should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    Assert.assertNull("Ambry Content Type should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.AMBRY_CONTENT_TYPE));
    Assert.assertNull("CreationTime should not be set", restResponseChannel.getHeader(RestUtils.Headers.CREATION_TIME));

    if (blobProperties.isPrivate()) {
      Assert.assertEquals("Cache-Control value not as expected", "private, no-cache, no-store, proxy-revalidate",
          restResponseChannel.getHeader(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertEquals("Pragma value not as expected", "no-cache",
          restResponseChannel.getHeader(RestUtils.Headers.PRAGMA));
    } else {
      Assert.assertNotSame("Expires value not as expected", new Date(0),
          restResponseChannel.getHeader(RestUtils.Headers.EXPIRES));
      Assert.assertEquals("Cache-Control value not as expected", "max-age=" + cacheValidityInSecs,
          restResponseChannel.getHeader(RestUtils.Headers.CACHE_CONTROL));
      Assert
          .assertNull("Pragma value should not have been set", restResponseChannel.getHeader(RestUtils.Headers.PRAGMA));
    }
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
    Assert.assertNotNull("Creation time not set", restResponseChannel.getHeader(RestUtils.Headers.CREATION_TIME));
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
    private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

    @Override
    public void onCompletion(Void result, Exception exception) {
      if (callbackInvoked.compareAndSet(false, true)) {
        this.exception = exception;
      } else {
        this.exception = new IllegalStateException("Callback invoked more than once");
      }
    }

    public void reset() {
      exception = null;
      callbackInvoked.set(false);
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
    public void close()
        throws IOException {
    }

    @Override
    public void onResponseComplete(Exception exception) {
    }

    @Override
    public void setStatus(ResponseStatus status)
        throws RestServiceException {
      throw new RestServiceException("Not Implemented", RestServiceErrorCode.InternalServerError);
    }

    @Override
    public void setHeader(String headerName, Object headerValue)
        throws RestServiceException {
      throw new RestServiceException("Not Implemented", RestServiceErrorCode.InternalServerError);
    }
  }
}
