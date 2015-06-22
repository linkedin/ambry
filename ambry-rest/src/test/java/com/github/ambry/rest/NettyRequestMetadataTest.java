package com.github.ambry.rest;

import com.github.ambry.restservice.RestMethod;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Tests functionality of {@link NettyRequestMetadata}.
 */
public class NettyRequestMetadataTest {

  /**
   * Tests conversion of {@link HttpRequest} to {@link NettyRequestMetadata} given good input.
   * @throws RestServiceException
   */
  @Test
  public void conversionWithGoodInputTest()
      throws RestServiceException {
    String key = "key";
    String value = "value";
    String uriAttachment = "?" + key + "=" + value;
    NettyRequestMetadata nettyRequest;
    String uri;

    uri = "/GET" + uriAttachment;
    nettyRequest = new NettyRequestMetadata(createRequest(HttpMethod.GET, uri, key, value));
    validateRequest(nettyRequest, RestMethod.GET, uri, key, value);

    uri = "/POST" + uriAttachment;
    nettyRequest = new NettyRequestMetadata(createRequest(HttpMethod.POST, uri, key, value));
    validateRequest(nettyRequest, RestMethod.POST, uri, key, value);

    uri = "/DELETE" + uriAttachment;
    nettyRequest = new NettyRequestMetadata(createRequest(HttpMethod.DELETE, uri, key, value));
    validateRequest(nettyRequest, RestMethod.DELETE, uri, key, value);

    uri = "/HEAD" + uriAttachment;
    nettyRequest = new NettyRequestMetadata(createRequest(HttpMethod.HEAD, uri, key, value));
    validateRequest(nettyRequest, RestMethod.HEAD, uri, key, value);
  }

  /**
   * Tests conversion of {@link HttpRequest} to {@link NettyRequestMetadata} given bad input (i.e. checks for the
   * correct {@link RestServiceErrorCode})
   * @throws RestServiceException
   */
  @Test
  public void conversionWithBadInputTest()
      throws RestServiceException {
    // unknown http method
    try {
      new NettyRequestMetadata(createRequest(HttpMethod.TRACE, "/", null, null));
      fail("Unknown http method was supplied to NettyRequestMetadata. It should have failed to construct");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", e.getErrorCode(), RestServiceErrorCode.UnsupportedHttpMethod);
    }
  }

  // helpers
  // general
  private HttpRequest createRequest(HttpMethod httpMethod, String uri, String key, String value) {
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri);
    if (key != null && value != null) {
      request.headers().add(key, value);
    }
    return request;
  }

  // conversionWithGoodInputTest() helpers
  private void validateRequest(NettyRequestMetadata nettyRequest, RestMethod restMethod, String uri, String key,
      String value) {
    assertEquals("Mismatch in rest method", restMethod, nettyRequest.getRestMethod());
    assertEquals("Mismatch in path", uri.substring(0, uri.indexOf("?")), nettyRequest.getPath());
    assertEquals("Mismatch in uri", uri, nettyRequest.getUri());
    assertEquals("Mismatch in argument value", value, nettyRequest.getArgs().get(key).get(0));
  }
}
