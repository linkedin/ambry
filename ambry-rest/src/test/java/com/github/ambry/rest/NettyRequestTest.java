package com.github.ambry.rest;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class NettyRequestTest {

  @Test
  public void conversionWithGoodInputTest() throws JSONException, RestException {
    NettyRequest nettyRequest;
    String uri;

    String key = "key";
    String value = "value";
    String uriAttachment = "?" + key + "=" + value;

    uri = "/GET" + uriAttachment;
    nettyRequest = new NettyRequest(createRequest(HttpMethod.GET, uri, key, value));
    validateRequest(nettyRequest, RestMethod.GET, uri, key, value);

    uri = "/POST" + uriAttachment;
    nettyRequest = new NettyRequest(createRequest(HttpMethod.POST, uri, key, value));
    validateRequest(nettyRequest, RestMethod.POST, uri, key, value);

    uri = "/DELETE" + uriAttachment;
    nettyRequest = new NettyRequest(createRequest(HttpMethod.DELETE, uri, key, value));
    validateRequest(nettyRequest, RestMethod.DELETE, uri, key, value);

    uri = "/HEAD" + uriAttachment;
    nettyRequest = new NettyRequest(createRequest(HttpMethod.HEAD, uri, key, value));
    validateRequest(nettyRequest, RestMethod.HEAD, uri, key, value);

    nettyRequest = new NettyRequest(createRequest(HttpMethod.GET, "/", null, null));
    assertEquals("Path part 0 is not empty", "", nettyRequest.getPathPart(0));
  }

  @Test
  public void conversionWithBadInputTest() throws JSONException, RestException {
    // unknown http method
    try {
      new NettyRequest(createRequest(HttpMethod.TRACE, "/", null, null));
      fail("Unknown http method was supplied to NettyRequest. It should have failed to construct");
    } catch (RestException e) {
      assertEquals("Unexpected RestErrorCode" , e.getErrorCode(), RestErrorCode.UnknownHttpMethod);
    }
  }

  // helpers
  // general
  private HttpRequest createRequest(HttpMethod httpMethod, String uri, String key, String value) throws JSONException {
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri);
    if(key != null && value != null) {
      request.headers().add(key, value);
    }
    return request;
  }

  // conversionWithGoodInputTest() helpers
  private void validateRequest(NettyRequest nettyRequest, RestMethod restMethod, String uri, String key, String value) {
    assertEquals("Mismatch in rest method", restMethod, nettyRequest.getRestMethod());
    assertEquals("Mismatch in path", uri.substring(0, uri.indexOf("?")), nettyRequest.getPath());
    assertEquals("Mismatch in uri", uri, nettyRequest.getUri());
    assertEquals("Mismatch in path part 0", restMethod.toString(), nettyRequest.getPathPart(0));
    assertNull("Path part 1 is not null", nettyRequest.getPathPart(1));
    assertEquals("Mismatch in parameter value", value, nettyRequest.getValuesOfParameterInURI(key).get(0));
    assertEquals("Mismatch in header value", value, nettyRequest.getValueOfHeader(key));
  }
}
