package com.github.ambry.rest;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.util.List;
import java.util.Map;


/**
 * Netty specific implementation of {@link RestRequestMetadata}.
 * <p/>
 * Just a wrapper over {@link HttpRequest}.
 */
class NettyRequestMetadata implements RestRequestMetadata {
  private final QueryStringDecoder query;
  private final HttpRequest request;
  private final RestMethod restMethod;

  public NettyRequestMetadata(HttpRequest request)
      throws RestServiceException {
    if (request == null) {
      throw new IllegalArgumentException("Received null HttpRequest");
    }
    this.request = request;
    this.query = new QueryStringDecoder(request.getUri());
    // convert HttpMethod to RestMethod
    HttpMethod httpMethod = request.getMethod();
    if (httpMethod == HttpMethod.GET) {
      restMethod = RestMethod.GET;
    } else if (httpMethod == HttpMethod.POST) {
      restMethod = RestMethod.POST;
    } else if (httpMethod == HttpMethod.DELETE) {
      restMethod = RestMethod.DELETE;
    } else if (httpMethod == HttpMethod.HEAD) {
      restMethod = RestMethod.HEAD;
    } else {
      throw new RestServiceException("http method not supported: " + httpMethod,
          RestServiceErrorCode.UnsupportedHttpMethod);
    }
  }

  @Override
  public String getUri() {
    return request.getUri();
  }

  @Override
  public String getPath() {
    return query.path();
  }

  @Override
  public RestMethod getRestMethod() {
    return restMethod;
  }

  @Override
  public Map<String, List<String>> getArgs() {
    return query.parameters();
  }

  @Override
  public void retain() {
    //nothing to do
  }

  @Override
  public void release() {
    //nothing to do
  }

  @Override
  public String toString() {
    return request.toString();
  }
}
