package com.github.ambry.server.httphandler;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * NotFoundHandler will be used when there is no handler registered to handle an incoming request,
 * regardless the HttpMethod.
 */
public class NotFoundHandler implements Handler {
  public static final NotFoundHandler HANDLER = new NotFoundHandler();

  @Override
  public FullHttpResponse handle(FullHttpRequest request, FullHttpResponse response) {
    response.setStatus(HttpResponseStatus.NOT_FOUND);
    return response;
  }

  @Override
  public HttpMethod supportedMethod() {
    return null;
  }

  @Override
  public boolean match(FullHttpRequest request) {
    return true;
  }
}
