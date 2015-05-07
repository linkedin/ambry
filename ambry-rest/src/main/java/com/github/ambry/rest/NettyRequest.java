package com.github.ambry.rest;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import java.util.List;


/**
 * Netty specific implementation of RestRequest
 *
 * Just a wrapper over HttpRequest.
 */
public class NettyRequest implements RestRequest {
  private final QueryStringDecoder query;
  private final HttpRequest request;
  private final RestMethod restMethod;

  public NettyRequest(HttpRequest request)
      throws IllegalArgumentException {
    this.request = request;
    this.query = new QueryStringDecoder(request.getUri());

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
      throw new IllegalArgumentException("http method not supported: " + httpMethod);
    }
  }

  public String getUri() {
    return request.getUri();
  }

  public RestMethod getRestMethod() {
    return restMethod;
  }

  public List<String> getValuesOfParameterInURI(String parameter) {
    if (query.parameters().get(parameter) != null) {
      return query.parameters().get(parameter);
    }
    return null;
  }

  public void release() {
    //nothing to do for this
  }
}
