package com.github.ambry.rest;

import com.github.ambry.restservice.RestMethod;
import com.github.ambry.restservice.RestRequest;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.util.List;


/**
 * Netty specific implementation of RestRequest
 * <p/>
 * Just a wrapper over HttpRequest.
 */
public class NettyRequest implements RestRequest {
  private final QueryStringDecoder query;
  private final HttpRequest request;
  private final RestMethod restMethod;

  public NettyRequest(HttpRequest request)
      throws RestServiceException {
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
      throw new RestServiceException("http method not supported: " + httpMethod,
          RestServiceErrorCode.UnknownRestMethod);
    }
  }

  public String getUri() {
    return request.getUri();
  }

  public String getPath() {
    return query.path();
  }

  public RestMethod getRestMethod() {
    return restMethod;
  }

  public Object getValueOfHeader(String name) {
    return request.headers().get(name);
  }

  public List<String> getValuesOfParameterInURI(String parameter) {
    return query.parameters().get(parameter);
  }

  public void release() {
    //nothing to do for this
  }
}
