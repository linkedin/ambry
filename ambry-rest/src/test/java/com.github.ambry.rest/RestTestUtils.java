package com.github.ambry.rest;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * Some common utilities used for tests in rest package
 */
public class RestTestUtils {

  /**
   * Creates a {@link io.netty.handler.codec.http.HttpRequest} with the given parameters.
   * @param httpMethod the {@link io.netty.handler.codec.http.HttpMethod} required.
   * @param uri the URI to hit.
   * @return a {@link io.netty.handler.codec.http.HttpRequest} with the given parameters.
   */
  public static HttpRequest createRequest(HttpMethod httpMethod, String uri, HttpHeaders headers) {
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri);
    if (headers != null) {
      httpRequest.headers().set(headers);
    }
    return httpRequest;
  }

  /**
   * Converts the content in {@code httpContent} to a human readable string.
   * @return content that is inside {@code httpContent} as a human readable string.
   * @throws java.io.IOException
   */
  public static String getContentString(byte[] data)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(data, 0, data.length);
    return out.toString("UTF-8");
  }
}
