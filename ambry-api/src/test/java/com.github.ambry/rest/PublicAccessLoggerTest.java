package com.github.ambry.rest;

import java.util.Arrays;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Unit tests {@link PublicAccessLogger}
 */
public class PublicAccessLoggerTest {
  private final String REQUEST_HEADER_PREFIX = "requestHeader";
  private final String RESPONSE_HEADER_PREFIX = "responseHeader";

  @Test
  public void testPublicAccessLoggerHeaders() {
    String[] requestHeaders = new String[]{REQUEST_HEADER_PREFIX + "1", REQUEST_HEADER_PREFIX + "2"};
    String[] responseHeaders = new String[]{RESPONSE_HEADER_PREFIX + "1", RESPONSE_HEADER_PREFIX + "2"};
    PublicAccessLogger publicAccessLogger = new PublicAccessLogger(requestHeaders, responseHeaders);
    Assert.assertTrue("Request Headers mismatch ",
        Arrays.deepEquals(publicAccessLogger.getRequestHeaders(), requestHeaders));
    Assert.assertTrue("Response Headers mismatch ",
        Arrays.deepEquals(publicAccessLogger.getResponseHeaders(), responseHeaders));
  }
}
