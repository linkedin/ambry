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
