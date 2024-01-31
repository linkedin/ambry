/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
 *
 */
package com.github.ambry.frontend;

import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.s3.S3BaseHandler;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class S3BaseHandlerTest {
  private FrontendConfig frontendConfig;
  @Test
  public void removeAmbryHeadersTest() throws Exception {
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(
        RestMethod.HEAD, "/s3/account/key", new JSONObject(), null);
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, "ambry-test"));
    S3BaseHandler<Void> s3BaseHandler = new S3BaseHandler() {
      @Override
      protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback callback) {
        restResponseChannel.setHeader("x-ambry-foo", "foo");
        // Verify that the response has Ambry headers
        assertTrue("Failed to set Ambry header", hasAmbryHeaders(restResponseChannel));
        callback.onCompletion(null, null);
      }
    };
    // Process the request
    s3BaseHandler.handle(request, restResponseChannel, (r, e) -> {
      // Verify that the response doesn't have Ambry headers
      assertNull("Result isn't null", r);
      assertNull("Error happened during request handling", e);
      assertFalse("Ambry header not removed", hasAmbryHeaders(restResponseChannel));
    });
  }

  @Before
  public void setup() {
    Properties properties = new Properties();
    CommonTestUtils.populateRequiredRouterProps(properties);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    frontendConfig = new FrontendConfig(verifiableProperties);
  }

  private boolean hasAmbryHeaders(RestResponseChannel restResponseChannel) {
    return restResponseChannel.getHeaders().stream()
        .anyMatch(header -> header.startsWith(S3BaseHandler.AMBRY_PARAMETERS_PREFIX));
  }
}
