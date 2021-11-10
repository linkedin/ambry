/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.AmbryRequests;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.commons.Callback;
import com.github.ambry.rest.ServerSecurityService;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingConsumer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLSession;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Unit tests for {@link AmbryServerSecurityService}
 */
public class AmbryServerSecurityServiceTest {
  private final ServerSecurityService serverSecurityService =
      new AmbryServerSecurityService(new ServerConfig(new VerifiableProperties(new Properties())),
          new ServerMetrics(new MetricRegistry(), AmbryRequests.class, AmbryServer.class));

  /**
   * Tests for {@link AmbryServerSecurityService#validateConnection(SSLSession, Callback)}
   * @throws Exception
   */
  @Test
  public void validateConnectionTest() throws Exception {
    //sslSession is null
    TestUtils.assertException(IllegalArgumentException.class,
        () -> serverSecurityService.validateConnection(null).get(), null);

    //success case
    SSLSession sslSession = Mockito.mock(SSLSession.class);
    serverSecurityService.validateConnection(sslSession, (r, e) -> {
      Assert.assertNull("result not null", r);
      Assert.assertNull("exception not null", e);
    });


    //service is closed
    serverSecurityService.close();
    ThrowingConsumer<ExecutionException> errorAction = e -> {
      Assert.assertTrue("Exception should have been an instance of RestServiceException",
          e.getCause() instanceof RestServiceException);
      RestServiceException re = (RestServiceException) e.getCause();
      Assert.assertEquals("Unexpected RestServerErrorCode (Future)", RestServiceErrorCode.ServiceUnavailable,
          re.getErrorCode());
    };
    TestUtils.assertException(ExecutionException.class, () -> serverSecurityService.validateConnection(sslSession).get(),
        errorAction);
  }

  /**
   * Tests for {@link AmbryServerSecurityService#validateRequest(RestRequest, Callback)}
   * @throws Exception
   */
  @Test
  public void validateRequestTest() throws Exception {
    //request is null
    TestUtils.assertException(IllegalArgumentException.class, () -> serverSecurityService.validateRequest(
        null).get(), null);

    //success case
    RestRequest request = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    serverSecurityService.validateRequest(request, (r, e) -> {
      Assert.assertNull("result not null", r);
      Assert.assertNull("exception not null", e);
    });


    //service is closed
    serverSecurityService.close();
    ThrowingConsumer<ExecutionException> errorAction = e -> {
      Assert.assertTrue("Exception should have been an instance of RestServiceException",
          e.getCause() instanceof RestServiceException);
      RestServiceException re = (RestServiceException) e.getCause();
      Assert.assertEquals("Unexpected RestServerErrorCode (Future)", RestServiceErrorCode.ServiceUnavailable,
          re.getErrorCode());
    };
    TestUtils.assertException(ExecutionException.class, () -> serverSecurityService.validateRequest(request).get(),
        errorAction);
  }
}
