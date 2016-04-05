/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.IdConverter;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.Callback;
import com.github.ambry.utils.UtilsTest;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Unit tests for {@link AmbryIdConverterFactory}.
 */
public class AmbryIdConverterFactoryTest {

  /**
   * Tests the instantiation and use of the {@link IdConverter} instance returned through the
   * {@link AmbryIdConverterFactory}.
   * @throws Exception
   */
  @Test
  public void ambryIdConverterTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    IdConversionCallback callback = new IdConversionCallback();

    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry());
    IdConverter idConverter = ambryIdConverterFactory.getIdConverter();
    assertNotNull("No IdConverter returned", idConverter);

    String input = UtilsTest.getRandomString(10);
    callback.reset();
    assertEquals("IdConverter should not have converted ID (Future)", input,
        idConverter.convert(new MockRestRequest(MockRestRequest.DUMMY_DATA, null), input, callback).get());
    assertEquals("IdConverter should not have converted ID (Callback)", input, callback.result);

    idConverter.close();
    callback.reset();
    try {
      idConverter.convert(new MockRestRequest(MockRestRequest.DUMMY_DATA, null), input, callback).get();
      fail("ID conversion should have failed because IdConverter is closed");
    } catch (ExecutionException e) {
      RestServiceException re = (RestServiceException) e.getCause();
      assertEquals("Unexpected RestServerErrorCode (Future)", RestServiceErrorCode.ServiceUnavailable,
          re.getErrorCode());
      re = (RestServiceException) callback.exception;
      assertEquals("Unexpected RestServerErrorCode (Callback)", RestServiceErrorCode.ServiceUnavailable,
          re.getErrorCode());
    }
  }

  /**
   * Callback implementation for testing {@link IdConverter#convert(RestRequest, String, Callback)}.
   */
  private static class IdConversionCallback implements Callback<String> {
    protected String result = null;
    protected Exception exception = null;

    @Override
    public void onCompletion(String result, Exception exception) {
      this.result = result;
      this.exception = exception;
    }

    /**
     * Resets the state of this callback.
     */
    protected void reset() {
      result = null;
      exception = null;
    }
  }
}
