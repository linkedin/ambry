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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.Callback;
import com.github.ambry.utils.UtilsTest;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


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
  public void ambryIdConverterTest() throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry());
    IdConverter idConverter = ambryIdConverterFactory.getIdConverter();
    assertNotNull("No IdConverter returned", idConverter);

    String input = UtilsTest.getRandomString(10);
    String inputWithLeadingSlash = "/" + input;
    // GET
    // without leading slash
    testConversion(idConverter, RestMethod.GET, input, input);
    // with leading slash
    testConversion(idConverter, RestMethod.GET, inputWithLeadingSlash, input);
    // POST
    // without leading slash (there will be no leading slashes returned from the Router)
    testConversion(idConverter, RestMethod.POST, input, inputWithLeadingSlash);

    idConverter.close();
    IdConversionCallback callback = new IdConversionCallback();
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
   * Tests the conversion by the {@code idConverter}.
   * @param idConverter the {@link IdConverter} instance to use.
   * @param restMethod the {@link RestMethod} of the {@link RestRequest} that will be created.
   * @param input the input string
   * @param expectedOutput the expected output from the {@code idConverter}.
   * @throws Exception
   */
  private void testConversion(IdConverter idConverter, RestMethod restMethod, String input, String expectedOutput)
      throws Exception {
    JSONObject requestData = new JSONObject();
    requestData.put(MockRestRequest.REST_METHOD_KEY, restMethod);
    requestData.put(MockRestRequest.URI_KEY, "/");
    RestRequest restRequest = new MockRestRequest(requestData, null);
    IdConversionCallback callback = new IdConversionCallback();
    assertEquals("Converted ID does not match expected (Future)", expectedOutput,
        idConverter.convert(restRequest, input, callback).get());
    assertEquals("Converted ID does not match expected (Callback)", expectedOutput, callback.result);
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
