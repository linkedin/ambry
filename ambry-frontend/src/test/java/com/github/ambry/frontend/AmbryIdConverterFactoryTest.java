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
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


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
    IdSigningService idSigningService = mock(IdSigningService.class);
    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry(), idSigningService);
    IdConverter idConverter = ambryIdConverterFactory.getIdConverter();
    assertNotNull("No IdConverter returned", idConverter);

    String input = TestUtils.getRandomString(10);
    String inputWithLeadingSlash = "/" + input;
    // GET
    // without leading slash
    reset(idSigningService);
    testConversion(idConverter, RestMethod.GET, null, input, input);
    verify(idSigningService, never()).parseSignedId(any());
    // with leading slash
    reset(idSigningService);
    testConversion(idConverter, RestMethod.GET, null, input, inputWithLeadingSlash);
    verify(idSigningService, never()).parseSignedId(any());
    // with signed ID input.
    String idFromParsedSignedId = "parsedId" + input;
    reset(idSigningService);
    when(idSigningService.isIdSigned(any())).thenReturn(true);
    when(idSigningService.parseSignedId(any())).thenReturn(new Pair<>(idFromParsedSignedId, Collections.emptyMap()));
    testConversion(idConverter, RestMethod.GET, null, idFromParsedSignedId, inputWithLeadingSlash);
    verify(idSigningService).parseSignedId(input);
    // test signed id parsing exception
    reset(idSigningService);
    when(idSigningService.isIdSigned(any())).thenReturn(true);
    when(idSigningService.parseSignedId(any())).thenThrow(
        new RestServiceException("expected", RestServiceErrorCode.InternalServerError));
    testConversionFailure(idConverter, new MockRestRequest(MockRestRequest.DUMMY_DATA, null), input,
        RestServiceErrorCode.InternalServerError);
    verify(idSigningService).parseSignedId(input);

    // POST
    // without leading slash (there will be no leading slashes returned from the Router)
    reset(idSigningService);
    testConversion(idConverter, RestMethod.POST, null, inputWithLeadingSlash, input);
    verify(idSigningService, never()).getSignedId(any(), any());
    // with signed id metadata set, requires signed ID.
    String signedId = "signedId/" + input;
    reset(idSigningService);
    when(idSigningService.getSignedId(any(), any())).thenReturn(signedId);
    Map<String, String> signedIdMetadata = Collections.singletonMap("test-key", "test-value");
    testConversion(idConverter, RestMethod.POST, signedIdMetadata, "/" + signedId, input);
    verify(idSigningService).getSignedId(input, signedIdMetadata);

    idConverter.close();
    testConversionFailure(idConverter, new MockRestRequest(MockRestRequest.DUMMY_DATA, null), input,
        RestServiceErrorCode.ServiceUnavailable);
  }

  /**
   * Tests the conversion by the {@code idConverter}.
   * @param idConverter the {@link IdConverter} instance to use.
   * @param restMethod the {@link RestMethod} of the {@link RestRequest} that will be created.
   * @param signedIdMetadata the headers of the {@link RestRequest}.
   * @param expectedOutput the expected output from the {@code idConverter}.
   * @param input the input string
   * @throws Exception
   */
  private void testConversion(IdConverter idConverter, RestMethod restMethod, Map<String, String> signedIdMetadata,
      String expectedOutput, String input) throws Exception {
    JSONObject requestData = new JSONObject();
    requestData.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    requestData.put(MockRestRequest.URI_KEY, "/");
    RestRequest restRequest = new MockRestRequest(requestData, null);
    if (signedIdMetadata != null) {
      restRequest.setArg(RestUtils.InternalKeys.SIGNED_ID_METADATA_KEY, signedIdMetadata);
    }
    IdConversionCallback callback = new IdConversionCallback();
    assertEquals("Converted ID does not match expected (Future)", expectedOutput,
        idConverter.convert(restRequest, input, callback).get());
    assertEquals("Converted ID does not match expected (Callback)", expectedOutput, callback.result);
  }

  /**
   * Test when id conversion is expected to fail
   * @param idConverter the {@link IdConverter} instance to use.
   * @param restRequest the {@link RestRequest} to use.
   * @param input the input string
   * @param expectedErrorCode the expected {@link RestServiceErrorCode}.
   * @throws Exception
   */
  private void testConversionFailure(IdConverter idConverter, RestRequest restRequest, String input,
      RestServiceErrorCode expectedErrorCode) throws Exception {
    IdConversionCallback callback = new IdConversionCallback();
    try {
      idConverter.convert(restRequest, input, callback).get();
      fail("ID conversion should have failed because IdConverter is closed");
    } catch (ExecutionException e) {
      RestServiceException re = (RestServiceException) e.getCause();
      assertEquals("Unexpected RestServerErrorCode (Future)", expectedErrorCode, re.getErrorCode());
      re = (RestServiceException) callback.exception;
      assertEquals("Unexpected RestServerErrorCode (Callback)", expectedErrorCode, re.getErrorCode());
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
