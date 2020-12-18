/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.commons;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.github.ambry.config.HostThrottleConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.MockClock;
import com.github.ambry.utils.RejectThrottler;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Tests for {@link HostLevelThrottler}.
 */
public class HostLevelThrottlerTest {
  /**
   * Test to make sure {@link HostLevelThrottler#shouldThrottle(RestRequest)} works as expected.
   */
  @Test
  public void quotaTest() throws Exception {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.REST_REQUEST_QUOTA_STRING,
        "{\"PUT\": \"20\",\"GET\": \"20\",\"POST\": \"20\",\"HEAD\": \"20\",\"OPTIONS\": \"20\",\"DELETE\": \"20\"}");
    HostThrottleConfig hostThrottleConfig = new HostThrottleConfig(new VerifiableProperties(props));
    MockClock clock = new MockClock();
    HostLevelThrottler quotaManager =
        new HostLevelThrottler(createQuotaMock(hostThrottleConfig, clock), new HashMap<>(), null);
    // Issue new requests. Since MockClock tick doesn't change, rate is 0.
    for (int i = 0; i < 100; i++) {
      for (RestMethod restMethod : RestMethod.values()) {
        RestRequest restRequest = createRestRequest(restMethod, "http://www.linkedin.com/");
        Assert.assertFalse("Should not throttle", quotaManager.shouldThrottle(restRequest));
      }
    }
    // Move MockClock ahead to 5 seconds later. Rate = 20. New requests should be denied unless its quota is not defined.
    clock.tick(5);
    for (RestMethod restMethod : RestMethod.values()) {
      RestRequest restRequest = createRestRequest(restMethod, "http://www.linkedin.com/");
      if (restMethod == RestMethod.UNKNOWN) {
        Assert.assertFalse("Should not throttle.", quotaManager.shouldThrottle(restRequest));
      } else {
        Assert.assertTrue("Should throttle", quotaManager.shouldThrottle(restRequest));
      }
    }
    // Clock tick to another 5 seconds later, rate < 20. Accept new requests.
    clock.tick(5);
    for (RestMethod restMethod : RestMethod.values()) {
      RestRequest restRequest = createRestRequest(restMethod, "http://www.linkedin.com/");
      Assert.assertFalse("Should not throttle", quotaManager.shouldThrottle(restRequest));
    }
  }

  /**
   * Test to make sure {@link HostLevelThrottler#shouldThrottle(RestRequest)} on hardware usage.
   */
  @Test
  public void hardwareUsageTest() throws Exception {
    String hardwareThresholdsString =
        new JSONObject().put("HEAP_MEMORY", new JSONObject().put("threshold", 30).put("boundType", "UpperBound"))
            .put("DIRECT_MEMORY", new JSONObject().put("threshold", 30).put("boundType", "UpperBound"))
            .put("CPU", new JSONObject().put("threshold", 30).put("boundType", "UpperBound"))
            .toString();

    List<int[]> usages = new ArrayList<>();
    List<Boolean> expectedResult = new ArrayList<>();
    usages.add(new int[]{10, 10, 10});
    expectedResult.add(false);
    usages.add(new int[]{30, 30, 30});
    expectedResult.add(false);
    usages.add(new int[]{31, 31, 31});
    expectedResult.add(true);
    usages.add(new int[]{10, 31, 10});
    expectedResult.add(true);
    usages.add(new int[]{31, 10, 10});
    expectedResult.add(true);
    usages.add(new int[]{10, 10, 31});
    expectedResult.add(true);

    for (int i = 0; i < usages.size(); i++) {
      Map<HardwareResource, Criteria> hardwareThresholdMap =
          HostLevelThrottler.getHardwareThresholdMap(hardwareThresholdsString);
      HardwareUsageMeter mockHardwareUsageMeter = Mockito.mock(HardwareUsageMeter.class);
      HostLevelThrottler throttler =
          new HostLevelThrottler(new HashMap<>(), hardwareThresholdMap, mockHardwareUsageMeter);
      Mockito.when(mockHardwareUsageMeter.getHardwareResourcePercentage(HardwareResource.CPU))
          .thenReturn(usages.get(i)[0]);
      Mockito.when(mockHardwareUsageMeter.getHardwareResourcePercentage(HardwareResource.HEAP_MEMORY))
          .thenReturn(usages.get(i)[1]);
      Mockito.when(mockHardwareUsageMeter.getHardwareResourcePercentage(HardwareResource.DIRECT_MEMORY))
          .thenReturn(usages.get(i)[2]);
      Assert.assertEquals("Incorrect result", expectedResult.get(i),
          throttler.shouldThrottle(createRestRequest(RestMethod.GET, "https://linkedin.com")));
    }
  }

  // A copy of FrontendRestRequestServiceTest.createRestRequest
  static RestRequest createRestRequest(RestMethod restMethod, String uri)
      throws UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    request.put(MockRestRequest.URI_KEY, uri);
    return new MockRestRequest(request, null);
  }

  /**
   * A helper function to create quotaMap with controllable clock.
   */
  private Map<RestMethod, RejectThrottler> createQuotaMock(HostThrottleConfig hostThrottleConfig, Clock clock) {
    JSONObject quota = new JSONObject(hostThrottleConfig.restRequestQuota);
    Map<RestMethod, RejectThrottler> quotaMap = new HashMap<>();
    for (RestMethod restMethod : RestMethod.values()) {
      int restMethodQuota = quota.optInt(restMethod.name(), -1);
      quotaMap.put(restMethod, new RejectThrottler(restMethodQuota, new Meter(clock)));
    }
    return quotaMap;
  }
}
