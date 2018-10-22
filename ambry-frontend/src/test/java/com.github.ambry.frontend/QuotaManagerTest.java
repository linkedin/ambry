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
package com.github.ambry.frontend;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.RejectThrottler;
import com.github.ambry.utils.RejectThrottlerTest;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.frontend.AmbryBlobStorageServiceTest.*;


/**
 * Tests for {@link QuotaManager}.
 */
public class QuotaManagerTest {
  /**
   * Test to make sure {@link QuotaManager#shouldThrottle(RestRequest)} works as expected.
   */
  @Test
  public void quotaTest() throws Exception {
    Properties props = new Properties();
    props.setProperty(FrontendConfig.REST_REQUEST_QUOTA_STRING,
        "{\"PUT\": \"20\",\"GET\": \"20\",\"POST\": \"20\",\"HEAD\": \"20\",\"OPTIONS\": \"20\",\"UNKNOWN\": \"20\",\"DELETE\": \"20\"}");
    FrontendConfig frontendConfig = new FrontendConfig(new VerifiableProperties(props));
    RejectThrottlerTest.MockClock clock = new RejectThrottlerTest.MockClock();
    QuotaManager quotaManager = new MockQuotaManager(frontendConfig, clock);
    // Issue new requests. Since MockClock tick doesn't change, rate is 0.
    for (int i = 0; i < 100; i++) {
      for (RestMethod resetMethod : RestMethod.values()) {
        RestRequest restRequest = createRestRequest(resetMethod, "http://www.linkedin.com/", null, null);
        Assert.assertFalse("Should not throttle", quotaManager.shouldThrottle(restRequest));
      }
    }
    // Move MockClock ahead to 5 seconds later. Rate = 20. New requests should be denied.
    clock.tick(5);
    for (RestMethod resetMethod : RestMethod.values()) {
      RestRequest restRequest = createRestRequest(resetMethod, "http://www.linkedin.com/", null, null);
      Assert.assertTrue("Should throttle", quotaManager.shouldThrottle(restRequest));
    }
    // Clock tick to another 5 seconds later, rate < 20. Accept new requests.
    clock.tick(5);
    for (RestMethod resetMethod : RestMethod.values()) {
      RestRequest restRequest = createRestRequest(resetMethod, "http://www.linkedin.com/", null, null);
      Assert.assertFalse("Should not throttle", quotaManager.shouldThrottle(restRequest));
    }
  }

  /**
   * A mock class of {@link QuotaManager} with a controllable clock.
   */
  static class MockQuotaManager extends QuotaManager {
    public MockQuotaManager(FrontendConfig frontendConfig, Clock clock) {
      super(frontendConfig);
      for (RestMethod restMethod : RestMethod.values()) {
        int restMethodQuota = quota.getInt(restMethod.name());
        quotaMap.put(restMethod, new RejectThrottler(restMethodQuota, new Meter(clock)));
      }
    }
  }
}
