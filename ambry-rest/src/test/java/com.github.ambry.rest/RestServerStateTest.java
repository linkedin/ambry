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

import junit.framework.Assert;
import org.junit.Test;


/**
 * Unit tests {@link RestServerState}
 */
public class RestServerStateTest {
  private final RestServerState serverState;
  private final String healthCheckUri = "/healthCheck";

  public RestServerStateTest() {
    serverState = new RestServerState(healthCheckUri);
  }

  @Test
  public void testRestServerState() {
    Assert.assertEquals("Health check Uri mismatch ", healthCheckUri, serverState.getHealthCheckUri());
    Assert.assertFalse("Service should be down by default ", serverState.isServiceUp());
    serverState.markServiceUp();
    Assert.assertTrue("Service is expected to be up ", serverState.isServiceUp());
    serverState.markServiceDown();
    Assert.assertFalse("Service is expected to be down ", serverState.isServiceUp());
  }
}
