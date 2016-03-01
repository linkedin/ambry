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
