package com.github.ambry.rest;

import junit.framework.Assert;
import org.junit.Test;


public class RestServerStateTest {
  RestServerState serverState;

  public RestServerStateTest() {
    serverState = new RestServerState("/healthCheck");
  }

  @Test
  public void testRestServerState() {
    Assert.assertFalse("Service should be down by default ", serverState.isServiceUp());
    serverState.markServiceUp();
    Assert.assertTrue("Service is expected to be up ", serverState.isServiceUp());
    serverState.markServiceDown();
    Assert.assertFalse("Service is expected to be down ", serverState.isServiceUp());
  }
}
