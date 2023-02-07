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
package com.github.ambry.router;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link TrackedRequestFinalState}.
 */
public class TrackedRequestFinalStateTest {

  /**
   * Tests for {@link TrackedRequestFinalState#fromRouterErrorCodeToFinalState}.
   */
  @Test
  public void testFromRouterErrorCodeToFinalState() {
    Assert.assertEquals(TrackedRequestFinalState.TIMED_OUT,
        TrackedRequestFinalState.fromRouterErrorCodeToFinalState(RouterErrorCode.OperationTimedOut));
    Assert.assertEquals(TrackedRequestFinalState.NOT_FOUND,
        TrackedRequestFinalState.fromRouterErrorCodeToFinalState(RouterErrorCode.BlobDoesNotExist));

    Set<RouterErrorCode> checkedErrorCodes = new HashSet<>(
        Arrays.asList(RouterErrorCode.OperationTimedOut, RouterErrorCode.BlobDoesNotExist,
            RouterErrorCode.TooManyRequests));
    for (RouterErrorCode routerErrorCode : RouterErrorCode.values()) {
      if (!checkedErrorCodes.contains(routerErrorCode)) {
        Assert.assertEquals(TrackedRequestFinalState.FAILURE,
            TrackedRequestFinalState.fromRouterErrorCodeToFinalState(routerErrorCode));
      }
    }
  }
}
