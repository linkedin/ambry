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
package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class RouterUtilsTest {
  ClusterMap clusterMap;
  PartitionId partition;
  BlobId originalBlobId;
  String blobIdStr;

  public void initialize() {
    try {
      clusterMap = new MockClusterMap();
    } catch (Exception e) {
      fail("Should not get any exception.");
    }
    partition = clusterMap.getWritablePartitionIds().get(0);
    originalBlobId = new BlobId(partition);
    blobIdStr = originalBlobId.getID();
  }

  @Test
  public void testInvalidInputString() {
    initialize();
    try {
      RouterUtils.getBlobIdFromString(null, clusterMap);
      fail("The input blob string is invalid. Should fail here");
    } catch (RouterException e) {
      assertEquals("The input blob string is invalid.", e.getErrorCode(), RouterErrorCode.InvalidBlobId);
    }
    try {
      RouterUtils.getBlobIdFromString("", clusterMap);
      fail("The input blob string is invalid. Should fail here");
    } catch (RouterException e) {
      assertEquals("", e.getErrorCode(), RouterErrorCode.InvalidBlobId);
    }
    try {
      RouterUtils.getBlobIdFromString("abc", clusterMap);
      fail("The input blob string is invalid. Should fail here");
    } catch (RouterException e) {
      assertEquals("", e.getErrorCode(), RouterErrorCode.InvalidBlobId);
    }
  }

  @Test
  public void testGoodCase()
      throws Exception {
    initialize();
    BlobId convertedBlobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
    assertEquals("The converted BlobId should be the same as the original.", originalBlobId, convertedBlobId);
  }

  /**
   * Test to ensure system health errors are interpreted correctly.
   */
  @Test
  public void testSystemHealthErrorInterpretation() {
    Assert.assertFalse(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.InvalidBlobId)));
    Assert.assertFalse(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.InvalidPutArgument)));
    Assert.assertFalse(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.BlobTooLarge)));
    Assert.assertFalse(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.BadInputChannel)));
    Assert.assertFalse(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.BlobDeleted)));
    Assert.assertFalse(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.BlobDoesNotExist)));
    Assert.assertFalse(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.BlobExpired)));

    Assert.assertTrue(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.AmbryUnavailable)));
    Assert.assertTrue(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.OperationTimedOut)));
    Assert.assertTrue(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.RouterClosed)));
    Assert
        .assertTrue(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.UnexpectedInternalError)));
    Assert.assertTrue(RouterUtils.isSystemHealthError(new RouterException("", RouterErrorCode.InsufficientCapacity)));
    Assert.assertTrue(RouterUtils.isSystemHealthError(new Exception()));
  }
}
