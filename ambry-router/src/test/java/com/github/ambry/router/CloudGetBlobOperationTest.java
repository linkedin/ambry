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
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestination;
import com.github.ambry.network.CompositeNetworkClient;

/**
 * Tests for {@link GetBlobOperation}
 * This class creates a {@link NonBlockingRouter} with a {@link MockServer} and
 * a {@link LatchBasedInMemoryCloudDestination} and does puts through it.
 * The gets, are done directly by the tests - that is, the tests create {@link GetBlobOperation} and get requests from
 * it and then use a {@link CompositeNetworkClient} directly to send requests to and get responses
 * from the {@link MockServer} and the {@link LatchBasedInMemoryCloudDestination}.
 */
@RunWith(Parameterized.class)
public class CloudGetBlobOperationTest extends GetBlobOperationTest {

  /**
   * Running for both {@link SimpleOperationTracker} and {@link AdaptiveOperationTracker} with and without encryption,
   * also include one Cloud Backed Data Center.
   * @return an array of {{@link SimpleOperationTracker}, Non-Encrypted, includeCloudDC},
   * {{@link AdaptiveOperationTracker}, Encrypted, includeCloudDC}
   * and {{@link AdaptiveOperationTracker}, Non-Encrypted, includeCloudDC}
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {SimpleOperationTracker.class.getSimpleName(), false, true},
        {AdaptiveOperationTracker.class.getSimpleName(), false, true},
        {AdaptiveOperationTracker.class.getSimpleName(), true, true}
    });
  }

  public CloudGetBlobOperationTest(String operationTrackerType, boolean testEncryption, boolean includeCloudDc) throws Exception {
    super(operationTrackerType, testEncryption, includeCloudDc);
  }

  /*
   * For the following tests, it runs the same test as GetBlobOperation Test. So disable them in CloudGetBlobOperationTest.
   */
  @Test
  public void testInstantiation() {}
  @Test
  public void testSimpleBlobGetSuccess() throws Exception {}
  @Test
  public void testSimpleBlobRawMode() throws Exception {}
  @Test
  public void testSimpleBlobGetChunkIdsOnly() throws Exception {}
  @Test
  public void testCompositeBlobRawMode() throws Exception {}
  @Test
  public void testCompositeBlobGetChunkIdsOnly() throws Exception {}
  @Test
  public void testZeroSizedBlobGetSuccess() throws Exception {}
  @Test
  public void testMissingDataChunkException() throws Exception {}
  @Test
  public void testCompositeBlobChunkSizeMultipleGetSuccess() throws Exception {}
  @Test
  public void testCompositeBlobNotChunkSizeMultipleGetSuccess() throws Exception {}
  @Test
  public void testNetworkClientTimeoutAllFailure() throws Exception {}
  @Test
  public void testRouterRequestTimeoutAllFailure() throws Exception {}
  @Test
  public void testTimeoutRequestUpdateHistogramByDefault() throws Exception {}
  @Test
  public void testRequestTimeoutAndBlobNotFoundLocalTimeout() throws Exception {}
  @Test
  public void testTimeoutAndBlobNotFoundInOriginDc() throws Exception {}
  @Test
  public void testBlobNotFoundCase() throws Exception {}
  @Test
  public void testGetBlobFromNewAddedNode() throws Exception {}
  @Test
  public void testAuthorizationFailureOverrideAll() throws Exception {}
  @Test
  public void testKMSFailure() throws Exception {}
  @Test
  public void testCryptoServiceFailure() throws Exception {}
  @Test
  public void testReadNotCalledBeforeChunkArrival() throws Exception {}
  @Test
  public void testDelayedChunks() throws Exception {}
  @Test
  public void testDataChunkFailure() throws Exception {}
  @Test
  public void testBlobSizeReplacement() throws Exception {}
  @Test
  public void testLegacyBlobGetSuccess() throws Exception {}
  @Test
  public void testRangeRequestSimpleBlob() throws Exception {}
  @Test
  public void testRangeRequestCompositeBlob() throws Exception {}
  @Test
  public void testRangeRequestEmptyBlob() throws Exception {}
  @Test
  public void testEarlyReadableStreamChannelClose() throws Exception {}
  @Test
  public void testSetOperationException() throws Exception {}

  /**
   * Disk backed DC returns either Disk Down or Not Found.
   * Cloud backed DC returns Not Found.
   */
  @Test
  public void testCombinedDiskDownAndNotFoundCase() throws Exception {
    super.testCombinedDiskDownAndNotFoundCase();
  }

  /**
   * We don't have Cloud Server Error Mock layer yet.
   * So this test case is not applicable.
   */
  @Test
  public void testErrorPrecedenceWithSpecialCase() throws Exception {

  }

  /**
   * Disk backed DC hit server failure while Cloud backed DC returns Not Found.
   */
  public void testFailureOnServerErrors() throws Exception {
    super.testFailureOnServerErrors();
  }

  /**
   * Disk backed DC returns different kinds of errors while cloud backed DC returns Not Found.
   */
  @Test
  public void testSuccessInThePresenceOfVariousErrors() throws Exception {
    super.testSuccessInThePresenceOfVariousErrors();
  }

  /**
   * Disk backed DC all returns failure but cloud backed DC returns the Blob information successfully.
   */
  @Test
  public void testFailoverToAzure() throws Exception {
    super.testFailoverToAzure();
  }
}
