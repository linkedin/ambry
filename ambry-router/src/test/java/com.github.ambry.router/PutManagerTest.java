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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.CompositeBlobInfo;
import com.github.ambry.messageformat.MetadataContentSerDe;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;


/**
 * A class to test the Put implementation of the {@link NonBlockingRouter}.
 */
public class PutManagerTest {
  private static final long MAX_WAIT_MS = 2000;
  private final MockServerLayout mockServerLayout;
  private final MockTime mockTime = new MockTime();
  private final MockClusterMap mockClusterMap;
  // this is a reference to the state used by the mockSelector. just allows tests to manipulate the state.
  private AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<MockSelectorState>();
  private NonBlockingRouter router;

  private final ArrayList<RequestAndResult> requestAndResultsList = new ArrayList<RequestAndResult>();
  private int chunkSize;
  private int requestParallelism;
  private int successTarget;
  private boolean instantiateNewRouterForPuts;
  private final Random random = new Random();

  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;

  /**
   * Pre-initialization common to all tests.
   */
  public PutManagerTest() throws Exception {
    // random chunkSize in the range [1, 1 MB]
    chunkSize = random.nextInt(1024 * 1024) + 1;
    requestParallelism = 3;
    successTarget = 2;
    mockSelectorState.set(MockSelectorState.Good);
    mockClusterMap = new MockClusterMap();
    mockServerLayout = new MockServerLayout(mockClusterMap);
    instantiateNewRouterForPuts = true;
  }

  /**
   * Every test in this class should leave the router closed in the end. Some tests do additional checks after
   * closing the router. This is just a guard to ensure that the tests are not broken (which helped when developing
   * these tests).
   */
  @After
  public void postCheck() {
    Assert.assertFalse("Router should be closed at the end of each test", router.isOpen());
  }

  /**
   * Tests puts of simple blobs, that is blobs that end up as a single chunk with no metadata content.
   * @throws Exception
   */
  @Test
  public void testSimpleBlobPutSuccess() throws Exception {
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize));
    submitPutsAndAssertSuccess(true);
    for (int i = 0; i < 10; i++) {
      // size in [1, chunkSize]
      requestAndResultsList.clear();
      requestAndResultsList.add(new RequestAndResult(random.nextInt(chunkSize) + 1));
      submitPutsAndAssertSuccess(true);
    }
  }

  /**
   * Tests put of a composite blob (blob with more than one data chunk) where the composite blob size is a multiple of
   * the chunk size.
   */
  @Test
  public void testCompositeBlobChunkSizeMultiplePutSuccess() throws Exception {
    for (int i = 1; i < 10; i++) {
      requestAndResultsList.clear();
      requestAndResultsList.add(new RequestAndResult(chunkSize * i));
      submitPutsAndAssertSuccess(true);
    }
  }

  /**
   * Tests put of a composite blob where the blob size is not a multiple of the chunk size.
   */
  @Test
  public void testCompositeBlobNotChunkSizeMultiplePutSuccess() throws Exception {
    for (int i = 1; i < 10; i++) {
      requestAndResultsList.clear();
      requestAndResultsList.add(new RequestAndResult(chunkSize * i + random.nextInt(chunkSize - 1) + 1));
      submitPutsAndAssertSuccess(true);
    }
  }

  /**
   * Test that a bad user defined callback will not crash the router.
   * @throws Exception
   */
  @Test
  public void testBadCallback() throws Exception {
    RequestAndResult req = new RequestAndResult(chunkSize * 5 + random.nextInt(chunkSize - 1) + 1);
    router = getNonBlockingRouter();
    final CountDownLatch callbackCalled = new CountDownLatch(1);
    requestAndResultsList.clear();
    for (int i = 0; i < 4; i++) {
      requestAndResultsList.add(new RequestAndResult(chunkSize + random.nextInt(5) * random.nextInt(chunkSize)));
    }
    instantiateNewRouterForPuts = false;
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(req.putContent));
    Future future = router.putBlob(req.putBlobProperties, req.putUserMetadata, putChannel, new Callback<String>() {
      @Override
      public void onCompletion(String result, Exception exception) {
        callbackCalled.countDown();
        throw new RuntimeException("Throwing an exception in the user callback");
      }
    });
    submitPutsAndAssertSuccess(false);
    //future.get() for operation with bad callback should still succeed
    future.get();
    Assert.assertTrue("Callback not called.", callbackCalled.await(2, TimeUnit.SECONDS));
    Assert.assertEquals("All operations should be finished.", 0, router.getOperationsCount());
    Assert.assertTrue("Router should not be closed", router.isOpen());
    // Test that PutManager is still operational
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize + random.nextInt(5) * random.nextInt(chunkSize)));
    instantiateNewRouterForPuts = false;
    submitPutsAndAssertSuccess(true);
  }

  /**
   * Tests put of a blob with blob size 0.
   */
  @Test
  public void testZeroSizedBlobPutSuccess() throws Exception {
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(0));
    submitPutsAndAssertSuccess(true);
  }

  /**
   * Tests a failure scenario where connects to server nodes throw exceptions.
   */
  @Test
  public void testFailureOnAllConnects() throws Exception {
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize * 5));
    mockSelectorState.set(MockSelectorState.ThrowExceptionOnConnect);
    Exception expectedException = new RouterException("", RouterErrorCode.OperationTimedOut);
    submitPutsAndAssertFailure(expectedException, false, true);
    // this should not close the router.
    Assert.assertTrue("Router should not be closed", router.isOpen());
    assertCloseCleanup();
  }

  /**
   * Tests a failure scenario where all sends to server nodes result in disconnections.
   */
  @Test
  public void testFailureOnAllSends() throws Exception {
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize * 5));
    mockSelectorState.set(MockSelectorState.DisconnectOnSend);
    Exception expectedException = new RouterException("", RouterErrorCode.OperationTimedOut);
    submitPutsAndAssertFailure(expectedException, false, false);
    // this should not have closed the router.
    Assert.assertTrue("Router should not be closed", router.isOpen());
    assertCloseCleanup();
  }

  /**
   * Tests a failure scenario where selector poll throws an exception when there is anything to send.
   */
  @Test
  public void testFailureOnAllPollThatSends() throws Exception {
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize * 5));
    mockSelectorState.set(MockSelectorState.ThrowExceptionOnSend);
    // In the case of an error in poll, the router gets closed, and all the ongoing operations are finished off with
    // RouterClosed error.
    Exception expectedException = new RouterException("", RouterErrorCode.OperationTimedOut);
    submitPutsAndAssertFailure(expectedException, true, true);
    // router should get closed automatically
    Assert.assertFalse("Router should be closed", router.isOpen());
    Assert.assertEquals("No ChunkFiller threads should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("No RequestResponseHandler threads should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Tests multiple concurrent puts; and puts in succession on the same router.
   */
  @Test
  public void testConcurrentPutsSuccess() throws Exception {
    requestAndResultsList.clear();
    for (int i = 0; i < 5; i++) {
      requestAndResultsList.add(new RequestAndResult(chunkSize + random.nextInt(5) * random.nextInt(chunkSize)));
    }
    submitPutsAndAssertSuccess(false);
    // do more puts on the same router (that is not closed).
    requestAndResultsList.clear();
    for (int i = 0; i < 5; i++) {
      requestAndResultsList.add(new RequestAndResult(chunkSize + random.nextInt(5) * random.nextInt(chunkSize)));
    }
    instantiateNewRouterForPuts = false;
    submitPutsAndAssertSuccess(true);
  }

  /**
   * Test ensures failure when all server nodes encounter an error.
   */
  @Test
  public void testPutWithAllNodesFailure() throws Exception {
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize * 5));
    List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
    for (int i = 0; i < dataNodeIds.size(); i++) {
      String host = dataNodeIds.get(i).getHostname();
      int port = dataNodeIds.get(i).getPort();
      MockServer server = mockServerLayout.getMockServer(host, port);
      server.setServerErrorForAllRequests(ServerErrorCode.Unknown_Error);
    }
    Exception expectedException = new RouterException("", RouterErrorCode.AmbryUnavailable);
    submitPutsAndAssertFailure(expectedException, true, false);
  }

  /**
   * Test ensures success in the presence of a single node failure.
   */
  @Test
  public void testOneNodeFailurePutSuccess() throws Exception {
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize * 5));
    List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
    // Now, fail every third node in every DC and ensure operation success.
    // Note 1: The assumption here is that there are 3 nodes per DC. However, cross DC is not applicable for puts.
    // Note 2: Although nodes are chosen for failure deterministically here, the order in which replicas are chosen
    // for puts is random, so the responses that fail could come in any order. What this and similar tests below
    // test is that as long as there are enough good responses, the operations should succeed,
    // and as long as that is not the case, operations should fail.
    // Note: The assumption is that there are 3 nodes per DC.
    for (int i = 0; i < dataNodeIds.size(); i++) {
      if (i % 3 == 0) {
        String host = dataNodeIds.get(i).getHostname();
        int port = dataNodeIds.get(i).getPort();
        MockServer server = mockServerLayout.getMockServer(host, port);
        server.setServerErrorForAllRequests(ServerErrorCode.Unknown_Error);
      }
    }
    submitPutsAndAssertSuccess(true);
  }

  /**
   * Test ensures failure when two out of three nodes fail.
   */
  @Test
  public void testPutWithTwoNodesFailure() throws Exception {
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize * 5));
    List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
    // Now, fail every node except the third in every DC and ensure operation success.
    // Note: The assumption is that there are 3 nodes per DC.
    for (int i = 0; i < dataNodeIds.size(); i++) {
      if (i % 3 != 0) {
        String host = dataNodeIds.get(i).getHostname();
        int port = dataNodeIds.get(i).getPort();
        MockServer server = mockServerLayout.getMockServer(host, port);
        server.setServerErrorForAllRequests(ServerErrorCode.Unknown_Error);
      }
    }
    Exception expectedException = new RouterException("", RouterErrorCode.AmbryUnavailable);
    submitPutsAndAssertFailure(expectedException, true, false);
  }

  /**
   * Tests slipped puts scenario. That is, the first attempt at putting a chunk ends up in a failure,
   * but a second attempt is made by the PutManager which results in a success.
   */
  @Test
  public void testSlippedPutsSuccess() throws Exception {
    // choose a simple blob, one that will result in a single chunk. This is to ensure setting state properly
    // to test things correctly. Note that slipped puts concern a chunk and not an operation (that is,
    // every chunk is slipped put on its own), so testing for simple blobs is sufficient.
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize));
    List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
    // Set the state of the mock servers so that they return an error for the first send issued,
    // but later ones succeed. With 3 nodes, for slipped puts, all partitions will come from the same nodes,
    // so we set the errors in such a way that the first request received by every node fails.
    // Note: The assumption is that there are 3 nodes per DC.
    List<ServerErrorCode> serverErrorList = new ArrayList<ServerErrorCode>();
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    serverErrorList.add(ServerErrorCode.No_Error);
    for (DataNodeId dataNodeId : dataNodeIds) {
      MockServer server = mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort());
      server.setServerErrors(serverErrorList);
    }
    submitPutsAndAssertSuccess(true);
  }

  /**
   * Tests a case where some chunks succeed and a later chunk fails and ensures that the operation fails in
   * such a scenario.
   */
  @Test
  public void testLaterChunkFailure() throws Exception {
    // choose a blob with two chunks, first one will succeed and second will fail.
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize * 2));
    List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
    // Set the state of the mock servers so that they return success for the first send issued,
    // but later ones fail. With 3 nodes, all partitions will come from the same nodes,
    // so we set the errors in such a way that the first request received by every node succeeds and later ones fail.
    List<ServerErrorCode> serverErrorList = new ArrayList<ServerErrorCode>();
    serverErrorList.add(ServerErrorCode.No_Error);
    for (int i = 0; i < 5; i++) {
      serverErrorList.add(ServerErrorCode.Unknown_Error);
    }
    for (DataNodeId dataNodeId : dataNodeIds) {
      MockServer server = mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort());
      server.setServerErrors(serverErrorList);
    }
    Exception expectedException = new RouterException("", RouterErrorCode.AmbryUnavailable);
    submitPutsAndAssertFailure(expectedException, true, false);
  }

  /**
   * Tests put of a blob with a channel that does not have all the data at once.
   */
  @Test
  public void testDelayedChannelPutSuccess() throws Exception {
    router = getNonBlockingRouter();
    int blobSize = chunkSize * random.nextInt(10) + 1;
    RequestAndResult requestAndResult = new RequestAndResult(blobSize);
    requestAndResultsList.add(requestAndResult);
    MockReadableStreamChannel putChannel = new MockReadableStreamChannel(blobSize);
    FutureResult<String> future =
        (FutureResult<String>) router.putBlob(requestAndResult.putBlobProperties, requestAndResult.putUserMetadata,
            putChannel, null);
    ByteBuffer src = ByteBuffer.wrap(requestAndResult.putContent);
    int pushedSoFar = 0;
    while (pushedSoFar < blobSize && !future.isDone()) {
      int toPush = random.nextInt(blobSize - pushedSoFar + 1);
      ByteBuffer buf = ByteBuffer.allocate(toPush);
      src.get(buf.array());
      putChannel.write(buf);
      Thread.yield();
      pushedSoFar += toPush;
    }
    future.await();
    requestAndResult.result = future;
    assertSuccess();
    assertCloseCleanup();
  }

  /**
   * Test a put where the put channel encounters an exception and finishes prematurely. Ensures that the operation
   * completes and with failure.
   */
  @Test
  public void testBadChannelPutFailure() throws Exception {
    router = getNonBlockingRouter();
    int blobSize = chunkSize * random.nextInt(10) + 1;
    RequestAndResult requestAndResult = new RequestAndResult(blobSize);
    requestAndResultsList.add(requestAndResult);
    MockReadableStreamChannel putChannel = new MockReadableStreamChannel(blobSize);
    FutureResult<String> future =
        (FutureResult<String>) router.putBlob(requestAndResult.putBlobProperties, requestAndResult.putUserMetadata,
            putChannel, null);
    ByteBuffer src = ByteBuffer.wrap(requestAndResult.putContent);
    int pushedSoFar = 0;

    //Make the channel act bad.
    putChannel.beBad();

    while (pushedSoFar < blobSize && !future.isDone()) {
      int toPush = random.nextInt(blobSize - pushedSoFar + 1);
      ByteBuffer buf = ByteBuffer.allocate(toPush);
      src.get(buf.array());
      putChannel.write(buf);
      Thread.yield();
      pushedSoFar += toPush;
    }
    future.await();
    requestAndResult.result = future;
    Exception expectedException = new Exception("Channel encountered an error");
    assertFailure(expectedException);
    assertCloseCleanup();
  }

  /**
   * A bad arguments test, where the channel size is different from the size in BlobProperties.
   */
  @Test
  public void testChannelSizeNotSizeInPropertiesPutFailure() throws Exception {
    requestAndResultsList.clear();
    int blobSize = chunkSize;
    RequestAndResult requestAndResult = new RequestAndResult(blobSize);
    // Change the actual content size.
    requestAndResult.putContent = new byte[blobSize + 1];
    requestAndResultsList.add(requestAndResult);
    Exception expectedException = new RouterException("", RouterErrorCode.BadInputChannel);
    submitPutsAndAssertFailure(expectedException, true, false);
  }

  /**
   * A bad arguments test, where the blob size is very large.
   */
  @Test
  public void testBlobTooLargePutFailure() throws Exception {
    // A blob size of 4G.
    // A chunkSize of 1 byte.
    // The max blob size that can be supported is a function of the chunk size. With the given parameters,
    // the operation will require more than Integer.MAX_VALUE number of data chunks which is too large.
    long blobSize = 4 * 1024 * 1024 * 1024L;
    chunkSize = 1;

    router = getNonBlockingRouter();
    RequestAndResult requestAndResult = new RequestAndResult(0);
    requestAndResultsList.add(requestAndResult);
    requestAndResult.putBlobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    MockReadableStreamChannel putChannel = new MockReadableStreamChannel(blobSize);
    FutureResult<String> future =
        (FutureResult<String>) router.putBlob(requestAndResult.putBlobProperties, requestAndResult.putUserMetadata,
            putChannel, null);
    future.await();
    requestAndResult.result = future;
    Exception expectedException = new RouterException("", RouterErrorCode.BlobTooLarge);
    assertFailure(expectedException);
    assertCloseCleanup();
  }

  /**
   * Test to verify that the chunk filler goes to sleep when there are no active operations and is woken up when
   * operations become active.
   * @throws Exception
   */
  @Test
  public void testChunkFillerSleep() throws Exception {
    router = getNonBlockingRouter();
    // At this time there are no put operations, so the ChunkFillerThread will eventually go into WAITING state.
    Thread chunkFillerThread = TestUtils.getThreadByThisName("ChunkFillerThread");
    Assert.assertTrue("ChunkFillerThread should have gone to WAITING state as there are no active operations",
        waitForThreadState(chunkFillerThread, Thread.State.WAITING));
    int blobSize = chunkSize * 2;
    RequestAndResult requestAndResult = new RequestAndResult(blobSize);
    requestAndResultsList.add(requestAndResult);
    MockReadableStreamChannel putChannel = new MockReadableStreamChannel(blobSize);
    FutureResult<String> future =
        (FutureResult<String>) router.putBlob(requestAndResult.putBlobProperties, requestAndResult.putUserMetadata,
            putChannel, null);
    ByteBuffer src = ByteBuffer.wrap(requestAndResult.putContent);
    // There will be two chunks written to the underlying writable channel, and so two events will be fired.
    int writeSize = blobSize / 2;
    ByteBuffer buf = ByteBuffer.allocate(writeSize);
    src.get(buf.array());
    putChannel.write(buf);
    // The first write will wake up (or will have woken up) the ChunkFiller thread and it will not go to WAITING until
    // the operation is complete as an attempt to fill chunks will be done by the ChunkFiller in every iteration
    // until the operation is complete.
    Assert.assertTrue(
        "ChunkFillerThread should have gone to RUNNABLE state as there is an active operation that is not yet complete",
        waitForThreadState(chunkFillerThread, Thread.State.RUNNABLE));
    buf.rewind();
    src.get(buf.array());
    putChannel.write(buf);
    // At this time all writes have finished, so the ChunkFiller thread will eventually go (or will have already gone)
    // to WAITING due to this write.
    Assert.assertTrue(
        "ChunkFillerThread should have gone to WAITING state as the only active operation is now complete",
        waitForThreadState(chunkFillerThread, Thread.State.WAITING));
    Assert.assertTrue("Operation should not take too long to complete",
        future.await(MAX_WAIT_MS, TimeUnit.MILLISECONDS));
    requestAndResult.result = future;
    assertSuccess();
    assertCloseCleanup();
  }

  /**
   * Wait for the given thread to reach the given thread state.
   * @param thread the thread whose state needs to be checked.
   * @param state the state that needs to be reached.
   * @return true if the state is reached within a predetermined timeout, false on timing out.
   */
  private boolean waitForThreadState(Thread thread, Thread.State state) {
    long checkStartTimeMs = SystemTime.getInstance().milliseconds();
    while (thread.getState() != state) {
      if (SystemTime.getInstance().milliseconds() - checkStartTimeMs > MAX_WAIT_MS) {
        return false;
      } else {
        Thread.yield();
      }
    }
    return true;
  }

  // Methods used by the tests

  /**
   * @return Return a {@link NonBlockingRouter} created with default {@link VerifiableProperties}
   */
  private NonBlockingRouter getNonBlockingRouter() throws IOException {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(chunkSize));
    properties.setProperty("router.put.request.parallelism", Integer.toString(requestParallelism));
    properties.setProperty("router.put.success.target", Integer.toString(successTarget));
    VerifiableProperties vProps = new VerifiableProperties(properties);
    router = new NonBlockingRouter(new RouterConfig(vProps), new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), new LoggingNotificationSystem(), mockClusterMap,
        mockTime);
    return router;
  }

  /**
   * Submits put blob operations that are expected to succeed, waits for completion, and asserts success.
   * @param shouldCloseRouterAfter whether the router should be closed after the operation.
   */
  private void submitPutsAndAssertSuccess(boolean shouldCloseRouterAfter) throws Exception {
    submitPut().await();
    assertSuccess();
    if (shouldCloseRouterAfter) {
      assertCloseCleanup();
    }
  }

  /**
   * Submits put blob operations that are expected to fail, waits for completion, and asserts failure.
   * The incrementTimer option is provided for testing timeout related failures that kick in only if the time has
   * elapsed. For other tests, we want to isolate the failures and not let the timeouts kick in,
   * and those tests will call this method with this flag set to false.
   * @param expectedException the expected Exception
   * @param shouldCloseRouterAfter whether the router should be closed after the operation.
   * @param incrementTimer whether mock time should be incremented in CHECKOUT_TIMEOUT_MS increments while waiting
   *                       for the operation to complete.
   */
  private void submitPutsAndAssertFailure(Exception expectedException, boolean shouldCloseRouterAfter,
      boolean incrementTimer) throws Exception {
    CountDownLatch doneLatch = submitPut();
    if (incrementTimer) {
      do {
        // increment mock time so failures (like connection checkout) can be induced.
        mockTime.sleep(CHECKOUT_TIMEOUT_MS + 1);
      } while (!doneLatch.await(1, TimeUnit.MILLISECONDS));
    } else {
      doneLatch.await();
    }
    assertFailure(expectedException);
    if (shouldCloseRouterAfter) {
      assertCloseCleanup();
    }
  }

  /**
   * Submits put operations. This is called by {@link #submitPutsAndAssertSuccess(boolean)} and
   * {@link #submitPutsAndAssertFailure(Exception, boolean, boolean)} methods.
   * @return a {@link CountDownLatch} to await on for operation completion.
   */
  private CountDownLatch submitPut() throws Exception {
    final CountDownLatch doneLatch = new CountDownLatch(requestAndResultsList.size());
    // This check is here for certain tests (like testConcurrentPuts) that require using the same router.
    if (instantiateNewRouterForPuts) {
      router = getNonBlockingRouter();
    }
    for (final RequestAndResult requestAndResult : requestAndResultsList) {
      Utils.newThread(new Runnable() {
        @Override
        public void run() {
          try {
            ReadableStreamChannel putChannel =
                new ByteBufferReadableStreamChannel(ByteBuffer.wrap(requestAndResult.putContent));
            requestAndResult.result = (FutureResult<String>) router.putBlob(requestAndResult.putBlobProperties,
                requestAndResult.putUserMetadata, putChannel, null);
            requestAndResult.result.await();
          } catch (Exception e) {
            requestAndResult.result = new FutureResult<>();
            requestAndResult.result.done(null, e);
          } finally {
            doneLatch.countDown();
          }
        }
      }, true).start();
    }
    return doneLatch;
  }

  /**
   * Go through all the requests and ensure all of them have succeeded.
   */
  private void assertSuccess() throws Exception {
    // Go through all the requests received by all the servers and ensure that all requests for the same blob id are
    // identical. In the process also fill in the map of blobId to serializedPutRequests.
    HashMap<String, ByteBuffer> allChunks = new HashMap<String, ByteBuffer>();
    for (MockServer mockServer : mockServerLayout.getMockServers()) {
      for (Map.Entry<String, StoredBlob> blobEntry : mockServer.getBlobs().entrySet()) {
        ByteBuffer chunk = allChunks.get(blobEntry.getKey());
        if (chunk == null) {
          allChunks.put(blobEntry.getKey(), blobEntry.getValue().serializedSentPutRequest);
        } else {
          Assert.assertTrue("All requests for the same blob id must be identical except for correlation id",
              areIdenticalPutRequests(chunk.array(), blobEntry.getValue().serializedSentPutRequest.array()));
        }
      }
    }

    // Go through each request, and ensure all of them have succeeded.
    for (RequestAndResult requestAndResult : requestAndResultsList) {
      String blobId = requestAndResult.result.result();
      Exception exception = requestAndResult.result.error();
      Assert.assertNotNull("blobId should not be null", blobId);
      Assert.assertNull("exception should be null", exception);
      verifyBlob(blobId, requestAndResult.putContent, allChunks);
    }
  }

  /**
   * Verifies that the blob associated with the blob id returned by a successful put operation has exactly the same
   * data as the original object that was put.
   * @param blobId the blobId of the blob that is to be verified.
   * @param serializedRequests the mapping from blob ids to their corresponding serialized {@link PutRequest}.
   */

  private void verifyBlob(String blobId, byte[] originalPutContent, HashMap<String, ByteBuffer> serializedRequests)
      throws Exception {
    ByteBuffer serializedRequest = serializedRequests.get(blobId);
    PutRequest.ReceivedPutRequest request = deserializePutRequest(serializedRequest);
    if (request.getBlobType() == BlobType.MetadataBlob) {
      byte[] data = Utils.readBytesFromStream(request.getBlobStream(), (int) request.getBlobSize());
      CompositeBlobInfo compositeBlobInfo = MetadataContentSerDe.deserializeMetadataContentRecord(ByteBuffer.wrap(data),
          new BlobIdFactory(mockClusterMap));
      Assert.assertEquals("Wrong max chunk size in metadata", chunkSize, compositeBlobInfo.getChunkSize());
      Assert.assertEquals("Wrong total size in metadata", originalPutContent.length, compositeBlobInfo.getTotalSize());
      List<StoreKey> dataBlobIds = compositeBlobInfo.getKeys();
      byte[] content = new byte[(int) request.getBlobProperties().getBlobSize()];
      int offset = 0;
      for (StoreKey key : dataBlobIds) {
        PutRequest.ReceivedPutRequest dataBlobPutRequest = deserializePutRequest(serializedRequests.get(key.getID()));
        Utils.readBytesFromStream(dataBlobPutRequest.getBlobStream(), content, offset,
            (int) dataBlobPutRequest.getBlobSize());
        offset += (int) dataBlobPutRequest.getBlobSize();
      }
      Assert.assertArrayEquals("Input blob and written blob should be the same", originalPutContent, content);
    } else {
      byte[] content = Utils.readBytesFromStream(request.getBlobStream(), (int) request.getBlobSize());
      Assert.assertArrayEquals("Input blob and written blob should be the same", originalPutContent, content);
    }
  }

  /**
   * Deserialize a {@link PutRequest} from the given serialized ByteBuffer.
   * @param serialized the serialized ByteBuffer.
   * @return returns the deserialized output.
   */
  private PutRequest.ReceivedPutRequest deserializePutRequest(ByteBuffer serialized) throws IOException {
    serialized.getLong();
    serialized.getShort();
    return PutRequest.readFrom(new DataInputStream(new ByteBufferInputStream(serialized)), mockClusterMap);
  }

  /**
   * Returns true if the two {@link PutRequest}s are identical. The comparison involves resetting the correlation
   * ids (as they will always be different for two different requests), so the check is not for strict identity.
   * @param p byte array containing one of the {@link PutRequest}.
   * @param q byte array containing the other {@link PutRequest}.
   * @return true if they are equal.
   */
  private boolean areIdenticalPutRequests(byte[] p, byte[] q) {
    if (p == q) {
      return true;
    }
    if (p.length != q.length) {
      return false;
    }
    // correlation ids will be different. Zero it out before comparing.
    // correlation id is an int that comes after size (Long), request type (Short) and version (Short)
    ByteBuffer pWrap = ByteBuffer.wrap(p);
    ByteBuffer qWrap = ByteBuffer.wrap(q);
    int offsetOfCorrelationId = 8 + 2 + 2;
    pWrap.putInt(offsetOfCorrelationId, 0);
    qWrap.putInt(offsetOfCorrelationId, 0);
    return Arrays.equals(p, q);
  }

  /**
   * Go through all the requests and ensure all of them have failed.
   * @param expectedException expected exception
   */
  private void assertFailure(Exception expectedException) {
    for (RequestAndResult requestAndResult : requestAndResultsList) {
      String blobId = requestAndResult.result.result();
      Exception exception = requestAndResult.result.error();
      Assert.assertNull("blobId should be null", blobId);
      Assert.assertNotNull("exception should not be null", exception);
      Assert.assertTrue("Exception received should be the expected Exception",
          exceptionsAreEqual(expectedException, exception));
    }
  }

  /**
   * Check if the given two exceptions are equal.
   * @return true if the exceptions are equal.
   */
  private boolean exceptionsAreEqual(Exception a, Exception b) {
    if (a instanceof RouterException) {
      return a.equals(b);
    } else {
      return a.getClass() == b.getClass() && a.getMessage().equals(b.getMessage());
    }
  }

  /**
   * Asserts that expected threads are running before the router is closed, and that they are not running after the
   * router is closed.
   */
  private void assertCloseCleanup() {
    Assert.assertEquals("Exactly one chunkFiller thread should be running before the router is closed", 1,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("Exactly two RequestResponseHandler thread should be running before the router is closed", 2,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    router.close();
    Assert.assertEquals("No ChunkFiller Thread should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("No RequestResponseHandler should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  private class RequestAndResult {
    BlobProperties putBlobProperties;
    byte[] putUserMetadata;
    byte[] putContent;
    FutureResult<String> result;

    RequestAndResult(int blobSize) {
      putBlobProperties =
          new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
      putUserMetadata = new byte[10];
      random.nextBytes(putUserMetadata);
      putContent = new byte[blobSize];
      random.nextBytes(putContent);
      // future result set after the operation is complete.
    }
  }
}

/**
 * A {@link ReadableStreamChannel} implementation whose {@link #readInto(AsyncWritableChannel, * Callback)} method reads into the {@link AsyncWritableChannel} passed into it as and when data is written to it and
 * not at once. Also if the beBad state is set, the callback is called with an exception.
 */
class MockReadableStreamChannel implements ReadableStreamChannel {
  private AsyncWritableChannel writableChannel;
  private FutureResult<Long> returnedFuture;
  private Callback<Long> callback;
  private final long size;
  private long readSoFar;
  private boolean beBad = false;

  /**
   * Constructs a MockReadableStreamChannel.
   * @param size the number of bytes that will eventually be written into this channel.
   */
  MockReadableStreamChannel(long size) {
    this.size = size;
    readSoFar = 0;
  }

  /**
   * Make the channel act bad from now on.
   */
  void beBad() {
    beBad = true;
  }

  /**
   * write this buffer into the associated {@link AsyncWritableChannel} and return only after the latter has
   * completely read it.
   * @param buf the buf to write.
   * @throws Exception if the future await throws an exception.
   */
  void write(ByteBuffer buf) throws Exception {
    // This is a mock class, that is going to be used for very specific tests. The tests should not call this method
    // before readInto() is called nor should it call it with a buffer with invalid size. However, this method guards
    // against these situations anyway to fail the tests immediately when this class is incorrectly used.
    int toRead = buf.remaining();
    if (writableChannel == null || toRead + readSoFar > size) {
      throw new IllegalStateException("Cannot push data");
    }
    if (beBad) {
      Exception exception = new Exception("Channel encountered an error");
      returnedFuture.done(null, exception);
      callback.onCompletion(null, exception);
      return;
    }
    Future<Long> future = writableChannel.write(buf, null);
    future.get();
    readSoFar += toRead;
    if (readSoFar == size) {
      returnedFuture.done(size, null);
      callback.onCompletion(size, null);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getSize() {
    return size;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    this.writableChannel = asyncWritableChannel;
    this.callback = callback;
    this.returnedFuture = new FutureResult<Long>();
    return returnedFuture;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isOpen() {
    return readSoFar < size;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    // no op.
  }
}
