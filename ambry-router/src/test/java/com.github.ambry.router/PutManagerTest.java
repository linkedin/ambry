/*
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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobId.BlobDataType;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.CompositeBlobInfo;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MetadataContentSerDe;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingConsumer;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * A class to test the Put implementation of the {@link NonBlockingRouter}.
 */
@RunWith(Parameterized.class)
public class PutManagerTest {
  static final GeneralSecurityException GSE = new GeneralSecurityException("Exception to throw for tests");
  private static final long MAX_WAIT_MS = 5000;
  private final boolean testEncryption;
  private final int metadataContentVersion;
  private final MockServerLayout mockServerLayout;
  private final MockTime mockTime = new MockTime();
  private final MockClusterMap mockClusterMap;
  private final InMemAccountService accountService;
  // this is a reference to the state used by the mockSelector. just allows tests to manipulate the state.
  private AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>();
  private TestNotificationSystem notificationSystem;
  private NonBlockingRouter router;
  private NonBlockingRouterMetrics metrics;
  private MockKeyManagementService kms;
  private MockCryptoService cryptoService;
  private CryptoJobHandler cryptoJobHandler;
  private boolean instantiateEncryptionCast = true;

  private final ArrayList<RequestAndResult> requestAndResultsList = new ArrayList<>();
  private int chunkSize;
  private int requestParallelism;
  private int successTarget;
  private boolean instantiateNewRouterForPuts;
  private final Random random = new Random();
  private NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  // here we set local dc to "DC3" because MockClusterMap uses DC3 as default local dc (where special class partition
  // has 3 replicas)
  private static final String LOCAL_DC = "DC3";
  private static final String EXTERNAL_ASSET_TAG = "ExternalAssetTag";

  /**
   * Running for both regular and encrypted blobs, and versions 2 and 3 of MetadataContent
   * @return an array with all four different choices
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {false, MessageFormatRecord.Metadata_Content_Version_V2},
        {false, MessageFormatRecord.Metadata_Content_Version_V3},
        {true, MessageFormatRecord.Metadata_Content_Version_V2},
        {true, MessageFormatRecord.Metadata_Content_Version_V3}
    });
  }

  /**
   * Pre-initialization common to all tests.
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public PutManagerTest(boolean testEncryption, int metadataContentVersion) throws Exception {
    this.testEncryption = testEncryption;
    this.metadataContentVersion = metadataContentVersion;
    // random chunkSize in the range [2, 1 MB]
    chunkSize = random.nextInt(1024 * 1024) + 2;
    requestParallelism = 3;
    successTarget = 2;
    mockSelectorState.set(MockSelectorState.Good);
    mockClusterMap = new MockClusterMap();
    mockClusterMap.setLocalDatacenterName(LOCAL_DC);
    mockServerLayout = new MockServerLayout(mockClusterMap);
    notificationSystem = new TestNotificationSystem();
    instantiateNewRouterForPuts = true;
    accountService = new InMemAccountService(false, true);
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  /**
   * Every test in this class should leave the router closed in the end. Some tests do additional checks after
   * closing the router. This is just a guard to ensure that the tests are not broken (which helped when developing
   * these tests).
   */
  @After
  public void postCheck() {
    if (router != null) {
      if (router.isOpen()) {
        router.close();
      }
    }
    nettyByteBufLeakHelper.afterTest();
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
      mockClusterMap.clearLastNRequestedPartitionClasses();
      submitPutsAndAssertSuccess(true);
      // since the puts are processed one at a time, it is fair to check the last partition class set
      checkLastRequestPartitionClasses(1, MockClusterMap.DEFAULT_PARTITION_CLASS);
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
      mockClusterMap.clearLastNRequestedPartitionClasses();
      submitPutsAndAssertSuccess(true);
      // since the puts are processed one "large" blob at a time, it is fair to check the last partition classes set
      // one extra call if there is a metadata blob
      checkLastRequestPartitionClasses(i == 1 ? 1 : i + 1, MockClusterMap.DEFAULT_PARTITION_CLASS);
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
      mockClusterMap.clearLastNRequestedPartitionClasses();
      submitPutsAndAssertSuccess(true);
      // since the puts are processed one "large" blob at a time, it is fair to check the last partition classes set
      checkLastRequestPartitionClasses(i + 2, MockClusterMap.DEFAULT_PARTITION_CLASS);
    }
  }

  /**
   * Test success cases with various {@link PutBlobOptions} set.
   */
  @Test
  public void testOptionsSuccess() throws Exception {
    ThrowingConsumer<RequestAndResult> runTest = requestAndResult -> {
      requestAndResultsList.clear();
      requestAndResultsList.add(requestAndResult);
      mockClusterMap.clearLastNRequestedPartitionClasses();
      submitPutsAndAssertSuccess(true);
    };
    runTest.accept(new RequestAndResult(chunkSize, null,
        new PutBlobOptionsBuilder().chunkUpload(true).maxUploadSize(chunkSize).build(), null));
    runTest.accept(new RequestAndResult(chunkSize - 1, null,
        new PutBlobOptionsBuilder().chunkUpload(true).maxUploadSize(chunkSize - 1).build(), null));
    runTest.accept(
        new RequestAndResult(chunkSize + 1, null, new PutBlobOptionsBuilder().maxUploadSize(2 * chunkSize - 1).build(),
            null));
    runTest.accept(new RequestAndResult(0, null, new PutBlobOptionsBuilder().maxUploadSize(0).build(), null));
  }

  /**
   * Test failure cases with various {@link PutBlobOptions} set.
   */
  @Test
  public void testOptionsFailures() throws Exception {
    ThrowingConsumer<Pair<RequestAndResult, RouterErrorCode>> runTest = requestAndErrorCode -> {
      requestAndResultsList.clear();
      requestAndResultsList.add(requestAndErrorCode.getFirst());
      mockClusterMap.clearLastNRequestedPartitionClasses();
      submitPutsAndAssertFailure(new RouterException("", requestAndErrorCode.getSecond()), true, false, false);
    };
    runTest.accept(new Pair<>(new RequestAndResult(chunkSize, null,
        new PutBlobOptionsBuilder().chunkUpload(true).maxUploadSize(chunkSize + 1).build(), null),
        RouterErrorCode.InvalidPutArgument));
    runTest.accept(new Pair<>(new RequestAndResult(chunkSize + 1, null,
        new PutBlobOptionsBuilder().chunkUpload(true).maxUploadSize(chunkSize).build(), null),
        RouterErrorCode.BlobTooLarge));
    runTest.accept(new Pair<>(new RequestAndResult(2, null, new PutBlobOptionsBuilder().maxUploadSize(1).build(), null),
        RouterErrorCode.BlobTooLarge));
    runTest.accept(new Pair<>(
        new RequestAndResult(2 * chunkSize, null, new PutBlobOptionsBuilder().maxUploadSize(2 * chunkSize - 1).build(),
            null), RouterErrorCode.BlobTooLarge));
    runTest.accept(
        new Pair<>(new RequestAndResult(0, null, new PutBlobOptionsBuilder().maxUploadSize(-1).build(), null),
            RouterErrorCode.BlobTooLarge));
  }

  /**
   * Test different cases where a stitch operation should succeed.
   */
  @Test
  public void testStitchBlobSuccess() throws Exception {
    ThrowingConsumer<List<ChunkInfo>> runTest = chunksToStitch -> {
      requestAndResultsList.clear();
      requestAndResultsList.add(new RequestAndResult(chunksToStitch));
      mockClusterMap.clearLastNRequestedPartitionClasses();
      submitPutsAndAssertSuccess(true);
      // since the puts are processed one at a time, it is fair to check the last partition class set
      checkLastRequestPartitionClasses(1, MockClusterMap.DEFAULT_PARTITION_CLASS);
    };

    for (int i = 1; i < 10; i++) {
      // Chunks are all the same size.
      runTest.accept(RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
          RouterTestHelpers.buildValidChunkSizeStream(chunkSize * i, chunkSize)));
      // All intermediate chunks are the same size. Last is smaller.
      runTest.accept(RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
          RouterTestHelpers.buildValidChunkSizeStream(chunkSize * i + random.nextInt(chunkSize - 1) + 1, chunkSize)));
      // Chunks are all the same size but smaller than routerMaxPutChunkSizeBytes.
      int dataChunkSize = 1 + random.nextInt(chunkSize - 1);
      runTest.accept(RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
          RouterTestHelpers.buildValidChunkSizeStream(dataChunkSize * i, dataChunkSize)));
      // All intermediate chunks are the same size but smaller than routerMaxPutChunkSizeBytes. Last is smaller.
      runTest.accept(RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
          RouterTestHelpers.buildValidChunkSizeStream(dataChunkSize * i + random.nextInt(dataChunkSize - 1) + 1,
              dataChunkSize)));
    }

    if (metadataContentVersion > 2) {
      // intermediate chunk sizes do not match
      runTest.accept(RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
          LongStream.of(200, 10, 200)));

      // last chunk larger than intermediate chunks
      runTest.accept(RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
          LongStream.of(200, 201)));
    }
  }

  /**
   * Test different cases where a stitch operation should fail.
   * @throws Exception
   */
  @Test
  public void testStitchBlobFailures() throws Exception {

    ThrowingConsumer<Pair<List<ChunkInfo>, RouterErrorCode>> runTest = chunksAndErrorCode -> {
      requestAndResultsList.clear();
      requestAndResultsList.add(new RequestAndResult(chunksAndErrorCode.getFirst()));
      submitPutsAndAssertFailure(new RouterException("", chunksAndErrorCode.getSecond()), true, false, true);
    };

    // chunk size issues
    if (metadataContentVersion <= 2) {
      // intermediate chunk sizes do not match
      runTest.accept(new Pair<>(
          RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
              LongStream.of(200, 10, 200)), RouterErrorCode.InvalidPutArgument));

      // last chunk larger than intermediate chunks
      runTest.accept(new Pair<>(
          RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
              LongStream.of(200, 201)), RouterErrorCode.InvalidPutArgument));
    }
    // last chunk is size 0 (0 not supported by current metadata format)
    runTest.accept(new Pair<>(
        RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
            LongStream.of(200, 0)), RouterErrorCode.InvalidPutArgument));
    // intermediate chunk is size 0 (0 not supported by current metadata format)
    runTest.accept(new Pair<>(
        RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
            LongStream.of(200, 0, 200)), RouterErrorCode.InvalidPutArgument));
    // invalid intermediate chunk size (0 not supported by current metadata format)
    runTest.accept(new Pair<>(
        RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
            LongStream.of(0, 0)), RouterErrorCode.InvalidPutArgument));
    // chunks sizes must be less than or equal to put chunk size config
    runTest.accept(new Pair<>(
        RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
            RouterTestHelpers.buildValidChunkSizeStream((chunkSize + 1) * 3, chunkSize + 1)),
        RouterErrorCode.InvalidPutArgument));

    // must provide at least 1 chunk for stitching
    runTest.accept(new Pair<>(
        RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, Utils.Infinite_Time,
            LongStream.empty()), RouterErrorCode.InvalidPutArgument));

    // TTL shorter than metadata blob TTL
    runTest.accept(new Pair<>(RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.DATACHUNK, 25,
        RouterTestHelpers.buildValidChunkSizeStream(chunkSize * 3, chunkSize)), RouterErrorCode.InvalidPutArgument));

    // Chunk IDs must all be DATACHUNK type
    runTest.accept(new Pair<>(
        RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.METADATA, Utils.Infinite_Time,
            RouterTestHelpers.buildValidChunkSizeStream(chunkSize * 3, chunkSize)), RouterErrorCode.InvalidBlobId));
    runTest.accept(new Pair<>(RouterTestHelpers.buildChunkList(mockClusterMap, BlobDataType.SIMPLE, Utils.Infinite_Time,
        RouterTestHelpers.buildValidChunkSizeStream(chunkSize * 3, chunkSize)), RouterErrorCode.InvalidBlobId));
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
    Future future =
        router.putBlob(req.putBlobProperties, req.putUserMetadata, putChannel, req.options, (result, exception) -> {
          callbackCalled.countDown();
          throw new RuntimeException("Throwing an exception in the user callback");
        });
    submitPutsAndAssertSuccess(false);
    //future.get() for operation with bad callback should still succeed
    future.get();
    Assert.assertTrue("Callback not called.", callbackCalled.await(MAX_WAIT_MS, TimeUnit.MILLISECONDS));
    assertEquals("All operations should be finished.", 0, router.getOperationsCount());
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
    submitPutsAndAssertFailure(expectedException, false, true, true);
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
    submitPutsAndAssertFailure(expectedException, false, false, true);
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
    submitPutsAndAssertFailure(expectedException, true, true, true);
    // router should get closed automatically
    Assert.assertFalse("Router should be closed", router.isOpen());
    assertEquals("No ChunkFiller threads should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    assertEquals("No RequestResponseHandler threads should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Tests a failure scenario where connects to server nodes throw exceptions.
   */
  @Test
  public void testFailureOnKMS() throws Exception {
    if (testEncryption) {
      setupEncryptionCast(new VerifiableProperties(new Properties()));
      instantiateEncryptionCast = false;
      kms.exceptionToThrow.set(GSE);
      // simple blob
      requestAndResultsList.clear();
      requestAndResultsList.add(new RequestAndResult(random.nextInt(chunkSize) + 1));
      Exception expectedException = new RouterException("", GSE, RouterErrorCode.UnexpectedInternalError);
      submitPutsAndAssertFailure(expectedException, false, true, true);
      // this should not close the router.
      Assert.assertTrue("Router should not be closed", router.isOpen());
      assertCloseCleanup();

      setupEncryptionCast(new VerifiableProperties(new Properties()));
      instantiateEncryptionCast = false;
      kms.exceptionToThrow.set(GSE);
      // composite blob
      requestAndResultsList.clear();
      requestAndResultsList.add(new RequestAndResult(chunkSize * random.nextInt(10)));
      submitPutsAndAssertFailure(expectedException, false, true, true);
      // this should not close the router.
      Assert.assertTrue("Router should not be closed", router.isOpen());
      assertCloseCleanup();
    }
  }

  /**
   * Tests a failure scenario where connects to server nodes throw exceptions.
   */
  @Test
  public void testFailureOnCryptoService() throws Exception {
    if (testEncryption) {
      setupEncryptionCast(new VerifiableProperties(new Properties()));
      instantiateEncryptionCast = false;
      cryptoService.exceptionOnEncryption.set(GSE);
      // simple blob
      requestAndResultsList.clear();
      requestAndResultsList.add(new RequestAndResult(random.nextInt(chunkSize) + 1));
      Exception expectedException = new RouterException("", GSE, RouterErrorCode.UnexpectedInternalError);
      submitPutsAndAssertFailure(expectedException, false, true, true);
      // this should not close the router.
      Assert.assertTrue("Router should not be closed", router.isOpen());
      assertCloseCleanup();

      setupEncryptionCast(new VerifiableProperties(new Properties()));
      instantiateEncryptionCast = false;
      cryptoService.exceptionOnEncryption.set(GSE);
      // composite blob
      requestAndResultsList.clear();
      requestAndResultsList.add(new RequestAndResult(chunkSize * random.nextInt(10)));
      submitPutsAndAssertFailure(expectedException, false, true, true);
      // this should not close the router.
      Assert.assertTrue("Router should not be closed", router.isOpen());
      assertCloseCleanup();
    }
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
    for (DataNodeId dataNodeId : dataNodeIds) {
      String host = dataNodeId.getHostname();
      int port = dataNodeId.getPort();
      MockServer server = mockServerLayout.getMockServer(host, port);
      server.setServerErrorForAllRequests(ServerErrorCode.Unknown_Error);
    }
    Exception expectedException = new RouterException("", RouterErrorCode.AmbryUnavailable);
    submitPutsAndAssertFailure(expectedException, true, false, true);
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
    submitPutsAndAssertFailure(expectedException, true, false, false);
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
    List<ServerErrorCode> serverErrorList = new ArrayList<>();
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
    List<ServerErrorCode> serverErrorList = new ArrayList<>();
    serverErrorList.add(ServerErrorCode.No_Error);
    for (int i = 0; i < 5; i++) {
      serverErrorList.add(ServerErrorCode.Unknown_Error);
    }
    for (DataNodeId dataNodeId : dataNodeIds) {
      MockServer server = mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort());
      server.setServerErrors(serverErrorList);
    }
    Exception expectedException = new RouterException("", RouterErrorCode.AmbryUnavailable);
    submitPutsAndAssertFailure(expectedException, true, false, true);
  }

  /**
   * Tests put of blobs with various sizes with channels that do not have all the data at once. This also tests cases
   * where zero-length buffers are given out by the channel.
   */
  @Test
  public void testDelayedChannelPutSuccess() throws Exception {
    router = getNonBlockingRouter();
    int[] blobSizes = {0, // zero sized blob.
        chunkSize, // blob size is the same as chunk size.
        chunkSize + random.nextInt(chunkSize - 1) + 1, // over a chunk but less than 2 chunks.
        chunkSize * (2 + random.nextInt(10)), // blob size is a multiple of chunk size.
        chunkSize + (2 + random.nextInt(10)) + random.nextInt(chunkSize - 1) + 1 // multiple of a chunk and over.
    };
    for (int blobSize : blobSizes) {
      testDelayed(blobSize, false);
      testDelayed(blobSize, true);
    }
    assertSuccess();
    assertCloseCleanup();
  }

  /**
   * Helper method to put a blob with a channel that does not have all the data at once.
   * @param blobSize the size of the blob to put.
   * @param sendZeroSizedBuffers whether this test should involve making the channel send zero-sized buffers between
   *                             sending data buffers.
   */
  private void testDelayed(int blobSize, boolean sendZeroSizedBuffers) throws Exception {
    RequestAndResult requestAndResult = new RequestAndResult(blobSize);
    requestAndResultsList.add(requestAndResult);
    MockReadableStreamChannel putChannel = new MockReadableStreamChannel(blobSize, sendZeroSizedBuffers);
    FutureResult<String> future =
        (FutureResult<String>) router.putBlob(requestAndResult.putBlobProperties, requestAndResult.putUserMetadata,
            putChannel, requestAndResult.options, null);
    ByteBuffer src = ByteBuffer.wrap(requestAndResult.putContent);
    pushWithDelay(src, putChannel, blobSize, future);
    future.await(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
    requestAndResult.result = future;
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
    MockReadableStreamChannel putChannel = new MockReadableStreamChannel(blobSize, false);
    FutureResult<String> future =
        (FutureResult<String>) router.putBlob(requestAndResult.putBlobProperties, requestAndResult.putUserMetadata,
            putChannel, requestAndResult.options, null);
    ByteBuffer src = ByteBuffer.wrap(requestAndResult.putContent);

    //Make the channel act bad.
    putChannel.beBad();

    pushWithDelay(src, putChannel, blobSize, future);
    future.await(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
    requestAndResult.result = future;
    Exception expectedException = new Exception("Channel encountered an error");
    assertFailure(expectedException, true);
    assertCloseCleanup();
  }

  /**
   * Test that the size in BlobProperties is ignored for puts, by attempting puts with varying values for size in
   * BlobProperties.
   */
  @Test
  public void testChannelSizeNotSizeInPropertiesPutSuccess() throws Exception {
    int actualBlobSizes[] = {0, chunkSize - 1, chunkSize, chunkSize + 1, chunkSize * 2, chunkSize * 2 + 1};
    for (int actualBlobSize : actualBlobSizes) {
      int sizesInProperties[] = {actualBlobSize - 1, actualBlobSize + 1};
      for (int sizeInProperties : sizesInProperties) {
        requestAndResultsList.clear();
        RequestAndResult requestAndResult = new RequestAndResult(0);
        // Change the actual content size.
        requestAndResult.putContent = new byte[actualBlobSize];
        requestAndResult.putBlobProperties =
            new BlobProperties(sizeInProperties, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
                Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false,
                EXTERNAL_ASSET_TAG);
        random.nextBytes(requestAndResult.putContent);
        requestAndResultsList.add(requestAndResult);
        submitPutsAndAssertSuccess(true);
      }
    }
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
    MockReadableStreamChannel putChannel = new MockReadableStreamChannel(blobSize, false);
    FutureResult<String> future =
        (FutureResult<String>) router.putBlob(requestAndResult.putBlobProperties, requestAndResult.putUserMetadata,
            putChannel, requestAndResult.options, null);
    ByteBuffer src = ByteBuffer.wrap(requestAndResult.putContent);
    // There will be two chunks written to the underlying writable channel, and so two events will be fired.
    int writeSize = blobSize / 2;
    ByteBuffer buf1 = ByteBuffer.allocate(writeSize);
    src.get(buf1.array());
    putChannel.write(buf1);
    // The first write will wake up (or will have woken up) the ChunkFiller thread and it will not go to WAITING until
    // the operation is complete as an attempt to fill chunks will be done by the ChunkFiller in every iteration
    // until the operation is complete.
    Assert.assertTrue(
        "ChunkFillerThread should have gone to RUNNABLE state as there is an active operation that is not yet complete",
        waitForThreadState(chunkFillerThread, Thread.State.RUNNABLE));
    ByteBuffer buf2 = ByteBuffer.allocate(writeSize);
    src.get(buf2.array());
    putChannel.write(buf2);
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
   * Tests that the replication policy in the container is respected
   * @throws Exception
   */
  @Test
  public void testReplPolicyToPartitionClassMapping() throws Exception {
    Account refAccount = accountService.createAndAddRandomAccount();
    Map<Container, String> containerToPartClass = new HashMap<>();
    Iterator<Container> allContainers = refAccount.getAllContainers().iterator();
    Container container = allContainers.next();
    // container with null replication policy
    container = accountService.addReplicationPolicyToContainer(container, null);
    containerToPartClass.put(container, MockClusterMap.DEFAULT_PARTITION_CLASS);
    container = allContainers.next();
    // container with configured default replication policy
    container = accountService.addReplicationPolicyToContainer(container, MockClusterMap.DEFAULT_PARTITION_CLASS);
    containerToPartClass.put(container, MockClusterMap.DEFAULT_PARTITION_CLASS);
    container = allContainers.next();
    // container with a special replication policy
    container = accountService.addReplicationPolicyToContainer(container, MockClusterMap.SPECIAL_PARTITION_CLASS);
    containerToPartClass.put(container, MockClusterMap.SPECIAL_PARTITION_CLASS);
    // adding this to test the random account and container case (does not actually happen if coming from the frontend)
    containerToPartClass.put(null, MockClusterMap.DEFAULT_PARTITION_CLASS);

    Map<Integer, Integer> sizeToChunkCount = new HashMap<>();
    // simple
    sizeToChunkCount.put(random.nextInt(chunkSize) + 1, 1);
    int count = random.nextInt(8) + 3;
    // composite
    sizeToChunkCount.put(chunkSize * count, count + 1);
    for (Map.Entry<Integer, Integer> sizeAndChunkCount : sizeToChunkCount.entrySet()) {
      for (Map.Entry<Container, String> containerAndPartClass : containerToPartClass.entrySet()) {
        requestAndResultsList.clear();
        requestAndResultsList.add(
            new RequestAndResult(sizeAndChunkCount.getKey(), containerAndPartClass.getKey(), PutBlobOptions.DEFAULT,
                null));
        mockClusterMap.clearLastNRequestedPartitionClasses();
        submitPutsAndAssertSuccess(true);
        // since the puts are processed one at a time, it is fair to check the last partition class set
        checkLastRequestPartitionClasses(sizeAndChunkCount.getValue(), containerAndPartClass.getValue());
      }
    }

    // exception if there is no partition class that conforms to the replication policy
    String nonExistentClass = TestUtils.getRandomString(3);
    accountService.addReplicationPolicyToContainer(container, nonExistentClass);
    requestAndResultsList.clear();
    requestAndResultsList.add(new RequestAndResult(chunkSize, container, PutBlobOptions.DEFAULT, null));
    mockClusterMap.clearLastNRequestedPartitionClasses();
    submitPutsAndAssertFailure(new RouterException("", RouterErrorCode.UnexpectedInternalError), true, false, false);
    // because of how the non-encrypted flow is, prepareForSending() may be called twice. So not checking for count
    checkLastRequestPartitionClasses(-1, nonExistentClass);
  }

  /**
   * Push from the src to the putChannel in random sized writes, with possible delays.
   * @param src the input {@link ByteBuffer}
   * @param putChannel the destination {@link MockReadableStreamChannel}
   * @param blobSize the total length of bytes to push.
   * @param future the future associated with the overall operation.
   * @throws Exception if the write to the channel throws an exception.
   */
  private void pushWithDelay(ByteBuffer src, MockReadableStreamChannel putChannel, int blobSize, Future future)
      throws Exception {
    int pushedSoFar = 0;
    do {
      int toPush = random.nextInt(blobSize - pushedSoFar + 1);
      ByteBuffer buf = ByteBuffer.allocate(toPush);
      src.get(buf.array());
      putChannel.write(buf);
      Thread.yield();
      pushedSoFar += toPush;
    } while (pushedSoFar < blobSize && !future.isDone());
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
  private NonBlockingRouter getNonBlockingRouter() throws IOException, GeneralSecurityException {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", LOCAL_DC);
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(chunkSize));
    properties.setProperty("router.put.request.parallelism", Integer.toString(requestParallelism));
    properties.setProperty("router.put.success.target", Integer.toString(successTarget));
    properties.setProperty("router.metadata.content.version", String.valueOf(metadataContentVersion));
    VerifiableProperties vProps = new VerifiableProperties(properties);
    if (testEncryption && instantiateEncryptionCast) {
      setupEncryptionCast(vProps);
    }
    metrics = new NonBlockingRouterMetrics(mockClusterMap, null);
    router = new NonBlockingRouter(new RouterConfig(vProps), metrics,
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), notificationSystem, mockClusterMap, kms, cryptoService,
        cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS);
    return router;
  }

  /**
   * Instantiates KMS, Crypto service and CryptoJobHandler to assist in encryption
   * @param vProps {@link VerifiableProperties} instance to use for instantiation
   * @throws GeneralSecurityException
   */
  private void setupEncryptionCast(VerifiableProperties vProps) throws GeneralSecurityException {
    kms = new MockKeyManagementService(new KMSConfig(vProps),
        TestUtils.getRandomKey(SingleKeyManagementServiceTest.DEFAULT_KEY_SIZE_CHARS));
    cryptoService = new MockCryptoService(new CryptoServiceConfig(vProps));
    cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
  }

  /**
   * Submits put blob operations that are expected to succeed, waits for completion, and asserts success.
   * @param shouldCloseRouterAfter whether the router should be closed after the operation.
   */
  private void submitPutsAndAssertSuccess(boolean shouldCloseRouterAfter) throws Exception {
    submitPut().await(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
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
   * @param testNotifications {@code true} to test that notifications for successfully put data chunks are received.
   */
  private void submitPutsAndAssertFailure(Exception expectedException, boolean shouldCloseRouterAfter,
      boolean incrementTimer, boolean testNotifications) throws Exception {
    CountDownLatch doneLatch = submitPut();
    if (incrementTimer) {
      do {
        // increment mock time so failures (like connection checkout) can be induced.
        mockTime.sleep(CHECKOUT_TIMEOUT_MS + 1);
      } while (!doneLatch.await(1, TimeUnit.MILLISECONDS));
    } else {
      doneLatch.await(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
    }
    assertFailure(expectedException, testNotifications);
    if (shouldCloseRouterAfter) {
      assertCloseCleanup();
    }
  }

  /**
   * Submits put operations. This is called by {@link #submitPutsAndAssertSuccess(boolean)} and
   * {@link #submitPutsAndAssertFailure(Exception, boolean, boolean, boolean)} methods.
   * @return a {@link CountDownLatch} to await on for operation completion.
   */
  private CountDownLatch submitPut() throws Exception {
    notificationSystem.blobCreatedEvents.clear();
    final CountDownLatch doneLatch = new CountDownLatch(requestAndResultsList.size());
    // This check is here for certain tests (like testConcurrentPuts) that require using the same router.
    if (instantiateNewRouterForPuts) {
      router = getNonBlockingRouter();
    }
    for (final RequestAndResult requestAndResult : requestAndResultsList) {
      Utils.newThread(() -> {
        try {
          ReadableStreamChannel putChannel =
              new ByteBufferReadableStreamChannel(ByteBuffer.wrap(requestAndResult.putContent));
          if (requestAndResult.chunksToStitch == null) {
            requestAndResult.result = (FutureResult<String>) router.putBlob(requestAndResult.putBlobProperties,
                requestAndResult.putUserMetadata, putChannel, requestAndResult.options, null);
          } else {
            requestAndResult.result = (FutureResult<String>) router.stitchBlob(requestAndResult.putBlobProperties,
                requestAndResult.putUserMetadata, requestAndResult.chunksToStitch, null);
          }
          requestAndResult.result.await(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          requestAndResult.result = new FutureResult<>();
          requestAndResult.result.done(null, e);
        } finally {
          doneLatch.countDown();
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
    HashMap<String, ByteBuffer> allChunks = new HashMap<>();
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
      if (exception != null) {
        throw exception;
      }
      Assert.assertNotNull("blobId should not be null", blobId);
      verifyBlob(requestAndResult, allChunks);
    }
  }

  /**
   * Verifies that the blob associated with the blob id returned by a successful put operation has exactly the same
   * data as the original object that was put.
   * @param requestAndResult the {@link RequestAndResult} to use for verification.
   * @param serializedRequests the mapping from blob ids to their corresponding serialized {@link PutRequest}.
   */
  private void verifyBlob(RequestAndResult requestAndResult, HashMap<String, ByteBuffer> serializedRequests)
      throws Exception {
    String blobId = requestAndResult.result.result();
    ByteBuffer serializedRequest = serializedRequests.get(blobId);
    PutRequest request = deserializePutRequest(serializedRequest);
    NotificationBlobType notificationBlobType;
    BlobId origBlobId = new BlobId(blobId, mockClusterMap);
    boolean stitchOperation = requestAndResult.chunksToStitch != null;
    if (stitchOperation) {
      assertEquals("Stitch operations should always produce metadata blobs", BlobType.MetadataBlob,
          request.getBlobType());
    }
    if (request.getBlobType() == BlobType.MetadataBlob) {
      notificationBlobType = NotificationBlobType.Composite;
      assertEquals("Expected metadata", BlobDataType.METADATA, origBlobId.getBlobDataType());
      byte[] data = Utils.readBytesFromStream(request.getBlobStream(), (int) request.getBlobSize());
      CompositeBlobInfo compositeBlobInfo = MetadataContentSerDe.deserializeMetadataContentRecord(ByteBuffer.wrap(data),
          new BlobIdFactory(mockClusterMap));
      List<StoreKey> dataBlobIds = compositeBlobInfo.getKeys();
      long expectedMaxChunkSize;
      long expectedTotalSize;
      int expectedNumChunks;
      if (stitchOperation) {
        expectedMaxChunkSize =
            requestAndResult.chunksToStitch.stream().mapToLong(ChunkInfo::getChunkSizeInBytes).max().orElse(0);
        expectedTotalSize = requestAndResult.chunksToStitch.stream().mapToLong(ChunkInfo::getChunkSizeInBytes).sum();
        expectedNumChunks = requestAndResult.chunksToStitch.size();
      } else {
        expectedMaxChunkSize = chunkSize;
        expectedTotalSize = requestAndResult.putContent.length;
        expectedNumChunks = RouterUtils.getNumChunksForBlobAndChunkSize(requestAndResult.putContent.length, chunkSize);
      }
      if (metadataContentVersion <= MessageFormatRecord.Metadata_Content_Version_V2) {
        assertEquals("Wrong max chunk size in metadata", expectedMaxChunkSize, compositeBlobInfo.getChunkSize());
      }
      assertEquals("Wrong total size in metadata", expectedTotalSize, compositeBlobInfo.getTotalSize());
      assertEquals("Number of chunks is not as expected", expectedNumChunks, dataBlobIds.size());
      // Verify all dataBlobIds are DataChunk
      for (StoreKey key : dataBlobIds) {
        BlobId origDataBlobId = (BlobId) key;
        assertEquals("Expected datachunk", BlobDataType.DATACHUNK, origDataBlobId.getBlobDataType());
      }
      // verify user-metadata
      if (requestAndResult.putBlobProperties.isEncrypted()) {
        ByteBuffer userMetadata = request.getUsermetadata();
        // reason to directly call run() instead of spinning up a thread instead of calling start() is that, any exceptions or
        // assertion failures in non main thread will not fail the test.
        new DecryptJob(origBlobId, request.getBlobEncryptionKey().duplicate(), null, userMetadata, cryptoService, kms,
            new CryptoJobMetricsTracker(metrics.decryptJobMetrics), (result, exception) -> {
          assertNull("Exception should not be thrown", exception);
          assertEquals("BlobId mismatch", origBlobId, result.getBlobId());
          assertArrayEquals("UserMetadata mismatch", requestAndResult.putUserMetadata,
              result.getDecryptedUserMetadata().array());
        }).run();
      } else {
        assertArrayEquals("UserMetadata mismatch", requestAndResult.putUserMetadata, request.getUsermetadata().array());
      }
      if (!stitchOperation) {
        verifyCompositeBlob(requestAndResult.putBlobProperties, requestAndResult.putContent,
            requestAndResult.putUserMetadata, dataBlobIds, request, serializedRequests);
      }
    } else {
      notificationBlobType =
          requestAndResult.options.isChunkUpload() ? NotificationBlobType.DataChunk : NotificationBlobType.Simple;
      // TODO: Currently, we don't have the logic to distinguish Simple vs DataChunk for the first chunk
      // Once the logic is fixed we should assert Simple.
      BlobDataType dataType = origBlobId.getBlobDataType();
      assertTrue("Invalid blob data type", dataType == BlobDataType.DATACHUNK || dataType == BlobDataType.SIMPLE);

      byte[] content = Utils.readBytesFromStream(request.getBlobStream(), (int) request.getBlobSize());
      if (!requestAndResult.putBlobProperties.isEncrypted()) {
        assertArrayEquals("Input blob and written blob should be the same", requestAndResult.putContent, content);
        assertArrayEquals("UserMetadata mismatch for simple blob", requestAndResult.putUserMetadata,
            request.getUsermetadata().array());
      } else {
        ByteBuffer userMetadata = request.getUsermetadata();
        // reason to directly call run() instead of spinning up a thread instead of calling start() is that, any exceptions or
        // assertion failures in non main thread will not fail the test.
        new DecryptJob(origBlobId, request.getBlobEncryptionKey().duplicate(), Unpooled.wrappedBuffer(content),
            userMetadata, cryptoService, kms, new CryptoJobMetricsTracker(metrics.decryptJobMetrics),
            (result, exception) -> {
              assertNull("Exception should not be thrown", exception);
              assertEquals("BlobId mismatch", origBlobId, result.getBlobId());
              assertArrayEquals("Content mismatch", requestAndResult.putContent,
                  result.getDecryptedBlobContent().array());
              assertArrayEquals("UserMetadata mismatch", requestAndResult.putUserMetadata,
                  result.getDecryptedUserMetadata().array());
            }).run();
      }
    }
    notificationSystem.verifyNotification(blobId, notificationBlobType, request.getBlobProperties());
  }

  /**
   * Verify Composite blob for content, userMetadata and
   * @param properties {@link BlobProperties} of the blob
   * @param originalPutContent original out content
   * @param originalUserMetadata original user-metadata
   * @param dataBlobIds {@link List} of {@link StoreKey}s of the composite blob in context
   * @param request {@link com.github.ambry.protocol.PutRequest} to fetch info from
   * @param serializedRequests the mapping from blob ids to their corresponding serialized {@link PutRequest}.
   * @throws Exception
   */
  private void verifyCompositeBlob(BlobProperties properties, byte[] originalPutContent, byte[] originalUserMetadata,
      List<StoreKey> dataBlobIds, PutRequest request, HashMap<String, ByteBuffer> serializedRequests) throws Exception {
    StoreKey lastKey = dataBlobIds.get(dataBlobIds.size() - 1);
    byte[] content = new byte[(int) request.getBlobProperties().getBlobSize()];
    AtomicInteger offset = new AtomicInteger(0);
    for (StoreKey key : dataBlobIds) {
      PutRequest dataBlobPutRequest = deserializePutRequest(serializedRequests.get(key.getID()));
      AtomicInteger dataBlobLength = new AtomicInteger((int) dataBlobPutRequest.getBlobSize());
      InputStream dataBlobStream = dataBlobPutRequest.getBlobStream();
      if (!properties.isEncrypted()) {
        Utils.readBytesFromStream(dataBlobStream, content, offset.get(), dataBlobLength.get());
        Assert.assertArrayEquals("UserMetadata mismatch", originalUserMetadata,
            dataBlobPutRequest.getUsermetadata().array());
      } else {
        byte[] dataBlobContent = Utils.readBytesFromStream(dataBlobStream, dataBlobLength.get());
        // reason to directly call run() instead of spinning up a thread instead of calling start() is that, any exceptions or
        // assertion failures in non main thread will not fail the test.
        new DecryptJob(dataBlobPutRequest.getBlobId(), dataBlobPutRequest.getBlobEncryptionKey().duplicate(),
            Unpooled.wrappedBuffer(dataBlobContent), dataBlobPutRequest.getUsermetadata().duplicate(), cryptoService,
            kms, new CryptoJobMetricsTracker(metrics.decryptJobMetrics), (result, exception) -> {
          Assert.assertNull("Exception should not be thrown", exception);
          assertEquals("BlobId mismatch", dataBlobPutRequest.getBlobId(), result.getBlobId());
          Assert.assertArrayEquals("UserMetadata mismatch", originalUserMetadata,
              result.getDecryptedUserMetadata().array());
          dataBlobLength.set(result.getDecryptedBlobContent().remaining());
          result.getDecryptedBlobContent().get(content, offset.get(), dataBlobLength.get());
        }).run();
      }
      if (metadataContentVersion <= MessageFormatRecord.Metadata_Content_Version_V2 && key != lastKey) {
        assertEquals("all chunks except last should be fully filled", chunkSize, dataBlobLength.get());
      } else if (key == lastKey) {
        assertEquals("Last chunk should be of non-zero length and equal to the length of the remaining bytes",
            (originalPutContent.length - 1) % chunkSize + 1, dataBlobLength.get());
      }
      offset.addAndGet(dataBlobLength.get());
      assertEquals("dataBlobStream should have no more data", -1, dataBlobStream.read());
      notificationSystem.verifyNotification(key.getID(), NotificationBlobType.DataChunk,
          dataBlobPutRequest.getBlobProperties());
    }
    Assert.assertArrayEquals("Input blob and written blob should be the same", originalPutContent, content);
  }

  /**
   * Deserialize a {@link PutRequest} from the given serialized ByteBuffer.
   * @param serialized the serialized ByteBuffer.
   * @return returns the deserialized output.
   */
  private PutRequest deserializePutRequest(ByteBuffer serialized) throws IOException {
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
   * @param testNotifications {@code true} to test that notifications for successfully put data chunks are received.
   */
  private void assertFailure(Exception expectedException, boolean testNotifications) {
    for (RequestAndResult requestAndResult : requestAndResultsList) {
      String blobId = requestAndResult.result.result();
      Exception exception = requestAndResult.result.error();
      Assert.assertNull("blobId should be null", blobId);
      Assert.assertNotNull("exception should not be null", exception);
      Assert.assertTrue("Exception received should be the expected Exception",
          exceptionsAreEqual(expectedException, exception));
    }
    if (testNotifications) {
      notificationSystem.verifyNotificationsForFailedPut();
    }
  }

  /**
   * Check if the given two exceptions are equal.
   * @return true if the exceptions are equal.
   */
  private boolean exceptionsAreEqual(Exception a, Exception b) {
    if (a instanceof RouterException) {
      return ((RouterException) a).getErrorCode().equals(((RouterException) b).getErrorCode());
    } else {
      return a.getClass() == b.getClass() && a.getMessage().equals(b.getMessage());
    }
  }

  /**
   * Asserts that expected threads are running before the router is closed, and that they are not running after the
   * router is closed.
   */
  private void assertCloseCleanup() {
    assertEquals("Exactly one chunkFiller thread should be running before the router is closed", 1,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    assertEquals("Exactly two RequestResponseHandler thread should be running before the router is closed", 2,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    router.close();
    assertEquals("No ChunkFiller Thread should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    assertEquals("No RequestResponseHandler should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Checks the last {@code expectedCount} partition classes requested from the clustermap to make sure they match with
   * {@code expectedClass}
   * @param expectedCount the number of times the {@code expectedClass} was requested. If < 0, the check is ignored
   * @param expectedClass the partition class requested
   */
  private void checkLastRequestPartitionClasses(int expectedCount, String expectedClass) {
    List<String> lastNRequestedPartitionClasses = mockClusterMap.getLastNRequestedPartitionClasses();
    if (expectedCount >= 0) {
      assertEquals("Last requested partition class count is not as expected", expectedCount,
          lastNRequestedPartitionClasses.size());
    }
    List<String> partitionClassesExpected = Collections.nCopies(lastNRequestedPartitionClasses.size(), expectedClass);
    assertEquals("Last requested partition classes not as expected", partitionClassesExpected,
        lastNRequestedPartitionClasses);
  }

  private class RequestAndResult {
    BlobProperties putBlobProperties;
    byte[] putUserMetadata;
    byte[] putContent;
    PutBlobOptions options;
    List<ChunkInfo> chunksToStitch;
    FutureResult<String> result;

    RequestAndResult(int blobSize) {
      this(blobSize, null, PutBlobOptions.DEFAULT, null);
    }

    RequestAndResult(List<ChunkInfo> chunksToStitch) {
      this(0, null, PutBlobOptions.DEFAULT, chunksToStitch);
    }

    RequestAndResult(int blobSize, Container container, PutBlobOptions options, List<ChunkInfo> chunksToStitch) {
      putBlobProperties = new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
          container == null ? Utils.getRandomShort(TestUtils.RANDOM) : container.getParentAccountId(),
          container == null ? Utils.getRandomShort(TestUtils.RANDOM) : container.getId(), testEncryption,
          EXTERNAL_ASSET_TAG);
      putUserMetadata = new byte[10];
      random.nextBytes(putUserMetadata);
      putContent = new byte[blobSize];
      random.nextBytes(putContent);
      this.options = options;
      this.chunksToStitch = chunksToStitch;
      // future result set after the operation is complete.
    }
  }

  /**
   * A notification system for testing that keeps track of data from onBlobCreated calls.
   */
  private class TestNotificationSystem extends LoggingNotificationSystem {
    Map<String, List<BlobCreatedEvent>> blobCreatedEvents = new HashMap<>();

    @Override
    public void onBlobCreated(String blobId, BlobProperties blobProperties, Account account, Container container,
        NotificationBlobType notificationBlobType) {
      blobCreatedEvents.computeIfAbsent(blobId, k -> new ArrayList<>())
          .add(new BlobCreatedEvent(blobProperties, notificationBlobType));
    }

    /**
     * Test that an onBlobCreated notification was generated as expected for this blob ID.
     * @param blobId The blob ID to look up a notification for.
     * @param expectedNotificationBlobType the expected {@link NotificationBlobType}.
     * @param expectedBlobProperties the expected {@link BlobProperties}.
     */
    private void verifyNotification(String blobId, NotificationBlobType expectedNotificationBlobType,
        BlobProperties expectedBlobProperties) {
      List<BlobCreatedEvent> events = blobCreatedEvents.get(blobId);
      Assert.assertTrue("Wrong number of events for blobId: " + events, events != null && events.size() == 1);
      BlobCreatedEvent event = events.get(0);
      assertEquals("NotificationBlobType does not match data in notification event.", expectedNotificationBlobType,
          event.notificationBlobType);
      Assert.assertTrue("BlobProperties does not match data in notification event.",
          RouterTestHelpers.arePersistedFieldsEquivalent(expectedBlobProperties, event.blobProperties));
      assertNull("Non-persistent filed should be null after serialization.",
          expectedBlobProperties.getExternalAssetTag());
      assertEquals("ExternalAssetTag in notification should match", EXTERNAL_ASSET_TAG,
          event.blobProperties.getExternalAssetTag());
      assertEquals("Expected blob size does not match data in notification event.",
          expectedBlobProperties.getBlobSize(), event.blobProperties.getBlobSize());
    }

    /**
     * Verify that onBlobCreated notifications were created for the data chunks of a failed put.
     */
    void verifyNotificationsForFailedPut() {
      Set<String> blobIdsVisited = new HashSet<>();
      for (MockServer mockServer : mockServerLayout.getMockServers()) {
        for (Map.Entry<String, StoredBlob> blobEntry : mockServer.getBlobs().entrySet()) {
          if (blobIdsVisited.add(blobEntry.getKey())) {
            StoredBlob blob = blobEntry.getValue();
            verifyNotification(blobEntry.getKey(), NotificationBlobType.DataChunk, blob.properties);
          }
        }
      }
    }
  }

  private static class BlobCreatedEvent {
    BlobProperties blobProperties;
    NotificationBlobType notificationBlobType;

    BlobCreatedEvent(BlobProperties blobProperties, NotificationBlobType notificationBlobType) {
      this.blobProperties = blobProperties;
      this.notificationBlobType = notificationBlobType;
    }
  }
}

/**
 * A {@link ReadableStreamChannel} implementation whose {@link #readInto(AsyncWritableChannel, Callback)} method
 * reads into the {@link AsyncWritableChannel} passed into it as and when data is written to it and
 * not at once. Also if the beBad state is set, the callback is called with an exception.
 */
class MockReadableStreamChannel implements ReadableStreamChannel {
  private AsyncWritableChannel writableChannel;
  private FutureResult<Long> returnedFuture;
  private Callback<Long> callback;
  private final long size;
  private long readSoFar;
  private boolean beBad = false;
  private final boolean sendZeroSizedBuffers;
  private final Random random = new Random();

  /**
   * Constructs a MockReadableStreamChannel.
   * @param size the number of bytes that will eventually be written into this channel.
   * @param sendZeroSizedBuffers if true, this channel will write out a zero-sized buffer at the end, and with
   *                             50% probability, send out zero-sized buffers after every data buffer.
   */
  MockReadableStreamChannel(long size, boolean sendZeroSizedBuffers) {
    this.size = size;
    this.sendZeroSizedBuffers = sendZeroSizedBuffers;
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
      if (callback != null) {
        callback.onCompletion(null, exception);
      }
      return;
    }
    writableChannel.write(buf, null).get();
    readSoFar += toRead;
    if (readSoFar == size) {
      if (sendZeroSizedBuffers) {
        // Send a final zero sized buffer.
        sendZeroAndWait();
      }
      returnedFuture.done(size, null);
      callback.onCompletion(size, null);
    } else {
      if (sendZeroSizedBuffers && random.nextInt(2) % 2 == 0) {
        // Send a zero-sized blob with 50% probability.
        sendZeroAndWait();
      }
    }
  }

  /**
   * Send a zero sized buffer to the associated {@link AsyncWritableChannel}
   */
  private void sendZeroAndWait() throws Exception {
    ByteBuffer buf = ByteBuffer.allocate(0);
    writableChannel.write(buf, null).get();
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
    this.returnedFuture = new FutureResult<>();
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
  public void close() {
    // no op.
  }
}
