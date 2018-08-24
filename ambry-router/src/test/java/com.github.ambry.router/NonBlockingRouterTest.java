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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.RouterTestHelpers.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Class to test the {@link NonBlockingRouter}
 */
@RunWith(Parameterized.class)
public class NonBlockingRouterTest {
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final int REQUEST_TIMEOUT_MS = 1000;
  private static final int PUT_REQUEST_PARALLELISM = 3;
  private static final int PUT_SUCCESS_TARGET = 2;
  private static final int GET_REQUEST_PARALLELISM = 2;
  private static final int GET_SUCCESS_TARGET = 1;
  private static final int DELETE_REQUEST_PARALLELISM = 3;
  private static final int DELETE_SUCCESS_TARGET = 2;
  private static final int PUT_CONTENT_SIZE = 1000;
  private int maxPutChunkSize = PUT_CONTENT_SIZE;
  private final Random random = new Random();
  private NonBlockingRouter router;
  private NonBlockingRouterMetrics routerMetrics;
  private PutManager putManager;
  private GetManager getManager;
  private DeleteManager deleteManager;
  private AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>(MockSelectorState.Good);
  private final MockTime mockTime;
  private final KeyManagementService kms;
  private final String singleKeyForKMS;
  private final CryptoService cryptoService;
  private final MockClusterMap mockClusterMap;
  private final boolean testEncryption;
  private final InMemAccountService accountService;
  private CryptoJobHandler cryptoJobHandler;

  // Request params;
  BlobProperties putBlobProperties;
  byte[] putUserMetadata;
  byte[] putContent;
  ReadableStreamChannel putChannel;

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Initialize parameters common to all tests.
   * @param testEncryption {@code true} to test with encryption enabled. {@code false} otherwise
   * @throws Exception
   */
  public NonBlockingRouterTest(boolean testEncryption) throws Exception {
    this.testEncryption = testEncryption;
    mockTime = new MockTime();
    mockClusterMap = new MockClusterMap();
    NonBlockingRouter.currentOperationsCount.set(0);
    VerifiableProperties vProps = new VerifiableProperties(new Properties());
    singleKeyForKMS = TestUtils.getRandomKey(SingleKeyManagementServiceTest.DEFAULT_KEY_SIZE_CHARS);
    kms = new SingleKeyManagementService(new KMSConfig(vProps), singleKeyForKMS);
    cryptoService = new GCMCryptoService(new CryptoServiceConfig(vProps));
    cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
    accountService = new InMemAccountService(false, true);
  }

  @After
  public void after() {
    Assert.assertEquals("Current operations count should be 0", 0, NonBlockingRouter.currentOperationsCount.get());
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link NonBlockingRouter}.
   * @return the created VerifiableProperties instance.
   */
  private Properties getNonBlockingRouterProperties(String routerDataCenter) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDataCenter);
    properties.setProperty("router.put.request.parallelism", Integer.toString(PUT_REQUEST_PARALLELISM));
    properties.setProperty("router.put.success.target", Integer.toString(PUT_SUCCESS_TARGET));
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxPutChunkSize));
    properties.setProperty("router.get.request.parallelism", Integer.toString(GET_REQUEST_PARALLELISM));
    properties.setProperty("router.get.success.target", Integer.toString(GET_SUCCESS_TARGET));
    properties.setProperty("router.delete.request.parallelism", Integer.toString(DELETE_REQUEST_PARALLELISM));
    properties.setProperty("router.delete.success.target", Integer.toString(DELETE_SUCCESS_TARGET));
    properties.setProperty("router.connection.checkout.timeout.ms", Integer.toString(CHECKOUT_TIMEOUT_MS));
    properties.setProperty("router.request.timeout.ms", Integer.toString(REQUEST_TIMEOUT_MS));
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "dc1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("kms.default.container.key", TestUtils.getRandomKey(128));
    return properties;
  }

  /**
   * Construct {@link Properties} and {@link MockServerLayout} and initialize and set the
   * router with them.
   */
  private void setRouter() throws IOException {
    setRouter(getNonBlockingRouterProperties("DC1"), new MockServerLayout(mockClusterMap),
        new LoggingNotificationSystem());
  }

  /**
   * Initialize and set the router with the given {@link Properties} and {@link MockServerLayout}
   * @param props the {@link Properties}
   * @param mockServerLayout the {@link MockServerLayout}
   * @param notificationSystem the {@link NotificationSystem} to use.
   */
  private void setRouter(Properties props, MockServerLayout mockServerLayout, NotificationSystem notificationSystem)
      throws IOException {
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap);
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), routerMetrics,
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), notificationSystem, mockClusterMap, kms, cryptoService,
        cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS);
  }

  private void setOperationParams() {
    putBlobProperties = new BlobProperties(-1, "serviceId", "memberId", "contentType", false, TTL_SECS,
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), testEncryption);
    putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    putContent = new byte[PUT_CONTENT_SIZE];
    random.nextBytes(putContent);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
  }

  /**
   * Test the {@link NonBlockingRouterFactory}
   */
  @Test
  public void testNonBlockingRouterFactory() throws Exception {
    Properties props = getNonBlockingRouterProperties("NotInClusterMap");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    try {
      router = (NonBlockingRouter) new NonBlockingRouterFactory(verifiableProperties, mockClusterMap,
          new LoggingNotificationSystem(), null, accountService).getRouter();
      Assert.fail("NonBlockingRouterFactory instantiation should have failed because the router datacenter is not in "
          + "the cluster map");
    } catch (IllegalStateException e) {
    }
    props = getNonBlockingRouterProperties("DC1");
    verifiableProperties = new VerifiableProperties((props));
    router = (NonBlockingRouter) new NonBlockingRouterFactory(verifiableProperties, mockClusterMap,
        new LoggingNotificationSystem(), null, accountService).getRouter();
    assertExpectedThreadCounts(2, 1);
    router.close();
    assertExpectedThreadCounts(0, 0);
  }

  /**
   * Test Router with a single scaling unit.
   */
  @Test
  public void testRouterBasic() throws Exception {
    setRouter();
    assertExpectedThreadCounts(2, 1);
    setOperationParams();

    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    String blobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
    router.updateBlobTtl(blobId, null, Utils.Infinite_Time);
    router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
    router.getBlob(blobId, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build())
        .get();
    router.deleteBlob(blobId, null).get();
    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
    } catch (ExecutionException e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
    }
    router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build()).get();
    router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_All).build()).get();
    router.close();
    assertExpectedThreadCounts(0, 0);

    //submission after closing should return a future that is already done.
    assertClosed();
  }

  /**
   * Test behavior with various null inputs to router methods.
   * @throws Exception
   */
  @Test
  public void testNullArguments() throws Exception {
    setRouter();
    assertExpectedThreadCounts(2, 1);
    setOperationParams();

    try {
      router.getBlob(null, new GetBlobOptionsBuilder().build());
      Assert.fail("null blobId should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      router.getBlob("", null);
      Assert.fail("null options should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      router.putBlob(putBlobProperties, putUserMetadata, null, new PutBlobOptionsBuilder().build());
      Assert.fail("null channel should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      router.putBlob(null, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build());
      Assert.fail("null blobProperties should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      router.deleteBlob(null, null);
      Assert.fail("null blobId should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      router.updateBlobTtl(null, null, Utils.Infinite_Time);
      Assert.fail("null blobId should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    // null user metadata should work.
    router.putBlob(putBlobProperties, null, putChannel, new PutBlobOptionsBuilder().build()).get();

    router.close();
    assertExpectedThreadCounts(0, 0);
    //submission after closing should return a future that is already done.
    assertClosed();
  }

  /**
   * Test router put operation in a scenario where there are no partitions available.
   */
  @Test
  public void testRouterPartitionsUnavailable() throws Exception {
    setRouter();
    setOperationParams();
    mockClusterMap.markAllPartitionsUnavailable();
    try {
      router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
      Assert.fail("Put should have failed if there are no partitions");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("Should have received AmbryUnavailable error", RouterErrorCode.AmbryUnavailable,
          r.getErrorCode());
    }
    router.close();
    assertExpectedThreadCounts(0, 0);
    assertClosed();
  }

  /**
   * Test router put operation in a scenario where there are partitions, but none in the local DC.
   * This should not ideally happen unless there is a bad config, but the router should be resilient and
   * just error out these operations.
   */
  @Test
  public void testRouterNoPartitionInLocalDC() throws Exception {
    // set the local DC to invalid, so that for puts, no partitions are available locally.
    Properties props = getNonBlockingRouterProperties("invalidDC");
    setRouter(props, new MockServerLayout(mockClusterMap), new LoggingNotificationSystem());
    setOperationParams();
    try {
      router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
      Assert.fail("Put should have failed if there are no partitions");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals(RouterErrorCode.UnexpectedInternalError, r.getErrorCode());
    }
    router.close();
    assertExpectedThreadCounts(0, 0);
    assertClosed();
  }

  /**
   * Test RequestResponseHandler thread exit flow. If the RequestResponseHandlerThread exits on its own (due to a
   * Throwable), then the router gets closed immediately along with the completion of all the operations.
   */
  @Test
  public void testRequestResponseHandlerThreadExitFlow() throws Exception {
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockServerLayout(mockClusterMap), mockTime), new LoggingNotificationSystem(),
        mockClusterMap, kms, cryptoService, cryptoJobHandler, accountService, mockTime,
        MockClusterMap.DEFAULT_PARTITION_CLASS);

    assertExpectedThreadCounts(2, 1);

    setOperationParams();
    mockSelectorState.set(MockSelectorState.ThrowExceptionOnAllPoll);
    Future future = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build());
    try {
      while (!future.isDone()) {
        mockTime.sleep(1000);
        Thread.yield();
      }
      future.get();
      Assert.fail("The operation should have failed");
    } catch (ExecutionException e) {
      Assert.assertEquals(RouterErrorCode.OperationTimedOut, ((RouterException) e.getCause()).getErrorCode());
    }

    setOperationParams();
    mockSelectorState.set(MockSelectorState.ThrowThrowableOnSend);
    future = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build());

    Thread requestResponseHandlerThreadRegular = TestUtils.getThreadByThisName("RequestResponseHandlerThread-0");
    Thread requestResponseHandlerThreadBackground =
        TestUtils.getThreadByThisName("RequestResponseHandlerThread-backgroundDeleter");
    if (requestResponseHandlerThreadRegular != null) {
      requestResponseHandlerThreadRegular.join(NonBlockingRouter.SHUTDOWN_WAIT_MS);
    }
    if (requestResponseHandlerThreadBackground != null) {
      requestResponseHandlerThreadBackground.join(NonBlockingRouter.SHUTDOWN_WAIT_MS);
    }

    try {
      future.get();
      Assert.fail("The operation should have failed");
    } catch (ExecutionException e) {
      Assert.assertEquals(RouterErrorCode.RouterClosed, ((RouterException) e.getCause()).getErrorCode());
    }

    assertClosed();

    // Ensure that both operations failed and with the right exceptions.
    Assert.assertEquals("No ChunkFiller Thread should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("No RequestResponseHandler should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Test that if a composite blob put fails, the successfully put data chunks are deleted.
   */
  @Test
  public void testUnsuccessfulPutDataChunkDelete() throws Exception {
    // Ensure there are 4 chunks.
    maxPutChunkSize = PUT_CONTENT_SIZE / 4;
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
    // Since this test wants to ensure that successfully put data chunks are deleted when the overall put operation
    // fails, it uses a notification system to track the deletions.
    final CountDownLatch deletesDoneLatch = new CountDownLatch(2);
    final Map<String, String> blobsThatAreDeleted = new HashMap<>();
    LoggingNotificationSystem deleteTrackingNotificationSystem = new LoggingNotificationSystem() {
      @Override
      public void onBlobDeleted(String blobId, String serviceId, Account account, Container container) {
        blobsThatAreDeleted.put(blobId, serviceId);
        deletesDoneLatch.countDown();
      }
    };
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), deleteTrackingNotificationSystem, mockClusterMap, kms,
        cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS);

    setOperationParams();

    List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
    List<ServerErrorCode> serverErrorList = new ArrayList<>();
    // There are 4 chunks for this blob.
    // All put operations make one request to each local server as there are 3 servers overall in the local DC.
    // Set the state of the mock servers so that they return success for the first 2 requests in order to succeed
    // the first two chunks.
    serverErrorList.add(ServerErrorCode.No_Error);
    serverErrorList.add(ServerErrorCode.No_Error);
    // fail requests for third and fourth data chunks including the slipped put attempts:
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    // all subsequent requests (no more puts, but there will be deletes) will succeed.
    for (DataNodeId dataNodeId : dataNodeIds) {
      MockServer server = mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort());
      server.setServerErrors(serverErrorList);
    }

    // Submit the put operation and wait for it to fail.
    try {
      router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    } catch (ExecutionException e) {
      Assert.assertEquals(RouterErrorCode.AmbryUnavailable, ((RouterException) e.getCause()).getErrorCode());
    }

    // Now, wait until the deletes of the successfully put blobs are complete.
    Assert.assertTrue("Deletes should not take longer than " + AWAIT_TIMEOUT_MS,
        deletesDoneLatch.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    for (Map.Entry<String, String> blobIdAndServiceId : blobsThatAreDeleted.entrySet()) {
      Assert.assertEquals("Unexpected service ID for deleted blob",
          BackgroundDeleteRequest.SERVICE_ID_PREFIX + putBlobProperties.getServiceId(), blobIdAndServiceId.getValue());
    }

    router.close();
    assertClosed();
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Test that if a composite blob is deleted, the data chunks are eventually deleted. Also check the service IDs used
   * for delete operations.
   */
  @Test
  public void testCompositeBlobDataChunksDelete() throws Exception {
    // Ensure there are 4 chunks.
    maxPutChunkSize = PUT_CONTENT_SIZE / 4;
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
    // metadata blob + data chunks.
    final AtomicReference<CountDownLatch> deletesDoneLatch = new AtomicReference<>();
    final Map<String, String> blobsThatAreDeleted = new HashMap<>();
    LoggingNotificationSystem deleteTrackingNotificationSystem = new LoggingNotificationSystem() {
      @Override
      public void onBlobDeleted(String blobId, String serviceId, Account account, Container container) {
        blobsThatAreDeleted.put(blobId, serviceId);
        deletesDoneLatch.get().countDown();
      }
    };
    NonBlockingRouterMetrics localMetrics = new NonBlockingRouterMetrics(mockClusterMap);
    router = new NonBlockingRouter(routerConfig, localMetrics,
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), deleteTrackingNotificationSystem, mockClusterMap, kms,
        cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS);
    setOperationParams();
    String blobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    String deleteServiceId = "delete-service";
    Set<String> blobsToBeDeleted = getBlobsInServers(mockServerLayout);
    int getRequestCount = mockServerLayout.getCount(RequestOrResponseType.GetRequest);
    // The second iteration is to test the case where the blob was already deleted.
    // The third iteration is to test the case where the blob has expired.
    for (int i = 0; i < 3; i++) {
      if (i == 2) {
        // Create a clean cluster and put another blob that immediate expires.
        setOperationParams();
        putBlobProperties = new BlobProperties(-1, "serviceId", "memberId", "contentType", false, 0,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false);
        blobId =
            router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
        Set<String> allBlobsInServer = getBlobsInServers(mockServerLayout);
        allBlobsInServer.removeAll(blobsToBeDeleted);
        blobsToBeDeleted = allBlobsInServer;
      }
      blobsThatAreDeleted.clear();
      deletesDoneLatch.set(new CountDownLatch(5));
      router.deleteBlob(blobId, deleteServiceId, null).get();
      Assert.assertTrue("Deletes should not take longer than " + AWAIT_TIMEOUT_MS,
          deletesDoneLatch.get().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      Assert.assertTrue("All blobs in server are deleted", blobsThatAreDeleted.keySet().containsAll(blobsToBeDeleted));
      Assert.assertTrue("Only blobs in server are deleted", blobsToBeDeleted.containsAll(blobsThatAreDeleted.keySet()));

      for (Map.Entry<String, String> blobIdAndServiceId : blobsThatAreDeleted.entrySet()) {
        String expectedServiceId = blobIdAndServiceId.getKey().equals(blobId) ? deleteServiceId
            : BackgroundDeleteRequest.SERVICE_ID_PREFIX + deleteServiceId;
        Assert.assertEquals("Unexpected service ID for deleted blob", expectedServiceId, blobIdAndServiceId.getValue());
      }
      // For 1 chunk deletion attempt, 1 background operation for Get is initiated which results in 2 Get Requests at
      // the servers.
      getRequestCount += 2;
      Assert.assertEquals("Only one attempt of chunk deletion should have been done", getRequestCount,
          mockServerLayout.getCount(RequestOrResponseType.GetRequest));
    }

    deletesDoneLatch.set(new CountDownLatch(5));
    router.deleteBlob(blobId, null, null).get();
    Assert.assertTrue("Deletes should not take longer than " + AWAIT_TIMEOUT_MS,
        deletesDoneLatch.get().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    Assert.assertEquals("Get should NOT have been skipped", 0, localMetrics.skippedGetBlobCount.getCount());

    router.close();
    assertClosed();
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Return the blob ids of all the blobs in the servers in the cluster.
   * @param mockServerLayout the {@link MockServerLayout} representing the cluster.
   * @return a Set of blob id strings of the blobs in the servers in the cluster.
   */
  private Set<String> getBlobsInServers(MockServerLayout mockServerLayout) {
    Set<String> blobsInServers = new HashSet<>();
    for (MockServer mockServer : mockServerLayout.getMockServers()) {
      blobsInServers.addAll(mockServer.getBlobs().keySet());
    }
    return blobsInServers;
  }

  /**
   * Test to ensure that for simple blob deletions, no additional background delete operations
   * are initiated.
   */
  @Test
  public void testSimpleBlobDelete() throws Exception {
    // Ensure there are 4 chunks.
    maxPutChunkSize = PUT_CONTENT_SIZE;
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
    String deleteServiceId = "delete-service";
    // metadata blob + data chunks.
    final AtomicInteger deletesInitiated = new AtomicInteger();
    final AtomicReference<String> receivedDeleteServiceId = new AtomicReference<>();
    LoggingNotificationSystem deleteTrackingNotificationSystem = new LoggingNotificationSystem() {
      @Override
      public void onBlobDeleted(String blobId, String serviceId, Account account, Container container) {
        deletesInitiated.incrementAndGet();
        receivedDeleteServiceId.set(serviceId);
      }
    };
    NonBlockingRouterMetrics localMetrics = new NonBlockingRouterMetrics(mockClusterMap);
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), localMetrics,
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), deleteTrackingNotificationSystem, mockClusterMap, kms,
        cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS);
    setOperationParams();
    String blobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    router.deleteBlob(blobId, deleteServiceId, null).get();
    long waitStart = SystemTime.getInstance().milliseconds();
    while (router.getBackgroundOperationsCount() != 0
        && SystemTime.getInstance().milliseconds() < waitStart + AWAIT_TIMEOUT_MS) {
      Thread.sleep(1000);
    }
    Assert.assertEquals("All background operations should be complete ", 0, router.getBackgroundOperationsCount());
    Assert.assertEquals("Only the original blob deletion should have been initiated", 1, deletesInitiated.get());
    Assert.assertEquals("The delete service ID should match the expected value", deleteServiceId,
        receivedDeleteServiceId.get());
    Assert.assertEquals("Get should have been skipped", 1, localMetrics.skippedGetBlobCount.getCount());
    router.close();
    assertClosed();
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Tests basic TTL update for simple (one chunk) blobs
   * @throws Exception
   */
  @Test
  public void testSimpleBlobTtlUpdate() throws Exception {
    doTtlUpdateTest(1);
  }

  /**
   * Tests basic TTL update for composite (multiple chunk) blobs
   * @throws Exception
   */
  @Test
  public void testCompositeBlobTtlUpdate() throws Exception {
    doTtlUpdateTest(4);
  }

  /**
   * Test for most error cases involving TTL updates
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdateErrors() throws Exception {
    String updateServiceId = "update-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    setRouter(getNonBlockingRouterProperties("DC1"), layout, new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    Map<ServerErrorCode, RouterErrorCode> testsAndExpected = new HashMap<>();
    testsAndExpected.put(ServerErrorCode.Blob_Not_Found, RouterErrorCode.BlobDoesNotExist);
    testsAndExpected.put(ServerErrorCode.Blob_Deleted, RouterErrorCode.BlobDeleted);
    testsAndExpected.put(ServerErrorCode.Blob_Expired, RouterErrorCode.BlobExpired);
    testsAndExpected.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.AmbryUnavailable);
    testsAndExpected.put(ServerErrorCode.Replica_Unavailable, RouterErrorCode.AmbryUnavailable);
    testsAndExpected.put(ServerErrorCode.Unknown_Error, RouterErrorCode.UnexpectedInternalError);
    for (Map.Entry<ServerErrorCode, RouterErrorCode> testAndExpected : testsAndExpected.entrySet()) {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(testAndExpected.getKey()));
      TestCallback<Void> testCallback = new TestCallback<>();
      Future<Void> future = router.updateBlobTtl(blobId, updateServiceId, Utils.Infinite_Time, testCallback);
      assertFailureAndCheckErrorCode(future, testCallback, testAndExpected.getValue());
    }
    layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    // bad blob id
    TestCallback<Void> testCallback = new TestCallback<>();
    Future<Void> future = router.updateBlobTtl("bad-blob-id", updateServiceId, Utils.Infinite_Time, testCallback);
    assertFailureAndCheckErrorCode(future, testCallback, RouterErrorCode.InvalidBlobId);
    router.close();
  }

  /**
   * Test that a bad user defined callback will not crash the router or the manager.
   * @throws Exception
   */
  @Test
  public void testBadCallbackForUpdateTtl() throws Exception {
    MockServerLayout serverLayout = new MockServerLayout(mockClusterMap);
    setRouter(getNonBlockingRouterProperties("DC1"), serverLayout, new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    String blobIdCheck =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    testWithErrorCodes(Collections.singletonMap(ServerErrorCode.No_Error, 9), serverLayout, null, expectedError -> {
      final CountDownLatch callbackCalled = new CountDownLatch(1);
      router.updateBlobTtl(blobId, null, Utils.Infinite_Time, (result, exception) -> {
        callbackCalled.countDown();
        throw new RuntimeException("Throwing an exception in the user callback");
      }).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Callback not called.", callbackCalled.await(10, TimeUnit.MILLISECONDS));
      assertEquals("All operations should be finished.", 0, router.getOperationsCount());
      assertTrue("Router should not be closed", router.isOpen());
      assertTtl(router, Collections.singleton(blobId), Utils.Infinite_Time);

      //Test that TtlUpdateManager is still functional
      router.updateBlobTtl(blobIdCheck, null, Utils.Infinite_Time).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTtl(router, Collections.singleton(blobIdCheck), Utils.Infinite_Time);
    });
    router.close();
  }

  /**
   * Test that multiple scaling units can be instantiated, exercised and closed.
   */
  @Test
  public void testMultipleScalingUnit() throws Exception {
    final int SCALING_UNITS = 3;
    Properties props = getNonBlockingRouterProperties("DC1");
    props.setProperty("router.scaling.unit.count", Integer.toString(SCALING_UNITS));
    setRouter(props, new MockServerLayout(mockClusterMap), new LoggingNotificationSystem());
    assertExpectedThreadCounts(SCALING_UNITS + 1, SCALING_UNITS);

    // Submit a few jobs so that all the scaling units get exercised.
    for (int i = 0; i < SCALING_UNITS * 10; i++) {
      setOperationParams();
      router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    }
    router.close();
    assertExpectedThreadCounts(0, 0);

    //submission after closing should return a future that is already done.
    setOperationParams();
    assertClosed();
  }

  /**
   * Response handling related tests for all operation managers.
   */
  @Test
  public void testResponseHandling() throws Exception {
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    setOperationParams();
    final List<ReplicaId> failedReplicaIds = new ArrayList<>();
    final AtomicInteger successfulResponseCount = new AtomicInteger(0);
    final AtomicBoolean invalidResponse = new AtomicBoolean(false);
    ResponseHandler mockResponseHandler = new ResponseHandler(mockClusterMap) {
      @Override
      public void onEvent(ReplicaId replicaId, Object e) {
        if (e instanceof ServerErrorCode) {
          if (e == ServerErrorCode.No_Error) {
            successfulResponseCount.incrementAndGet();
          } else {
            invalidResponse.set(true);
          }
        } else {
          failedReplicaIds.add(replicaId);
        }
      }
    };

    // Instantiate a router just to put a blob successfully.
    MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
    setRouter(props, mockServerLayout, new LoggingNotificationSystem());
    setOperationParams();

    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    String blobIdStr =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    BlobId blobId = RouterUtils.getBlobIdFromString(blobIdStr, mockClusterMap);
    router.close();
    for (MockServer mockServer : mockServerLayout.getMockServers()) {
      mockServer.setServerErrorForAllRequests(ServerErrorCode.No_Error);
    }

    NetworkClient networkClient =
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime).getNetworkClient();
    cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
    KeyManagementService localKMS = new MockKeyManagementService(new KMSConfig(verifiableProperties), singleKeyForKMS);
    putManager = new PutManager(mockClusterMap, mockResponseHandler, new LoggingNotificationSystem(),
        new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new RouterCallback(networkClient, new ArrayList<>()), "0", localKMS, cryptoService, cryptoJobHandler,
        accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS);
    OperationHelper opHelper = new OperationHelper(OperationType.PUT);
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, null, successfulResponseCount,
        invalidResponse, -1);
    // Test that if a failed response comes before the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, null, successfulResponseCount,
        invalidResponse, 0);
    // Test that if a failed response comes after the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, null, successfulResponseCount,
        invalidResponse, PUT_REQUEST_PARALLELISM - 1);
    testNoResponseNoNotification(opHelper, failedReplicaIds, null, successfulResponseCount, invalidResponse);
    testResponseDeserializationError(opHelper, networkClient, null);

    opHelper = new OperationHelper(OperationType.GET);
    getManager = new GetManager(mockClusterMap, mockResponseHandler, new RouterConfig(verifiableProperties),
        new NonBlockingRouterMetrics(mockClusterMap),
        new RouterCallback(networkClient, new ArrayList<BackgroundDeleteRequest>()), localKMS, cryptoService,
        cryptoJobHandler, mockTime);
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, -1);
    // Test that if a failed response comes before the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, 0);
    // Test that if a failed response comes after the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, GET_REQUEST_PARALLELISM - 1);
    testNoResponseNoNotification(opHelper, failedReplicaIds, blobId, successfulResponseCount, invalidResponse);
    testResponseDeserializationError(opHelper, networkClient, blobId);

    opHelper = new OperationHelper(OperationType.DELETE);
    deleteManager =
        new DeleteManager(mockClusterMap, mockResponseHandler, accountService, new LoggingNotificationSystem(),
            new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
            new RouterCallback(null, new ArrayList<BackgroundDeleteRequest>()), mockTime);
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, -1);
    // Test that if a failed response comes before the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, 0);
    // Test that if a failed response comes after the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, DELETE_REQUEST_PARALLELISM - 1);
    testNoResponseNoNotification(opHelper, failedReplicaIds, blobId, successfulResponseCount, invalidResponse);
    testResponseDeserializationError(opHelper, networkClient, blobId);
    putManager.close();
    getManager.close();
    deleteManager.close();
  }

  /**
   * Test that failure detector is correctly notified for all responses regardless of the order in which successful
   * and failed responses arrive.
   * @param opHelper the {@link OperationHelper}
   * @param networkClient the {@link NetworkClient}
   * @param failedReplicaIds the list that will contain all the replicas for which failure was notified.
   * @param blobId the id of the blob to get/delete. For puts, this will be null.
   * @param successfulResponseCount the AtomicInteger that will contain the count of replicas for which success was
   *                                notified.
   * @param invalidResponse the AtomicBoolean that will contain whether an unexpected failure was notified.
   * @param indexToFail if greater than 0, the index representing which response for which failure is to be simulated.
   *                    For example, if index is 0, then the first response will be failed.
   *                    If the index is -1, no responses will be failed, and successful responses will be returned to
   *                    the operation managers.
   */
  private void testFailureDetectorNotification(OperationHelper opHelper, NetworkClient networkClient,
      List<ReplicaId> failedReplicaIds, BlobId blobId, AtomicInteger successfulResponseCount,
      AtomicBoolean invalidResponse, int indexToFail) throws Exception {
    failedReplicaIds.clear();
    successfulResponseCount.set(0);
    invalidResponse.set(false);
    mockSelectorState.set(MockSelectorState.Good);
    FutureResult futureResult = opHelper.submitOperation(blobId);
    int requestParallelism = opHelper.requestParallelism;
    List<RequestInfo> allRequests = new ArrayList<>();
    long loopStartTimeMs = SystemTime.getInstance().milliseconds();
    while (allRequests.size() < requestParallelism) {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for requests.");
      }
      opHelper.pollOpManager(allRequests);
    }
    ReplicaId replicaIdToFail =
        indexToFail == -1 ? null : ((RouterRequestInfo) allRequests.get(indexToFail)).getReplicaId();
    for (RequestInfo requestInfo : allRequests) {
      ResponseInfo responseInfo;
      if (replicaIdToFail != null && replicaIdToFail.equals(((RouterRequestInfo) requestInfo).getReplicaId())) {
        responseInfo = new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null);
      } else {
        List<RequestInfo> requestInfoListToSend = new ArrayList<>();
        requestInfoListToSend.add(requestInfo);
        List<ResponseInfo> responseInfoList;
        loopStartTimeMs = SystemTime.getInstance().milliseconds();
        do {
          if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
            Assert.fail("Waited too long for the response.");
          }
          responseInfoList = networkClient.sendAndPoll(requestInfoListToSend, 10);
          requestInfoListToSend.clear();
        } while (responseInfoList.size() == 0);
        responseInfo = responseInfoList.get(0);
      }
      opHelper.handleResponse(responseInfo);
    }
    // Poll once again so that the operation gets a chance to complete.
    allRequests.clear();
    if (testEncryption) {
      opHelper.awaitOpCompletionOrTimeOut(futureResult);
    } else {
      opHelper.pollOpManager(allRequests);
    }
    futureResult.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    if (indexToFail == -1) {
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          opHelper.requestParallelism, successfulResponseCount.get());
      Assert.assertEquals("Failure detector should not have been notified", 0, failedReplicaIds.size());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    } else {
      Assert.assertEquals("Failure detector should have been notified", 1, failedReplicaIds.size());
      Assert.assertEquals("Failed notification should have arrived for the failed replica", replicaIdToFail,
          failedReplicaIds.get(0));
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          opHelper.requestParallelism - 1, successfulResponseCount.get());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    }
  }

  /**
   * Test that failure detector is not notified when the router times out requests.
   * @param opHelper the {@link OperationHelper}
   * @param failedReplicaIds the list that will contain all the replicas for which failure was notified.
   * @param blobId the id of the blob to get/delete. For puts, this will be null.
   * @param successfulResponseCount the AtomicInteger that will contain the count of replicas for which success was
   *                                notified.
   * @param invalidResponse the AtomicBoolean that will contain whether an unexpected failure was notified.
   */
  private void testNoResponseNoNotification(OperationHelper opHelper, List<ReplicaId> failedReplicaIds, BlobId blobId,
      AtomicInteger successfulResponseCount, AtomicBoolean invalidResponse) throws Exception {
    failedReplicaIds.clear();
    successfulResponseCount.set(0);
    invalidResponse.set(false);
    FutureResult futureResult = opHelper.submitOperation(blobId);
    List<RequestInfo> allRequests = new ArrayList<>();
    long loopStartTimeMs = SystemTime.getInstance().milliseconds();
    while (!futureResult.isDone()) {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for requests.");
      }
      opHelper.pollOpManager(allRequests);
      mockTime.sleep(REQUEST_TIMEOUT_MS + 1);
    }
    Assert.assertEquals("Successful notification should not have arrived for replicas that were up", 0,
        successfulResponseCount.get());
    Assert.assertEquals("Failure detector should not have been notified", 0, failedReplicaIds.size());
    Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
  }

  /**
   * Test that operations succeed even in the presence of responses that are corrupt and fail to deserialize.
   * @param opHelper the {@link OperationHelper}
   * @param networkClient the {@link NetworkClient}
   * @param blobId the id of the blob to get/delete. For puts, this will be null.
   * @throws Exception
   */
  private void testResponseDeserializationError(OperationHelper opHelper, NetworkClient networkClient, BlobId blobId)
      throws Exception {
    mockSelectorState.set(MockSelectorState.Good);
    FutureResult futureResult = opHelper.submitOperation(blobId);
    int requestParallelism = opHelper.requestParallelism;
    List<RequestInfo> allRequests = new ArrayList<>();
    long loopStartTimeMs = SystemTime.getInstance().milliseconds();
    while (allRequests.size() < requestParallelism) {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for requests.");
      }
      opHelper.pollOpManager(allRequests);
    }
    List<ResponseInfo> responseInfoList = new ArrayList<>();
    loopStartTimeMs = SystemTime.getInstance().milliseconds();
    do {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for the response.");
      }
      responseInfoList.addAll(networkClient.sendAndPoll(allRequests, 10));
      allRequests.clear();
    } while (responseInfoList.size() < requestParallelism);
    // corrupt the first response.
    ByteBuffer response = responseInfoList.get(0).getResponse();
    byte b = response.get(response.limit() - 1);
    response.put(response.limit() - 1, (byte) ~b);
    for (ResponseInfo responseInfo : responseInfoList) {
      opHelper.handleResponse(responseInfo);
    }
    allRequests.clear();
    if (testEncryption) {
      opHelper.awaitOpCompletionOrTimeOut(futureResult);
    } else {
      opHelper.pollOpManager(allRequests);
    }
    try {
      futureResult.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Assert.fail("Operation should have succeeded with one corrupt response");
    }
  }

  /**
   * Assert that the number of ChunkFiller and RequestResponseHandler threads running are as expected.
   * @param expectedRequestResponseHandlerCount the expected number of ChunkFiller and RequestResponseHandler threads.
   * @param expectedChunkFillerCount the expected number of ChunkFiller threads.
   */
  private void assertExpectedThreadCounts(int expectedRequestResponseHandlerCount, int expectedChunkFillerCount) {
    Assert.assertEquals("Number of RequestResponseHandler threads running should be as expected",
        expectedRequestResponseHandlerCount, TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("Number of chunkFiller threads running should be as expected", expectedChunkFillerCount,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    if (expectedRequestResponseHandlerCount == 0) {
      Assert.assertFalse("Router should be closed if there are no worker threads running", router.isOpen());
      Assert.assertEquals("All operations should have completed if the router is closed", 0,
          router.getOperationsCount());
    }
  }

  /**
   * Assert that submission after closing the router returns a future that is already done and an appropriate
   * exception.
   */
  private void assertClosed() {
    Future<String> future =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build());
    Assert.assertTrue(future.isDone());
    RouterException e = (RouterException) ((FutureResult<String>) future).error();
    Assert.assertEquals(e.getErrorCode(), RouterErrorCode.RouterClosed);
  }

  /**
   * Does the TTL update test by putting a blob, checking its TTL, updating TTL and then rechecking the TTL again.
   * @param numChunks the number of chunks required when the blob is put. Has to divide {@link #PUT_CONTENT_SIZE}
   *                  perfectly for test to work.
   * @throws Exception
   */
  private void doTtlUpdateTest(int numChunks) throws Exception {
    Assert.assertEquals("This test works only if the number of chunks is a perfect divisor of PUT_CONTENT_SIZE", 0,
        PUT_CONTENT_SIZE % numChunks);
    maxPutChunkSize = PUT_CONTENT_SIZE / numChunks;
    String updateServiceId = "update-service";
    TtlUpdateNotificationSystem notificationSystem = new TtlUpdateNotificationSystem();
    setRouter(getNonBlockingRouterProperties("DC1"), new MockServerLayout(mockClusterMap), notificationSystem);
    setOperationParams();
    Assert.assertFalse("The original ttl should not be infinite for this test to work",
        putBlobProperties.getTimeToLiveInSeconds() == Utils.Infinite_Time);
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertTtl(router, Collections.singleton(blobId), TTL_SECS);
    router.updateBlobTtl(blobId, updateServiceId, Utils.Infinite_Time).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    // if more than one chunk is created, also account for metadata blob
    notificationSystem.checkNotifications(numChunks == 1 ? 1 : numChunks + 1, updateServiceId, Utils.Infinite_Time);
    assertTtl(router, Collections.singleton(blobId), Utils.Infinite_Time);
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
    if (numChunks == 1) {
      Assert.assertEquals("Get should have been skipped", 1, routerMetrics.skippedGetBlobCount.getCount());
    } else {
      Assert.assertEquals("Get should NOT have been skipped", 0, routerMetrics.skippedGetBlobCount.getCount());
    }
    router.close();
    // check that ttl update won't work after router close
    Future<Void> future = router.updateBlobTtl(blobId, updateServiceId, Utils.Infinite_Time);
    Assert.assertTrue(future.isDone());
    RouterException e = (RouterException) ((FutureResult<Void>) future).error();
    Assert.assertEquals(e.getErrorCode(), RouterErrorCode.RouterClosed);
  }

  /**
   * Enum for the three operation types.
   */
  private enum OperationType {
    PUT, GET, DELETE,
  }

  /**
   * A helper class to abstract away the details about specific operation manager.
   */
  private class OperationHelper {
    final OperationType opType;
    int requestParallelism = 0;

    /**
     * Construct an OperationHelper object with the associated type.
     * @param opType the type of operation.
     */
    OperationHelper(OperationType opType) {
      this.opType = opType;
      switch (opType) {
        case PUT:
          requestParallelism = PUT_REQUEST_PARALLELISM;
          break;
        case GET:
          requestParallelism = GET_REQUEST_PARALLELISM;
          break;
        case DELETE:
          requestParallelism = DELETE_REQUEST_PARALLELISM;
          break;
      }
    }

    /**
     * Submit a put, get or delete operation based on the associated {@link OperationType} of this object.
     * @param blobId the blobId to get or delete. For puts, this is ignored.
     * @return the {@link FutureResult} associated with the submitted operation.
     * @throws RouterException if the blobIdStr is invalid.
     */
    FutureResult submitOperation(BlobId blobId) throws RouterException {
      FutureResult futureResult = null;
      switch (opType) {
        case PUT:
          futureResult = new FutureResult<String>();
          ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
          putManager.submitPutBlobOperation(putBlobProperties, putUserMetadata, putChannel, futureResult, null);
          break;
        case GET:
          final FutureResult getFutureResult = new FutureResult<GetBlobResultInternal>();
          getManager.submitGetBlobOperation(blobId.getID(), new GetBlobOptionsInternal(
              new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build(), false,
              routerMetrics.ageAtGet), new Callback<GetBlobResultInternal>() {
            @Override
            public void onCompletion(GetBlobResultInternal result, Exception exception) {
              getFutureResult.done(result, exception);
            }
          });
          futureResult = getFutureResult;
          break;
        case DELETE:
          futureResult = new FutureResult<Void>();
          deleteManager.submitDeleteBlobOperation(blobId.getID(), null, futureResult, null);
          break;
      }
      NonBlockingRouter.currentOperationsCount.incrementAndGet();
      return futureResult;
    }

    /**
     * Poll the associated operation manager.
     * @param requestInfos the list of {@link RequestInfo} to pass in the poll call.
     */
    void pollOpManager(List<RequestInfo> requestInfos) {
      switch (opType) {
        case PUT:
          putManager.poll(requestInfos);
          break;
        case GET:
          getManager.poll(requestInfos);
          break;
        case DELETE:
          deleteManager.poll(requestInfos);
          break;
      }
    }

    /**
     * Polls all managers at regular intervals until the operation is complete or timeout is reached
     * @param futureResult {@link FutureResult} that needs to be tested for completion
     * @throws InterruptedException
     */
    private void awaitOpCompletionOrTimeOut(FutureResult futureResult) throws InterruptedException {
      int timer = 0;
      List<RequestInfo> allRequests = new ArrayList<>();
      while (timer < AWAIT_TIMEOUT_MS / 2 && !futureResult.completed()) {
        pollOpManager(allRequests);
        Thread.sleep(50);
        timer += 50;
        allRequests.clear();
      }
    }

    /**
     * Hand over a responseInfo to the operation manager.
     * @param responseInfo the {@link ResponseInfo} to hand over.
     */
    void handleResponse(ResponseInfo responseInfo) {
      switch (opType) {
        case PUT:
          putManager.handleResponse(responseInfo);
          break;
        case GET:
          getManager.handleResponse(responseInfo);
          break;
        case DELETE:
          deleteManager.handleResponse(responseInfo);
          break;
      }
    }
  }
}
