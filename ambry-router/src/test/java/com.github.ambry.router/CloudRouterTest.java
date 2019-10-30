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
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.LocalNetworkClientFactory;
import com.github.ambry.network.LocalRequestResponseChannel;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.router.RouterTestHelpers.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Class to test the {@link NonBlockingRouter} returned by {@link CloudRouterFactory}.
 */
@RunWith(Parameterized.class)
public class CloudRouterTest {
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final int REQUEST_TIMEOUT_MS = 1000;
  private static final int SUCCESS_TARGET = 1;
  private static final int REQUEST_PARALLELISM = 1;
  private static final int PUT_CONTENT_SIZE = 1000;
  private static final int USER_METADATA_SIZE = 10;
  private int maxPutChunkSize = PUT_CONTENT_SIZE;
  private final Random random = new Random();
  private NonBlockingRouter router;
  private NonBlockingRouterMetrics routerMetrics;
  private final MockTime mockTime;
  private final KeyManagementService kms;
  private final String singleKeyForKMS;
  private final CryptoService cryptoService;
  private final MockClusterMap mockClusterMap;
  private final boolean testEncryption;
  private final int metadataContentVersion;
  private final InMemAccountService accountService;
  private CryptoJobHandler cryptoJobHandler;
  private static final Logger logger = LoggerFactory.getLogger(CloudRouterTest.class);
  // Request params;
  BlobProperties putBlobProperties;
  byte[] putUserMetadata;
  byte[] putContent;
  ReadableStreamChannel putChannel;

  /**
   * Running for both regular and encrypted blobs, and versions 2 and 3 of MetadataContent
   * @return an array with all four different choices
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false, MessageFormatRecord.Metadata_Content_Version_V2},
        {false, MessageFormatRecord.Metadata_Content_Version_V3},
        {true, MessageFormatRecord.Metadata_Content_Version_V2},
        {true, MessageFormatRecord.Metadata_Content_Version_V3}});
  }

  /**
   * Initialize parameters common to all tests.
   * @param testEncryption {@code true} to test with encryption enabled. {@code false} otherwise
   * @throws Exception
   */
  public CloudRouterTest(boolean testEncryption, int metadataContentVersion) throws Exception {
    this.testEncryption = testEncryption;
    this.metadataContentVersion = metadataContentVersion;
    mockTime = new MockTime();
    // Single node cloud clustermap
    mockClusterMap = new MockClusterMap(false, 1, 1, 1, false);
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
    // Any single test failure should not impact others.
    if (router != null && router.isOpen()) {
      router.close();
    }
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
    properties.setProperty("router.put.request.parallelism", Integer.toString(REQUEST_PARALLELISM));
    properties.setProperty("router.put.success.target", Integer.toString(SUCCESS_TARGET));
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxPutChunkSize));
    properties.setProperty("router.get.request.parallelism", Integer.toString(REQUEST_PARALLELISM));
    properties.setProperty("router.get.success.target", Integer.toString(SUCCESS_TARGET));
    properties.setProperty("router.delete.request.parallelism", Integer.toString(REQUEST_PARALLELISM));
    properties.setProperty("router.delete.success.target", Integer.toString(SUCCESS_TARGET));
    properties.setProperty("router.ttl.update.request.parallelism", Integer.toString(REQUEST_PARALLELISM));
    properties.setProperty("router.ttl.update.success.target", Integer.toString(SUCCESS_TARGET));
    properties.setProperty("router.connection.checkout.timeout.ms", Integer.toString(CHECKOUT_TIMEOUT_MS));
    properties.setProperty("router.request.timeout.ms", Integer.toString(REQUEST_TIMEOUT_MS));
    properties.setProperty("router.connections.local.dc.warm.up.percentage", Integer.toString(67));
    properties.setProperty("router.connections.remote.dc.warm.up.percentage", Integer.toString(34));
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "dc1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("clustermap.port", "1666");
    properties.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    properties.setProperty("kms.default.container.key", TestUtils.getRandomKey(128));
    properties.setProperty("router.metadata.content.version", String.valueOf(metadataContentVersion));
    properties.setProperty(CloudConfig.CLOUD_DESTINATION_FACTORY_CLASS,
        LatchBasedInMemoryCloudDestinationFactory.class.getName());
    properties.setProperty(CloudConfig.VCR_MIN_TTL_DAYS, "0");
    return properties;
  }

  /**
   * Construct {@link Properties} and {@link MockServerLayout} and initialize and set the
   * router with them.
   */
  private void setRouter() throws Exception {
    setRouter(getNonBlockingRouterProperties("DC1"), new LoggingNotificationSystem());
  }

  /**
   * Initialize and set the router with the given {@link Properties} and {@link MockServerLayout}
   * @param props the {@link Properties}
   * @param notificationSystem the {@link NotificationSystem} to use.
   */
  private void setRouter(Properties props, NotificationSystem notificationSystem) throws Exception {
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    RequestHandlerPool requestHandlerPool =
        CloudRouterFactory.getRequestHandlerPool(verifiableProperties, mockClusterMap, notificationSystem);
    NetworkClientFactory networkClientFactory =
        new LocalNetworkClientFactory((LocalRequestResponseChannel) requestHandlerPool.getChannel(),
            new NetworkConfig(verifiableProperties), new NetworkMetrics(routerMetrics.getMetricRegistry()),
            SystemTime.getInstance());
    router =
        new NonBlockingRouter(routerConfig, routerMetrics, networkClientFactory, notificationSystem, mockClusterMap,
            kms, cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS);
    router.setRequestHandlerPool(requestHandlerPool);
  }

  /**
   * Setup test suite to perform a {@link Router#putBlob} call using the constant {@link #PUT_CONTENT_SIZE}
   */
  private void setOperationParams() {
    setOperationParams(PUT_CONTENT_SIZE, TTL_SECS);
  }

  /**
   * Setup test suite to perform a {@link Router#putBlob} call.
   * @param putContentSize the size of the content to put
   * @param ttlSecs the TTL in seconds for the blob.
   */
  private void setOperationParams(int putContentSize, long ttlSecs) {
    putBlobProperties = new BlobProperties(-1, "serviceId", "memberId", "contentType", false, ttlSecs,
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), testEncryption, null);
    putUserMetadata = new byte[USER_METADATA_SIZE];
    random.nextBytes(putUserMetadata);
    putContent = new byte[putContentSize];
    random.nextBytes(putContent);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
  }

  /**
   * Test the {@link CloudRouterFactory}
   */
  @Test
  public void testCloudRouterFactory() throws Exception {
    Properties props = getNonBlockingRouterProperties("NotInClusterMap");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    try {
      router = (NonBlockingRouter) new CloudRouterFactory(verifiableProperties, mockClusterMap,
          new LoggingNotificationSystem(), null, accountService).getRouter();
      Assert.fail("NonBlockingRouterFactory instantiation should have failed because the router datacenter is not in "
          + "the cluster map");
    } catch (IllegalStateException e) {
    }
    props = getNonBlockingRouterProperties("DC1");
    verifiableProperties = new VerifiableProperties((props));
    router = (NonBlockingRouter) new CloudRouterFactory(verifiableProperties, mockClusterMap,
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

    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    List<String> blobIds = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      setOperationParams();
      String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT).get();
      logger.debug("Successfully put blob {}", blobId);
      blobIds.add(blobId);
    }
    setOperationParams();
    String stitchedBlobId = router.stitchBlob(putBlobProperties, putUserMetadata, blobIds.stream()
        .map(blobId -> new ChunkInfo(blobId, PUT_CONTENT_SIZE, Utils.Infinite_Time))
        .collect(Collectors.toList())).get();
    blobIds.add(stitchedBlobId);

    for (String blobId : blobIds) {
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
    }

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
    setRouter(props, new LoggingNotificationSystem());
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
   * Test to ensure that for simple blob deletions, no additional background delete operations
   * are initiated.
   */
  @Test
  public void testSimpleBlobDelete() throws Exception {
    // Ensure there are 4 chunks.
    maxPutChunkSize = PUT_CONTENT_SIZE;
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
    Properties props = getNonBlockingRouterProperties("DC1");
    setRouter(props, deleteTrackingNotificationSystem);
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
    Assert.assertEquals("Get should have been skipped", 1, routerMetrics.skippedGetBlobCount.getCount());
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
   * Test that stitched blobs are usable by the other router methods.
   * @throws Exception
   */
  @Test
  public void testStitchGetUpdateDelete() throws Exception {
    AtomicReference<CountDownLatch> deletesDoneLatch = new AtomicReference<>();
    Set<String> deletedBlobs = ConcurrentHashMap.newKeySet();
    LoggingNotificationSystem deleteTrackingNotificationSystem = new LoggingNotificationSystem() {
      @Override
      public void onBlobDeleted(String blobId, String serviceId, Account account, Container container) {
        deletedBlobs.add(blobId);
        deletesDoneLatch.get().countDown();
      }
    };
    setRouter(getNonBlockingRouterProperties("DC1"), deleteTrackingNotificationSystem);
    for (int intermediateChunkSize : new int[]{maxPutChunkSize, maxPutChunkSize / 2}) {
      for (LongStream chunkSizeStream : new LongStream[]{
          RouterTestHelpers.buildValidChunkSizeStream(3 * intermediateChunkSize, intermediateChunkSize),
          RouterTestHelpers.buildValidChunkSizeStream(
              3 * intermediateChunkSize + random.nextInt(intermediateChunkSize - 1) + 1, intermediateChunkSize)}) {
        // Upload data chunks
        ByteArrayOutputStream stitchedContentStream = new ByteArrayOutputStream();
        List<ChunkInfo> chunksToStitch = new ArrayList<>();
        PrimitiveIterator.OfLong chunkSizeIter = chunkSizeStream.iterator();
        while (chunkSizeIter.hasNext()) {
          long chunkSize = chunkSizeIter.nextLong();
          setOperationParams((int) chunkSize, TTL_SECS);
          String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel,
              new PutBlobOptionsBuilder().chunkUpload(true).maxUploadSize(PUT_CONTENT_SIZE).build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          long expirationTime = Utils.addSecondsToEpochTime(putBlobProperties.getCreationTimeInMs(),
              putBlobProperties.getTimeToLiveInSeconds());
          chunksToStitch.add(new ChunkInfo(blobId, chunkSize, expirationTime));
          stitchedContentStream.write(putContent);
        }
        byte[] expectedContent = stitchedContentStream.toByteArray();

        // Stitch the chunks together
        setOperationParams(0, TTL_SECS / 2);
        String stitchedBlobId = router.stitchBlob(putBlobProperties, putUserMetadata, chunksToStitch)
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // Fetch the stitched blob
        GetBlobResult getBlobResult = router.getBlob(stitchedBlobId, new GetBlobOptionsBuilder().build())
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertTrue("Blob properties must be the same", RouterTestHelpers.arePersistedFieldsEquivalent(putBlobProperties,
            getBlobResult.getBlobInfo().getBlobProperties()));
        assertEquals("Unexpected blob size", expectedContent.length,
            getBlobResult.getBlobInfo().getBlobProperties().getBlobSize());
        assertArrayEquals("User metadata must be the same", putUserMetadata,
            getBlobResult.getBlobInfo().getUserMetadata());
        RouterTestHelpers.compareContent(expectedContent, null, getBlobResult.getBlobDataChannel());

        // TtlUpdate the blob.
        router.updateBlobTtl(stitchedBlobId, "update-service", Utils.Infinite_Time)
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // Ensure that TTL was updated on the metadata blob and all data chunks
        Set<String> allBlobIds = chunksToStitch.stream().map(ChunkInfo::getBlobId).collect(Collectors.toSet());
        allBlobIds.add(stitchedBlobId);
        assertTtl(router, allBlobIds, Utils.Infinite_Time);

        // Delete and ensure that all stitched chunks are deleted
        deletedBlobs.clear();
        deletesDoneLatch.set(new CountDownLatch(chunksToStitch.size() + 1));
        router.deleteBlob(stitchedBlobId, "delete-service").get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        TestUtils.awaitLatchOrTimeout(deletesDoneLatch.get(), AWAIT_TIMEOUT_MS);
        assertEquals("Metadata chunk and all data chunks should be deleted", allBlobIds, deletedBlobs);
      }
    }
    router.close();
    assertExpectedThreadCounts(0, 0);
  }

  /**
   * Test that multiple scaling units can be instantiated, exercised and closed.
   */
  @Test
  public void testMultipleScalingUnit() throws Exception {
    final int SCALING_UNITS = 3;
    Properties props = getNonBlockingRouterProperties("DC1");
    props.setProperty("router.scaling.unit.count", Integer.toString(SCALING_UNITS));
    setRouter(props, new LoggingNotificationSystem());
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
    setRouter(getNonBlockingRouterProperties("DC1"), notificationSystem);
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
}
