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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.router.RouterTestHelpers.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/**
 * Class to test the {@link NonBlockingRouter}.
 */
@RunWith(Parameterized.class)
public class NonBlockingRouterTest extends NonBlockingRouterTestBase {
  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouterTest.class);

  /**
   * Initialize parameters common to all tests.
   * @param testEncryption {@code true} to test with encryption enabled. {@code false} otherwise
   * @param metadataContentVersion the metadata content version to test with.
   * @param includeCloudDc {@code true} to make the local datacenter a cloud DC.
   * @throws Exception if initialization fails
   */
  public NonBlockingRouterTest(boolean testEncryption, int metadataContentVersion, boolean includeCloudDc)
      throws Exception {
    super(testEncryption, metadataContentVersion, includeCloudDc);
  }

  /**
   * Running for both regular and encrypted blobs, and versions 2 and 3 of MetadataContent
   * @return an array with all four different choices
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false, MessageFormatRecord.Metadata_Content_Version_V2, false},
        {false, MessageFormatRecord.Metadata_Content_Version_V3, false},
        {true, MessageFormatRecord.Metadata_Content_Version_V2, false},
        {true, MessageFormatRecord.Metadata_Content_Version_V3, false}});
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
    testRouterBasicForRegularBlob();
    testRouterBasicForStitchedBlob();
    router.close();
    assertExpectedThreadCounts(0, 0);

    //submission after closing should return a future that is already done.
    assertClosed();
  }

  /**
   * Test Router with a single scaling unit for regular blob operations.
   */
  void testRouterBasicForRegularBlob() throws Exception {

    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    List<String> blobIds = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      setOperationParams();
      RestRequest restRequest = createRestRequestForPutOperation();
      PutBlobOptions putBlobOptions = new PutBlobOptionsBuilder().restRequest(restRequest).build();
      String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, putBlobOptions).get();
      if (testEncryption) {
        Assert.assertEquals(restRequest, kms.getCurrentRestRequest());
      }
      logger.debug("Put blob {}", blobId);
      blobIds.add(blobId);
    }

    for (String blobId : blobIds) {
      RestRequest restRequest = createRestRequestForGetOperation();
      GetBlobOptions getBlobOptions = new GetBlobOptionsBuilder().restRequest(restRequest).build();
      router.getBlob(blobId, getBlobOptions).get();
      if (testEncryption) {
        Assert.assertEquals(restRequest, kms.getCurrentRestRequest());
      }

      router.updateBlobTtl(blobId, null, Utils.Infinite_Time).get();
      router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
      router.getBlob(blobId, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build())
          .get();
      router.deleteBlob(blobId, null).get();
      try {
        router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
        fail("Get blob should fail");
      } catch (ExecutionException e) {
        RouterException r = (RouterException) e.getCause();
        Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
      }
      router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build()).get();
      router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_All).build()).get();
    }
  }

  /**
   * Test Router with a single scaling unit for stitched blob operations.
   */
  void testRouterBasicForStitchedBlob() throws Exception {
    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    LinkedList<String> blobIds = new LinkedList<>();
    for (int i = 0; i < 2; i++) {
      setOperationParams();
      String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT).get();
      logger.debug("Put blob {}", blobId);
      blobIds.add(blobId);
    }
    setOperationParams();
    String stitchedBlobId = router.stitchBlob(putBlobProperties, putUserMetadata, blobIds.stream()
        .map(blobId -> new ChunkInfo(blobId, PUT_CONTENT_SIZE, Utils.Infinite_Time))
        .collect(Collectors.toList())).get();

    // Test router for individual blob operations on the stitched blob's chunks.
    router.getBlob(stitchedBlobId, new GetBlobOptionsBuilder().build()).get();
    router.updateBlobTtl(stitchedBlobId, null, Utils.Infinite_Time).get();
    router.getBlob(stitchedBlobId, new GetBlobOptionsBuilder().build()).get();
    router.getBlob(stitchedBlobId,
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build()).get();
    router.deleteBlob(stitchedBlobId, null).get();
    try {
      router.getBlob(stitchedBlobId, new GetBlobOptionsBuilder().build()).get();
      fail("Get blob should fail");
    } catch (ExecutionException e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
    }
    router.getBlob(stitchedBlobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build())
        .get();
    router.getBlob(stitchedBlobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_All).build()).get();

    Consumer<Callable<Future>> deletedBlobHandlerConsumer = (Callable<Future> c) -> {
      try {
        c.call().get();
      } catch (Exception ex) {
        assertTrue(ex.getCause() instanceof RouterException);
        assertEquals(RouterErrorCode.BlobDeleted, ((RouterException) ex.getCause()).getErrorCode());
      }
    };
    for (String blobId : blobIds) {
      deletedBlobHandlerConsumer.accept(() -> router.getBlob(blobId, new GetBlobOptionsBuilder().build()));
      deletedBlobHandlerConsumer.accept(() -> router.updateBlobTtl(blobId, null, Utils.Infinite_Time));
      router.deleteBlob(blobId, null).get();
      router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build()).get();
      router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_All).build()).get();
    }
  }

  /**
   * Test undelete with Router with a single scaling unit.
   * @throws Exception
   */
  @Test
  public void testUndeleteBasic() throws Exception {
    assumeTrue(!testEncryption && !includeCloudDc);
    // Setting put and delete parallelism to 2, same as the put and delete success target.
    // If put or delete requests' parallelism is 3 (default value), when we ensure all the servers has the correspoding
    // requests, it might have override by the dangling request.
    // For example, when deleting a blob, there are three delete requests being sent to the mock server. But only two of
    // them required to be acknowledged. There is a chance that when we undelete this blob, this third unacknowledged
    // delete request would override the undelete state.
    setRouter(getNonBlockingRouterProperties("DC1", 3, 2), mockServerLayout, new LoggingNotificationSystem());
    assertExpectedThreadCounts(2, 1);

    // 1. Test undelete a composite blob
    List<String> blobIds = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      setOperationParams();
      String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT).get();
      ensurePutInAllServers(blobId, mockServerLayout);
      logger.debug("Put blob {}", blobId);
      blobIds.add(blobId);
    }
    setOperationParams();
    List<ChunkInfo> chunksToStitch = blobIds.stream()
        .map(blobId -> new ChunkInfo(blobId, PUT_CONTENT_SIZE, Utils.Infinite_Time))
        .collect(Collectors.toList());
    String stitchedBlobId = router.stitchBlob(putBlobProperties, putUserMetadata, chunksToStitch).get();
    ensureStitchInAllServers(stitchedBlobId, mockServerLayout, chunksToStitch, PUT_CONTENT_SIZE);
    blobIds.add(stitchedBlobId);

    for (String blobId : blobIds) {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
      router.deleteBlob(blobId, null).get();
      ensureDeleteInAllServers(blobId, mockServerLayout);
      try {
        router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
      } catch (ExecutionException e) {
        RouterException r = (RouterException) e.getCause();
        Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
      }
      router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build()).get();
      router.getBlob(blobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_All).build()).get();
    }
    // StitchedBlob is a composite blob
    router.undeleteBlob(stitchedBlobId, "undelete_server_id").get();
    for (String blobId : blobIds) {
      ensureUndeleteInAllServers(blobId, mockServerLayout);
    }
    // Now we should be able to fetch all the blobs
    for (String blobId : blobIds) {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
    }

    // 2. Test undelete a simple blob
    setOperationParams();
    String simpleBlobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT).get();
    ensurePutInAllServers(simpleBlobId, mockServerLayout);
    router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().build()).get();
    router.deleteBlob(simpleBlobId, null).get();
    ensureDeleteInAllServers(simpleBlobId, mockServerLayout);

    try {
      router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().build()).get();
    } catch (ExecutionException e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
    }
    router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build()).get();
    router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_All).build()).get();
    router.undeleteBlob(simpleBlobId, "undelete_server_id").get();
    ensureUndeleteInAllServers(simpleBlobId, mockServerLayout);
    router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().build()).get();

    // 3. Test delete after undelete
    router.deleteBlob(simpleBlobId, null).get();
    ensureDeleteInAllServers(simpleBlobId, mockServerLayout);

    try {
      router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().build()).get();
    } catch (ExecutionException e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
    }
    router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build()).get();
    router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_All).build()).get();

    // 4. Test undelete more than once
    router.undeleteBlob(simpleBlobId, "undelete_server_id").get();
    ensureUndeleteInAllServers(simpleBlobId, mockServerLayout);
    router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().build()).get();

    // 5. Undelete the same blob again
    router.undeleteBlob(simpleBlobId, "undelete_server_id").get();

    // 6. Test ttl update after undelete
    router.updateBlobTtl(simpleBlobId, null, Utils.Infinite_Time);
    router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().build()).get();

    router.close();
    assertExpectedThreadCounts(0, 0);

    //submission after closing should return a future that is already done.
    assertClosed();
  }

  /**
   * Test undelete notification system when successfully undelete a blob.
   * @throws Exception
   */
  @Test
  public void testUndeleteWithNotificationSystem() throws Exception {
    assumeTrue(!includeCloudDc);

    final CountDownLatch undeletesDoneLatch = new CountDownLatch(2);
    final Set<String> blobsThatAreUndeleted = new HashSet<>();
    LoggingNotificationSystem undeleteTrackingNotificationSystem = new LoggingNotificationSystem() {
      @Override
      public void onBlobUndeleted(String blobId, String serviceId, Account account, Container container) {
        blobsThatAreUndeleted.add(blobId);
        undeletesDoneLatch.countDown();
      }
    };
    setRouter(getNonBlockingRouterProperties("DC1"), mockServerLayout, undeleteTrackingNotificationSystem);

    List<String> blobIds = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      setOperationParams();
      String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT).get();
      ensurePutInAllServers(blobId, mockServerLayout);
      blobIds.add(blobId);
    }
    setOperationParams();
    List<ChunkInfo> chunksToStitch = blobIds.stream()
        .map(blobId -> new ChunkInfo(blobId, PUT_CONTENT_SIZE, Utils.Infinite_Time))
        .collect(Collectors.toList());
    String blobId = router.stitchBlob(putBlobProperties, putUserMetadata, chunksToStitch).get();
    ensureStitchInAllServers(blobId, mockServerLayout, chunksToStitch, PUT_CONTENT_SIZE);
    blobIds.add(blobId);
    Set<String> blobsToBeUndeleted = getBlobsInServers(mockServerLayout);

    router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
    router.deleteBlob(blobId, null).get();
    for (String chunkBlobId : blobIds) {
      ensureDeleteInAllServers(chunkBlobId, mockServerLayout);
    }
    router.undeleteBlob(blobId, "undelete_server_id").get();

    Assert.assertTrue("Undelete should not take longer than " + AWAIT_TIMEOUT_MS,
        undeletesDoneLatch.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    Assert.assertTrue("All blobs in server are deleted", blobsThatAreUndeleted.containsAll(blobsToBeUndeleted));
    Assert.assertTrue("Only blobs in server are undeleted", blobsToBeUndeleted.containsAll(blobsThatAreUndeleted));

    router.close();
    assertClosed();
  }

  /**
   * Test failure cases of undelete.
   * @throws Exception
   */
  @Test
  public void testUndeleteFailure() throws Exception {
    assumeTrue(!testEncryption && !includeCloudDc);
    setRouter();
    assertExpectedThreadCounts(2, 1);

    // 1. Test undelete a non-exist blob
    setOperationParams();
    String nonExistBlobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), putBlobProperties.getAccountId(), putBlobProperties.getContainerId(),
        mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK).getID();
    try {
      router.getBlob(nonExistBlobId, new GetBlobOptionsBuilder().build()).get();
      fail("Should fail because of non-existed id");
    } catch (ExecutionException e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDoesNotExist error is expected", RouterErrorCode.BlobDoesNotExist, r.getErrorCode());
    }
    try {
      router.undeleteBlob(nonExistBlobId, "undelete_server_id").get();
      fail("Should fail because of non-existed id");
    } catch (ExecutionException e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDoesNotExist error is expected", RouterErrorCode.BlobDoesNotExist, r.getErrorCode());
    }

    // 2. Test not-deleted blob
    setOperationParams();
    String notDeletedBlobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT).get();
    ensurePutInAllServers(notDeletedBlobId, mockServerLayout);
    router.getBlob(notDeletedBlobId, new GetBlobOptionsBuilder().build()).get();
    try {
      router.undeleteBlob(notDeletedBlobId, "undelete_server_id").get();
      fail("Should fail because of not-deleted id");
    } catch (ExecutionException e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobNotDeleted error is expected", RouterErrorCode.BlobNotDeleted, r.getErrorCode());
    }

    // 3. Test lifeVersion conflict blob
    setOperationParams();
    String conflictBlobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT).get();
    ensurePutInAllServers(conflictBlobId, mockServerLayout);
    router.getBlob(conflictBlobId, new GetBlobOptionsBuilder().build()).get();
    router.deleteBlob(conflictBlobId, null).get();
    ensureDeleteInAllServers(conflictBlobId, mockServerLayout); // All lifeVersion should be 0
    int count = 0;
    for (MockServer server : mockServerLayout.getMockServers()) {
      server.getBlobs().get(conflictBlobId).lifeVersion = 3;
      count++;
      if (count == 4) {
        // Only change 4 servers, since they are 3 datacenters and 9 servers. If we chang less than 4 servers, eg 3, then
        // this 3 changes might be distributed to 3 datacenters and undelete can still reach global quorum.
        break;
      }
    }

    try {
      router.undeleteBlob(conflictBlobId, "undelete_server_id").get();
      fail("Should fail because of lifeVersion conflict");
    } catch (ExecutionException e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("LifeVersionConflict error is expected", RouterErrorCode.LifeVersionConflict,
          r.getErrorCode());
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

    try {
      router.undeleteBlob(null, null);
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
    nettyByteBufLeakHelper.setDisabled(true);
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(mockClusterMap, routerConfig),
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
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
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
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(mockClusterMap, routerConfig),
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
    // Test when there is no limit on how many concurrent background delete operations
    testCompositeBlobDataChunksDeleteMaxDeleteOperation(0);
  }

  /**
   * Test that if a composite blob is deleted, the data chunks are eventually deleted. Also check the service IDs used
   * for delete operations. But this time, we limit the number of background delete operations.
   */
  @Test
  public void testCompositeBlobDataChunksDeleteWithMaxBackgroudDeleteOperation() throws Exception {
    // Test when the maximum number of background delete operations is 2;
    testCompositeBlobDataChunksDeleteMaxDeleteOperation(2);
  }

  protected void testCompositeBlobDataChunksDeleteMaxDeleteOperation(int maxDeleteOperation) throws Exception {
    // Ensure there are 4 chunks.
    maxPutChunkSize = PUT_CONTENT_SIZE / 4;
    Properties props = getNonBlockingRouterProperties("DC1");
    if (maxDeleteOperation != 0) {
      props.setProperty(RouterConfig.ROUTER_BACKGROUND_DELETER_MAX_CONCURRENT_OPERATIONS,
          Integer.toString(maxDeleteOperation));
    }
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
    NonBlockingRouterMetrics localMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
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
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);
        blobId =
            router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
        Set<String> allBlobsInServer = getBlobsInServers(mockServerLayout);
        allBlobsInServer.removeAll(blobsToBeDeleted);
        blobsToBeDeleted = allBlobsInServer;
      }
      blobsThatAreDeleted.clear();
      deletesDoneLatch.set(new CountDownLatch(5));
      router.deleteBlob(blobId, deleteServiceId).get();
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
    router.deleteBlob(blobId, null).get();
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
    setRouter(props, new MockServerLayout(mockClusterMap), deleteTrackingNotificationSystem);
    setOperationParams();
    String blobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    router.deleteBlob(blobId, deleteServiceId).get();
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
    setRouter(getNonBlockingRouterProperties("DC1"), new MockServerLayout(mockClusterMap),
        deleteTrackingNotificationSystem);
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
    testsAndExpected.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.BlobDoesNotExist);
    testsAndExpected.put(ServerErrorCode.Replica_Unavailable, RouterErrorCode.AmbryUnavailable);
    testsAndExpected.put(ServerErrorCode.Unknown_Error, RouterErrorCode.UnexpectedInternalError);
    // note that this test makes all nodes return same server error code. For Disk_Unavailable error, the router will
    // return BlobDoesNotExist because all disks are down (which should be extremely rare) and blob is gone.
    for (Map.Entry<ServerErrorCode, RouterErrorCode> testAndExpected : testsAndExpected.entrySet()) {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(testAndExpected.getKey()));
      TestCallback<Void> testCallback = new TestCallback<>();
      Future<Void> future = router.updateBlobTtl(blobId, updateServiceId, Utils.Infinite_Time, testCallback, null);
      assertFailureAndCheckErrorCode(future, testCallback, testAndExpected.getValue());
    }
    layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    // bad blob id
    TestCallback<Void> testCallback = new TestCallback<>();
    Future<Void> future = router.updateBlobTtl("bad-blob-id", updateServiceId, Utils.Infinite_Time, testCallback, null);
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
      }, null).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
   * Test that Response Handler correctly handles disconnected connections after warming up.
   */
  @Test
  public void testWarmUpConnectionFailureHandling() throws Exception {
    Properties props = getNonBlockingRouterProperties("DC3");
    MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
    mockSelectorState.set(MockSelectorState.FailConnectionInitiationOnPoll);
    setRouter(props, mockServerLayout, new LoggingNotificationSystem());
    for (DataNodeId node : mockClusterMap.getDataNodes()) {
      assertTrue("Node should be marked as timed out by ResponseHandler.", ((MockDataNodeId) node).isTimedOut());
    }
    router.close();
    mockSelectorState.set(MockSelectorState.Good);
  }

  /**
   * Test the case where request is timed out in the pending queue and network client returns response with null requestInfo
   * to mark node down via response handler.
   * @throws Exception
   */
  @Test
  public void testResponseWithNullRequestInfo() throws Exception {
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    NetworkClient mockNetworkClient = Mockito.mock(NetworkClient.class);
    Mockito.when(mockNetworkClient.warmUpConnections(anyList(), anyInt(), anyLong(), anyList())).thenReturn(1);
    doNothing().when(mockNetworkClient).close();
    List<ResponseInfo> responseInfoList = new ArrayList<>();
    MockDataNodeId testDataNode = (MockDataNodeId) mockClusterMap.getDataNodeIds().get(0);
    responseInfoList.add(new ResponseInfo(null, NetworkClientErrorCode.NetworkError, null, testDataNode));
    // By default, there are 1 operation controller and 1 background deleter thread. We set CountDownLatch to 3 so that
    // at least one thread has completed calling onResponse() and test node's state has been updated in ResponseHandler
    CountDownLatch invocationLatch = new CountDownLatch(3);
    doAnswer(invocation -> {
      invocationLatch.countDown();
      return responseInfoList;
    }).when(mockNetworkClient).sendAndPoll(anyList(), anySet(), anyInt());
    NetworkClientFactory networkClientFactory = Mockito.mock(NetworkClientFactory.class);
    Mockito.when(networkClientFactory.getNetworkClient()).thenReturn(mockNetworkClient);
    NonBlockingRouter testRouter =
        new NonBlockingRouter(routerConfig, routerMetrics, networkClientFactory, new LoggingNotificationSystem(),
            mockClusterMap, kms, cryptoService, cryptoJobHandler, accountService, mockTime,
            MockClusterMap.DEFAULT_PARTITION_CLASS);
    assertTrue("Invocation latch didn't count to 0 within 10 seconds", invocationLatch.await(10, TimeUnit.SECONDS));
    // verify the test node is considered timeout
    assertTrue("The node should be considered timeout", testDataNode.isTimedOut());
    testRouter.close();
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

    SocketNetworkClient networkClient =
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime).getNetworkClient();
    cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
    KeyManagementService localKMS = new MockKeyManagementService(new KMSConfig(verifiableProperties), singleKeyForKMS);
    putManager = new PutManager(mockClusterMap, mockResponseHandler, new LoggingNotificationSystem(),
        new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap, null),
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
        new NonBlockingRouterMetrics(mockClusterMap, null),
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
            new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap, null),
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

  private RestRequest createRestRequestForPutOperation() throws Exception {
    JSONObject header = new JSONObject();
    header.put(MockRestRequest.REST_METHOD_KEY, RestMethod.POST.name());
    header.put(MockRestRequest.URI_KEY, "/");
    return new MockRestRequest(header, null);
  }

  private RestRequest createRestRequestForGetOperation() throws Exception {
    JSONObject header = new JSONObject();
    header.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    header.put(MockRestRequest.URI_KEY, "/something.bin");
    return new MockRestRequest(header, null);
  }
}
