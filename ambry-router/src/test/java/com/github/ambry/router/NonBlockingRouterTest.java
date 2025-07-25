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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ReadableStreamChannelInputStream;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.MysqlRepairRequestsDbConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.IdConverter;
import com.github.ambry.frontend.IdConverterFactory;
import com.github.ambry.frontend.Operations;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.repair.MysqlRepairRequestsDb;
import com.github.ambry.repair.MysqlRepairRequestsDbFactory;
import com.github.ambry.repair.RepairRequestRecord;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.sql.DataSource;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.RestUtils.Headers.*;
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

  protected static MysqlRepairRequestsDb repairDb = null;
  protected static MockTime staticMockTime = new MockTime();
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

  @BeforeClass
  public static void setup() throws Exception {
    repairDb = createRepairRequestsConnection("DC3", staticMockTime);
  }

  @AfterClass
  public static void tearDown() {
    repairDb.close();
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{false, MessageFormatRecord.Metadata_Content_Version_V2, false}, {false, MessageFormatRecord.Metadata_Content_Version_V3, false}, {true, MessageFormatRecord.Metadata_Content_Version_V2, false}, {true, MessageFormatRecord.Metadata_Content_Version_V3, false}});
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link QuotaAwareOperationController}.
   * @return the created VerifiableProperties instance.
   */
  @Override
  protected Properties getNonBlockingRouterProperties(String routerDataCenter) {
    Properties properties =
        super.getNonBlockingRouterProperties(routerDataCenter, PUT_REQUEST_PARALLELISM, DELETE_REQUEST_PARALLELISM);
    properties.setProperty("router.operation.controller", "com.github.ambry.router.QuotaAwareOperationController");

    return properties;
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * @param enableODR enable On demand replication
   * @param enableOfflineRepair enable the offline repair
   * @param enableForceDelete enable force delete for delete operations
   * @return the created VerifiableProperties instance.
   */
  protected Properties getNonBlockingRouterPropertiesForRepairRequests(String routerDataCenter, boolean enableODR,
      boolean enableOfflineRepair, boolean enableForceDelete) {
    Properties properties =
        super.getNonBlockingRouterProperties(routerDataCenter, PUT_REQUEST_PARALLELISM, DELETE_REQUEST_PARALLELISM);
    properties.setProperty("router.operation.controller", "com.github.ambry.router.QuotaAwareOperationController");

    if (enableODR) {
      // enable replicate blob on TtlUpdate
      properties.setProperty("router.repair.with.replicate.blob.enabled", "true");
      // enable replicate blob on Delete
      properties.setProperty("router.repair.with.replicate.blob.on.delete.enabled", "true");
    }
    if (enableOfflineRepair) {
      // set up the RepairRequestsDbFactory
      properties.setProperty("router.repair.requests.db.factory",
          "com.github.ambry.repair.MysqlRepairRequestsDbFactory");
      String dbInfo = "["
          + "{\"url\":\"jdbc:mysql://localhost/AmbryRepairRequests?serverTimezone=UTC\",\"datacenter\":\"DC1\",\"isWriteable\":\"true\",\"username\":\"travis\",\"password\":\"\"},"
          + "{\"url\":\"jdbc:mysql://localhost/AmbryRepairRequests?serverTimezone=UTC\",\"datacenter\":\"DC2\",\"isWriteable\":\"true\",\"username\":\"travis\",\"password\":\"\"},"
          + "{\"url\":\"jdbc:mysql://localhost/AmbryRepairRequests?serverTimezone=UTC\",\"datacenter\":\"DC3\",\"isWriteable\":\"true\",\"username\":\"travis\",\"password\":\"\"}"
          + "]";
      properties.setProperty("mysql.repair.requests.db.info", dbInfo);
      // enable offline repair for delete and ttlupdate
      properties.setProperty("router.delete.offline.repair.enabled", "true");
      properties.setProperty("router.ttlupdate.offline.repair.enabled", "true");
    }
    if (enableForceDelete) {
      properties.setProperty(RouterConfig.ROUTER_FORCE_DELETE_ENABLED, "true");
    }

    return properties;
  }

  /**
   * Test the {@link NonBlockingRouterFactory}
   */
  @Test
  public void testNonBlockingRouterFactory() throws Exception {
    try {
      Properties props = getNonBlockingRouterProperties("NotInClusterMap");
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      try {
        router = (NonBlockingRouter) new NonBlockingRouterFactory(verifiableProperties, mockClusterMap,
            new LoggingNotificationSystem(), null, accountService).getRouter();
        Assert.fail("NonBlockingRouterFactory instantiation should have failed because the router datacenter is not in "
            + "the cluster map");
      } catch (IllegalStateException e) {
      }
      props = getNonBlockingRouterProperties(localDcName);
      verifiableProperties = new VerifiableProperties((props));
      router = (NonBlockingRouter) new NonBlockingRouterFactory(verifiableProperties, mockClusterMap,
          new LoggingNotificationSystem(), null, accountService).getRouter();
      assertExpectedThreadCounts(2, 1);
    } finally {
      if (router != null) {
        router.close();
        assertExpectedThreadCounts(0, 0);
      }
    }
  }

  /**
   * Test Router with a single scaling unit.
   */
  @Test
  public void testRouterBasic() throws Exception {
    try {
      setRouter();
      assertExpectedThreadCounts(2, 1);
      testRouterBasicForRegularBlob();
      testRouterBasicForStitchedBlob();
    } finally {
      if (router != null) {
        router.close();
        assertExpectedThreadCounts(0, 0);

        //submission after closing should return a future that is already done.
        assertClosed();
      }
    }
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
      String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, putOptionsForChunkedUpload).get();
      logger.debug("Put blob {}", blobId);
      blobIds.add(blobId);
    }
    setOperationParams();
    String stitchedBlobId = router.stitchBlob(putBlobProperties, putUserMetadata, blobIds.stream()
        .map(blobId -> new ChunkInfo(blobId, PUT_CONTENT_SIZE, Utils.Infinite_Time, null))
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
    try {
      assumeTrue(!testEncryption && !includeCloudDc);
      // Setting put and delete parallelism to 2, same as the put and delete success target.
      // If put or delete requests' parallelism is 3 (default value), when we ensure all the servers has the correspoding
      // requests, it might have override by the dangling request.
      // For example, when deleting a blob, there are three delete requests being sent to the mock server. But only two of
      // them required to be acknowledged. There is a chance that when we undelete this blob, this third unacknowledged
      // delete request would override the undelete state.
      setRouter(getNonBlockingRouterProperties(localDcName, 3, 2), mockServerLayout, new LoggingNotificationSystem());
      assertExpectedThreadCounts(2, 1);

      // 1. Test undelete a composite blob
      List<String> blobIds = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        setOperationParams();
        String blobId =
            router.putBlob(putBlobProperties, putUserMetadata, putChannel, putOptionsForChunkedUpload).get();
        ensurePutInAllServers(blobId, mockServerLayout);
        logger.debug("Put blob {}", blobId);
        blobIds.add(blobId);
      }
      setOperationParams();
      List<ChunkInfo> chunksToStitch = blobIds.stream()
          .map(blobId -> new ChunkInfo(blobId, PUT_CONTENT_SIZE, Utils.Infinite_Time, null))
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
      String simpleBlobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT).get();
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
      router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build())
          .get();
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
      router.getBlob(simpleBlobId, new GetBlobOptionsBuilder().getOption(GetOption.Include_Deleted_Blobs).build())
          .get();
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
    } finally {
      if (router != null) {
        router.close();
        assertExpectedThreadCounts(0, 0);

        //submission after closing should return a future that is already done.
        assertClosed();
      }
    }
  }

  /**
   * Test undelete notification system when successfully undelete a blob.
   * @throws Exception
   */
  @Test
  public void testUndeleteWithNotificationSystem() throws Exception {
    try {
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
      setRouter(getNonBlockingRouterProperties(localDcName), mockServerLayout, undeleteTrackingNotificationSystem);

      List<String> blobIds = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        setOperationParams();
        String blobId =
            router.putBlob(putBlobProperties, putUserMetadata, putChannel, putOptionsForChunkedUpload).get();
        ensurePutInAllServers(blobId, mockServerLayout);
        blobIds.add(blobId);
      }
      setOperationParams();
      List<ChunkInfo> chunksToStitch = blobIds.stream()
          .map(blobId -> new ChunkInfo(blobId, PUT_CONTENT_SIZE, Utils.Infinite_Time, null))
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
    } finally {
      if (router != null) {
        router.close();
        assertClosed();
      }
    }
  }

  /**
   * Test failure cases of undelete.
   * @throws Exception
   */
  @Test
  public void testUndeleteFailure() throws Exception {
    try {
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
    } finally {
      if (router != null) {
        router.close();
        assertExpectedThreadCounts(0, 0);

        //submission after closing should return a future that is already done.
        assertClosed();
      }
    }
  }

  /**
   * Test behavior with various null inputs to router methods.
   * @throws Exception
   */
  @Test
  public void testNullArguments() throws Exception {
    try {
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
    } finally {
      if (router != null) {
        router.close();
        assertExpectedThreadCounts(0, 0);
        //submission after closing should return a future that is already done.
        assertClosed();
      }
    }
  }

  /**
   * Test router put operation in a scenario where there are no partitions available.
   */
  @Test
  public void testRouterPartitionsUnavailable() throws Exception {
    try {
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
    } finally {
      if (router != null) {
        router.close();
        assertExpectedThreadCounts(0, 0);
        assertClosed();
      }
    }
  }

  /**
   * Test router put operation in a scenario where there are partitions, but none in the local DC.
   * This should not ideally happen unless there is a bad config, but the router should be resilient and
   * just error out these operations.
   */
  @Test
  public void testRouterNoPartitionInLocalDC() throws Exception {
    try {
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
    } finally {
      if (router != null) {
        router.close();
        assertExpectedThreadCounts(0, 0);
        assertClosed();
      }
    }
  }

  /**
   * Test RequestResponseHandler thread exit flow. If the RequestResponseHandlerThread exits on its own (due to a
   * Throwable), then the router gets closed immediately along with the completion of all the operations.
   */
  @Test
  public void testRequestResponseHandlerThreadExitFlow() throws Exception {
    nettyByteBufLeakHelper.setDisabled(true);
    Properties props = getNonBlockingRouterProperties(localDcName);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(mockClusterMap, routerConfig),
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockServerLayout(mockClusterMap), mockTime), new LoggingNotificationSystem(),
        mockClusterMap, kms, cryptoService, cryptoJobHandler, accountService, mockTime,
        MockClusterMap.DEFAULT_PARTITION_CLASS, null);

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
    try {
      // Ensure there are 4 chunks.
      maxPutChunkSize = PUT_CONTENT_SIZE / 4;
      Properties props = getNonBlockingRouterProperties(localDcName);
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
          cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);

      setOperationParams();

      List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
      List<ServerErrorCode> serverErrorList = new ArrayList<>();
      // There are 4 chunks for this blob.
      // All put operations make one request to each local server as there are 3 servers overall in the local DC.
      // Set the state of the mock servers so that they return success for the first 2 requests in order to succeed
      // the first two chunks.
      serverErrorList.add(ServerErrorCode.NoError);
      serverErrorList.add(ServerErrorCode.NoError);
      // fail requests for third and fourth data chunks including the slipped put attempts:
      serverErrorList.add(ServerErrorCode.UnknownError);
      serverErrorList.add(ServerErrorCode.UnknownError);
      serverErrorList.add(ServerErrorCode.UnknownError);
      serverErrorList.add(ServerErrorCode.UnknownError);
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
            BackgroundDeleteRequest.SERVICE_ID_PREFIX + putBlobProperties.getServiceId(),
            blobIdAndServiceId.getValue());
      }
    } finally {
      if (router != null) {
        router.close();
        assertClosed();
        Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
      }
    }
  }

  /**
   * Test that even when a composite blob put succeeds, the slipped put data chunks are deleted.
   * Background deleter hits Blob_Not_Found and swallow the error.
   */
  @Test
  public void testSuccessfulPutDataChunkDeleteCase1() throws Exception {
    testSuccessfulPutDataChunkDelete(ServerErrorCode.BlobNotFound);
  }

  /**
   * Test that even when a composite blob put succeeds, the slipped put data chunks are deleted.
   * Background deleter hits Replica_Unavailable and swallow the error.
   */
  @Test
  public void testSuccessfulPutDataChunkDeleteCase2() throws Exception {
    testSuccessfulPutDataChunkDelete(ServerErrorCode.ReplicaUnavailable);
  }

  public void testSuccessfulPutDataChunkDelete(ServerErrorCode backgroundDeletorError) throws Exception {
    try {
      // This test is somehow probabilistic. Since it is not possible to devise a mocking to enforce the occurrence of
      // slipped puts given we cannot control the order of the hosts requests are sent and not all requests are sent when
      // put requests are guaranteed to fail/succeed. So, we are setting the number of chunks and max attempts high enough
      // to guarantee that slipped puts would eventually happen and operation would succeed.
      maxPutChunkSize = PUT_CONTENT_SIZE / 8;
      final int NUM_MAX_ATTEMPTS = 100;
      Properties props = getNonBlockingRouterProperties(localDcName);
      props.setProperty("router.max.slipped.put.attempts", Integer.toString(NUM_MAX_ATTEMPTS));
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      RouterConfig routerConfig = new RouterConfig(verifiableProperties);
      MockClusterMap mockClusterMap = new MockClusterMap();
      MockTime mockTime = new MockTime();
      MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
      // Since this test wants to ensure that successfully put data chunks are deleted when the overall put operation
      // succeeds but some chunks succeed only after a retry, it uses a notification system to track the deletions.
      final CountDownLatch deletesDoneLatch = new CountDownLatch(1);
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
          cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);

      setOperationParams();

      // In local DC, set up the servers such that one node always succeeds and the other nodes return an unknown_error and
      // no_error alternately. This will make it with a very high probability that there will at least be a time that a
      // put will succeed on a node but will fail on the other two.
      List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
      List<ServerErrorCode> serverErrorList = new ArrayList<>();
      List<ServerErrorCode> deleteErrorList = new ArrayList<>();
      for (int i = 0; i < NUM_MAX_ATTEMPTS; i++) {
        serverErrorList.add(ServerErrorCode.UnknownError);
        serverErrorList.add(ServerErrorCode.NoError);
        deleteErrorList.add(backgroundDeletorError);
      }

      // return error for background deleter. Test router will ignore NOT_FOUND or Unavailable for background deleter.
      DataNodeId healthyLocalNode = null;
      for (DataNodeId dataNodeId : dataNodeIds) {
        MockServer server = mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort());
        if (server.getDataCenter().equals(localDcName)) {
          if (healthyLocalNode == null) {
            // one healthy local node
            server.resetServerErrors();
            healthyLocalNode = dataNodeId;
          } else {
            // the other two local nodes
            server.setServerErrors(serverErrorList);
            server.setServerErrorsByType(RequestOrResponseType.DeleteRequest, deleteErrorList);
          }
        } else {
          // remote nodes
          server.resetServerErrors();
          server.setServerErrorsByType(RequestOrResponseType.DeleteRequest, deleteErrorList);
        }
      }

      // Submit the put operation and wait for it to succeed.
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();

      // Now, wait until at least one delete happens within AWAIT_TIMEOUT_MS.
      Assert.assertTrue("Some blobs should have been deleted within " + AWAIT_TIMEOUT_MS,
          deletesDoneLatch.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      // Wait for the rest of the deletes to finish.
      long waitStart = SystemTime.getInstance().milliseconds();
      while (router.getBackgroundOperationsCount() != 0
          && SystemTime.getInstance().milliseconds() < waitStart + AWAIT_TIMEOUT_MS) {
        Thread.sleep(1000);
      }
      for (Map.Entry<String, String> blobIdAndServiceId : blobsThatAreDeleted.entrySet()) {
        Assert.assertNotSame("We should not be deleting the valid blob by mistake", blobId,
            blobIdAndServiceId.getKey());
        Assert.assertEquals("Unexpected service ID for deleted blob",
            BackgroundDeleteRequest.SERVICE_ID_PREFIX + putBlobProperties.getServiceId(),
            blobIdAndServiceId.getValue());
      }
    } finally {
      if (router != null) {
        router.close();
        assertClosed();
      }
    }
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
    try {
      // Ensure there are 4 chunks.
      maxPutChunkSize = PUT_CONTENT_SIZE / 4;
      Properties props = getNonBlockingRouterProperties(localDcName);
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
          cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
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
        Assert.assertTrue("All blobs in server are deleted",
            blobsThatAreDeleted.keySet().containsAll(blobsToBeDeleted));
        Assert.assertTrue("Only blobs in server are deleted",
            blobsToBeDeleted.containsAll(blobsThatAreDeleted.keySet()));

        for (Map.Entry<String, String> blobIdAndServiceId : blobsThatAreDeleted.entrySet()) {
          String expectedServiceId = blobIdAndServiceId.getKey().equals(blobId) ? deleteServiceId
              : BackgroundDeleteRequest.SERVICE_ID_PREFIX + deleteServiceId;
          Assert.assertEquals("Unexpected service ID for deleted blob", expectedServiceId,
              blobIdAndServiceId.getValue());
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
    } finally {
      if (router != null) {
        router.close();
        assertClosed();
        Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
      }
    }
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
    try {
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
      Properties props = getNonBlockingRouterProperties(localDcName);
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
    } finally {
      if (router != null) {
        router.close();
        assertClosed();
        Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
      }
    }
  }

  /**
   * Test named blob stitch after we move id converter into router.
   */
  @Test
  public void testStitchedNamedBlob() throws Exception {
    try {
      final AtomicInteger stitchInitiated = new AtomicInteger();
      Properties props = getNonBlockingRouterProperties(localDcName);
      LoggingNotificationSystem updateTtlTrackingNotificationSystem = new LoggingNotificationSystem() {
        @Override
        public void onBlobCreated(String blobId, BlobProperties blobProperties, Account account, Container container,
            NotificationBlobType notificationBlobType) {
          stitchInitiated.incrementAndGet();
        }
      };
      // setup mock idconverter
      IdConverterFactory mockIdConverterFactory = mock(IdConverterFactory.class);
      IdConverter mockIdConverter = mock(IdConverter.class);
      when(mockIdConverterFactory.getIdConverter()).thenReturn(mockIdConverter);
      when(mockIdConverter.convert(any(RestRequest.class), anyString(), any(), any())).thenAnswer(invocation -> {
        FutureResult<String> futureResult = new FutureResult<>();
        String blobId = invocation.getArgument(1);
        Callback<String> callback = invocation.getArgument(3);
        if (callback != null) {
          callback.onCompletion(blobId, null);
        }
        futureResult.done(blobId, null);
        return futureResult;
      });

      setRouterWithIdConverterFactory(props, new MockServerLayout(mockClusterMap), updateTtlTrackingNotificationSystem,
          mockIdConverterFactory);

      LinkedList<String> blobIds = new LinkedList<>();
      for (int i = 0; i < 2; i++) {
        setOperationParams();
        String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, putOptionsForChunkedUpload)
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        blobIds.add(blobId);
      }

      final String ACCOUNT_NAME = "accountName";
      final String CONTAINER_NAME = "containerName";
      final String BLOB_NAME = "blobName";
      setOperationParams();
      RestRequest request =
          createNamedBlobRestRequest(RestMethod.PUT, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, false, false, true);

      String stitchedBlobId = router.stitchBlob(request, putBlobProperties, putUserMetadata, blobIds.stream()
          .map(blobId -> new ChunkInfo(blobId, PUT_CONTENT_SIZE, Utils.Infinite_Time, null))
          .collect(Collectors.toList()), null, null, null).get();

      GetBlobResult getBlobResult =
          router.getBlob(stitchedBlobId, new GetBlobOptionsBuilder().build(), null, null).get();

      Assert.assertEquals("Blob Size should match", PUT_CONTENT_SIZE * 2,
          getBlobResult.getBlobInfo().getBlobProperties().getBlobSize());

      when(mockIdConverter.convert(any(RestRequest.class), anyString(), any(), any())).thenAnswer(invocation -> {
        FutureResult<String> futureResult = new FutureResult<>();
        Callback<String> callback = invocation.getArgument(3);

        if (callback != null) {
          callback.onCompletion(stitchedBlobId, null);
        }
        futureResult.done(stitchedBlobId, null);

        return futureResult;
      });

      request =
          createNamedBlobRestRequest(RestMethod.DELETE, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, false, false, false);
      router.deleteBlob(request, null, "deleteServiceId", null, null).get();
    } finally {
      if (router != null) {
        router.close();
        assertClosed();
        Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
      }
    }
  }


    /**
     * Test named blob put and ttl update after we move id converter into router.
     * @throws Exception
     */
  @Test
  public void testNamedBlobPutAndTtlUpdate() throws Exception {
    try {
      maxPutChunkSize = PUT_CONTENT_SIZE;
      String updateTtlServiceId = "updateTtl-service";
      final AtomicInteger updateTtlInitiated = new AtomicInteger();
      final AtomicReference<String> receivedTtlUpdateServiceId = new AtomicReference<>();

      LoggingNotificationSystem updateTtlTrackingNotificationSystem = new LoggingNotificationSystem() {
        @Override
        public void onBlobTtlUpdated(String blobId, String serviceId, long expiresAtMs, Account account,
            Container container) {
          updateTtlInitiated.incrementAndGet();
          receivedTtlUpdateServiceId.set(serviceId);
        }
      };

      final String ACCOUNT_NAME = "accountName";
      final String CONTAINER_NAME = "containerName";
      final String BLOB_NAME = "blobName";
      // setup mock idconverter
      IdConverterFactory mockIdConverterFactory = mock(IdConverterFactory.class);
      IdConverter mockIdConverter = mock(IdConverter.class);
      when(mockIdConverterFactory.getIdConverter()).thenReturn(mockIdConverter);
      when(mockIdConverter.convert(any(RestRequest.class), anyString(), any(), any())).thenAnswer(invocation -> {
        FutureResult<String> futureResult = new FutureResult<>();
        String blobId = invocation.getArgument(1);
        Callback<String> callback = invocation.getArgument(3);
        if (callback != null) {
          callback.onCompletion(blobId, null);
        }
        futureResult.done(blobId, null);
        return futureResult;
      });
      Properties props = getNonBlockingRouterProperties(localDcName);
      setRouterWithIdConverterFactory(props, new MockServerLayout(mockClusterMap), updateTtlTrackingNotificationSystem,
          mockIdConverterFactory);
      //put blob
      setOperationParams();
      RestRequest request =
          createNamedBlobRestRequest(RestMethod.PUT, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, false, true, false);
      String blobIdFromRouter =
          router.putBlob(request, putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build(),
              null, null).get();
      GetBlobResult getBlobResult =
          router.getBlob(blobIdFromRouter, new GetBlobOptionsBuilder().build(), null, null).get();
      Assert.assertEquals(getBlobResult.getBlobInfo().getBlobProperties().getTimeToLiveInSeconds(), TTL_SECS);

      when(mockIdConverter.convert(any(RestRequest.class), anyString(), any(), any())).thenAnswer(invocation -> {
        FutureResult<String> futureResult = new FutureResult<>();
        Callback<String> callback = invocation.getArgument(3);

        if (callback != null) {
          callback.onCompletion(blobIdFromRouter, null);
        }
        futureResult.done(blobIdFromRouter, null);

        return futureResult;
      });
      // ttl update
      request = createNamedBlobRestRequest(RestMethod.PUT, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, true, false, false);
      router.updateBlobTtl(request, RestUtils.getHeader(request.getArgs(), RestUtils.Headers.BLOB_ID, true),
          updateTtlServiceId, -1, null, null).get();
      getBlobResult = router.getBlob(blobIdFromRouter, new GetBlobOptionsBuilder().build(), null, null).get();
      Assert.assertEquals(getBlobResult.getBlobInfo().getBlobProperties().getTimeToLiveInSeconds(),
          Utils.Infinite_Time);
      request = createNamedBlobRestRequest(RestMethod.DELETE, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME,
          false, false, false);
      router.deleteBlob(request, null, "deleteServiceId", null, null).get();
      long waitStart = SystemTime.getInstance().milliseconds();
      while (router.getBackgroundOperationsCount() != 0
          && SystemTime.getInstance().milliseconds() < waitStart + AWAIT_TIMEOUT_MS) {
        Thread.sleep(1000);
      }
      Assert.assertEquals("All background operations should be complete ", 0, router.getBackgroundOperationsCount());
      Assert.assertEquals("The update ttl service ID should match the expected value", updateTtlServiceId,
          receivedTtlUpdateServiceId.get());
      Assert.assertEquals(updateTtlInitiated.get(), 1);
    } finally {
      if (router != null) {
        router.close();
        assertClosed();
        Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
      }
    }
  }

  /**
   * Test to ensure when deleting a named blob, all versions are deleted and all blob ids are deleted.
   * @throws Exception
   */
  @Test
  public void testNamedBlobDelete() throws Exception {
    try {
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

      final int NUM_VERSIONS = 3;
      final String ACCOUNT_NAME = "accountName";
      final String CONTAINER_NAME = "containerName";
      final String BLOB_NAME = "blobName";
      List<String> blobIds = new ArrayList<>();
      // setup mock idconverter
      IdConverterFactory mockIdConverterFactory = mock(IdConverterFactory.class);
      IdConverter mockIdConverter = mock(IdConverter.class);
      when(mockIdConverterFactory.getIdConverter()).thenReturn(mockIdConverter);
      when(mockIdConverter.convert(any(RestRequest.class), anyString(), any(), any())).thenAnswer(invocation -> {
        FutureResult<String> futureResult = new FutureResult<>();
        RestRequest restRequest = invocation.getArgument(0);
        String blobId = invocation.getArgument(1);
        Callback<String> callback = invocation.getArgument(3);
        String returnValue = null;
        if (restRequest.getRestMethod() == RestMethod.DELETE) {
          // This is coming from a delete request
          returnValue = blobIds.stream().reduce((blobId1, blobId2) -> blobId1 + "," + blobId2).orElse("");
        } else {
          // This is coming from a put request
          returnValue = blobId;
        }
        if (callback != null) {
          callback.onCompletion(returnValue, null);
        }
        futureResult.done(returnValue, null);
        return futureResult;
      });

      Properties props = getNonBlockingRouterProperties(localDcName);
      setRouterWithIdConverterFactory(props, new MockServerLayout(mockClusterMap), deleteTrackingNotificationSystem,
          mockIdConverterFactory);
      // Create 3 versions of the same named blob
      for (int i = 0; i < NUM_VERSIONS; i++) {
        setOperationParams();
        RestRequest request = createNamedBlobRestRequest(RestMethod.PUT, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, false,
            true, false);
        String blobId =
            router.putBlob(request, putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build(),
                null, null).get();
        blobIds.add(blobId);
      }
      RestRequest request = createNamedBlobRestRequest(RestMethod.DELETE, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME,
          false, false, false);
      router.deleteBlob(request, null, deleteServiceId, null, null).get();
      long waitStart = SystemTime.getInstance().milliseconds();
      while (router.getBackgroundOperationsCount() != 0
          && SystemTime.getInstance().milliseconds() < waitStart + AWAIT_TIMEOUT_MS) {
        Thread.sleep(1000);
      }
      Assert.assertEquals("All background operations should be complete ", 0, router.getBackgroundOperationsCount());
      Assert.assertEquals("Only the original blob deletion should have been initiated", NUM_VERSIONS,
          deletesInitiated.get());
      Assert.assertEquals("The delete service ID should match the expected value", deleteServiceId,
          receivedDeleteServiceId.get());
      Assert.assertEquals("Get should have been skipped", NUM_VERSIONS, routerMetrics.skippedGetBlobCount.getCount());
      // Now try to get the blob ids, expected not found
      for (String blobId : blobIds) {
        try {
          router.getBlob(blobId, new GetBlobOptionsBuilder().build(), null, null).get();
          fail("Blob id " + blobId + " should be deleted");
        } catch (Exception e) {
          RouterException r = (RouterException) e.getCause();
          Assert.assertEquals(RouterErrorCode.BlobDeleted, r.getErrorCode());
        }
      }
    } finally {
      if (router != null) {
        router.close();
        assertClosed();
        Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
      }
    }
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
    try {
      AtomicReference<CountDownLatch> deletesDoneLatch = new AtomicReference<>();
      Set<String> deletedBlobs = ConcurrentHashMap.newKeySet();
      LoggingNotificationSystem deleteTrackingNotificationSystem = new LoggingNotificationSystem() {
        @Override
        public void onBlobDeleted(String blobId, String serviceId, Account account, Container container) {
          deletedBlobs.add(blobId);
          deletesDoneLatch.get().countDown();
        }
      };
      setRouter(getNonBlockingRouterProperties(localDcName), new MockServerLayout(mockClusterMap),
          deleteTrackingNotificationSystem);
      for (int intermediateChunkSize : new int[]{maxPutChunkSize, maxPutChunkSize / 2}) {
        for (LongStream chunkSizeStream : new LongStream[]{RouterTestHelpers.buildValidChunkSizeStream(
            3 * intermediateChunkSize, intermediateChunkSize), RouterTestHelpers.buildValidChunkSizeStream(
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
            chunksToStitch.add(new ChunkInfo(blobId, chunkSize, expirationTime, null));
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
          assertTrue("Blob properties must be the same",
              RouterTestHelpers.arePersistedFieldsEquivalent(putBlobProperties,
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
    } finally {
      if (router != null) {
        router.close();
        assertExpectedThreadCounts(0, 0);
      }
    }
  }

  /**
   * Test for most error cases involving TTL updates
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdateErrors() throws Exception {
    try {
      String updateServiceId = "update-service";
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      setOperationParams();
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      Map<ServerErrorCode, RouterErrorCode> testsAndExpected = new HashMap<>();
      testsAndExpected.put(ServerErrorCode.BlobNotFound, RouterErrorCode.BlobDoesNotExist);
      testsAndExpected.put(ServerErrorCode.BlobDeleted, RouterErrorCode.BlobDeleted);
      testsAndExpected.put(ServerErrorCode.BlobExpired, RouterErrorCode.BlobExpired);
      testsAndExpected.put(ServerErrorCode.DiskUnavailable, RouterErrorCode.AmbryUnavailable);
      testsAndExpected.put(ServerErrorCode.ReplicaUnavailable, RouterErrorCode.AmbryUnavailable);
      testsAndExpected.put(ServerErrorCode.UnknownError, RouterErrorCode.UnexpectedInternalError);
      // note that this test makes all nodes return same server error code.
      for (Map.Entry<ServerErrorCode, RouterErrorCode> testAndExpected : testsAndExpected.entrySet()) {
        layout.getMockServers()
            .forEach(mockServer -> mockServer.setServerErrorForAllRequests(testAndExpected.getKey()));
        TestCallback<Void> testCallback = new TestCallback<>();
        Future<Void> future =
            router.updateBlobTtl(null, blobId, updateServiceId, Utils.Infinite_Time, testCallback, null);
        assertFailureAndCheckErrorCode(future, testCallback, testAndExpected.getValue());

        // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
        router.getNotFoundCache().invalidateAll();
      }
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      // bad blob id
      TestCallback<Void> testCallback = new TestCallback<>();
      Future<Void> future =
          router.updateBlobTtl(null, "bad-blob-id", updateServiceId, Utils.Infinite_Time, testCallback, null);
      assertFailureAndCheckErrorCode(future, testCallback, RouterErrorCode.InvalidBlobId);
    } finally {
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test that a bad user defined callback will not crash the router or the manager.
   * @throws Exception
   */
  @Test
  public void testBadCallbackForUpdateTtl() throws Exception {
    try {
      MockServerLayout serverLayout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterProperties(localDcName), serverLayout, new LoggingNotificationSystem());
      setOperationParams();
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
      String blobIdCheck =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      testWithErrorCodes(Collections.singletonMap(ServerErrorCode.NoError, 9), serverLayout, null, expectedError -> {
        final CountDownLatch callbackCalled = new CountDownLatch(1);
        router.updateBlobTtl(null, blobId, null, Utils.Infinite_Time, (result, exception) -> {
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
    } finally {
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test for ReplicateBlob success case
   * Replicating one Blob to the remote replicas when two local replicas were unavailable.
   * @throws Exception
   */
  @Test
  public void testReplicateBlobToRemoteSuccess() throws Exception {
    try {
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      String serviceId = "replicate-blob-service";
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      setOperationParams();
      // PutBlob to the three local replicas
      String blobIdStr =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      GetBlobResult localGetBlobResult =
          router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();

      // set error status for all the local replica. read from the remote replica, it should fail
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }
      try {
        router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();
        Assert.fail("Should return Ambry unavailable.");
      } catch (ExecutionException e) {
        Throwable t = e.getCause();
        assertTrue("Cause should be RouterException", t instanceof RouterException);
        assertEquals("ErrorCode mismatch", RouterErrorCode.AmbryUnavailable, ((RouterException) t).getErrorCode());
      }
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));

      // Replicate Blob from the local replica to some remote replicas.
      // choose one local replica as the source DataNode. Set the other two replicas as Replica_Unavailable
      DataNodeId sourceDataNode = null;
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
          }
        }
      }
      // since there is only one local available, router will replicate the blob to at least 1 remote replicas.
      router.replicateBlob(blobIdStr, serviceId, sourceDataNode).get();

      // set unavailable to all local replicas. So router.getBlob will get the Blob from remote replicas.
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }
      GetBlobResult remoteGetBlobResult =
          router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();

      assertEqual(localGetBlobResult, remoteGetBlobResult);
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();

      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test for replicating the deleted Blob.
   * We should replicate both the PutBlob and DeleteBlob to the target DataNode.
   * @throws Exception
   */
  @Test
  public void testReplicateDeletedBlob() throws Exception {
    try {
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      String serviceId = "replicate-blob-service";
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      setOperationParams();
      // PutBlob to the three local replicas
      String blobIdStr =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      GetBlobResult localGetBlobResult =
          router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();
      ReadableStreamChannel localStreamChannel = localGetBlobResult.getBlobDataChannel();
      router.deleteBlob(blobIdStr, serviceId).get();

      // Replicate Blob from the local replica to some remote replicas.
      // choose one local replica as the source DataNode. Set the other two replicas to Replica_Unavailable
      DataNodeId sourceDataNode = null;
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
          }
        }
      }
      // since there is only one local available, it'll replicate to at least 1 remote replica.
      router.replicateBlob(blobIdStr, serviceId, sourceDataNode).get();

      // set unavailable to all local replicas. So router.getBlob will get the Blob from remote replicas.
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }
      router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();
      Assert.fail("Shouldn't come here.");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof RouterException);
      RouterException routerException = (RouterException) e.getCause();
      Assert.assertEquals(routerException.getErrorCode(), RouterErrorCode.BlobDeleted);
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();

      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test for replicating one TtlUpdated Blob. We should replicate both PutBlob and TtlUpdate
   * @throws Exception
   */
  @Test
  public void testReplicateTtlUpdatedBlob() throws Exception {
    try {
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      String serviceId = "replicate-blob-service";
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      setOperationParams();
      // PutBlob to the three local replicas
      String blobIdStr =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      router.updateBlobTtl(blobIdStr, serviceId, Utils.Infinite_Time);
      GetBlobResult localGetBlobResult =
          router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();

      // Replicate Blob from the local replica to some remote replicas.
      // choose one local replica as the source DataNode. Set the other two replicas to Replica_Unavailable
      DataNodeId sourceDataNode = null;
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
          }
        }
      }
      // since there is only one local available, it'll replicate to at least 1 remote replica.
      router.replicateBlob(blobIdStr, serviceId, sourceDataNode).get();

      // set unavailable to all local replicas. So router.getBlob will get the Blob from remote replicas.
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }
      GetBlobResult remoteGetBlobResult =
          router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();

      assertEqual(localGetBlobResult, remoteGetBlobResult);
      // check the ttl is updated on the remote replica.
      Assert.assertEquals(remoteGetBlobResult.getBlobInfo().getBlobProperties().getTimeToLiveInSeconds(),
          Utils.Infinite_Time);
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();

      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * compare the two {@link GetBlobResult} and assert they are equal.
   * @param result1 the first {@link GetBlobResult}
   * @param result2 the second {@link GetBlobResult}
   */
  void assertEqual(GetBlobResult result1, GetBlobResult result2) throws IOException {
    ReadableStreamChannel steamChannel1 = result1.getBlobDataChannel();
    ReadableStreamChannel steamChannel2 = result2.getBlobDataChannel();

    Assert.assertEquals(result1.getBlobInfo().getBlobProperties(), result2.getBlobInfo().getBlobProperties());
    Assert.assertTrue(Arrays.equals(result1.getBlobInfo().getUserMetadata(), result2.getBlobInfo().getUserMetadata()));

    long size = result1.getBlobInfo().getBlobProperties().getBlobSize();
    InputStream input1 = new ReadableStreamChannelInputStream(steamChannel1);
    InputStream input2 = new ReadableStreamChannelInputStream(steamChannel2);
    byte[] content1 = Utils.readBytesFromStream(input1, (int) size);
    byte[] content2 = Utils.readBytesFromStream(input2, (int) size);
    Assert.assertTrue(Arrays.equals(content1, content2));
  }

  /**
   * Test for ReplicateBlob when replica already has the Blob
   * replicateBlob should return success status
   * @throws Exception
   */
  @Test
  public void testReplicateBlobAlreadyExist() throws Exception {
    try {
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      String serviceId = "replicate-blob-service";
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      setOperationParams();
      // PutBlob to the three local replicas
      String blobIdStr =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();

      // Replicate Blob but target DataNode has the Blob already.
      DataNodeId sourceDataNode = null;
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrorForAllRequests(ServerErrorCode.BlobAlreadyExists);
          }
        } else {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }

      router.replicateBlob(blobIdStr, serviceId, sourceDataNode).get();
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test for ReplicateBlob success when globally we only have two replicas available.
   * @throws Exception
   */
  @Test
  public void testReplicateBlobMaxUnavailableAllowed() throws Exception {
    try {
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      String serviceId = "replicate-blob-service";
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      setOperationParams();
      // PutBlob to the three local replicas
      String blobIdStr =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      GetBlobResult localGetBlobResult =
          router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();

      // Only one local replica and one remote replica available.
      DataNodeId sourceDataNode = null;
      DataNodeId remoteDataNode = null;
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
          }
        } else {
          if (remoteDataNode == null) {
            remoteDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
          }
        }
      }
      router.replicateBlob(blobIdStr, serviceId, sourceDataNode).get();

      // set unavailable to all replicas except for that single remote replica
      for (MockServer server : layout.getMockServers()) {
        if (server.getHostName().equals(remoteDataNode.getHostname()) && server.getHostPort()
            .equals(remoteDataNode.getPort())) {
          server.setServerErrorForAllRequests(null);
        } else {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }

      GetBlobResult remoteGetBlobResult =
          router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();
      assertEqual(localGetBlobResult, remoteGetBlobResult);
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test for ReplicateBlob failure when there is no available target node.
   * @throws Exception
   */
  @Test
  public void testReplicateBlobFailureDueToNotEnoughQuorum() throws Exception {
    try {
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      String serviceId = "replicate-blob-service";
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      setOperationParams();
      // PutBlob to the three local replicas
      String blobIdStr =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      router.getBlob(blobIdStr, new GetBlobOptionsBuilder().build(), null, null).get();

      // Except for source host, all other replicas are down.
      layout.getMockServers()
          .forEach(mockServer -> mockServer.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable));
      DataNodeId sourceDataNode = null;
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          server.setServerErrorForAllRequests(null);
          break;
        }
      }
      router.replicateBlob(blobIdStr, serviceId, sourceDataNode).get();
      Assert.fail("Shouldn't come here. ");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof RouterException);
      RouterException routerException = (RouterException) e.getCause();
      Assert.assertEquals(routerException.getErrorCode(), RouterErrorCode.AmbryUnavailable);
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test for ReplicateBlob success case on CompositeBlob
   * Replicating one Blob to the remote replicas when two local replicas were unavailable.
   * @throws Exception
   */
  @Test
  public void testReplicateBlobOnCompositeBlob() throws Exception {
    try {
      // CompositeBlob with four chunks.
      maxPutChunkSize = PUT_CONTENT_SIZE / 4;
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      String serviceId = "replicate-blob-service";
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      setOperationParams();
      // PutBlob to the three local replicas
      String compositeBlobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      GetBlobResult localGetBlobResult =
          router.getBlob(compositeBlobId, new GetBlobOptionsBuilder().build(), null, null).get();
      BlobProperties localProperties = localGetBlobResult.getBlobInfo().getBlobProperties();

      // set error status for all the local replica. read from the remote replica, it should fail
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }

      try {
        router.getBlob(compositeBlobId, new GetBlobOptionsBuilder().build(), null, null).get();
        Assert.fail("Should return Ambry unavailable.");
      } catch (ExecutionException e) {
        Throwable t = e.getCause();
        assertTrue("Cause should be RouterException", t instanceof RouterException);
        assertEquals("ErrorCode mismatch", RouterErrorCode.AmbryUnavailable, ((RouterException) t).getErrorCode());
      }

      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));

      // Replicate Blob from the local replica to some remote replicas.
      // choose one local replica as the source DataNode. Set the other two replicas as Replica_Unavailable
      DataNodeId sourceDataNode = null;
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
          }
        }
      }
      // since there is only one local available, router will replicate the blob to at least 1 remote replicas.
      // Replicating the four chunks. Also replicate the composite metadata blob.
      GetBlobResult localGetChunkResult = router.getBlob(compositeBlobId,
              new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobChunkIds).build(), null, null)
          .get();
      List<StoreKey> chunkIds = localGetChunkResult.getBlobChunkIds();
      for (StoreKey chunk : chunkIds) {
        router.replicateBlob(chunk.getID(), serviceId, sourceDataNode).get();
      }
      router.replicateBlob(compositeBlobId, serviceId, sourceDataNode).get();

      // set unavailable to all local replicas. So router.getBlob will get the Blob from remote replicas.
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }
      GetBlobResult remoteGetBlobResult =
          router.getBlob(compositeBlobId, new GetBlobOptionsBuilder().build(), null, null).get();

      // verify BlobProperties
      Assert.assertEquals(remoteGetBlobResult.getBlobInfo().getBlobProperties(), localProperties);
      // verify userMetadata
      Assert.assertArrayEquals(remoteGetBlobResult.getBlobInfo().getUserMetadata(), putUserMetadata);
      // verify data content
      RetainingAsyncWritableChannel retainingAsyncWritableChannel = new RetainingAsyncWritableChannel();
      remoteGetBlobResult.getBlobDataChannel().readInto(retainingAsyncWritableChannel, null).get();
      InputStream input = retainingAsyncWritableChannel.consumeContentAsInputStream();
      retainingAsyncWritableChannel.close();
      byte[] content = Utils.readBytesFromStream(input, PUT_CONTENT_SIZE);
      input.close();
      Assert.assertArrayEquals(putContent, content);

      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();

      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test for ReplicateBlob failure when source is not available.
   * @throws Exception
   */
  @Test
  public void testReplicateBlobFailureDueToSourceNodeFailure() throws Exception {
    // ON_DEMAND_REPLICATION_TODO:
    // Ideally we want to simulate source node down and target fails to replicate the Blob.
    // But for mock server, we mock the response with its own error setting. We don't check other server's error status.
  }

  /**
   * Test that multiple scaling units can be instantiated, exercised and closed.
   */
  @Test
  public void testMultipleScalingUnit() throws Exception {
    try {
      final int SCALING_UNITS = 3;
      Properties props = getNonBlockingRouterProperties(localDcName);
      props.setProperty("router.scaling.unit.count", Integer.toString(SCALING_UNITS));
      setRouter(props, new MockServerLayout(mockClusterMap), new LoggingNotificationSystem());
      assertExpectedThreadCounts(SCALING_UNITS + 1, SCALING_UNITS);

      // Submit a few jobs so that all the scaling units get exercised.
      for (int i = 0; i < SCALING_UNITS * 10; i++) {
        setOperationParams();
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
      }
    } finally {
      if (router != null) {
        router.close();
        assertExpectedThreadCounts(0, 0);

        //submission after closing should return a future that is already done.
        setOperationParams();
        assertClosed();
      }
    }
  }

  /**
   * Test that Response Handler correctly handles disconnected connections after warming up.
   */
  @Test
  public void testWarmUpConnectionFailureHandling() throws Exception {
    try {
      Properties props = getNonBlockingRouterProperties("DC3");
      MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
      mockSelectorState.set(MockSelectorState.FailConnectionInitiationOnPoll);
      setRouter(props, mockServerLayout, new LoggingNotificationSystem());
      for (DataNodeId node : mockClusterMap.getDataNodes()) {
        assertTrue("Node should be marked as timed out by ResponseHandler.", ((MockDataNodeId) node).isTimedOut());
      }
    } finally {
      if (router != null) {
        router.close();
      }
      mockSelectorState.set(MockSelectorState.Good);
    }
  }

  /**
   * Test the case where request is timed out in the pending queue and network client returns response with null requestInfo
   * to mark node down via response handler.
   * @throws Exception
   */
  @Test
  public void testResponseWithNullRequestInfo() throws Exception {
    try {
      Properties props = getNonBlockingRouterProperties(localDcName);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      RouterConfig routerConfig = new RouterConfig(verifiableProperties);
      routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
      NetworkClient mockNetworkClient = Mockito.mock(NetworkClient.class);
      Mockito.when(mockNetworkClient.warmUpConnections(anyList(), anyInt(), anyLong(), anyList())).thenReturn(1);
      doNothing().when(mockNetworkClient).close();
      List<ResponseInfo> responseInfoList = new ArrayList<>();
      MockDataNodeId testDataNode = (MockDataNodeId) mockClusterMap.getDataNodeIds().get(0);
      responseInfoList.add(new ResponseInfo(null, NetworkClientErrorCode.NetworkError, null, testDataNode, false));
      // By default, there are 1 operation controller and 1 background deleter thread. We set CountDownLatch to 3 so that
      // at least one thread has completed calling onResponse() and test node's state has been updated in ResponseHandler
      CountDownLatch invocationLatch = new CountDownLatch(3);
      doAnswer(invocation -> {
        invocationLatch.countDown();
        return responseInfoList;
      }).when(mockNetworkClient).sendAndPoll(anyList(), anySet(), anyInt());
      NetworkClientFactory networkClientFactory = Mockito.mock(NetworkClientFactory.class);
      Mockito.when(networkClientFactory.getNetworkClient()).thenReturn(mockNetworkClient);
      router = new NonBlockingRouter(routerConfig, routerMetrics, networkClientFactory, new LoggingNotificationSystem(),
          mockClusterMap, kms, cryptoService, cryptoJobHandler, accountService, mockTime,
          MockClusterMap.DEFAULT_PARTITION_CLASS, null);
      assertTrue("Invocation latch didn't count to 0 within 10 seconds", invocationLatch.await(10, TimeUnit.SECONDS));
      // verify the test node is considered timeout
      assertTrue("The node should be considered timeout", testDataNode.isTimedOut());
    } finally {
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Response handling related tests for all operation managers.
   */
  @Test
  public void testResponseHandling() throws Exception {
    try {
      Properties props = getNonBlockingRouterProperties(localDcName);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      setOperationParams();
      final List<ReplicaId> failedReplicaIds = new ArrayList<>();
      final AtomicInteger successfulResponseCount = new AtomicInteger(0);
      final AtomicBoolean invalidResponse = new AtomicBoolean(false);
      ResponseHandler mockResponseHandler = new ResponseHandler(mockClusterMap) {
        @Override
        public void onEvent(ReplicaId replicaId, Object e) {
          if (e instanceof ServerErrorCode) {
            if (e == ServerErrorCode.NoError) {
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
        mockServer.setServerErrorForAllRequests(ServerErrorCode.NoError);
      }

      SocketNetworkClient networkClient =
          new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
              CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime).getNetworkClient();
      cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
      KeyManagementService localKMS =
          new MockKeyManagementService(new KMSConfig(verifiableProperties), singleKeyForKMS);
      putManager = new PutManager(mockClusterMap, mockResponseHandler, new LoggingNotificationSystem(),
          new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap, null),
          new RouterCallback(networkClient, new ArrayList<>()), "0", localKMS, cryptoService, cryptoJobHandler,
          accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, router);
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
          cryptoJobHandler, mockTime, null, router);
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
              new RouterCallback(null, new ArrayList<BackgroundDeleteRequest>()), mockTime, router);
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
    } finally {
      if (putManager != null) {
        putManager.close();
      }
      if (getManager != null) {
        getManager.close();
      }
      if (deleteManager != null) {
        deleteManager.close();
      }
    }
  }

  @Test
  public void testRouterMetadataCacheSimpleBlob() throws Exception {
    String blobId = null;
    try {
      Properties properties = getNonBlockingRouterProperties(localDcName);
      properties.setProperty("router.blob.metadata.cache.enabled", Boolean.toString(true));
      properties.setProperty("router.smallest.blob.for.metadata.cache", Long.toString(0));
      setRouterWithMetadataCache(properties, new AmbryCacheStats(1, 1, 1, 0));
      AmbryCacheWithStats ambryCacheWithStats = (AmbryCacheWithStats) router.getBlobMetadataCache();
      assertNotNull("Metadata cache must be created", ambryCacheWithStats);

      // put a simple blob and test it is absent in the metadata cache
      setOperationParams(PUT_CONTENT_SIZE, TTL_SECS);
      blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      // get the entire simple blob, and it's metadata must be absent from cache
      router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      // Metadata for simple blob must be absent from metadata cache
      assertNull("Blob metadata must be absent in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      // get the one byte from simple blob, and it's metadata must be absent from cache
      ByteRange range = ByteRanges.fromOffsetRange(1, 2);
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      // Metadata for simple blob must be absent from metadata cache
      assertNull("Blob metadata must be absent in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      // delete simple blob
      router.deleteBlob(blobId, null).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      // Metadata for simple blob must be absent from metadata cache
      assertNull("Blob metadata must be absent in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      try {
        // get the one byte from simple blob and it's metadata must be absent from cache
        router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        fail("Blob must be deleted");
      } catch (Exception e) {
        RouterException r = (RouterException) e.getCause();
        Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
        // Metadata for simple blob must be absent from metadata cache
        assertNull("Blob metadata must be absent in metadata cache", router.getBlobMetadataCache().getObject(blobId));
      }
    } finally {
      router.close();
    }
  }

  /**
   * Tests that with a metadata cache eviction works correctly with a cache size of 1.
   * @throws Exception
   */
  @Test
  public void testRouterMetadataCacheEviction() throws Exception {
    try {
      int NUM_PUT_COMPOSITE_BLOBS = 3;
      int NUM_GET_COMPOSITE_BLOBS = NUM_PUT_COMPOSITE_BLOBS * 5;
      int TEST_MAX_NUM_METADATA_CACHE_ENTRIES = 1;
      int numMisses = NUM_GET_COMPOSITE_BLOBS;
      int numInserts = NUM_GET_COMPOSITE_BLOBS;
      Properties properties = getNonBlockingRouterProperties(localDcName);
      properties.setProperty("router.blob.metadata.cache.enabled", Boolean.toString(true));
      properties.setProperty("router.smallest.blob.for.metadata.cache", Long.toString(0));
      // Set the size of cache to 1 entry which means the cache can store at max of one entry at any time
      properties.setProperty(RouterConfig.ROUTER_MAX_NUM_METADATA_CACHE_ENTRIES,
          Integer.toString(TEST_MAX_NUM_METADATA_CACHE_ENTRIES));
      setRouterWithMetadataCache(properties, new AmbryCacheStats(0, numMisses, numInserts, 0));
      AmbryCacheWithStats ambryCacheWithStats = (AmbryCacheWithStats) router.getBlobMetadataCache();
      assertNotNull("Metadata cache must be created", ambryCacheWithStats);
      assertEquals(
          String.format("Expected %s to be %s but found %s", RouterConfig.ROUTER_MAX_NUM_METADATA_CACHE_ENTRIES,
              TEST_MAX_NUM_METADATA_CACHE_ENTRIES, ambryCacheWithStats.getMaxNumCacheEntries()),
          TEST_MAX_NUM_METADATA_CACHE_ENTRIES, ambryCacheWithStats.getMaxNumCacheEntries());

      ArrayList<String> blobIds = new ArrayList<>();
      for (int i = 0; i < NUM_PUT_COMPOSITE_BLOBS; ++i) {
        // put few composite blob and test it is absent in the metadata cache
        setOperationParams(2 * PUT_CONTENT_SIZE, TTL_SECS);
        String blobId =
            router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
                .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        blobIds.add(blobId);
      }

      for (int i = 0; i < NUM_GET_COMPOSITE_BLOBS; ++i) {
        // cache miss and populate cache
        // get the one byte from composite blob, and it's metadata must be present in cache
        ByteRange range = ByteRanges.fromOffsetRange(1, 2);
        String blobId = blobIds.get(i % NUM_PUT_COMPOSITE_BLOBS);
        router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull("Blob metadata must be present in metadata cache",
            router.getBlobMetadataCache().getObject(blobId));
      }

      assertTrue(String.format("Must encounter %s cache misses", numMisses),
          ambryCacheWithStats.getCacheMissCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertTrue(String.format("Must encounter %s cache inserts", numInserts),
          ambryCacheWithStats.getPutObjectCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    } finally {
      router.close();
    }
  }

  /**
   * Tests that there no metadata cache evictions with a large enough cache.
   * @throws Exception
   */
  @Test
  public void testRouterMetadataCacheNoEviction() throws Exception {
    try {
      int NUM_PUT_COMPOSITE_BLOBS = 3;
      int NUM_GET_COMPOSITE_BLOBS = NUM_PUT_COMPOSITE_BLOBS * 5;
      int TEST_MAX_NUM_METADATA_CACHE_ENTRIES = 10;
      int numHits = NUM_GET_COMPOSITE_BLOBS - NUM_PUT_COMPOSITE_BLOBS;
      int numMisses = NUM_PUT_COMPOSITE_BLOBS;
      int numInserts = NUM_PUT_COMPOSITE_BLOBS;
      Properties properties = getNonBlockingRouterProperties(localDcName);
      properties.setProperty("router.blob.metadata.cache.enabled", Boolean.toString(true));
      properties.setProperty("router.smallest.blob.for.metadata.cache", Long.toString(0));
      // Set the size of cache to 1 entry which means the cache can store at max of one entry at any time
      properties.setProperty(RouterConfig.ROUTER_MAX_NUM_METADATA_CACHE_ENTRIES,
          Integer.toString(TEST_MAX_NUM_METADATA_CACHE_ENTRIES));
      setRouterWithMetadataCache(properties, new AmbryCacheStats(numHits, numMisses, numInserts, 0));
      AmbryCacheWithStats ambryCacheWithStats = (AmbryCacheWithStats) router.getBlobMetadataCache();
      assertNotNull("Metadata cache must be created", ambryCacheWithStats);
      assertEquals(
          String.format("Expected %s to be %s but found %s", RouterConfig.ROUTER_MAX_NUM_METADATA_CACHE_ENTRIES,
              TEST_MAX_NUM_METADATA_CACHE_ENTRIES, ambryCacheWithStats.getMaxNumCacheEntries()),
          TEST_MAX_NUM_METADATA_CACHE_ENTRIES, ambryCacheWithStats.getMaxNumCacheEntries());

      ArrayList<String> blobIds = new ArrayList<>();
      for (int i = 0; i < NUM_PUT_COMPOSITE_BLOBS; ++i) {
        // put few composite blob and test it is absent in the metadata cache
        setOperationParams(2 * PUT_CONTENT_SIZE, TTL_SECS);
        String blobId =
            router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
                .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        blobIds.add(blobId);
      }

      for (int i = 0; i < NUM_GET_COMPOSITE_BLOBS; ++i) {
        // cache miss and populate cache
        // get the one byte from composite blob, and it's metadata must be present in cache
        ByteRange range = ByteRanges.fromOffsetRange(1, 2);
        String blobId = blobIds.get(i % NUM_PUT_COMPOSITE_BLOBS);
        router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull("Blob metadata must be present in metadata cache",
            router.getBlobMetadataCache().getObject(blobId));
      }

      assertTrue(String.format("Must encounter %s cache hits", numHits),
          ambryCacheWithStats.getCacheHitCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertTrue(String.format("Must encounter %s cache misses", numMisses),
          ambryCacheWithStats.getCacheMissCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertTrue(String.format("Must encounter %s cache inserts", numInserts),
          ambryCacheWithStats.getPutObjectCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    } finally {
      router.close();
    }
  }

  @Test
  public void testRouterMetadataCacheGetCompositeBlob() throws Exception {
    try {
      Properties properties = getNonBlockingRouterProperties(localDcName);
      properties.setProperty("router.blob.metadata.cache.enabled", Boolean.toString(true));
      properties.setProperty("router.smallest.blob.for.metadata.cache", Long.toString(0));
      setRouterWithMetadataCache(properties, new AmbryCacheStats(1, 1, 1, 0));
      AmbryCacheWithStats ambryCacheWithStats = (AmbryCacheWithStats) router.getBlobMetadataCache();
      assertNotNull("Metadata cache must be created", ambryCacheWithStats);

      // put a composite blob and test it is absent in the metadata cache
      setOperationParams(2 * PUT_CONTENT_SIZE, TTL_SECS);
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      // get the entire composite blob, and it's metadata must be absent from cache
      router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertNull("Blob metadata must be absent in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      if (testEncryption) {
        // get the entire composite blob in raw mode, and it's metadata must be absent from cache
        router.getBlob(blobId, new GetBlobOptionsBuilder().rawMode(true).build())
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertNull("Blob metadata must be absent in metadata cache", router.getBlobMetadataCache().getObject(blobId));
      }

      // get one segment of composite blob, and it's metadata must be absent from cache
      router.getBlob(blobId, new GetBlobOptionsBuilder().blobSegment(1).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertNull("Blob metadata must be absent in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      // cache miss and populate cache
      // get the one byte from composite blob, and it's metadata must be present in cache
      ByteRange range = ByteRanges.fromOffsetRange(1, 2);
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Must encounter a cache miss",
          ambryCacheWithStats.getCacheMissCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertTrue("Must put metadata in cache",
          ambryCacheWithStats.getPutObjectCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertNotNull("Blob metadata must be present in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      // cache hit
      // get one byte from composite blob, and it's metadata must be present in cache
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Must encounter a cache hit",
          ambryCacheWithStats.getCacheHitCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertNotNull("Blob metadata must be present in metadata cache", router.getBlobMetadataCache().getObject(blobId));
    } finally {
      router.close();
    }
  }

  @Test
  public void testRouterMetadataCacheDeleteCompositeBlob() throws Exception {
    try {
      Properties properties = getNonBlockingRouterProperties(localDcName);
      properties.setProperty("router.blob.metadata.cache.enabled", Boolean.toString(true));
      properties.setProperty("router.smallest.blob.for.metadata.cache", Long.toString(0));
      setRouterWithMetadataCache(properties, new AmbryCacheStats(1, 1, 1, 1));
      AmbryCacheWithStats ambryCacheWithStats = (AmbryCacheWithStats) router.getBlobMetadataCache();
      assertNotNull("Metadata cache must be created", ambryCacheWithStats);

      // put a composite blob and test it is absent in the metadata cache
      setOperationParams(2 * PUT_CONTENT_SIZE, TTL_SECS);
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      // get the one byte from composite blob, and it's metadata must be present in cache
      ByteRange range = ByteRanges.fromOffsetRange(1, 2);
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Must encounter a cache miss",
          ambryCacheWithStats.getCacheMissCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertTrue("Must put metadata in cache",
          ambryCacheWithStats.getPutObjectCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertNotNull("Blob metadata must be present in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      // delete composite blob
      router.deleteBlob(blobId, null).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Must encounter a cache hit",
          ambryCacheWithStats.getCacheHitCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertTrue("Must delete metadata in cache",
          ambryCacheWithStats.getDeleteObjectCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertNull("Blob metadata must be absent in metadata cache", router.getBlobMetadataCache().getObject(blobId));
    } finally {
      router.close();
    }
  }

  @Test
  public void testRouterMetadataCacheUpdateTTLCompositeBlob() throws Exception {
    try {
      Properties properties = getNonBlockingRouterProperties(localDcName);
      properties.setProperty("router.blob.metadata.cache.enabled", Boolean.toString(true));
      properties.setProperty("router.smallest.blob.for.metadata.cache", Long.toString(0));
      setRouterWithMetadataCache(properties, new AmbryCacheStats(1, 1, 1, 1));
      AmbryCacheWithStats ambryCacheWithStats = (AmbryCacheWithStats) router.getBlobMetadataCache();
      assertNotNull("Metadata cache must be created", ambryCacheWithStats);

      // put a composite blob and test it is absent in the metadata cache
      setOperationParams(2 * PUT_CONTENT_SIZE, TTL_SECS);
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTtl(router, Collections.singleton(blobId), TTL_SECS);

      // get one byte from composite blob, and it's metadata must be present in cache
      ByteRange range = ByteRanges.fromOffsetRange(1, 2);
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Must encounter a cache miss",
          ambryCacheWithStats.getCacheMissCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertTrue("Must put metadata in cache",
          ambryCacheWithStats.getPutObjectCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertNotNull("Blob metadata must be present in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      // update TTL and make it permanent
      router.updateBlobTtl(blobId, null, Utils.Infinite_Time).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTtl(router, Collections.singleton(blobId), Utils.Infinite_Time);

      // get one byte from composite blob, and it's metadata must be present in cache
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Must encounter a cache hit",
          ambryCacheWithStats.getCacheHitCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertNotNull("Blob metadata must be present in metadata cache", router.getBlobMetadataCache().getObject(blobId));
    } finally {
      router.close();
    }
  }

  void routerMetadataCacheErrorOnGetCompositeBlob(ServerErrorCode serverErrorCode, RouterErrorCode routerErrorCode,
      boolean deleteCacheEntryOnFail) throws Exception {
    try {
      Properties properties = getNonBlockingRouterProperties(localDcName);
      properties.setProperty("router.blob.metadata.cache.enabled", Boolean.toString(true));
      properties.setProperty("router.smallest.blob.for.metadata.cache", Long.toString(0));
      setRouterWithMetadataCache(properties, new AmbryCacheStats(1, 1, 1, 1));
      AmbryCacheWithStats ambryCacheWithStats = (AmbryCacheWithStats) router.getBlobMetadataCache();
      assertNotNull("Metadata cache must be created", ambryCacheWithStats);

      // put a composite blob and test it is absent in the metadata cache
      setOperationParams(2 * PUT_CONTENT_SIZE, TTL_SECS);
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      // successful get
      ByteRange range = ByteRanges.fromOffsetRange(1, 2);
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Must encounter a cache miss",
          ambryCacheWithStats.getCacheMissCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertTrue("Must put metadata in cache",
          ambryCacheWithStats.getPutObjectCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertNotNull("Blob metadata must be present in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      // failed get
      mockServerLayout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(serverErrorCode));
      TestCallback<GetBlobResult> testCallback = new TestCallback<>();
      Future<GetBlobResult> future =
          router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build(), testCallback, null);
      assertFailureAndCheckErrorCode(future, testCallback, routerErrorCode);
      assertTrue("Must encounter a cache hit",
          ambryCacheWithStats.getCacheHitCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      if (deleteCacheEntryOnFail) {
        assertTrue("Must delete metadata in cache",
            ambryCacheWithStats.getDeleteObjectCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
        assertNull("Blob metadata must be absent in metadata cache", router.getBlobMetadataCache().getObject(blobId));
      } else {
        assertNotNull("Blob metadata must be present in metadata cache",
            router.getBlobMetadataCache().getObject(blobId));
      }
      mockServerLayout.getMockServers().forEach(mockServer -> mockServer.resetServerErrors());
      // There should be flag to disable this cache !
      router.getNotFoundCache().invalidateAll();

      // successful get, cache must repopulate
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertNotNull("Blob metadata must be present in metadata cache", router.getBlobMetadataCache().getObject(blobId));
    } finally {
      router.close();
    }
  }

  @Test
  public void testRouterMetadataCacheBlobDoesNotExistOnGetCompositeBlob() throws Exception {
    routerMetadataCacheErrorOnGetCompositeBlob(ServerErrorCode.BlobNotFound, RouterErrorCode.BlobDoesNotExist, true);
  }

  @Test
  public void testRouterMetadataCacheBlobDeletedOnGetCompositeBlob() throws Exception {
    routerMetadataCacheErrorOnGetCompositeBlob(ServerErrorCode.BlobDeleted, RouterErrorCode.BlobDeleted, true);
  }

  @Test
  public void testRouterMetadataCacheBlobExpiredOnGetCompositeBlob() throws Exception {
    routerMetadataCacheErrorOnGetCompositeBlob(ServerErrorCode.BlobExpired, RouterErrorCode.BlobExpired, true);
  }

  @Test
  public void testRouterMetadataCacheUnexpectedInternalErrorOnGetCompositeBlob() throws Exception {
    routerMetadataCacheErrorOnGetCompositeBlob(ServerErrorCode.UnknownError, RouterErrorCode.UnexpectedInternalError,
        false);
  }

  void routerMetadataCacheErrorOnDeleteCompositeBlob(ServerErrorCode serverErrorCode, RouterErrorCode routerErrorCode)
      throws Exception {
    try {
      Properties properties = getNonBlockingRouterProperties(localDcName);
      properties.setProperty("router.blob.metadata.cache.enabled", Boolean.toString(true));
      properties.setProperty("router.smallest.blob.for.metadata.cache", Long.toString(0));
      setRouterWithMetadataCache(properties, new AmbryCacheStats(1, 1, 1, 1));
      AmbryCacheWithStats ambryCacheWithStats = (AmbryCacheWithStats) router.getBlobMetadataCache();
      assertNotNull("Metadata cache must be created", ambryCacheWithStats);

      // put a composite blob and test it is absent in the metadata cache
      setOperationParams(2 * PUT_CONTENT_SIZE, TTL_SECS);
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      // successful get
      ByteRange range = ByteRanges.fromOffsetRange(1, 2);
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Must encounter a cache miss",
          ambryCacheWithStats.getCacheMissCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertTrue("Must put metadata in cache",
          ambryCacheWithStats.getPutObjectCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertNotNull("Blob metadata must be present in metadata cache", router.getBlobMetadataCache().getObject(blobId));

      // failed delete
      mockServerLayout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(serverErrorCode));
      TestCallback<Void> testCallback = new TestCallback<>();
      Future<Void> future = router.deleteBlob(null, blobId, null, testCallback, null);
      assertFailureAndCheckErrorCode(future, testCallback, routerErrorCode);
      assertNotNull("Blob metadata must be present in metadata cache", router.getBlobMetadataCache().getObject(blobId));
      mockServerLayout.getMockServers().forEach(mockServer -> mockServer.resetServerErrors());
      // There should be flag to disable this cache !
      router.getNotFoundCache().invalidateAll();

      // successful get, cache must be intact
      router.getBlob(blobId, new GetBlobOptionsBuilder().range(range).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue("Must encounter a cache hit",
          ambryCacheWithStats.getCacheHitCountDown().await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    } finally {
      router.close();
    }
  }

  @Test
  public void testRouterMetadataCacheUnexpectedInternalErrorOnDeleteCompositeBlob() throws Exception {
    routerMetadataCacheErrorOnDeleteCompositeBlob(ServerErrorCode.UnknownError,
        RouterErrorCode.UnexpectedInternalError);
  }

  @Test
  public void testRouterMetadataCacheBlobDoesNotExistOnDeleteCompositeBlob() throws Exception {
    routerMetadataCacheErrorOnDeleteCompositeBlob(ServerErrorCode.BlobNotFound, RouterErrorCode.BlobDoesNotExist);
  }

  /**
   * Test that blobs not found are cached and we don't get them from servers again
   * @throws Exception
   */
  @Test
  public void testBlobNotFoundCache() throws Exception {
    try {
      String updateServiceId = "update-service";
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      setOperationParams();
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      Map<ServerErrorCode, RouterErrorCode> testsAndExpected = new HashMap<>();
      testsAndExpected.put(ServerErrorCode.BlobNotFound, RouterErrorCode.BlobDoesNotExist);

      layout.getMockServers()
          .forEach(mockServer -> mockServer.setServerErrorForAllRequests(ServerErrorCode.BlobNotFound));
      TestCallback<Void> testCallback = new TestCallback<>();
      Future<Void> future =
          router.updateBlobTtl(null, blobId, updateServiceId, Utils.Infinite_Time, testCallback, null);
      assertFailureAndCheckErrorCode(future, testCallback, RouterErrorCode.BlobDoesNotExist);

      // Verify that not-found cache is populated with the blob ID.
      assertNotNull("Cache should contain the blob ID", router.getNotFoundCache().getIfPresent(blobId));

      // Reset server error but we should still receive error since the ID is present in cache
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));

      // Verify that operations on the same blob ID are retrieved from cache.
      TestCallback<GetBlobResult> getTestCallback = new TestCallback<>();
      Future<GetBlobResult> getBlobFuture =
          router.getBlob(blobId, new GetBlobOptionsBuilder().build(), getTestCallback, null);
      assertFailureAndCheckErrorCode(getBlobFuture, getTestCallback, RouterErrorCode.BlobDoesNotExist);

      TestCallback<GetBlobResult> getBlobInfoTestCallback = new TestCallback<>();
      Future<GetBlobResult> getBlobInfoFuture = router.getBlob(blobId,
          new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build(),
          getBlobInfoTestCallback, null);
      assertFailureAndCheckErrorCode(getBlobInfoFuture, getBlobInfoTestCallback, RouterErrorCode.BlobDoesNotExist);

      TestCallback<Void> ttlUpdateTestCallback = new TestCallback<>();
      Future<Void> ttlUpdateFuture =
          router.updateBlobTtl(null, blobId, updateServiceId, Utils.Infinite_Time, ttlUpdateTestCallback, null);
      assertFailureAndCheckErrorCode(ttlUpdateFuture, ttlUpdateTestCallback, RouterErrorCode.BlobDoesNotExist);

      TestCallback<Void> deleteTestCallback = new TestCallback<>();
      Future<Void> deleteFuture = router.deleteBlob(null, blobId, updateServiceId, deleteTestCallback, null);
      assertFailureAndCheckErrorCode(deleteFuture, deleteTestCallback, RouterErrorCode.BlobDoesNotExist);

      TestCallback<Void> unDeleteTestCallback = new TestCallback<>();
      Future<Void> unDeleteFuture = router.undeleteBlob(blobId, updateServiceId, unDeleteTestCallback, null);
      assertFailureAndCheckErrorCode(unDeleteFuture, unDeleteTestCallback, RouterErrorCode.BlobDoesNotExist);

      // wait for auto expiration of blob ID in cache
      Thread.sleep(NOT_FOUND_CACHE_TTL_MS + 100);
      router.updateBlobTtl(blobId, null, Utils.Infinite_Time).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTtl(router, Collections.singleton(blobId), Utils.Infinite_Time);

      router.deleteBlob(blobId, null).get();
      try {
        router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
        fail("Get blob should fail");
      } catch (ExecutionException e) {
        RouterException r = (RouterException) e.getCause();
        Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
      }
    } finally {
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test get blob chunk Ids for simple blob.
   * @throws Exception
   */
  @Test
  public void testGetBlobChunkIdsForSimpleBlob() throws Exception {
    try {
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      int numberOfChunks = 1;
      setOperationParams(numberOfChunks * PUT_CONTENT_SIZE, TTL_SECS);
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      GetBlobResult result = router.getBlob(blobId,
              new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobChunkIds).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertNull("Blob chunk Ids should be null for simple blob", result.getBlobChunkIds());
    } finally {
      router.close();
    }
  }

  /**
   * Test get blob chunk Ids for composite blob.
   * @throws Exception
   */
  @Test
  public void testGetBlobChunkIdsForCompositeBlob() throws Exception {
    try {
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterProperties(localDcName), layout, new LoggingNotificationSystem());
      int numberOfChunks = 4;
      setOperationParams(numberOfChunks * PUT_CONTENT_SIZE, TTL_SECS);
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      GetBlobResult result = router.getBlob(blobId,
              new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobChunkIds).build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertEquals("Number of blob chunk Ids mismatch", numberOfChunks, result.getBlobChunkIds().size());
    } finally {
      router.close();
    }
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

  private RestRequest createNamedBlobRestRequest(RestMethod method, String accountName, String containerName,
      String blobName, boolean isTtlUpdate, boolean isTemporary, boolean isStitchRequest) throws Exception {
    JSONObject headers = new JSONObject();
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, method.name());
    if (isTtlUpdate) {
      headers.put(BLOB_ID,
          String.format("/%s/%s/%s/%s", Operations.NAMED_BLOB, accountName, containerName, blobName));
      request.put(MockRestRequest.URI_KEY, Operations.UPDATE_TTL);
    } else if (isStitchRequest) {
      headers.put(BLOB_ID,
          String.format("/%s/%s/%s/%s", Operations.NAMED_BLOB, accountName, containerName, blobName));
      request.put(MockRestRequest.URI_KEY, Operations.STITCH);
    } else {
      if (isTemporary) {
        headers.put(RestUtils.Headers.TTL, TTL_SECS);
      }
      headers.put(RestUtils.Headers.NAMED_UPSERT, true);
      request.put(MockRestRequest.URI_KEY,
          String.format("/%s/%s/%s/%s", Operations.NAMED_BLOB, accountName, containerName, blobName));
    }
    request.put(MockRestRequest.HEADERS_KEY, headers);
    MockRestRequest restRequest = new MockRestRequest(request, null);
    restRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(restRequest, Collections.emptyList(), null));
    return restRequest;
  }

  /**
   * TtlUpdate first hits 503. Then it's either fixed by ODR or by offline repair.
   * @param enableODR enable On demand replication
   * @param enableOfflineRepair enable the offline repair
   * @return a pair of the blob id and source replica which returns the success status.
   * @throws Exception
   */
  public Pair<BlobId, DataNodeId> blobTtlUpdate503AndThenFixedByODROrOffline(boolean enableODR,
      boolean enableOfflineRepair) throws Exception {
    String updateServiceId = "update-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, enableODR, enableOfflineRepair, false),
        layout, new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertTtl(router, Collections.singleton(blobId), TimeUnit.DAYS.toSeconds(7));

    // set two replicas Replica_Unavailable.
    DataNodeId sourceDataNode = null;
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        // local DC, return NO_ERROR for one replica,
        if (sourceDataNode == null) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
        } else {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }
    }

    try {
      router.updateBlobTtl(blobId, updateServiceId, Utils.Infinite_Time).get();
      if (!enableODR && !enableOfflineRepair) {
        fail("Should fail with un-available if ODR and offline repair are disabled.");
      }
    } catch (Exception e) {
      if (!enableODR && !enableOfflineRepair) {
        RouterException r = (RouterException) e.getCause();
        Assert.assertEquals(RouterErrorCode.AmbryUnavailable, r.getErrorCode());
      } else {
        fail("Should be successful when ODR or offline repair is enabled.");
      }
    }

    // simulate Replica_Unavailable for all local replicas. Then verify ttl from the remote replica
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
      }
    }
    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build(), null, null).get();
      if (enableODR) {
        assertTtl(router, Collections.singleton(blobId), Utils.Infinite_Time);
      } else {
        fail("Should return AmbryUnavailable if ODR is not enabled.");
      }
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      if (!enableODR) {
        Assert.assertEquals("GetBlob error is not expected", RouterErrorCode.AmbryUnavailable, r.getErrorCode());
      } else {
        fail("Shouldn't hit exception when ODR is enabled.");
      }
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }

    return new Pair<>(RouterUtils.getBlobIdFromString(blobId, mockClusterMap), sourceDataNode);
  }

  /**
   * Test TTLUpdate hits 503. Do nothing and fail.
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdate503AndThenFail() throws Exception {
    blobTtlUpdate503AndThenFixedByODROrOffline(false, false);
  }

  /**
   * Test TTLUpdate first hits 503. Then run ReplicateBlob and Retry. It is successful.
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdate503AndThenODRSuccess() throws Exception {
    blobTtlUpdate503AndThenFixedByODROrOffline(true, false);
  }

  /**
   * Test TTLUpdate first hits 503. Then run offline Repair. It is successful. Database has the record.
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdate503AndOfflineRepair() throws Exception {
    cleanRepairRequestsDb(repairDb);
    Pair<BlobId, DataNodeId> result = blobTtlUpdate503AndThenFixedByODROrOffline(false, true);
    BlobId blobId = result.getFirst();
    DataNodeId sourceDataNode = result.getSecond();
    long operationTime = SystemTime.getInstance().milliseconds();
    RepairRequestRecord expectedRecord =
        new RepairRequestRecord(blobId.getID(), blobId.getPartition().getId(), sourceDataNode.getHostname(),
            sourceDataNode.getPort(), RepairRequestRecord.OperationType.TtlUpdateRequest, operationTime,
            LIFE_VERSION_FROM_FRONTEND, Utils.Infinite_Time);
    verifyRepairRequestRecordInDb(repairDb, blobId, expectedRecord);
    cleanRepairRequestsDb(repairDb);
  }

  /**
   * Test TTLUpdate first hits 503. Enable both ODR and offline repair. But only ODR takes effect. No db record.
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdate503AndThenODRAndOffRepair() throws Exception {
    cleanRepairRequestsDb(repairDb);
    Pair<BlobId, DataNodeId> result = blobTtlUpdate503AndThenFixedByODROrOffline(true, true);
    BlobId blobId = result.getFirst();
    verifyRepairRequestRecordInDb(repairDb, blobId, null);
  }

  /**
   * CompositeBlob TTLUpdate first hits 503. Then run ReplicateBlob and Retry. It is successful.
   * @throws Exception
   */
  @Test
  public void testCompositeBlobTtlUpdate503AndThenRetrySuccess() throws Exception {
    try {
      // CompositeBlob with four chunks.
      maxPutChunkSize = PUT_CONTENT_SIZE / 4;
      String updateServiceId = "update-service";
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
          new LoggingNotificationSystem());
      setOperationParams();
      String compositeBlobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTtl(router, Collections.singleton(compositeBlobId), TimeUnit.DAYS.toSeconds(7));

      // set two replicas Replica_Unavailable. Leave one local replica available as the source of the ReplicationBlob
      DataNodeId sourceDataNode = null;
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          // local DC, return NO_ERROR for one replica,
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
          }
        }
      }
      router.updateBlobTtl(compositeBlobId, updateServiceId, Utils.Infinite_Time).get();

      // simulate Replica_Unavailable for all local replicas. Then verify ttl from the remote replica
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }
      assertTtl(router, Collections.singleton(compositeBlobId), Utils.Infinite_Time);

      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();

      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * CompositeBlob TTLUpdate first hits 503. Then run ReplicateBlob and Retry.
   * ReplicateBlob fails and then TtlUpdate fails.
   * @throws Exception
   */
  @Test
  public void testCompositeBlobTtlUpdate503AndThenRetryFailure() throws Exception {
    try {
      // CompositeBlob with four chunks.
      maxPutChunkSize = PUT_CONTENT_SIZE / 4;
      String updateServiceId = "update-service";
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
          new LoggingNotificationSystem());
      setOperationParams();
      String compositeBlobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTtl(router, Collections.singleton(compositeBlobId), TimeUnit.DAYS.toSeconds(7));

      // set two replicas Replica_Unavailable. Leave one local replica available as the source of the ReplicationBlob
      // But all remote replicas fail the ReplicateBlob.
      DataNodeId sourceDataNode = null;
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          // local DC, return NO_ERROR for one replica,
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
          }
        } else {
          server.setServerErrorForAllRequests(ServerErrorCode.BlobNotFound);
        }
      }
      try {
        router.updateBlobTtl(compositeBlobId, updateServiceId, Utils.Infinite_Time).get();
      } catch (Exception e) {
        assertTrue(e.getCause() instanceof RouterException);
        assertEquals(RouterErrorCode.AmbryUnavailable, ((RouterException) e.getCause()).getErrorCode());
      }
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();

      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test TTLUpdate first hits Not_FOUND. Then run ReplicateBlob and Retry. It is successful.
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdate404AndThenRetrySuccess() throws Exception {
    try {
      String updateServiceId = "update-service";
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
          new LoggingNotificationSystem());
      setOperationParams();
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTtl(router, Collections.singleton(blobId), TimeUnit.DAYS.toSeconds(7));

      // for the three local replicas, two replicas return Blob_Not_Found due to host swap.
      DataNodeId sourceDataNode = null;
      List<ServerErrorCode> serverErrors = new ArrayList<>();
      serverErrors.add(ServerErrorCode.BlobNotFound); // return NOT_FOUND for the first TtlUpdate Request
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          // local DC, return NO_ERROR for one replica,
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
          } else {
            server.setServerErrors(serverErrors);
          }
        }
      }
      router.updateBlobTtl(blobId, updateServiceId, Utils.Infinite_Time).get();

      // simulate Replica_Unavailable for the source host node. Read from the other two.
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      for (MockServer server : layout.getMockServers()) {
        if (server.getHostName().equals(sourceDataNode.getHostname()) && server.getHostPort()
            .equals(sourceDataNode.getPort())) {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }
      assertTtl(router, Collections.singleton(blobId), Utils.Infinite_Time);

      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();

      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test TTLUpdate first hits 503. The following ReplicateBlob failed.
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdate503ReplicateBlobFailure() throws Exception {
    try {
      String updateServiceId = "update-service";
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
          new LoggingNotificationSystem());
      setOperationParams();
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      // Since 1 success, 1 Blob_Not_Found, 1 Disk_Unavailable
      boolean oneSuccess = false;
      boolean oneDiskUnavailable = false;
      List<ServerErrorCode> serverErrors = new ArrayList<>();
      serverErrors.add(ServerErrorCode.BlobNotFound); // return NOT_FOUND for the first TtlUpdate Request
      serverErrors.add(ServerErrorCode.UnknownError); // return Unknown_Error for the ReplicateBlob Request
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          // local DC, return NO_ERROR for one replica and Disk unavailable for another replica.
          if (!oneSuccess) {
            oneSuccess = true;
          } else if (!oneDiskUnavailable) {
            server.setServerErrors(Arrays.asList(ServerErrorCode.DiskUnavailable, ServerErrorCode.UnknownError));
            oneDiskUnavailable = true;
          } else {
            server.setServerErrors(serverErrors);
          }
        } else {
          server.setServerErrors(serverErrors);
        }
      }

      TestCallback<Void> testCallback = new TestCallback<>();
      Future<Void> future =
          router.updateBlobTtl(null, blobId, updateServiceId, Utils.Infinite_Time, testCallback, null);
      // It should return the error code of the TtlUpdate which is AmbryUnavailable,
      // shouldn't return the error code of the ReplicateBlob which is Unknown_Error.
      assertFailureAndCheckErrorCode(future, testCallback, RouterErrorCode.AmbryUnavailable);

      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test TTLUpdate first hits 503. The following ReplicateBlob is successful but retry fails.
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdate503ReplicateBlobSuccessRetryFailure() throws Exception {
    try {
      String updateServiceId = "update-service";
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
          new LoggingNotificationSystem());
      setOperationParams();
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      // set error status for all the replicas except one local replica
      boolean oneSuccess = false;
      List<ServerErrorCode> serverErrors = new ArrayList<>();
      serverErrors.add(ServerErrorCode.BlobNotFound); // for the first TtlUpdate request
      serverErrors.add(ServerErrorCode.NoError); // for the ReplicateBlob request
      serverErrors.add(ServerErrorCode.ReplicaUnavailable); // for the TtlUpdate retry
      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          // local DC, return NO_ERROR for one replica,
          if (!oneSuccess) {
            oneSuccess = true;
          } else {
            server.setServerErrors(serverErrors);
          }
        }
      }

      TestCallback<Void> testCallback = new TestCallback<>();
      Future<Void> future =
          router.updateBlobTtl(null, blobId, updateServiceId, Utils.Infinite_Time, testCallback, null);
      assertFailureAndCheckErrorCode(future, testCallback, RouterErrorCode.AmbryUnavailable);

      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test TTLUpdate hits 503. But since there is no single replica is successful, won't do ReplicateBlob and retry
   * @throws Exception
   */
  @Test
  public void testBlobTtlUpdate503NoRetrySinceNoSuccessReplica() throws Exception {
    try {
      String updateServiceId = "update-service";
      MockServerLayout layout = new MockServerLayout(mockClusterMap);
      setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
          new LoggingNotificationSystem());
      setOperationParams();
      String blobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      for (MockServer server : layout.getMockServers()) {
        if (server.getDataCenter().equals(localDcName)) {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      }

      TestCallback<Void> testCallback = new TestCallback<>();
      Future<Void> future =
          router.updateBlobTtl(null, blobId, updateServiceId, Utils.Infinite_Time, testCallback, null);
      assertFailureAndCheckErrorCode(future, testCallback, RouterErrorCode.AmbryUnavailable);

      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
    } finally {
      // Since we are using same blob ID for all tests, clear cache which stores blobs that are not found in router.
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Deletion first hits 503. Then it's either fixed by ODR or by offline repair.
   * @param enableODR enable On demand replication
   * @param enableOfflineRepair enable the offline repair
   * @return a pair of the blob id and source replica which returns the success status.
   * @throws Exception
   */
  public Pair<BlobId, DataNodeId> blobDelete503AndThenFixedByODROrOffline(boolean enableODR,
      boolean enableOfflineRepair) throws Exception {
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = "DC3";
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, enableODR, enableOfflineRepair, false),
        layout, new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // set two local replicas Replica_Unavailable.
    DataNodeId sourceDataNode = null;
    List<ServerErrorCode> serverErrors = new ArrayList<>();
    serverErrors.add(ServerErrorCode.BlobNotFound); // return NOT_FOUND for the first Delete Request
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        // local DC, return NO_ERROR for one replica,
        if (sourceDataNode == null) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
        } else {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      } else {
        server.setServerErrors(serverErrors);
      }
    }
    try {
      router.deleteBlob(blobId, serviceID).get();
      if (!enableODR && !enableOfflineRepair) {
        fail("Should fail with un-available if ODR and offline repair are disabled.");
      }
    } catch (Exception e) {
      if (!enableODR && !enableOfflineRepair) {
        RouterException r = (RouterException) e.getCause();
        Assert.assertEquals(RouterErrorCode.AmbryUnavailable, r.getErrorCode());
      } else {
        fail("Should be successful when ODR or offline repair is enabled.");
      }
    }

    // simulate Replica_Unavailable for all local replicas. Then verify delete from the remote replica
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
      }
    }
    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build(), null, null).get();
      fail("Blob must be deleted");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      if (!enableODR) {
        Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.AmbryUnavailable, r.getErrorCode());
      } else {
        Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
      }
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }

    return new Pair<>(RouterUtils.getBlobIdFromString(blobId, mockClusterMap), sourceDataNode);
  }

  /**
   * Deletion hits 503 and fail.
   * @throws Exception
   */
  @Test
  public void testBlobDelete503AndThenNothing() throws Exception {
    blobDelete503AndThenFixedByODROrOffline(false, false);
  }

  /**
   * Deletion hits 503 and then run ODR and retry successfully.
   * @throws Exception
   */
  @Test
  public void testBlobDelete503AndThenODRSuccess() throws Exception {
    blobDelete503AndThenFixedByODROrOffline(true, false);
  }

  /**
   * Deletion hits 503 and then trigger offline repair. Delete returns success status.
   * @throws Exception
   */
  @Test
  public void testBlobDelete503AndThenOfflineRepair() throws Exception {
    cleanRepairRequestsDb(repairDb);
    Pair<BlobId, DataNodeId> result = blobDelete503AndThenFixedByODROrOffline(false, true);
    BlobId blobId = result.getFirst();
    DataNodeId sourceDataNode = result.getSecond();
    long operationTime = SystemTime.getInstance().milliseconds();
    RepairRequestRecord expectedRecord =
        new RepairRequestRecord(blobId.getID(), blobId.getPartition().getId(), sourceDataNode.getHostname(),
            sourceDataNode.getPort(), RepairRequestRecord.OperationType.DeleteRequest, operationTime,
            LIFE_VERSION_FROM_FRONTEND, Utils.Infinite_Time);
    verifyRepairRequestRecordInDb(repairDb, blobId, expectedRecord);
    cleanRepairRequestsDb(repairDb);
  }

  /**
   * Deletion hits 503 and ODR fixes the error. So offline repair is not triggered even it's enabled.
   * @throws Exception
   */
  @Test
  public void testBlobDelete503AndThenODRAndOfflineRepair() throws Exception {
    cleanRepairRequestsDb(repairDb);
    Pair<BlobId, DataNodeId> result = blobDelete503AndThenFixedByODROrOffline(true, true);
    BlobId blobId = result.getFirst();
    verifyRepairRequestRecordInDb(repairDb, blobId, null);
  }

  /**
   * CompositeBlob Deletion first hits 503. Then run ReplicateBlob and Retry. It is successful.
   * @throws Exception
   */
  @Test
  public void testCompositeBlobDelete503AndThenRetrySuccess() throws Exception {
    // CompositeBlob with four chunks.
    maxPutChunkSize = PUT_CONTENT_SIZE / 4;
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = "DC3";
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
        new LoggingNotificationSystem());
    setOperationParams();
    String compositeBlobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // set two replicas Replica_Unavailable. Leave one local replica available as the source of the ReplicationBlob
    DataNodeId sourceDataNode = null;
    List<ServerErrorCode> serverErrors = new ArrayList<>();
    serverErrors.add(ServerErrorCode.BlobNotFound); // return NOT_FOUND for the first Delete Request
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        // local DC, return NO_ERROR for one replica,
        if (sourceDataNode == null) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
        } else {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      } else {
        server.setServerErrors(serverErrors);
      }
    }
    router.deleteBlob(compositeBlobId, serviceID).get();

    // simulate Replica_Unavailable for all local replicas. Then verify delete from the remote replica
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
      }
    }
    try {
      router.getBlob(compositeBlobId, new GetBlobOptionsBuilder().build(), null, null).get();
      fail("Blob must be deleted");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Deletion first hits 503. The following ReplicateBlob fails.
   * @param enableOfflineRepair if true, we inject the repair request to the db and return success status. if not, fail.
   * @return a pair of the blob id and source replica which returns the success status.
   * @throws Exception
   */
  public Pair<BlobId, DataNodeId> blobDelete503ReplicateBlobFailure(boolean enableOfflineRepair) throws Exception {
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = "DC3";
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, enableOfflineRepair, false), layout,
        new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // set error status for all the replicas except one local replica
    DataNodeId sourceDataNode = null;
    List<ServerErrorCode> localServerErrors = new ArrayList<>();
    List<ServerErrorCode> remoteServerErrors = new ArrayList<>();
    localServerErrors.add(ServerErrorCode.ReplicaUnavailable); // return Replica_Unavailable for the first Delete
    localServerErrors.add(ServerErrorCode.UnknownError);  // return Unknown_Error for the ReplicateBlob Request
    remoteServerErrors.add(ServerErrorCode.BlobNotFound); // return NOT_FOUND for the first Delete Request
    remoteServerErrors.add(ServerErrorCode.UnknownError);  // return Unknown_Error for the ReplicateBlob Request
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        // local DC, return NO_ERROR for one replica,
        if (sourceDataNode == null) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
        } else {
          server.setServerErrors(localServerErrors);
        }
      } else {
        server.setServerErrors(remoteServerErrors);
      }
    }

    // Delete should fail with Unavailable. The error status should come from the deletion, not from the ReplicateBlob.
    try {
      router.deleteBlob(blobId, serviceID).get();
      if (!enableOfflineRepair) {
        fail("Deletion should fail when offline repair is disabled.");
      }
    } catch (Exception e) {
      if (!enableOfflineRepair) {
        RouterException r = (RouterException) e.getCause();
        Assert.assertEquals(RouterErrorCode.AmbryUnavailable, r.getErrorCode());
      } else {
        fail("Should be successful when offline repair is enabled.");
      }
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
    return new Pair<>(RouterUtils.getBlobIdFromString(blobId, mockClusterMap), sourceDataNode);
  }

  /**
   * Test Deletion first hits 503. The following ReplicateBlob fails.
   * RepairRequest offline repair is disabled, the Delete request will fail.
   * @throws Exception
   */
  @Test
  public void testBlobDelete503ReplicateBlobFailure() throws Exception {
    blobDelete503ReplicateBlobFailure(false);
  }

  /**
   * Test Deletion first hits 503. The following ReplicateBlob failed.
   * RepairRequest offline repair is enabled, the Delete request will succeed.
   * @throws Exception
   */
  @Test
  public void testBlobDelete503ReplicateBlobFailureThenEnableOfflineRepair() throws Exception {
    cleanRepairRequestsDb(repairDb);
    Pair<BlobId, DataNodeId> result = blobDelete503ReplicateBlobFailure(true);
    BlobId blobId = result.getFirst();
    DataNodeId sourceDataNode = result.getSecond();
    long operationTime = SystemTime.getInstance().milliseconds();
    RepairRequestRecord expectedRecord =
        new RepairRequestRecord(blobId.getID(), blobId.getPartition().getId(), sourceDataNode.getHostname(),
            sourceDataNode.getPort(), RepairRequestRecord.OperationType.DeleteRequest, operationTime,
            LIFE_VERSION_FROM_FRONTEND, Utils.Infinite_Time);
    verifyRepairRequestRecordInDb(repairDb, blobId, expectedRecord);
  }

  /**
   * CompositeBlob deletion first hits 503. Then run ReplicateBlob and Retry.
   * ReplicateBlob fails and then deletion fails.
   * @throws Exception
   */
  @Test
  public void testCompositeBlobDelete503AndThenRetryFailure() throws Exception {
    // CompositeBlob with four chunks.
    maxPutChunkSize = PUT_CONTENT_SIZE / 4;
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = "DC3";
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
        new LoggingNotificationSystem());
    setOperationParams();
    String compositeBlobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // set two replicas Replica_Unavailable. Leave one local replica available as the source of the ReplicationBlob
    // But all remote replicas fail the ReplicateBlob.
    DataNodeId sourceDataNode = null;
    List<ServerErrorCode> localServerErrors = new ArrayList<>();
    List<ServerErrorCode> remoteServerErrors = new ArrayList<>();
    localServerErrors.add(ServerErrorCode.ReplicaUnavailable); // return Replica_Unavailable for the first Delete
    localServerErrors.add(
        ServerErrorCode.BlobAuthorizationFailure); // return Blob_Authorization_Failure for the ReplicateBlob Request
    remoteServerErrors.add(ServerErrorCode.BlobNotFound); // return NOT_FOUND for the first Delete Request
    remoteServerErrors.add(
        ServerErrorCode.BlobAuthorizationFailure); // return Blob_Authorization_Failure for the ReplicateBlob Request
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        // local DC, return NO_ERROR for one replica,
        if (sourceDataNode == null) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
        } else {
          server.setServerErrors(localServerErrors);
        }
      } else {
        server.setServerErrors(remoteServerErrors);
      }
    }

    // Delete should fail with Unavailable. The error status should come from the deletion, not from the ReplicateBlob.
    try {
      router.deleteBlob(compositeBlobId, serviceID).get();
      fail("Deletion should fail");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals(RouterErrorCode.AmbryUnavailable, r.getErrorCode());
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test Deletion first hits Not_FOUND. Then run ODR or offline repair. It is successful.
   * @param enableODR enable On demand replication
   * @param enableOfflineRepair enable the offline repair
   * @return a pair of the blob id and source replica which returns the success status.
   * @throws Exception
   */
  public Pair<BlobId, DataNodeId> blobDelete404AndThenFixedByODROrOfflineRepair(boolean enableODR,
      boolean enableOfflineRepair) throws Exception {
    // test the case either ODR or offline repair is enabled.
    assertTrue(enableODR || enableOfflineRepair);
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = "DC3";
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, enableODR, enableOfflineRepair, false),
        layout, new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // for the three local replicas, two replicas return Blob_Not_Found due to host swap.
    DataNodeId sourceDataNode = null;
    List<ServerErrorCode> serverErrors = new ArrayList<>();
    serverErrors.add(ServerErrorCode.BlobNotFound); // return NOT_FOUND for the first Delete Request
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        // local DC, return NO_ERROR for one replica,
        if (sourceDataNode == null) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
        } else {
          server.setServerErrors(serverErrors);
        }
      } else {
        server.setServerErrors(serverErrors);
      }
    }
    router.deleteBlob(blobId, serviceID).get();

    if (enableOfflineRepair) {
      return new Pair<>(RouterUtils.getBlobIdFromString(blobId, mockClusterMap), sourceDataNode);
    }

    // simulate Replica_Unavailable on the single sourceDataNode. Then verify delete from other local replicas.
    for (MockServer server : layout.getMockServers()) {
      if (server.getHostName().equals(sourceDataNode.getHostname()) && server.getHostPort()
          .equals(sourceDataNode.getPort())) {
        server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
      }
    }
    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build(), null, null).get();
      fail("Blob must be deleted");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
    return new Pair<>(RouterUtils.getBlobIdFromString(blobId, mockClusterMap), sourceDataNode);
  }

  /**
   * Test Deletion first hits Not_FOUND. Then run ODR and Deletion is successful.
   * @throws Exception
   */
  @Test
  public void testBlobDelete404AndODRSuccess() throws Exception {
    blobDelete404AndThenFixedByODROrOfflineRepair(true, false);
  }

  /**
   * Test Deletion first hits Not_FOUND. Then trigger offline repair and Deletion is successful.
   * @throws Exception
   */
  @Test
  public void testBlobDelete404AndOfflineRepairSuccess() throws Exception {
    cleanRepairRequestsDb(repairDb);
    Pair<BlobId, DataNodeId> result = blobDelete404AndThenFixedByODROrOfflineRepair(false, true);
    BlobId blobId = result.getFirst();
    DataNodeId sourceDataNode = result.getSecond();
    long operationTime = SystemTime.getInstance().milliseconds();
    RepairRequestRecord expectedRecord =
        new RepairRequestRecord(blobId.getID(), blobId.getPartition().getId(), sourceDataNode.getHostname(),
            sourceDataNode.getPort(), RepairRequestRecord.OperationType.DeleteRequest, operationTime,
            LIFE_VERSION_FROM_FRONTEND, Utils.Infinite_Time);
    verifyRepairRequestRecordInDb(repairDb, blobId, expectedRecord);
    cleanRepairRequestsDb(repairDb);
  }

  /**
   * Test Deletion first hits 503. The following ReplicateBlob is successful but retry fails.
   * Test the cases that enable offline repair is enabled or disabled.
   * @param enableOfflineRepair enable the offline repair
   * @return a pair of the blob id and source replica which returns the success status.
   * @throws Exception
   */
  public Pair<BlobId, DataNodeId> blobDelete503ReplicateBlobSuccessRetryFailure(boolean enableOfflineRepair)
      throws Exception {
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = "DC3";
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, enableOfflineRepair, false), layout,
        new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // set error status for all the replicas except one local replica
    DataNodeId sourceDataNode = null;
    List<ServerErrorCode> serverErrors = new ArrayList<>();
    serverErrors.add(ServerErrorCode.BlobNotFound);       // for the first Delete request
    serverErrors.add(ServerErrorCode.NoError);             // for the ReplicateBlob request
    serverErrors.add(ServerErrorCode.ReplicaUnavailable);  // for the Delete retry
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        // local DC, return NO_ERROR for one replica,
        if (sourceDataNode == null) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
        } else {
          server.setServerErrors(serverErrors);
        }
      } else {
        server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
      }
    }

    // ReplicateBlob is successful on the other two local replicas but retry fail with unavailable.
    try {
      router.deleteBlob(blobId, serviceID).get();
      if (!enableOfflineRepair) {
        fail("Deletion should fail if offline repair is disabled. ");
      }
    } catch (Exception e) {
      if (!enableOfflineRepair) {
        RouterException r = (RouterException) e.getCause();
        Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.AmbryUnavailable, r.getErrorCode());
      } else {
        fail("Deletion should succeed if offline repair is enabled.");
      }
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
    return new Pair<>(RouterUtils.getBlobIdFromString(blobId, mockClusterMap), sourceDataNode);
  }

  /**
   * Test Deletion first hits 503. The following ReplicateBlob is successful but retry fails.
   * Offline repair is disabled. So deletion fails.
   * @throws Exception
   */
  @Test
  public void testBlobDelete503ReplicateBlobSuccessRetryFailure() throws Exception {
    blobDelete503ReplicateBlobSuccessRetryFailure(false);
  }

  /**
   * Test Deletion first hits 503. The following ReplicateBlob is successful but retry fails.
   * Offline repair is enabled, so deletion is successful.
   * @throws Exception
   */
  @Test
  public void testBlobDelete503ReplicateBlobSuccessRetryFailureThenOfflineRepair() throws Exception {
    cleanRepairRequestsDb(repairDb);
    Pair<BlobId, DataNodeId> result = blobDelete503ReplicateBlobSuccessRetryFailure(true);
    BlobId blobId = result.getFirst();
    DataNodeId sourceDataNode = result.getSecond();
    long operationTime = SystemTime.getInstance().milliseconds();
    RepairRequestRecord expectedRecord =
        new RepairRequestRecord(blobId.getID(), blobId.getPartition().getId(), sourceDataNode.getHostname(),
            sourceDataNode.getPort(), RepairRequestRecord.OperationType.DeleteRequest, operationTime,
            LIFE_VERSION_FROM_FRONTEND, Utils.Infinite_Time);
    verifyRepairRequestRecordInDb(repairDb, blobId, expectedRecord);
  }

  /**
   * Test Deletion hits 503. But since there is no single replica is successful, won't do ODR or offline repair
   * @param enableODR enable On demand replication
   * @param enableOfflineRepair enable the offline repair
   * @throws Exception
   */
  public void testBlobDelete503NoSuccessReplica(boolean enableODR, boolean enableOfflineRepair) throws Exception {
    // test the case either ODR or offline repair is enabled.
    assertTrue(enableODR || enableOfflineRepair);
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = "DC3";
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, enableODR, enableOfflineRepair, false),
        layout, new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    for (MockServer server : layout.getMockServers()) {
      server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
    }

    try {
      router.deleteBlob(blobId, serviceID).get();
      fail("Deletion should fail");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals(RouterErrorCode.AmbryUnavailable, r.getErrorCode());
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /**
   * Test Deletion hits 503. But since there is no single replica is successful, won't do ReplicateBlob and retry
   * @throws Exception
   */
  @Test
  public void testBlobDelete503NoODRSinceNoSuccessReplica() throws Exception {
    testBlobDelete503NoSuccessReplica(true, false);
  }

  /**
   * Test Deletion hits 503. But since there is no single replica is successful, won't do offline retry
   * @throws Exception
   */
  @Test
  public void testBlobDelete503NoOfflineRepairSinceNoSuccessReplica() throws Exception {
    cleanRepairRequestsDb(repairDb);
    testBlobDelete503NoSuccessReplica(false, true);
    assertEquals(0, getRepairRequestsCount(repairDb));
  }

  /**
   * Test Deletion first hits Not_FOUND. Then run ReplicateBlob.
   * If the PutBlob is compacted already, ReplicateBlob may fail with ID_Deleted.
   * @throws Exception
   */
  @Test
  public void testReplicateBlobOnDeleteButPutBlobCompacted() throws Exception {
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = "DC3";
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
        new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // two local replicas are unavailable. Remote replica doesn't have the Blob.
    DataNodeId sourceDataNode = null;
    List<ServerErrorCode> serverErrors = new ArrayList<>();
    serverErrors.add(ServerErrorCode.BlobNotFound); // return NOT_FOUND for the first Delete Request
    serverErrors.add(ServerErrorCode.BlobDeleted); // source PutBlob is compacted, replication fails with Blob_Deleted.
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        // local DC, return NO_ERROR for one replica. This replica will be the source replica of the ReplicateBlob.
        if (sourceDataNode == null) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
        } else {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      } else {
        server.setServerErrors(serverErrors);
      }
    }

    // ReplicateBlob fails, it returns the error of the delete operation.
    try {
      router.deleteBlob(blobId, serviceID).get();
      fail("deleteBlob should fail since ReplicateBlob fails.");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("AmbryUnavailable error is expected", RouterErrorCode.AmbryUnavailable, r.getErrorCode());
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      router.close();
    }
  }

  /**
   * When a composite blob put fails, background deleter will kick off and delete all the data chunks.
   * Test ODR or offline repair won't get triggered for the background deleter.
   */
  public void testRetryBlobIgnoreBackgroundDeleter(boolean enableODR, boolean enableOfflineRepair) throws Exception {
    // test the case either ODR or offline repair is enabled.
    assertTrue(enableODR || enableOfflineRepair);
    try {
      // Ensure there are 2 chunks.
      maxPutChunkSize = PUT_CONTENT_SIZE / 2;
      Properties props = getNonBlockingRouterPropertiesForRepairRequests("DC3", enableODR, enableOfflineRepair, false);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      RouterConfig routerConfig = new RouterConfig(verifiableProperties);
      MockClusterMap mockClusterMap = new MockClusterMap();
      MockTime mockTime = new MockTime();
      MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);

      // uses a notification system to track the deletions for all the data chunks.
      // there are three blobs to be deleted.
      // the first chunk which is put successfully.
      // the second chunk is failed to put and the retried blob also fails.
      final int dataChunkNumber = 3;
      final CountDownLatch deletesDoneLatch = new CountDownLatch(dataChunkNumber);
      final Map<String, String> blobsThatAreDeleted = new HashMap<>();
      LoggingNotificationSystem deleteTrackingNotificationSystem = new LoggingNotificationSystem() {
        @Override
        public void onBlobDeleted(String blobId, String serviceId, Account account, Container container) {
          System.out.println("deleted " + blobId);
          blobsThatAreDeleted.put(blobId, serviceId);
          deletesDoneLatch.countDown();
        }
      };
      NonBlockingRouterMetrics localMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
      router = new NonBlockingRouter(routerConfig, localMetrics,
          new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
              CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), deleteTrackingNotificationSystem, mockClusterMap, kms,
          cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
      setOperationParams();

      List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
      List<ServerErrorCode> sourceServerError = new ArrayList<>();
      List<ServerErrorCode> otherServerError = new ArrayList<>();
      List<ServerErrorCode> remoteServerError = new ArrayList<>();
      // There are 2 chunks for this blob.
      // All put operations make one request to each local server as there are 3 servers overall in the local DC.
      // Set the state of the mock servers so that they return success for the first request/chunk.
      sourceServerError.add(ServerErrorCode.NoError);
      // fail requests for the second data chunk including the slipped put attempts.
      sourceServerError.add(ServerErrorCode.UnknownError); // second data chunk
      sourceServerError.add(ServerErrorCode.UnknownError); // slipped chunk
      // all subsequent requests including delete will succeed on the source server.

      // For all the other two local servers
      otherServerError.add(ServerErrorCode.NoError);      // Put for the first data chunk
      otherServerError.add(ServerErrorCode.UnknownError); // second data chunk
      otherServerError.add(ServerErrorCode.UnknownError); // slipped chunk
      // also it fails the background deletion with Unknown_Error
      otherServerError.add(ServerErrorCode.UnknownError); // delete 1st blob
      otherServerError.add(ServerErrorCode.UnknownError); // delete 2nd blob
      otherServerError.add(ServerErrorCode.UnknownError); // delete 3rd blob

      // For the remote servers. Won't receive PutRequest. The following errors are for deletion
      remoteServerError.add(ServerErrorCode.UnknownError); // delete 1st blob
      remoteServerError.add(ServerErrorCode.UnknownError); // delete 2nd blob
      remoteServerError.add(ServerErrorCode.UnknownError); // delete 3rd blob

      DataNodeId sourceDataNode = null;
      String localDcName = "DC3";
      for (DataNodeId dataNodeId : dataNodeIds) {
        MockServer server = mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort());
        if (server.getDataCenter().equals(localDcName)) {
          if (sourceDataNode == null) {
            sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
            server.setServerErrors(sourceServerError);
          } else {
            server.setServerErrors(otherServerError);
          }
        } else {
          server.setServerErrors(remoteServerError);
        }
      }

      // Submit the put operation and wait for it to fail.
      try {
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
        fail("putBlob should fail");
      } catch (ExecutionException e) {
        Assert.assertEquals(RouterErrorCode.AmbryUnavailable, ((RouterException) e.getCause()).getErrorCode());
      }

      // Now, wait AWAIT_TIMEOUT_MS to see if any blob is deleted.
      // Suppose on-demand replication won't get triggered.
      // The background blob deletion will hit exception. Right now we swallow the background deleter exception and emit metrics.
      Assert.assertTrue("We swallow background deleter exception." + AWAIT_TIMEOUT_MS,
          deletesDoneLatch.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      // we should hit background deleter exceptions
      Assert.assertEquals(localMetrics.backgroundDeleterExceptionCount.getCount(), dataChunkNumber);
      Assert.assertEquals(deletesDoneLatch.getCount(), 0);
      Assert.assertEquals(blobsThatAreDeleted.entrySet().size(), dataChunkNumber);
    } finally {
      if (router != null) {
        router.close();
        assertClosed();
        Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
      }
    }
  }

  /**
   * When a composite blob put fails, background deleter will kick off and delete all the data chunks.
   * Test ODR won't get triggered for the background deleter.
   */
  @Test
  public void testODRIgnoreBackgroundDeleter() throws Exception {
    testRetryBlobIgnoreBackgroundDeleter(true, false);
  }

  /**
   * When a composite blob put fails, background deleter will kick off and delete all the data chunks.
   * Test offline repair won't get triggered for the background deleter.
   */
  @Test
  public void testOfflineRepairIgnoreBackgroundDeleter() throws Exception {
    cleanRepairRequestsDb(repairDb);
    testRetryBlobIgnoreBackgroundDeleter(false, true);
    assertEquals(0, getRepairRequestsCount(repairDb));
  }

  /**
   * Test On-Demand Replication on deletion success case. And the service id of the delete request is null.
   * @throws Exception
   */
  @Test
  public void testBlobDeleteWithNullServiceID() throws Exception {
    // service id is null
    String serviceID = null;
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = "DC3";
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, false, false), layout,
        new LoggingNotificationSystem());
    setOperationParams();
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // set two local replicas Replica_Unavailable.
    DataNodeId sourceDataNode = null;
    List<ServerErrorCode> serverErrors = new ArrayList<>();
    serverErrors.add(ServerErrorCode.BlobNotFound); // return NOT_FOUND for the first Delete Request
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        // local DC, return NO_ERROR for one replica,
        if (sourceDataNode == null) {
          sourceDataNode = mockClusterMap.getDataNodeId(server.getHostName(), server.getHostPort());
        } else {
          server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
        }
      } else {
        server.setServerErrors(serverErrors);
      }
    }
    router.deleteBlob(blobId, serviceID).get();

    // simulate Replica_Unavailable for all local replicas. Then verify delete from the remote replica
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
      }
    }
    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build(), null, null).get();
      fail("Blob must be deleted");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDeleted, r.getErrorCode());
    } finally {
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  @Test
  public void testForceDeleteForSimpleBlobSuccess() throws Exception {
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, true, true), layout,
        new LoggingNotificationSystem());
    // Create a random id that doesn't exist
    List<PartitionId> mockPartitions = mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partition = mockPartitions.get(ThreadLocalRandom.current().nextInt(mockPartitions.size()));
    String blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partition, false, BlobId.BlobDataType.DATACHUNK).getID();

    List<MockServer> localServers = new ArrayList<>();
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        localServers.add(server);
        server.setServerErrors(Collections.singletonList(ServerErrorCode.ReplicaUnavailable));
      } else {
        server.setServerErrorForAllRequests(ServerErrorCode.BlobNotFound);
      }
    }
    // After set server errors, we would see
    // 1. when issuing regular delete requests, we have three originating replicas returning unavailable, and others return NOT_FOUND
    //    this would result in unavailable and would trigger force delete
    // 2. when force delete, all servers return No Error.
    //    delete parallelism is 3, so 3 originating replicas would have delete tombstones

    long forceDeleteCountBefore = routerMetrics.forceDeleteBlobCount.getCount();
    try {
      router.deleteBlob(blobId, serviceID).get();
    } catch (Exception e) {
      fail("Expecting force delete to work");
    } finally {
      assertEquals(forceDeleteCountBefore + 1, routerMetrics.forceDeleteBlobCount.getCount());
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      for (MockServer server : localServers) {
        StoredBlob sblob = server.getBlobs().get(blobId);
        assertNotNull(sblob);
        assertTrue(sblob.isDeleted());
        assertTrue(sblob.isDeleteTombstone());
      }
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  @Test
  public void testForceDeleteForSimpleBlobSuccessOnRemote() throws Exception {
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, true, true), layout,
        new LoggingNotificationSystem());
    // Create a random id that doesn't exist
    List<PartitionId> mockPartitions = mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partition = mockPartitions.get(ThreadLocalRandom.current().nextInt(mockPartitions.size()));
    String blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partition, false, BlobId.BlobDataType.DATACHUNK).getID();

    List<MockServer> localServers = new ArrayList<>();
    List<MockServer> remoteServers = new ArrayList<>();
    for (MockServer server : layout.getMockServers()) {
      if (server.getDataCenter().equals(localDcName)) {
        localServers.add(server);
        server.setServerErrorForAllRequests(
            ServerErrorCode.ReplicaUnavailable); // originating replicas rejects all delete requests
      } else {
        server.setServerErrors(Collections.singletonList(
            ServerErrorCode.BlobNotFound)); // remote replicas reject first delete request, but accept force delete
        remoteServers.add(server);
      }
    }
    // After set server errors, we would see
    // 1. when issuing regular delete requests, we have three originating replicas returning unavailable, and others return NOT_FOUND
    //    this would result in unavailable and would trigger force delete
    // 2. when force delete, originating replicas still return unavailable, not remote replicas return No_Error
    //    delete parallelism is 3, so 3 remote replicas would have delete tombstones

    long forceDeleteCountBefore = routerMetrics.forceDeleteBlobCount.getCount();
    try {
      router.deleteBlob(blobId, serviceID).get();
    } catch (Exception e) {
      fail("Expecting force delete to work");
    } finally {
      assertEquals(forceDeleteCountBefore + 1, routerMetrics.forceDeleteBlobCount.getCount());
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      for (MockServer server : localServers) {
        StoredBlob sblob = server.getBlobs().get(blobId);
        assertNull(sblob);
      }
      int numTombstone = 0;
      for (MockServer server : remoteServers) {
        StoredBlob sblob = server.getBlobs().get(blobId);
        if (sblob != null) {
          assertTrue(sblob.isDeleted());
          assertTrue(sblob.isDeleteTombstone());
          numTombstone++;
        }
      }
      assertEquals(DELETE_REQUEST_PARALLELISM, numTombstone);

      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  @Test
  public void testForceDeleteForSimpleBlobFailure() throws Exception {
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, true, true), layout,
        new LoggingNotificationSystem());
    // Create a random id that doesn't exist
    List<PartitionId> mockPartitions = mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partition = mockPartitions.get(ThreadLocalRandom.current().nextInt(mockPartitions.size()));
    String blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partition, false, BlobId.BlobDataType.DATACHUNK).getID();

    for (MockServer server : layout.getMockServers()) {
      server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable); // replicas rejects all delete requests
    }

    long forceDeleteCountBefore = routerMetrics.forceDeleteBlobCount.getCount();
    long forceDeleteErrorCountBefore = routerMetrics.forceDeleteBlobErrorCount.getCount();
    try {
      router.deleteBlob(blobId, serviceID).get();
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.AmbryUnavailable, r.getErrorCode());
    } finally {
      assertEquals(forceDeleteCountBefore + 1, routerMetrics.forceDeleteBlobCount.getCount());
      assertEquals(forceDeleteErrorCountBefore + 1, routerMetrics.forceDeleteBlobErrorCount.getCount());
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  @Test
  public void testForceDeleteIgnoreBackgroundDeleter() throws Exception {
    String serviceID = BackgroundDeleteRequest.SERVICE_ID_PREFIX + "service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, true, true, true), layout,
        new LoggingNotificationSystem());
    // Create a random id that doesn't exist
    List<PartitionId> mockPartitions = mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partition = mockPartitions.get(ThreadLocalRandom.current().nextInt(mockPartitions.size()));
    String blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partition, false, BlobId.BlobDataType.DATACHUNK).getID();

    for (MockServer server : layout.getMockServers()) {
      server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable); // replicas rejects all delete requests
    }

    long forceDeleteCountBefore = routerMetrics.forceDeleteBlobCount.getCount();
    try {
      router.deleteBlob(blobId, serviceID).get();
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.AmbryUnavailable, r.getErrorCode());
    } finally {
      assertEquals(forceDeleteCountBefore, routerMetrics.forceDeleteBlobCount.getCount());
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  @Test
  public void testForceDeleteIgnoreSuccessResponse() throws Exception {
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, false, false, true), layout,
        new LoggingNotificationSystem());
    // Create a random id that doesn't exist
    List<PartitionId> mockPartitions = mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partition = mockPartitions.get(ThreadLocalRandom.current().nextInt(mockPartitions.size()));
    String blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partition, false, BlobId.BlobDataType.DATACHUNK).getID();

    boolean setSuccess = false;
    for (MockServer server : layout.getMockServers()) {
      if (!setSuccess) {
        server.setServerErrorForAllRequests(ServerErrorCode.NoError); // one replica to return success
        setSuccess = true;
      } else {
        server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable); // replicas rejects all delete requests
      }
    }
    // We have only one success, and we disabled ODR and offline repair, however, we shouldn't do force delete

    long forceDeleteCountBefore = routerMetrics.forceDeleteBlobCount.getCount();
    try {
      router.deleteBlob(blobId, serviceID).get();
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.AmbryUnavailable, r.getErrorCode());
    } finally {
      assertEquals(forceDeleteCountBefore, routerMetrics.forceDeleteBlobCount.getCount());
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  @Test
  public void testForceDeleteIgnoreNotFound() throws Exception {
    String serviceID = "delete-service";
    MockServerLayout layout = new MockServerLayout(mockClusterMap);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    setRouter(getNonBlockingRouterPropertiesForRepairRequests(localDcName, false, false, true), layout,
        new LoggingNotificationSystem());
    // Create a random id that doesn't exist
    List<PartitionId> mockPartitions = mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partition = mockPartitions.get(ThreadLocalRandom.current().nextInt(mockPartitions.size()));
    String blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), partition, false, BlobId.BlobDataType.DATACHUNK).getID();

    for (MockServer server : layout.getMockServers()) {
      server.setServerErrorForAllRequests(ServerErrorCode.BlobNotFound); // replicas returns not found
    }

    long forceDeleteCountBefore = routerMetrics.forceDeleteBlobCount.getCount();
    try {
      router.deleteBlob(blobId, serviceID).get();
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("BlobDeleted error is expected", RouterErrorCode.BlobDoesNotExist, r.getErrorCode());
    } finally {
      assertEquals(forceDeleteCountBefore, routerMetrics.forceDeleteBlobCount.getCount());
      layout.getMockServers().forEach(mockServer -> mockServer.setServerErrorForAllRequests(null));
      router.getNotFoundCache().invalidateAll();
      if (router != null) {
        router.close();
      }
    }
  }

  /*
   * Create a db connection to the RepairRequests db and cleanup the db.
   * @param localDc : name of the local data center.
   * @param time : System Time
   */
  static MysqlRepairRequestsDb createRepairRequestsConnection(String localDc, Time time) throws Exception {
    Properties properties = new Properties();
    String dbInfo = "["
        + "{\"url\":\"jdbc:mysql://localhost/AmbryRepairRequests?serverTimezone=UTC\",\"datacenter\":\"DC1\",\"isWriteable\":\"true\",\"username\":\"travis\",\"password\":\"\"},"
        + "{\"url\":\"jdbc:mysql://localhost/AmbryRepairRequests?serverTimezone=UTC\",\"datacenter\":\"DC2\",\"isWriteable\":\"true\",\"username\":\"travis\",\"password\":\"\"},"
        + "{\"url\":\"jdbc:mysql://localhost/AmbryRepairRequests?serverTimezone=UTC\",\"datacenter\":\"DC3\",\"isWriteable\":\"true\",\"username\":\"travis\",\"password\":\"\"}"
        + "]";
    properties.setProperty(MysqlRepairRequestsDbConfig.DB_INFO, dbInfo);
    properties.setProperty(MysqlRepairRequestsDbConfig.LIST_MAX_RESULTS, Integer.toString(100));
    properties.setProperty(MysqlRepairRequestsDbConfig.LOCAL_POOL_SIZE, "5");

    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metrics = new MetricRegistry();
    MysqlRepairRequestsDbFactory factory =
        new MysqlRepairRequestsDbFactory(verifiableProperties, metrics, localDc, time);
    MysqlRepairRequestsDb repairRequestsDb = factory.getRepairRequestsDb();

    // cleanup the database
    cleanRepairRequestsDb(repairRequestsDb);
    return repairRequestsDb;
  }

  /**
   * Cleanup the repair request db
   * @param repairRequestsDb the repair request db
   * @throws SQLException
   */
  static void cleanRepairRequestsDb(MysqlRepairRequestsDb repairRequestsDb) throws SQLException {
    DataSource dataSource = repairRequestsDb.getDataSource();
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate("DELETE FROM ambry_repair_requests;");
      }
    }
  }

  /**
   * Get record count of the RepairRequests DB
   * @param repairRequestsDb the repair request db
   * @throws SQLException
   */
  static long getRepairRequestsCount(MysqlRepairRequestsDb repairRequestsDb) throws SQLException {
    DataSource dataSource = repairRequestsDb.getDataSource();
    long numberOfRows = 0;
    String rowQuerySql = "SELECT COUNT(*) as total FROM ambry_repair_requests";
    do {
      try (Connection connection = dataSource.getConnection()) {
        try (PreparedStatement statement = connection.prepareStatement(rowQuerySql)) {
          try (ResultSet result = statement.executeQuery()) {
            while (result.next()) {
              numberOfRows = result.getLong("total");
            }
          }
        }
      }
    } while (numberOfRows != 0);
    return numberOfRows;
  }

  /**
   * Verify the db has one expected RepairRequestRecord
   * @param db the RepairRequests DB
   * @param blobId the blob id
   * @param expectedRecord expected RepairRequestRecord
   * @throws Exception
   */
  static void verifyRepairRequestRecordInDb(MysqlRepairRequestsDb db, BlobId blobId, RepairRequestRecord expectedRecord)
      throws Exception {
    List<RepairRequestRecord> records = db.getRepairRequestsForPartition(blobId.getPartition().getId());
    if (expectedRecord == null) {
      assertEquals(records.size(), 0);
    } else {
      assertEquals(records.size(), 1);
      RepairRequestRecord record = records.get(0);
      assertEquals(expectedRecord.getBlobId(), record.getBlobId());
      assertEquals(expectedRecord.getPartitionId(), record.getPartitionId());
      assertEquals(expectedRecord.getSourceHostName(), record.getSourceHostName());
      assertEquals(expectedRecord.getSourceHostPort(), record.getSourceHostPort());
      assertEquals(expectedRecord.getOperationType(), record.getOperationType());
      // cannot verify the operationTime
      assertEquals(expectedRecord.getLifeVersion(), record.getLifeVersion());
      assertEquals(expectedRecord.getExpirationTimeMs(), record.getExpirationTimeMs());
    }
  }
}
