package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


/**
 * Unit test for {@link DeleteManager} and {@link DeleteOperation}.
 */
public class DeleteManagerTest {
  private CountDownLatch operationCompletedlatch;
  private Time mockTime;
  private AtomicReference<MockSelectorState> mockSelectorState;
  private MockServerLayout serverLayout;
  private Router router;
  private BlobId blobId;
  private String blobIdString;
  private PartitionId mockPartition;
  private Exception exception;
  private Future<Void> future;

  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;

  private Properties getNonBlockingRouterProperties(String deleteParallelism) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.delete.request.parallelism", deleteParallelism);
    return properties;
  }

  /**
   * Initialize ClusterMap, Router, mock servers, and blobId. The blobId is to be deleted.
   * @throws Exception
   */
  private void initialize(boolean serverNoResponse, String deleteParallelism)
      throws Exception {
    operationCompletedlatch = new CountDownLatch(1);
    mockTime = new MockTime();
    mockSelectorState = new AtomicReference<MockSelectorState>(MockSelectorState.Good);
    VerifiableProperties vProps = new VerifiableProperties(getNonBlockingRouterProperties(deleteParallelism));
    MockClusterMap mockClusterMap = new MockClusterMap();
    serverLayout = new MockServerLayout(mockClusterMap, mockTime, serverNoResponse);
    router = new NonBlockingRouter(new RouterConfig(vProps), new NonBlockingRouterMetrics(new MetricRegistry()),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), mockClusterMap, mockTime);
    List<PartitionId> mockPartitions = mockClusterMap.getWritablePartitionIds();
    mockPartition = mockPartitions.get(ThreadLocalRandom.current().nextInt(mockPartitions.size()));
    blobId = new BlobId(mockPartition);
    blobIdString = blobId.getID();
  }

  /**
   * Test the basic delete operation and it will succeed.
   */
  @Test
  public void testBasicDelete()
      throws Exception {
    initialize(false, "9");
    ServerErrorCode[] serverErrorCodes =
        {ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error};
    writeBlobRecordToServers(serverErrorCodes);
    blobRecordVerification(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new UserCallback());
    try {
      future.get();
      assertNull(exception);
    } catch (Exception e) {
      fail();
    }
  }

  /**
   * Test the case when an invalid blobIs string is passed to delete.
   */
  @Test
  public void testBlobNotValid()
      throws Exception {
    initialize(false, "9");
    future = router.deleteBlob("123", new UserCallback());
    try {
      future.get();
      fail();
    } catch (Exception e) {
      assertEquals(RouterErrorCode.InvalidBlobId, ((RouterException) exception).getErrorCode());
    }

    future = router.deleteBlob("", new UserCallback());
    try {
      future.get();
      fail();
    } catch (Exception e) {
      assertEquals(RouterErrorCode.InvalidBlobId, ((RouterException) exception).getErrorCode());
    }

    future = router.deleteBlob(null, new UserCallback());
    try {
      future.get();
      fail();
    } catch (Exception e) {
      assertEquals(RouterErrorCode.InvalidBlobId, ((RouterException) exception).getErrorCode());
    }
  }

  /**
   * Test the case when deleting a blob that has already been expired.
   */
  // @todo to confirm what to return if a blob is already expired.
  @Test
  public void testBlobExpired()
      throws Exception {
    initialize(false, "9");
    ServerErrorCode[] serverErrorCodes =
        {ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Expired};
    writeBlobRecordToServers(serverErrorCodes);
    blobRecordVerification(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new UserCallback());
    try {
      future.get();
      fail();
    } catch (Exception e) {
      assertEquals(RouterErrorCode.BlobExpired, ((RouterException) exception).getErrorCode());
    }
  }

  /**
   * Test the case when the blob cannot be found in store servers.
   */
  // @todo to check if this would happen, and under what situation.
  @Test
  public void testBlobNotFound()
      throws Exception {
    initialize(false, "9");
    ServerErrorCode[] serverErrorCodes =
        {ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found};
    writeBlobRecordToServers(serverErrorCodes);
    blobRecordVerification(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new UserCallback());
    try {
      future.get();
      fail();
    } catch (Exception e) {
      assertEquals(RouterErrorCode.BlobDoesNotExist, ((RouterException) exception).getErrorCode());
    }
  }

  /**
   * Test the case when the blob has already been deleted. Note that only one server will return
   * {@code ServerErrorCode.Blob_Deleted}.
   */
  @Test
  public void testBlobDeleted()
      throws Exception {
    initialize(false, "9");
    ServerErrorCode[] serverErrorCodes =
        {ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Not_Found, ServerErrorCode.Blob_Deleted};
    writeBlobRecordToServers(serverErrorCodes);
    blobRecordVerification(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new UserCallback());
    try {
      future.get();
      fail();
    } catch (Exception e) {
      assertEquals(RouterErrorCode.BlobDeleted, ((RouterException) exception).getErrorCode());
    }
  }

  /**
   * Test the case where servers return different {@link ServerErrorCode}, and the
   * {@link DeleteOperation} is able to resolve and conclude the correct {@link RouterErrorCode}.
   */
  @Test
  public void testVariousServerErrorCodes()
      throws Exception {
    initialize(false, "9");
    ServerErrorCode[] serverErrorCodes =
        {ServerErrorCode.Blob_Not_Found, ServerErrorCode.Data_Corrupt, ServerErrorCode.IO_Error, ServerErrorCode.Partition_Unknown, ServerErrorCode.Disk_Unavailable, ServerErrorCode.No_Error, ServerErrorCode.Data_Corrupt, ServerErrorCode.Unknown_Error, ServerErrorCode.Disk_Unavailable};
    writeBlobRecordToServers(serverErrorCodes);
    blobRecordVerification(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new UserCallback());
    try {
      future.get();
      fail();
    } catch (Exception e) {
      assertEquals(RouterErrorCode.AmbryUnavailable, ((RouterException) exception).getErrorCode());
    }
  }

  /**
   * Test the case where servers return different {@link ServerErrorCode}, and the
   * {@link DeleteOperation} is able to resolve and conclude the correct {@link RouterErrorCode}.
   */
  @Test
  public void testVariousServerErrorCodesForThreeParallelism()
      throws Exception {
    initialize(false, "3");
    ServerErrorCode[] serverErrorCodes =
        {ServerErrorCode.Blob_Not_Found, ServerErrorCode.Data_Corrupt, ServerErrorCode.IO_Error, ServerErrorCode.Partition_Unknown, ServerErrorCode.Disk_Unavailable, ServerErrorCode.No_Error, ServerErrorCode.Data_Corrupt, ServerErrorCode.Unknown_Error, ServerErrorCode.Disk_Unavailable};
    writeBlobRecordToServers(serverErrorCodes);
    blobRecordVerification(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new UserCallback());
    try {
      future.get();
      fail();
    } catch (Exception e) {
      assertEquals(RouterErrorCode.AmbryUnavailable, ((RouterException) exception).getErrorCode());
    }
  }

  /**
   * Test the case when request gets expired before the corresponding store server sends
   * back a response.
   */
  @Test
  public void testResponseTimeout()
      throws Exception {
    // True will be passed to initialize() to indicate no response from MockServer.
    initialize(true, "9");
    ServerErrorCode[] serverErrorCodes =
        {ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error};
    writeBlobRecordToServers(serverErrorCodes);
    blobRecordVerification(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new UserCallback());
    do {
      // increment mock time
      mockTime.sleep(CHECKOUT_TIMEOUT_MS + 1);
    } while (!operationCompletedlatch.await(10, TimeUnit.MILLISECONDS));
    assertEquals(RouterErrorCode.OperationTimedOut, ((RouterException) exception).getErrorCode());
  }

  /**
   * Test the case when the {@link com.github.ambry.network.Selector} of
   * {@link com.github.ambry.network.NetworkClient} is malfunctioning.
   */
  @Test
  public void testSelectorError()
      throws Exception {
    ServerErrorCode[] serverErrorCodes =
        {ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error};
    initialize(false, "9");
    for (MockSelectorState state : MockSelectorState.values()) {
      if(state == MockSelectorState.Good) {
        continue;
      }
      mockSelectorState.set(state);
      writeBlobRecordToServers(serverErrorCodes);
      blobRecordVerification(serverErrorCodes);
      future = router.deleteBlob(blobIdString, new UserCallback());
      do {
        // increment mock time
        mockTime.sleep(CHECKOUT_TIMEOUT_MS + 1);
        //System.out.println(operationCompletedlatch.getCount());
      } while (!operationCompletedlatch.await(10, TimeUnit.MILLISECONDS));
      assertEquals(RouterErrorCode.OperationTimedOut, ((RouterException) exception).getErrorCode());
    }
  }

  /**
   * Test the case when a router is closed.
   * @throws Exception
   */
  @Test
  public void testCloseRouter()
      throws Exception {
    ServerErrorCode[] serverErrorCodes =
        {ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error, ServerErrorCode.No_Error};
    initialize(true, "9");
    writeBlobRecordToServers(serverErrorCodes);
    blobRecordVerification(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new UserCallback());
    router.close();
    assertEquals(RouterErrorCode.RouterClosed, ((RouterException) exception).getErrorCode());
  }

  private void writeBlobRecordToServers(ServerErrorCode[] serverErrorCodes) {
    int i = 0;
    for (ReplicaId replica : mockPartition.getReplicaIds()) {
      DataNodeId node = replica.getDataNodeId();
      MockServer mockServer = serverLayout.getMockServer(node.getHostname(), node.getPort());
      mockServer.setBlobIdToServerErrorCode(blobIdString, serverErrorCodes[i++]);
    }
  }

  private void blobRecordVerification(ServerErrorCode[] serverErrorCodes) {
    int i = 0;
    List<ReplicaId> replicas = blobId.getPartition().getReplicaIds();
    for (ReplicaId replica : replicas) {
      DataNodeId node = replica.getDataNodeId();
      MockServer server = serverLayout.getMockServer(node.getHostname(), node.getPort());
      assertEquals(serverErrorCodes[i++], server.getErrorFromBlobIdStr(blobIdString));
    }
  }

  private class UserCallback implements Callback<Void> {
    @Override
    public void onCompletion(Void t, Exception e) {
      exception = e;
      operationCompletedlatch.countDown();
    }
  }
}

