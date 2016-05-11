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
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Unit test for {@link DeleteManager} and {@link DeleteOperation}.
 */
public class DeleteManagerTest {
  private CountDownLatch operationCompleteLatch;
  private Time mockTime;
  private AtomicReference<MockSelectorState> mockSelectorState;
  private MockClusterMap clusterMap;
  private MockServerLayout serverLayout;
  private NonBlockingRouter router;
  private BlobId blobId;
  private String blobIdString;
  private PartitionId partition;
  private Future<Void> future;

  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;

  // The maximum number of inflight requests for a single delete operation.
  private static final String DELETE_PARALLELISM = "9";

  /**
   * Initializes ClusterMap, Router, mock servers, and an {@code BlobId} to be deleted.
   */
  @Before
  public void init()
      throws Exception {
    VerifiableProperties vProps = new VerifiableProperties(getNonBlockingRouterProperties());
    operationCompleteLatch = new CountDownLatch(1);
    mockTime = new MockTime();
    mockSelectorState = new AtomicReference<MockSelectorState>(MockSelectorState.Good);
    clusterMap = new MockClusterMap();
    serverLayout = new MockServerLayout(clusterMap);
    router = new NonBlockingRouter(new RouterConfig(vProps), new NonBlockingRouterMetrics(new MetricRegistry()),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, mockTime);
    List<PartitionId> mockPartitions = clusterMap.getWritablePartitionIds();
    partition = mockPartitions.get(ThreadLocalRandom.current().nextInt(mockPartitions.size()));
    blobId = new BlobId(partition);
    blobIdString = blobId.getID();
  }

  /**
   * Closes the router and does some post verification.
   */
  @After
  public void cleanUp() {
    assertCloseCleanup();
  }

  /**
   * Test a basic delete operation that will succeed.
   */
  @Test
  public void testBasicDeletion()
      throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.No_Error);
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    future.get();
  }

  /**
   * Test the cases for invalid blobId strings.
   */
  @Test
  public void testBlobIdNotValid()
      throws Exception {
    String[] input = {"123", "abcd", "", "/", null};
    for (String s : input) {
      future = router.deleteBlob(s, new ClientCallback());
      try {
        future.get();
        fail("Deletion should be unsuccessful. Exception is expected.");
      } catch (Exception e) {
        assertEquals("RouterErrorCode should be InvalidBlobId", RouterErrorCode.InvalidBlobId,
            ((RouterException) e.getCause()).getErrorCode());
      }
    }
  }

  /**
   * Test the case when one server store responds with {@code Blob_Expired}, and other servers
   * respond with {@code Blob_Not_Found}. The delete operation should be able to resolve the
   * router error code as {@code Blob_Expired}. The order of received responses is the same as
   * defined in {@code serverErrorCodes}.
   */
  @Test
  public void testBlobExpired()
      throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.Blob_Not_Found);
    serverErrorCodes[5] = ServerErrorCode.Blob_Expired;
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    try {
      future.get();
      fail("Deletion should be unsuccessful. Error is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCode should be BlobExpired", RouterErrorCode.BlobExpired,
          ((RouterException) e.getCause()).getErrorCode());
    }
  }

  /**
   * Test if the {@link RouterErrorCode} is as expected for different {@link ServerErrorCode}.
   */
  @Test
  public void testVariousServerErrorCode()
      throws Exception {
    HashMap<ServerErrorCode, RouterErrorCode> map = new HashMap<>();
    map.put(ServerErrorCode.Blob_Expired, RouterErrorCode.BlobExpired);
    map.put(ServerErrorCode.Blob_Not_Found, RouterErrorCode.BlobDoesNotExist);
    map.put(ServerErrorCode.Partition_Unknown, RouterErrorCode.BlobDoesNotExist);
    map.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.AmbryUnavailable);
    for (ServerErrorCode serverErrorCode : ServerErrorCode.values()) {
      if (serverErrorCode != ServerErrorCode.No_Error && serverErrorCode != ServerErrorCode.Blob_Deleted && !map
          .containsKey(serverErrorCode)) {
        map.put(serverErrorCode, RouterErrorCode.UnexpectedInternalError);
      }
    }
    for (Map.Entry<ServerErrorCode, RouterErrorCode> entity : map.entrySet()) {
      testResponseForServerErrorUtil(entity.getKey(), entity.getValue());
    }
  }

  /**
   * Test the case when the blob cannot be found in store servers, though the last response is {@code IO_Error}.
   * The delete operation is expected to return {@link RouterErrorCode#BlobDoesNotExist}, since the delete operation will be completed
   * before the last response according to its {@link OperationTracker}. The order of received responses is the
   * same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testBlobNotFoundWithLastResponseNotBlobNotFound()
      throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.Blob_Not_Found);
    serverErrorCodes[8] = ServerErrorCode.IO_Error;
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    try {
      future.get();
      fail("Deletion should be unsuccessful. Error is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCode should be BlobDoesNotExist", RouterErrorCode.BlobDoesNotExist,
          ((RouterException) e.getCause()).getErrorCode());
    }
  }

  /**
   * Test the case when the two server responses are {@code ServerErrorCode.Blob_Deleted}, one is in the middle
   * of the responses, and the other is the last response. In this case, we should return {@code Blob_Deleted},
   * as we treat {@code Blob_Deleted} as a successful response, and we have met the {@code successTarget}.
   * The order of received responses is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testBlobNotFoundWithTwoBlobDeleted()
      throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.IO_Error);
    serverErrorCodes[5] = ServerErrorCode.Blob_Deleted;
    serverErrorCodes[8] = ServerErrorCode.Blob_Deleted;
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    future.get();
  }

  /**
   * In this test, there is only one server that returns {@code ServerErrorCode.Blob_Deleted}, which is
   * not sufficient to meet the success target, therefore a router exception should be expected. The order
   * of received responses is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testSingleBlobDeletedReturned()
      throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.Unknown_Error);
    serverErrorCodes[7] = ServerErrorCode.Blob_Deleted;
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    try {
      future.get();
      fail("Deletion should be unsuccessful. Error is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCode should be BlobDeleted", RouterErrorCode.UnexpectedInternalError,
          ((RouterException) e.getCause()).getErrorCode());
    }
  }

  /**
   * Test the case where servers return different {@link ServerErrorCode}, and the {@link DeleteOperation}
   * is able to resolve and conclude the correct {@link RouterErrorCode}. The {@link ServerErrorCode} tested
   * are those could be mapped to {@link RouterErrorCode#AmbryUnavailable}. The order of received responses
   * is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testVariousServerErrorCodes()
      throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    serverErrorCodes[0] = ServerErrorCode.Blob_Not_Found;
    serverErrorCodes[1] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[2] = ServerErrorCode.IO_Error;
    serverErrorCodes[3] = ServerErrorCode.Partition_Unknown;
    serverErrorCodes[4] = ServerErrorCode.Disk_Unavailable;
    serverErrorCodes[5] = ServerErrorCode.No_Error;
    serverErrorCodes[6] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[7] = ServerErrorCode.Unknown_Error;
    serverErrorCodes[8] = ServerErrorCode.Disk_Unavailable;
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    try {
      future.get();
      fail("Deletion should be unsuccessful. Error is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCode should be AmbryUnavailable", RouterErrorCode.AmbryUnavailable,
          ((RouterException) e.getCause()).getErrorCode());
    }
  }

  /**
   * The parallelism is set to 3 not 9.
   *
   * Test the case where servers return different {@link ServerErrorCode}, and the {@link DeleteOperation}
   * is able to resolve and conclude the correct {@link RouterErrorCode}. The {link ServerErrorCode} tested
   * are those could be mapped to {@link RouterErrorCode#AmbryUnavailable}. The order of received responses
   * is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testVariousServerErrorCodesForThreeParallelism()
      throws Exception {
    assertCloseCleanup();
    Properties props = getNonBlockingRouterProperties();
    props.setProperty("router.delete.request.parallelism", "3");
    VerifiableProperties vProps = new VerifiableProperties(props);
    router = new NonBlockingRouter(new RouterConfig(vProps), new NonBlockingRouterMetrics(new MetricRegistry()),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, mockTime);
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    serverErrorCodes[0] = ServerErrorCode.Blob_Not_Found;
    serverErrorCodes[1] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[2] = ServerErrorCode.IO_Error;
    serverErrorCodes[3] = ServerErrorCode.Partition_Unknown;
    serverErrorCodes[4] = ServerErrorCode.Disk_Unavailable;
    serverErrorCodes[5] = ServerErrorCode.No_Error;
    serverErrorCodes[6] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[7] = ServerErrorCode.Unknown_Error;
    serverErrorCodes[8] = ServerErrorCode.Disk_Unavailable;
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    try {
      future.get();
      fail("Deletion should be unsuccessful. Exception is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCode should be AmbryUnavailable", RouterErrorCode.AmbryUnavailable,
          ((RouterException) e.getCause()).getErrorCode());
    }
  }

  /**
   * Test the case when request gets expired before the corresponding store server sends
   * back a response. Set servers to not respond any requests, so {@link DeleteOperation}
   * can be "in flight" all the time. The order of received responses is the same as defined
   * in {@code serverErrorCodes}.
   */
  @Test
  public void testResponseTimeout()
      throws Exception {
    setServerResponse(false);
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.No_Error);
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    do {
      // increment mock time
      mockTime.sleep(1000);
    } while (!operationCompleteLatch.await(10, TimeUnit.MILLISECONDS));
    try {
      future.get();
      fail("Deletion should be unsuccessful. Exception is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCode should be OperationTimedOut", RouterErrorCode.OperationTimedOut,
          ((RouterException) e.getCause()).getErrorCode());
    }
  }

  /**
   * Test the case when the {@link com.github.ambry.network.Selector} of {@link com.github.ambry.network.NetworkClient}
   * experiences various exceptions. The order of received responses is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testSelectorError()
      throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.No_Error);
    HashMap<MockSelectorState, RouterErrorCode> errorCodeHashMap = new HashMap<>();
    errorCodeHashMap.put(MockSelectorState.DisconnectOnSend, RouterErrorCode.OperationTimedOut);
    errorCodeHashMap.put(MockSelectorState.ThrowExceptionOnAllPoll, RouterErrorCode.RouterClosed);
    errorCodeHashMap.put(MockSelectorState.ThrowExceptionOnConnect, RouterErrorCode.OperationTimedOut);
    errorCodeHashMap.put(MockSelectorState.ThrowExceptionOnSend, RouterErrorCode.RouterClosed);
    for (MockSelectorState state : MockSelectorState.values()) {
      if (state == MockSelectorState.Good) {
        continue;
      }
      mockSelectorState.set(state);
      presetServerErrorCode(serverErrorCodes);
      future = router.deleteBlob(blobIdString, new ClientCallback());
      do {
        // increment mock time
        mockTime.sleep(1000);
      } while (!operationCompleteLatch.await(10, TimeUnit.MILLISECONDS));
      try {
        future.get();
        fail("Deletion should be unsuccessful. Exception is expected.");
      } catch (Exception e) {
        assertEquals("RouterErrorCode should be the same as pre defined", errorCodeHashMap.get(state),
            ((RouterException) e.getCause()).getErrorCode());
      }
    }
  }

  /**
   * Test the case how a {@link DeleteManager} acts when a router is closed, and when there are inflight
   * operations. Setting servers to not respond any requests, so {@link DeleteOperation} can be "in flight".
   */
  @Test
  public void testRouterClosedDuringOperation()
      throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.No_Error);
    setServerResponse(false);
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    router.close();
    try {
      future.get();
      fail("Deletion should be unsuccessful. Exception is expected.");
    } catch (ExecutionException e) {
      assertEquals("RouterErrorCode should be RouterClosed", RouterErrorCode.RouterClosed,
          ((RouterException) e.getCause()).getErrorCode());
    }
  }

  /**
   * Prepare {@link MockServer}"s" so that each of them will respond to a request with a predefined
   * {@link ServerErrorCode}. Since each server will have only one replica, so the size of {@code
   * serverErrorCodes} should be the same as the number of replicas for a {@link com.github.ambry.clustermap.Partition}.
   * @param serverErrorCodes The array of {@link ServerErrorCode}.
   */
  private void presetServerErrorCode(ServerErrorCode[] serverErrorCodes) {
    int i = 0;
    for (ReplicaId replica : partition.getReplicaIds()) {
      DataNodeId node = replica.getDataNodeId();
      MockServer mockServer = serverLayout.getMockServer(node.getHostname(), node.getPort());
      mockServer.setBlobIdToServerErrorCode(blobIdString, serverErrorCodes[i++]);
    }
  }

  /**
   * User callback that is called when the {@link DeleteOperation} is completed.
   */
  private class ClientCallback implements Callback<Void> {
    @Override
    public void onCompletion(Void t, Exception e) {
      operationCompleteLatch.countDown();
    }
  }

  /**
   * Asserts that expected threads are not running after the router is closed.
   */
  private void assertCloseCleanup() {
    router.close();
    Assert.assertEquals("No ChunkFiller Thread should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("No RequestResponseHandler should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Sets all the servers if they should respond requests or not.
   *
   * @param shouldRespond {@code true} if the servers should respond, otherwise {@code false}.
   */
  private void setServerResponse(boolean shouldRespond) {
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      MockServer server = serverLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort());
      server.setShouldRespond(shouldRespond);
    }
  }

  /**
   * Generates {@link Properties} that includes initial configuration.
   *
   * @return Properties
   */
  private Properties getNonBlockingRouterProperties() {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.delete.request.parallelism", DELETE_PARALLELISM);
    return properties;
  }

  /**
   * A util method that is used to test when all servers respond the same {@code serverErrorCode},
   * the exception thrown by the router has the error code the same as {@code expectedRouterErrorCode}.
   * @param serverErrorCode The {@link ServerErrorCode} that all servers return.
   * @param expectedRouterErrorCode The excepted {@link RouterErrorCode}.
   */
  private void testResponseForServerErrorUtil(ServerErrorCode serverErrorCode, RouterErrorCode expectedRouterErrorCode)
      throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, serverErrorCode);
    presetServerErrorCode(serverErrorCodes);
    future = router.deleteBlob(blobIdString, new ClientCallback());
    try {
      future.get();
      fail("Deletion should be unsuccessful. Error is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCodes mistmatch.", expectedRouterErrorCode,
          ((RouterException) e.getCause()).getErrorCode());
    }
  }
}
