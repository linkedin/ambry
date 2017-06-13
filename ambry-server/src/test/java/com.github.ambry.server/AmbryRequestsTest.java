/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.network.Request;
import com.github.ambry.network.Send;
import com.github.ambry.network.ServerNetworkResponseMetrics;
import com.github.ambry.network.SocketRequestResponseChannel;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.protocol.RequestControlAdminRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.protocol.Response;
import com.github.ambry.replication.MockFindTokenFactory;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreStats;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link AmbryRequests}.
 */
public class AmbryRequestsTest {
  private static final FindTokenFactory FIND_TOKEN_FACTORY = new MockFindTokenFactory();

  private final MockClusterMap clusterMap;
  private final DataNodeId dataNodeId;
  private final MockStorageManager storageManager;
  private final AmbryRequests ambryRequests;
  private final MockRequestResponseChannel requestResponseChannel = new MockRequestResponseChannel();

  public AmbryRequestsTest() throws IOException, ReplicationException, StoreException {
    clusterMap = new MockClusterMap();
    storageManager = new MockStorageManager();
    Properties properties = new Properties();
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("replication.token.factory", "com.github.ambry.store.StoreFindTokenFactory");
    properties.setProperty("replication.no.of.intra.dc.replica.threads", "0");
    properties.setProperty("replication.no.of.inter.dc.replica.threads", "0");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ReplicationConfig replicationConfig = new ReplicationConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    StoreConfig storeConfig = new StoreConfig(verifiableProperties);
    dataNodeId = clusterMap.getDataNodeIds().get(0);
    ReplicationManager replicationManager =
        new ReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, new StoreKeyFactory() {
          @Override
          public StoreKey getStoreKey(DataInputStream stream) throws IOException {
            return null;
          }
        }, clusterMap, null, dataNodeId, null, clusterMap.getMetricRegistry(), null);
    ambryRequests = new AmbryRequests(storageManager, requestResponseChannel, clusterMap, dataNodeId,
        clusterMap.getMetricRegistry(), FIND_TOKEN_FACTORY, null, replicationManager, null);
  }

  /**
   * Tests that compactions are scheduled correctly.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void scheduleCompactionSuccessTest() throws InterruptedException, IOException {
    List<? extends PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    for (PartitionId id : partitionIds) {
      doScheduleCompactionTest(id, ServerErrorCode.No_Error);
      assertEquals("Partition scheduled for compaction not as expected", id,
          storageManager.compactionScheduledPartitionId);
    }
  }

  /**
   * Tests failure scenarios for compaction - disk down, store not scheduled for compaction, exception while scheduling.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void scheduleCompactionFailureTest() throws InterruptedException, IOException {
    // partitionId not specified
    doScheduleCompactionTest(null, ServerErrorCode.Bad_Request);

    PartitionId id = clusterMap.getWritablePartitionIds().get(0);

    // store is not started - Disk_Unavailable
    storageManager.returnNullStore = true;
    doScheduleCompactionTest(id, ServerErrorCode.Disk_Unavailable);
    storageManager.returnNullStore = false;
    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.

    // disk unavailable
    ReplicaId replicaId = null;
    for (ReplicaId replica : id.getReplicaIds()) {
      if (replica.getDataNodeId().equals(dataNodeId)) {
        replicaId = replica;
        break;
      }
    }
    assertNotNull("Should have found a replicaId", replicaId);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
    doScheduleCompactionTest(id, ServerErrorCode.Disk_Unavailable);
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Ok);

    // store cannot be scheduled for compaction - Unknown_Error
    storageManager.returnValueOfSchedulingCompaction = false;
    doScheduleCompactionTest(id, ServerErrorCode.Unknown_Error);
    storageManager.returnValueOfSchedulingCompaction = true;

    // exception while attempting to schedule - InternalServerError
    storageManager.exceptionToThrowOnSchedulingCompaction = new IllegalStateException();
    doScheduleCompactionTest(id, ServerErrorCode.Unknown_Error);
    storageManager.exceptionToThrowOnSchedulingCompaction = null;
  }

  /**
   * Tests that {@link AdminRequestOrResponseType#RequestControl} works correctly.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void controlRequestSuccessTest() throws InterruptedException, IOException {
    RequestOrResponseType[] requestOrResponseTypes =
        {RequestOrResponseType.PutRequest, RequestOrResponseType.DeleteRequest, RequestOrResponseType.GetRequest, RequestOrResponseType.ReplicaMetadataRequest};
    for (RequestOrResponseType requestType : requestOrResponseTypes) {
      List<? extends PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
      for (PartitionId id : partitionIds) {
        doRequestControlRequestTest(requestType, id);
      }
      doRequestControlRequestTest(requestType, null);
    }
  }

  /**
   * Tests that {@link AdminRequestOrResponseType#RequestControl} fails when bad input is provided (or when there is
   * bad internal state).
   */
  @Test
  public void controlRequestFailureTest() throws InterruptedException, IOException {
    // cannot disable admin request
    sendAndVerifyRequestControlRequest(RequestOrResponseType.AdminRequest, false, null, ServerErrorCode.Bad_Request);
    // PartitionUnknown is hard to simulate without betraying knowledge of the internals of MockClusterMap.
  }

  // helpers

  // general

  /**
   * Calls {@link AmbryRequests#handleRequests(Request)} with {@code request} and returns the {@link Response} received.
   * @param request the {@link Request} to process
   * @param expectedServerErrorCode the expected {@link ServerErrorCode} in the server response.
   * @return the {@link Response} received.
   * @throws InterruptedException
   * @throws IOException
   */
  private Response sendRequestGetResponse(RequestOrResponse request, ServerErrorCode expectedServerErrorCode)
      throws InterruptedException, IOException {
    Request mockRequest = MockRequest.fromRequest(request);
    ambryRequests.handleRequests(mockRequest);
    assertEquals("Request accompanying response does not match original request", mockRequest,
        requestResponseChannel.lastOriginalRequest);
    assertNotNull("Response not sent", requestResponseChannel.lastResponse);
    Response response = (Response) requestResponseChannel.lastResponse;
    assertNotNull("Response is not of type Response", response);
    assertEquals("Correlation id in response does match the one in the request", request.getCorrelationId(),
        response.getCorrelationId());
    assertEquals("Client id in response does match the one in the request", request.getClientId(),
        response.getClientId());
    assertEquals("Error code does not match expected", expectedServerErrorCode, response.getError());
    return response;
  }

  // scheduleCompactionSuccessTest() and scheduleCompactionFailuresTest() helpers

  /**
   * Schedules a compaction for {@code id} and checks that the {@link ServerErrorCode} returned matches
   * {@code expectedServerErrorCode}.
   * @param id the {@link PartitionId} to schedule compaction for.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected when the request is processed.
   * @throws InterruptedException
   * @throws IOException
   */
  private void doScheduleCompactionTest(PartitionId id, ServerErrorCode expectedServerErrorCode)
      throws InterruptedException, IOException {
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = UtilsTest.getRandomString(10);
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.TriggerCompaction, id, correlationId, clientId);
    Response response = sendRequestGetResponse(adminRequest, expectedServerErrorCode);
    assertTrue("Response not of type AdminResponse", response instanceof AdminResponse);
  }

  // controlRequestSuccessTest() and controlRequestFailureTest() helpers

  /**
   * Does the test for {@link AdminRequestOrResponseType#RequestControl} by checking that
   * 1. The request that is being disabled works
   * 2. The disabling of the request works
   * 3. The request disabling has taken effect
   * 4. The enabling of the request works
   * 5. The re-enabled request works correctly
   * @param toControl the {@link RequestOrResponseType} to control.
   * @param id the {@link PartitionId} to disable {@code toControl} on. Can be {@code null}.
   * @throws InterruptedException
   * @throws IOException
   */
  private void doRequestControlRequestTest(RequestOrResponseType toControl, PartitionId id)
      throws InterruptedException, IOException {
    List<? extends PartitionId> idsToTest;
    if (id == null) {
      idsToTest = clusterMap.getAllPartitionIds();
    } else {
      idsToTest = Collections.singletonList(id);
    }
    // check that everything works
    sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.No_Error);
    // disable the request
    sendAndVerifyRequestControlRequest(toControl, false, id, ServerErrorCode.No_Error);
    // check that it is disabled
    sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.Temporarily_Disabled);
    // ok to call disable again
    sendAndVerifyRequestControlRequest(toControl, false, id, ServerErrorCode.No_Error);
    // enable
    sendAndVerifyRequestControlRequest(toControl, true, id, ServerErrorCode.No_Error);
    // check that everything works
    sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.No_Error);
    // ok to call enable again
    sendAndVerifyRequestControlRequest(toControl, true, id, ServerErrorCode.No_Error);
    // check that everything works
    sendAndVerifyOperationRequest(toControl, idsToTest, ServerErrorCode.No_Error);
  }

  /**
   * Sends and verifies that an operation specific request works correctly.
   * @param requestType the type of the request to send.
   * @param ids the partitionIds to send requests for.
   * @param expectedErrorCode the {@link ServerErrorCode} expected in the response. For some requests this is the
   *                          response in the constituents rather than the actual response ({@link GetResponse} and
   *                          {@link ReplicaMetadataResponse}).
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendAndVerifyOperationRequest(RequestOrResponseType requestType, List<? extends PartitionId> ids,
      ServerErrorCode expectedErrorCode) throws InterruptedException, IOException {
    for (PartitionId id : ids) {
      int correlationId = TestUtils.RANDOM.nextInt();
      String clientId = UtilsTest.getRandomString(10);
      BlobId blobId = new BlobId(BlobId.DEFAULT_FLAG, ClusterMapUtils.UNKNOWN_DATACENTER_ID, Account.UNKNOWN_ACCOUNT_ID,
          Container.UNKNOWN_CONTAINER_ID, id);
      RequestOrResponse request;
      switch (requestType) {
        case PutRequest:
          BlobProperties properties = new BlobProperties(0, "serviceId", blobId.getAccountId(), blobId.getAccountId());
          request = new PutRequest(correlationId, clientId, blobId, properties, ByteBuffer.allocate(0),
              ByteBuffer.allocate(0), 0, BlobType.DataBlob);
          break;
        case DeleteRequest:
          request = new DeleteRequest(correlationId, clientId, blobId, SystemTime.getInstance().milliseconds());
          break;
        case GetRequest:
          PartitionRequestInfo pRequestInfo = new PartitionRequestInfo(id, Collections.singletonList(blobId));
          request =
              new GetRequest(correlationId, clientId, MessageFormatFlags.All, Collections.singletonList(pRequestInfo),
                  GetOption.Include_All);
          break;
        case ReplicaMetadataRequest:
          ReplicaMetadataRequestInfo rRequestInfo =
              new ReplicaMetadataRequestInfo(id, FIND_TOKEN_FACTORY.getNewFindToken(), "localhost", "/tmp");
          request = new ReplicaMetadataRequest(correlationId, clientId, Collections.singletonList(rRequestInfo),
              Long.MAX_VALUE);
          break;
        default:
          throw new IllegalArgumentException(requestType + " not supported by this function");
      }
      storageManager.resetStore();
      Response response = sendRequestGetResponse(request,
          requestType == RequestOrResponseType.GetRequest || requestType == RequestOrResponseType.ReplicaMetadataRequest
              ? ServerErrorCode.No_Error : expectedErrorCode);
      if (expectedErrorCode.equals(ServerErrorCode.No_Error)) {
        assertEquals("Operation received at the store not as expected", requestType,
            MockStorageManager.operationReceived);
      }
      if (requestType == RequestOrResponseType.GetRequest) {
        GetResponse getResponse = (GetResponse) response;
        for (PartitionResponseInfo info : getResponse.getPartitionResponseInfoList()) {
          assertEquals("Error code does not match expected", expectedErrorCode, info.getErrorCode());
        }
      } else if (requestType == RequestOrResponseType.ReplicaMetadataRequest) {
        ReplicaMetadataResponse replicaMetadataResponse = (ReplicaMetadataResponse) response;
        for (ReplicaMetadataResponseInfo info : replicaMetadataResponse.getReplicaMetadataResponseInfoList()) {
          assertEquals("Error code does not match expected", expectedErrorCode, info.getError());
        }
      }
    }
  }

  /**
   * Sends and verifies that a {@link AdminRequestOrResponseType#RequestControl} request received the error code
   * expected.
   * @param toControl the {@link AdminRequestOrResponseType#RequestControl} to control.
   * @param enable {@code true} if {@code toControl} needs to be enabled. {@code false} otherwise.
   * @param id the {@link PartitionId} to send the request for. Can be {@code null}.
   * @param expectedServerErrorCode the {@link ServerErrorCode} expected in the response.
   * @throws InterruptedException
   * @throws IOException
   */
  private void sendAndVerifyRequestControlRequest(RequestOrResponseType toControl, boolean enable, PartitionId id,
      ServerErrorCode expectedServerErrorCode) throws InterruptedException, IOException {
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = UtilsTest.getRandomString(10);
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.RequestControl, id, correlationId, clientId);
    RequestControlAdminRequest controlRequest = new RequestControlAdminRequest(toControl, enable, adminRequest);
    Response response = sendRequestGetResponse(controlRequest, expectedServerErrorCode);
    assertTrue("Response not of type AdminResponse", response instanceof AdminResponse);
  }

  /**
   * Implementation of {@link Request} to help with tests.
   */
  private static class MockRequest implements Request {

    private final InputStream stream;

    /**
     * Constructs a {@link MockRequest} from {@code request}.
     * @param request the {@link RequestOrResponse} to construct the {@link MockRequest} for.
     * @return an instance of {@link MockRequest} that represents {@code request}.
     * @throws IOException
     */
    static MockRequest fromRequest(RequestOrResponse request) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate((int) request.sizeInBytes());
      request.writeTo(new ByteBufferChannel(buffer));
      buffer.flip();
      // read length (to bring it to a state where AmbryRequests can handle it).
      buffer.getLong();
      return new MockRequest(new ByteBufferInputStream(buffer));
    }

    /**
     * Constructs a {@link MockRequest}.
     * @param stream the {@link InputStream} that will be returned on a call to {@link #getInputStream()}.
     */
    private MockRequest(InputStream stream) {
      this.stream = stream;
    }

    @Override
    public InputStream getInputStream() {
      return stream;
    }

    @Override
    public long getStartTimeInMs() {
      return 0;
    }
  }

  /**
   * An extension of {@link SocketRequestResponseChannel} to help with tests.
   */
  private static class MockRequestResponseChannel extends SocketRequestResponseChannel {
    /**
     * {@link Request} provided in the last call to {@link #sendResponse(Send, Request, ServerNetworkResponseMetrics).
     */
    Request lastOriginalRequest = null;

    /**
     * The {@link Send} provided in the last call to {@link #sendResponse(Send, Request, ServerNetworkResponseMetrics).
     */
    Send lastResponse = null;

    MockRequestResponseChannel() {
      super(1, 1);
    }

    @Override
    public void sendResponse(Send payloadToSend, Request originalRequest, ServerNetworkResponseMetrics metrics) {
      lastResponse = payloadToSend;
      lastOriginalRequest = originalRequest;
    }
  }

  /**
   * An extension of {@link StorageManager} to help with tests.
   */
  private static class MockStorageManager extends StorageManager {

    /**
     * The operation received at the store.
     */
    static RequestOrResponseType operationReceived = null;

    /**
     * An empty {@link Store} implementation.
     */
    private static Store store = new Store() {

      @Override
      public void start() throws StoreException {

      }

      @Override
      public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions)
          throws StoreException {
        operationReceived = RequestOrResponseType.GetRequest;
        return new StoreInfo(new MessageReadSet() {
          @Override
          public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize)
              throws IOException {
            return 0;
          }

          @Override
          public int count() {
            return 0;
          }

          @Override
          public long sizeInBytes(int index) {
            return 0;
          }

          @Override
          public StoreKey getKeyAt(int index) {
            return null;
          }
        }, Collections.EMPTY_LIST);
      }

      @Override
      public void put(MessageWriteSet messageSetToWrite) throws StoreException {
        operationReceived = RequestOrResponseType.PutRequest;
      }

      @Override
      public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
        operationReceived = RequestOrResponseType.DeleteRequest;
      }

      @Override
      public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
        operationReceived = RequestOrResponseType.ReplicaMetadataRequest;
        return new FindInfo(Collections.EMPTY_LIST, FIND_TOKEN_FACTORY.getNewFindToken());
      }

      @Override
      public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
        return null;
      }

      @Override
      public StoreStats getStoreStats() {
        return null;
      }

      @Override
      public boolean isKeyDeleted(StoreKey key) throws StoreException {
        return false;
      }

      @Override
      public long getSizeInBytes() {
        return 0;
      }

      @Override
      public void shutdown() throws StoreException {

      }
    };

    private static final VerifiableProperties VPROPS = new VerifiableProperties(new Properties());

    /**
     * if {@code true}, a {@code null} {@link Store} is returned on a call to {@link #getStore(PartitionId)}. Otherwise
     * {@link #store} is returned.
     */
    boolean returnNullStore = false;
    /**
     * If non-null, the given exception is thrown when {@link #scheduleNextForCompaction(PartitionId)} is called.
     */
    RuntimeException exceptionToThrowOnSchedulingCompaction = null;
    /**
     * The return value for a call to {@link #scheduleNextForCompaction(PartitionId)}.
     */
    boolean returnValueOfSchedulingCompaction = true;
    /**
     * The {@link PartitionId} that was provided in the call to {@link #scheduleNextForCompaction(PartitionId)}
     */
    PartitionId compactionScheduledPartitionId = null;

    MockStorageManager() throws StoreException {
      super(new StoreConfig(VPROPS), new DiskManagerConfig(VPROPS), new MetricRegistry(), Collections.EMPTY_LIST, null,
          null, null, new MockTime(), Utils.newScheduler(1, true));
    }

    @Override
    public Store getStore(PartitionId id) {
      return returnNullStore ? null : store;
    }

    @Override
    public boolean scheduleNextForCompaction(PartitionId id) {
      if (exceptionToThrowOnSchedulingCompaction != null) {
        throw exceptionToThrowOnSchedulingCompaction;
      }
      compactionScheduledPartitionId = id;
      return returnValueOfSchedulingCompaction;
    }

    void resetStore() {
      operationReceived = null;
    }
  }
}
