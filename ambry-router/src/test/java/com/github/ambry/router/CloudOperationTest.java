/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestination;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.network.CompositeNetworkClientFactory;
import com.github.ambry.network.LocalNetworkClientFactory;
import com.github.ambry.network.LocalRequestResponseChannel;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Tests cross colo error handling for {@link GetBlobOperation}, {@link TtlUpdateOperation} and {@link DeleteOperation}
 * This class creates a {@link NonBlockingRouter} with a {@link MockServer} and
 * a {@link LatchBasedInMemoryCloudDestination} and does puts through it.
 * Test uses a {@link MockCompositeNetworkClient} to send requests to and get responses from either disk backed or cloud backed colo
 * MockCompositeNetworkClient has two sub clients.
 * 1. SocketNetworkClient talks to mock disk colo {@link MockServer}
 * 2. LocalNetworkClient talks to mock cloud colo {@link LatchBasedInMemoryCloudDestination}
 * Test may inject error to the mock disk colo {@link MockServer} and Mock cloud colo {@link LatchBasedInMemoryCloudDestination}
 * to verify the cross colo error handling.
 */
@RunWith(Parameterized.class)
public class CloudOperationTest {
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final String LOCAL_DC = "DC3";
  private final int maxChunkSize;
  private final MockTime time = new MockTime();
  private final Random random = new Random();
  private final MockClusterMap mockClusterMap;
  private final BlobIdFactory blobIdFactory;
  private final NonBlockingRouterMetrics routerMetrics;
  private final MockServerLayout mockServerLayout;
  private final ResponseHandler responseHandler;
  private final NonBlockingRouter router;
  private final MockCompositeNetworkClient mockNetworkClient;
  private final RouterCallback routerCallback;
  private final String operationTrackerType;

  // Mock servers include disk backed "mockServers" and cloud backed "cloudDestination"
  private final LatchBasedInMemoryCloudDestination cloudDestination;
  private final Collection<MockServer> mockServers;

  private final RouterConfig routerConfig;

  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  /**
   * Running for both {@link SimpleOperationTracker} and {@link AdaptiveOperationTracker}
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{SimpleOperationTracker.class.getSimpleName()},
        {AdaptiveOperationTracker.class.getSimpleName()}});
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    router.close();
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Instantiate a router.
   * @param operationTrackerType the type of {@link OperationTracker} to use.
   */
  public CloudOperationTest(final String operationTrackerType) throws Exception {
    final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>();

    this.operationTrackerType = operationTrackerType;
    maxChunkSize = random.nextInt(1024 * 1024) + 1;

    mockSelectorState.set(MockSelectorState.Good);
    VerifiableProperties vprops = new VerifiableProperties(getDefaultNonBlockingRouterProperties(LOCAL_DC));
    routerConfig = new RouterConfig(vprops);
    // include cloud backed colo
    mockClusterMap = new MockClusterMap(false, true, 9, 3, 3, false, true, LOCAL_DC);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    Assert.assertEquals("Local DC Name is same as the one we set.", LOCAL_DC, localDcName);

    blobIdFactory = new BlobIdFactory(mockClusterMap);
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    mockServerLayout = new MockServerLayout(mockClusterMap);
    mockServers = mockServerLayout.getMockServers();
    responseHandler = new ResponseHandler(mockClusterMap);

    CloudConfig cloudConfig = new CloudConfig(vprops);
    CloudDestinationFactory cloudDestinationFactory =
        Utils.getObj(cloudConfig.cloudDestinationFactoryClass, vprops, mockClusterMap.getMetricRegistry(),
            mockClusterMap);
    cloudDestination = (LatchBasedInMemoryCloudDestination) cloudDestinationFactory.getCloudDestination();

    AccountService accountService = new InMemAccountService(false, true);
    CloudRouterFactory cloudRouterFactory = new CloudRouterFactory(vprops, mockClusterMap,
        new LoggingNotificationSystem(), null, accountService);

    RequestHandlerPool requestHandlerPool =
        cloudRouterFactory.getRequestHandlerPool(vprops, mockClusterMap, cloudDestination, cloudConfig);

    Map<ReplicaType, NetworkClientFactory> childFactories = new EnumMap<>(ReplicaType.class);
    // requestHandlerPool and its thread pool handle the cloud blob operations.
    LocalNetworkClientFactory cloudClientFactory =
        new LocalNetworkClientFactory((LocalRequestResponseChannel) requestHandlerPool.getChannel(),
            new NetworkConfig(vprops), new NetworkMetrics(routerMetrics.getMetricRegistry()), time);
    childFactories.put(ReplicaType.CLOUD_BACKED, cloudClientFactory);

    MockNetworkClientFactory diskClientFactory =
        new MockNetworkClientFactory(vprops, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, time);
    childFactories.put(ReplicaType.DISK_BACKED, diskClientFactory);

    NetworkClientFactory networkClientFactory = new CompositeNetworkClientFactory(childFactories);
    router = new NonBlockingRouter(routerConfig, routerMetrics, networkClientFactory, new LoggingNotificationSystem(),
        mockClusterMap, null, null, null, new InMemAccountService(false, true), time,
        MockClusterMap.DEFAULT_PARTITION_CLASS);

    NetworkClient compNetworkClient = networkClientFactory.getNetworkClient();
    mockNetworkClient = new MockCompositeNetworkClient(compNetworkClient);
    routerCallback = new RouterCallback(mockNetworkClient, new ArrayList<>());
  }

  /**
   * Do a put directly to the mock servers. This allows for blobs with malformed properties to be constructed.
   * @param blobProperties the {@link BlobProperties} for the blob.
   * @param userMetadata user meta data of the blob.
   * @param blobContent the raw content for the blob to upload (i.e. this can be serialized composite blob metadata or
   *                    an encrypted blob).
   * @return the blob id
   * @throws Exception Any unexpected exception
   */
  private BlobId doDirectPut(BlobProperties blobProperties, byte[] userMetadata, ByteBuf blobContent) throws Exception {
    List<PartitionId> writablePartitionIds =
        mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partitionId = writablePartitionIds.get(random.nextInt(writablePartitionIds.size()));
    BlobId blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), blobProperties.getAccountId(), blobProperties.getContainerId(),
        partitionId, blobProperties.isEncrypted(), BlobId.BlobDataType.DATACHUNK);
    Iterator<MockServer> servers = partitionId.getReplicaIds()
        .stream()
        .map(ReplicaId::getDataNodeId)
        .map(dataNodeId -> mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort()))
        .iterator();

    ByteBuffer userMetadataBuf = ByteBuffer.wrap(userMetadata);
    while (servers.hasNext()) {
      MockServer server = servers.next();
      PutRequest request =
          new PutRequest(random.nextInt(), "clientId", blobId, blobProperties, userMetadataBuf.duplicate(),
              blobContent.retainedDuplicate(), blobContent.readableBytes(), BlobType.DataBlob, null);
      // Make sure we release the BoundedNettyByteBufReceive.
      server.send(request).release();
      request.release();
    }

    // send to Cloud destinations.
    PutRequest request =
        new PutRequest(random.nextInt(), "clientId", blobId, blobProperties, userMetadataBuf.duplicate(),
            blobContent.retainedDuplicate(), blobContent.readableBytes(), BlobType.DataBlob, null);
    // Get the cloud replica.
    ReplicaId replica = partitionId.getReplicaIds().get(0);
    Assert.assertEquals("It should be a cloud backed replica.", replica.getReplicaType(), ReplicaType.CLOUD_BACKED);
    String hostname = replica.getDataNodeId().getHostname();
    Port port = new Port(-1, PortType.PLAINTEXT);

    List<RequestInfo> requestList = new ArrayList<>();
    RequestInfo requestInfo = new RequestInfo(hostname, port, request, replica, null);
    requestList.add(requestInfo);
    List<ResponseInfo> responseList = sendAndWaitForResponses(requestList);
    request.release();
    blobContent.release();
    return blobId;
  }

  /**
   * Do a put directly to the mock servers. This allows for blobs with malformed properties to be constructed.
   * @return the blob id
   * @throws Exception Any unexpected exception
   */
  private String doDirectPut() throws Exception {
    // a blob size that is greater than the maxChunkSize and is not a multiple of it. Will result in a composite blob.
    int blobSize = maxChunkSize * random.nextInt(10) + random.nextInt(maxChunkSize - 1) + 1;
    BlobProperties blobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(random), Utils.getRandomShort(random), false, null, null, null);
    byte[] userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    byte[] putContent = new byte[blobSize];
    random.nextBytes(putContent);
    ByteBuf putContentBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(blobSize);
    putContentBuf.writeBytes(putContent);
    BlobId blobId = doDirectPut(blobProperties, userMetadata, putContentBuf.retainedDuplicate());
    putContentBuf.release();
    return blobId.getID();
  }

  /**
   * Submit all the requests that were handed over by the operation and wait until a response is received for every
   * one of them.
   * @param requestList the list containing the requests handed over by the operation.
   * @return the list of responses from the network client.
   */
  private List<ResponseInfo> sendAndWaitForResponses(List<RequestInfo> requestList) {
    int sendCount = requestList.size();
    // Shuffle the replicas to introduce randomness in the order in which responses arrive.
    Collections.shuffle(requestList);
    List<ResponseInfo> responseList = new ArrayList<>();
    responseList.addAll(mockNetworkClient.sendAndPoll(requestList, Collections.emptySet(), 100));
    requestList.clear();
    while (responseList.size() < sendCount) {
      responseList.addAll(mockNetworkClient.sendAndPoll(requestList, Collections.emptySet(), 100));
    }
    return responseList;
  }

  /**
   * Construct GetBlob operations with appropriate callbacks, then poll those operations until they complete,
   * and ensure that the whole blob data is read out and the contents match.
   * @param blobId id of the blob to get
   * @param expectedLifeVersion the expected lifeVersion from get operation.
   * @param expectedBlobSize the expected blob size
   * @param expectedBlobProperties  the expected {@link BlobProperties} for the blob.
   * @param expectedUserMetadata the expected user meta data
   * @param expectPutContent the expected blob content
   * @param options options of the get blob operation
   * @throws Exception Any unexpected exception
   */
  private void getBlobAndAssertSuccess(final BlobId blobId, final short expectedLifeVersion, final int expectedBlobSize,
      final BlobProperties expectedBlobProperties, final byte[] expectedUserMetadata, final byte[] expectPutContent,
      final GetBlobOptionsInternal options) throws Exception {
    final CountDownLatch readCompleteLatch = new CountDownLatch(1);
    final AtomicLong readCompleteResult = new AtomicLong(0);
    // callback to compare the data
    Callback<GetBlobResultInternal> callback = (result, exception) -> {
      Assert.assertNull("Shouldn't have exception", exception);
      try {
        BlobInfo blobInfo;
        switch (options.getBlobOptions.getOperationType()) {
          case All:
            Assert.assertFalse("not supposed to be raw mode", options.getBlobOptions.isRawMode());
            blobInfo = result.getBlobResult.getBlobInfo();
            Assert.assertTrue("Blob properties must be the same",
                RouterTestHelpers.arePersistedFieldsEquivalent(expectedBlobProperties, blobInfo.getBlobProperties()));
            Assert.assertEquals("Blob size should in received blobProperties should be the same as actual",
                expectedBlobSize, blobInfo.getBlobProperties().getBlobSize());
            Assert.assertArrayEquals("User metadata must be the same", expectedUserMetadata,
                blobInfo.getUserMetadata());
            Assert.assertEquals("LifeVersion mismatch", expectedLifeVersion, blobInfo.getLifeVersion());
            break;
          case Data:
            Assert.assertNull("Unexpected blob info in operation result", result.getBlobResult.getBlobInfo());
            break;
          case BlobInfo:
            blobInfo = result.getBlobResult.getBlobInfo();
            Assert.assertTrue("Blob properties must be the same",
                RouterTestHelpers.arePersistedFieldsEquivalent(expectedBlobProperties, blobInfo.getBlobProperties()));
            Assert.assertEquals("Blob size should in received blobProperties should be the same as actual",
                expectedBlobSize, blobInfo.getBlobProperties().getBlobSize());
            Assert.assertNull("Unexpected blob data in operation result", result.getBlobResult.getBlobDataChannel());
            Assert.assertEquals("LifeVersion mismatch", expectedLifeVersion, blobInfo.getLifeVersion());
        }
      } catch (Throwable e) {
        Assert.fail("Shouldn't receive exception here");
      }

      if (options.getBlobOptions.getOperationType() != GetBlobOptions.OperationType.BlobInfo) {
        final ByteBufferAsyncWritableChannel asyncWritableChannel = new ByteBufferAsyncWritableChannel();
        Utils.newThread(() -> {
          Future<Long> readIntoFuture = result.getBlobResult.getBlobDataChannel().readInto(asyncWritableChannel, null);
          assertBlobReadSuccess(options.getBlobOptions, readIntoFuture, asyncWritableChannel,
              result.getBlobResult.getBlobDataChannel(), readCompleteLatch, readCompleteResult, expectedBlobSize,
              expectPutContent);
        }, false).start();
      } else {
        readCompleteLatch.countDown();
      }
    };

    // create GetBlobOperation
    final Map<Integer, GetOperation> correlationIdToGetOperation = new HashMap<>();
    final RequestRegistrationCallback<GetOperation> requestRegistrationCallback =
        new RequestRegistrationCallback<>(correlationIdToGetOperation);
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobOperation op =
        new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, callback,
            routerCallback, blobIdFactory, null, null, null, time, false, null);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());

    // Wait operation to complete
    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      List<ResponseInfo> responses = sendAndWaitForResponses(requestRegistrationCallback.getRequestsToSend());
      for (ResponseInfo responseInfo : responses) {
        GetResponse getResponse =
            RouterUtils.extractResponseAndNotifyResponseHandler(responseHandler, routerMetrics, responseInfo,
                stream -> GetResponse.readFrom(stream, mockClusterMap), response -> {
                  ServerErrorCode serverError = response.getError();
                  if (serverError == ServerErrorCode.No_Error) {
                    serverError = response.getPartitionResponseInfoList().get(0).getErrorCode();
                  }
                  return serverError;
                });
        op.handleResponse(responseInfo, getResponse);
        responseInfo.release();
      }
    }

    readCompleteLatch.await();
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());

    // Ensure that a ChannelClosed exception is not set when the ReadableStreamChannel is closed correctly.
    Assert.assertNull("Callback operation exception should be null", op.getOperationException());
    if (options.getBlobOptions.getOperationType() != GetBlobOptions.OperationType.BlobInfo
        && !options.getBlobOptions.isRawMode() && !options.getChunkIdsOnly) {
      int sizeWritten = expectedBlobSize;
      if (options.getBlobOptions.getRange() != null) {
        ByteRange range = options.getBlobOptions.getRange()
            .toResolvedByteRange(expectedBlobSize, options.getBlobOptions.resolveRangeOnEmptyBlob());
        sizeWritten = (int) range.getRangeSize();
      }
      Assert.assertEquals("Size read must equal size written", sizeWritten, readCompleteResult.get());
    }
  }

  /**
   * Assert that the operation is complete and successful. Note that the future completion and callback invocation
   * happens outside of the GetOperation, so those are not checked here. But at this point, the operation result should
   * be ready.
   * @param options The {@link GetBlobOptions} for the operation to check.
   * @param readIntoFuture The future associated with the read on the {@link ReadableStreamChannel} result of the
   *                       operation.
   * @param asyncWritableChannel The {@link ByteBufferAsyncWritableChannel} to which bytes will be written by the
   *                             operation.
   * @param readableStreamChannel The {@link ReadableStreamChannel} that bytes are read from in the operation.
   * @param readCompleteLatch The latch to count down once the read is completed.
   * @param readCompleteResult This will contain the bytes written on return.
   * @param blobSize size of the blob
   * @param putContent expected content of the blob
   */
  private void assertBlobReadSuccess(GetBlobOptions options, Future<Long> readIntoFuture,
      ByteBufferAsyncWritableChannel asyncWritableChannel, ReadableStreamChannel readableStreamChannel,
      CountDownLatch readCompleteLatch, AtomicLong readCompleteResult, final int blobSize, final byte[] putContent) {
    try {
      ByteBuffer putContentBuf;
      Assert.assertTrue("Not intended to test raw mode.", options == null || !options.isRawMode());

      // If a range is set, compare the result against the specified byte range.
      if (options != null && options.getRange() != null) {
        ByteRange range = options.getRange().toResolvedByteRange(blobSize, options.resolveRangeOnEmptyBlob());
        putContentBuf = ByteBuffer.wrap(putContent, (int) range.getStartOffset(), (int) range.getRangeSize());
      } else {
        putContentBuf = ByteBuffer.wrap(putContent);
      }

      long written;
      // Compare byte by byte.
      final int bytesToRead = putContentBuf.remaining();
      int readBytes = 0;
      do {
        ByteBuffer buf = asyncWritableChannel.getNextChunk();
        int bufLength = buf.remaining();
        Assert.assertTrue("total content read should not be greater than length of put content",
            readBytes + bufLength <= bytesToRead);
        while (buf.hasRemaining()) {
          Assert.assertEquals("Get and Put blob content should match", putContentBuf.get(), buf.get());
          readBytes++;
        }
        asyncWritableChannel.resolveOldestChunk(null);
      } while (readBytes < bytesToRead);
      written = readIntoFuture.get();
      Assert.assertEquals("the returned length in the future should be the length of data written", readBytes, written);
      Assert.assertNull("There should be no more data in the channel", asyncWritableChannel.getNextChunk(0));
      readableStreamChannel.close();
      readCompleteResult.set(written);
    } catch (Throwable e) {
      Assert.fail("Shouldn't receive exception here.");
    } finally {
      readCompleteLatch.countDown();
    }
  }

  /**
   * Disk backed DC returns either Disk Down or Not Found.
   * Cloud backed DC returns Not Found.
   */
  @Test
  public void testGetBlobCombinedDiskDownAndNotFoundCase() throws Exception {
    // put blob to all the DCs.
    String blobId = doDirectPut();

    // All other DCs except LOCAL_DC will return Blob_Not_Found
    List<MockServer> localDcServers =
        mockServers.stream().filter(s -> s.getDataCenter().equals(LOCAL_DC)).collect(Collectors.toList());
    mockServers.forEach(s -> {
      if (!localDcServers.contains(s)) {
        s.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
      }
    });
    // Local data center, two nodes will return Disk_Unavailable, one node will return Blob_Not_Found
    for (int i = 0; i < 3; ++i) {
      if (i < 2) {
        localDcServers.get(i).setServerErrorForAllRequests(ServerErrorCode.Disk_Unavailable);
      } else {
        localDcServers.get(i).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
      }
    }
    // cloud set to ID_Not_Found
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.ID_Not_Found);

    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
      fail("Expecting exception");
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      assertTrue("Cause should be RouterException", t instanceof RouterException);
      // ServerErrorCode.Data_Corrupt will be transferred to RouterErrorCode.UnexpectedInternalError
      assertSame("ErrorCode mismatch", ((RouterException) t).getErrorCode(), RouterErrorCode.BlobDoesNotExist);
    }
  }

  /**
   * Disk backed DC all returns failure but cloud backed DC returns the Blob information successfully.
   * Check the meta data and user data are correctly returned by the cloud colo.
   */
  @Test
  public void testGetBlobFailoverToAzureAndCheckData() throws Exception {
    // a blob size that is greater than the maxChunkSize and is not a multiple of it. Will result in a composite blob.
    int blobSize = maxChunkSize * random.nextInt(10) + random.nextInt(maxChunkSize - 1) + 1;
    BlobProperties blobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(random), Utils.getRandomShort(random), false, null, null, null);
    byte[] userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    byte[] putContent = new byte[blobSize];
    random.nextBytes(putContent);
    ByteBuf putContentBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(blobSize);
    putContentBuf.writeBytes(putContent);
    BlobId blobId = doDirectPut(blobProperties, userMetadata, putContentBuf.retainedDuplicate());
    putContentBuf.release();

    // Confirm we can get the blob from the local dc.
    GetBlobOptionsInternal options =
        new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet);
    getBlobAndAssertSuccess(blobId, (short) 0, blobSize, blobProperties, userMetadata, putContent, options);

    // Local DC will fail with different errors but cloud will return the blob data.
    ArrayList<MockServer> mockServersArray = new ArrayList<>(mockServers);
    ArrayList<ServerErrorCode> serverErrors = new ArrayList<>(Arrays.asList(ServerErrorCode.values()));
    // set the disk backed server status to various server level or partition level errors (not Blob_Deleted or Blob_Expired - as they are final)
    serverErrors.remove(ServerErrorCode.Blob_Deleted);
    serverErrors.remove(ServerErrorCode.Blob_Expired);
    serverErrors.remove(ServerErrorCode.No_Error);
    serverErrors.remove(ServerErrorCode.Blob_Authorization_Failure);
    for (MockServer mockServer : mockServersArray) {
      ServerErrorCode code = serverErrors.get(random.nextInt(serverErrors.size()));
      mockServer.setServerErrorForAllRequests(code);
    }
    // cloud DC will return the data
    getBlobAndAssertSuccess(blobId, (short) 0, blobSize, blobProperties, userMetadata, putContent, options);
  }

  /**
   * TtlUpdate fails on all disk DCs.
   * Test both cases that cloud colo succeeds or fails.
   */
  @Test
  public void testTtlUpdateFailoverToAzure() throws Exception {
    String blobId = doDirectPut();

    // configure all the disk backed server will return failure
    mockServers.forEach(s -> s.setServerErrorForAllRequests(ServerErrorCode.Unknown_Error));
    // although all disk colo will fail, cloud colo will return it successfully.
    router.updateBlobTtl(blobId, null, Utils.Infinite_Time).get();

    // inject error for cloud colo as well. Now will fail
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.TTL_Expired);
    try {
      router.updateBlobTtl(blobId, null, Utils.Infinite_Time).get();
      fail("Expecting exception");
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      assertTrue("Cause should be RouterException", t instanceof RouterException);
      // ServerErrorCode.Blob_Expired will be transferred to RouterErrorCode.BlobExpired
      assertSame("ErrorCode mismatch", ((RouterException) t).getErrorCode(), RouterErrorCode.BlobExpired);
    }
  }

  /**
   * GetBlobOperations fails on all disk DCs.
   * Test both cases that cloud colo succeeds or fails.
   */
  @Test
  public void testGetBlobFailoverToAzure() throws Exception {
    String blobId = doDirectPut();

    mockServers.forEach(s -> s.setServerErrorForAllRequests(ServerErrorCode.Data_Corrupt));
    // although all disk colo will fail, cloud colo will return it successfully.
    router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();

    // inject error for cloud colo as well.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.ID_Not_Found);
    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
      fail("Expecting exception");
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      assertTrue("Cause should be RouterException", t instanceof RouterException);
      // ServerErrorCode.Data_Corrupt will be transferred to RouterErrorCode.UnexpectedInternalError
      assertTrue("ErrorCode mismatch", ((RouterException) t).getErrorCode() == RouterErrorCode.BlobDoesNotExist
          || ((RouterException) t).getErrorCode() == RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * GetBlobInfoOperations fails on all disk DCs.
   * Test both cases that cloud colo succeeds or fails.
   */
  @Test
  public void testGetBlobInfoFailoverToAzure() throws Exception {
    String blobId = doDirectPut();

    mockServers.forEach(s -> s.setServerErrorForAllRequests(ServerErrorCode.Data_Corrupt));
    // although all disk colo will fail, cloud colo will return it successfully.
    router.getBlob(blobId, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build())
        .get();

    // inject error for cloud colo as well.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.ID_Not_Found);
    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build())
          .get();
      fail("Expecting exception");
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      assertTrue("Cause should be RouterException", t instanceof RouterException);
      // ServerErrorCode.Data_Corrupt will be transferred to RouterErrorCode.UnexpectedInternalError
      assertTrue("ErrorCode mismatch", ((RouterException) t).getErrorCode() == RouterErrorCode.BlobDoesNotExist
          || ((RouterException) t).getErrorCode() == RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * DeleteBlob fails on all disk DCs.
   * Test both cases that cloud colo succeeds or fails.
   */
  @Test
  public void testDeleteBlobFailoverToAzure() throws Exception {
    String blobId = doDirectPut();

    mockServers.forEach(s -> s.setServerErrorForAllRequests(ServerErrorCode.IO_Error));
    // although all disk colo will fail, cloud colo will return it successfully.
    router.deleteBlob(blobId, null).get();

    // inject error for cloud colo as well.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.Unknown_Error);
    try {
      router.deleteBlob(blobId, null).get();
      fail("Expecting exception");
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      assertTrue("Cause should be RouterException", t instanceof RouterException);
      // ServerErrorCode.IO_Error or StoreErrorCodes.Unknown_Error will be transferred to RouterErrorCode.UnexpectedInternalError
      assertSame("ErrorCode mismatch", ((RouterException) t).getErrorCode(), RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * Get the default {@link Properties} for the {@link NonBlockingRouter}.
   * @return the constructed {@link Properties}
   * @param routerDataCenter the local data center
   */
  private Properties getDefaultNonBlockingRouterProperties(String routerDataCenter) {
    Properties properties = new Properties();

    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDataCenter);
    properties.setProperty("router.put.request.parallelism", Integer.toString(3));
    properties.setProperty("router.put.success.target", Integer.toString(2));
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxChunkSize));
    properties.setProperty("router.get.request.parallelism", Integer.toString(2));
    properties.setProperty("router.get.success.target", Integer.toString(1));
    properties.setProperty("router.get.operation.tracker.type", operationTrackerType);
    properties.setProperty("router.request.timeout.ms", Integer.toString(20));
    properties.setProperty("router.operation.tracker.exclude.timeout.enabled", Boolean.toString(true));
    properties.setProperty("router.operation.tracker.terminate.on.not.found.enabled", "true");
    properties.setProperty("router.get.blob.operation.share.memory", "true");

    properties.setProperty("router.connection.checkout.timeout.ms", Integer.toString(CHECKOUT_TIMEOUT_MS));
    properties.setProperty("router.connections.local.dc.warm.up.percentage", Integer.toString(67));
    properties.setProperty("router.connections.remote.dc.warm.up.percentage", Integer.toString(34));
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "dc1");
    properties.setProperty("clustermap.host.name", "localhost");

    properties.setProperty("clustermap.port", "1666");
    properties.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    properties.setProperty("clustermap.resolve.hostnames", "false");
    properties.setProperty(CloudConfig.CLOUD_DESTINATION_FACTORY_CLASS,
        LatchBasedInMemoryCloudDestinationFactory.class.getName());
    properties.setProperty(CloudConfig.VCR_MIN_TTL_DAYS, "0");

    properties.setProperty("kms.default.container.key",
        "B374A26A71490437AA024E4FADD5B497FDFF1A8EA6FF12F6FB65AF2720B59CCF");
    return properties;
  }
}
