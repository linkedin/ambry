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


import com.github.ambry.account.InMemAccountService;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
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
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Properties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class InMemoryCloudDestinationErrorSimulationTest {
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final String LOCAL_DC = "DC3";

  private final int maxChunkSize;
  private final Random random = new Random();
  private final MockClusterMap mockClusterMap;
  private final MockServerLayout mockServerLayout;
  private final NonBlockingRouter router;
  private final MockCompositeNetworkClient mockNetworkClient;

  // Mock servers include disk backed "mockServers" and cloud backed "cloudDestination"
  private final CloudDestination cloudDestination;
  private final Collection<MockServer> mockServers;

  // Certain tests recreate the routerConfig with different properties.
  private final RouterConfig routerConfig;

  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() { nettyByteBufLeakHelper.afterTest(); }

  /**
   * Instantiate a router.
   */
  public InMemoryCloudDestinationErrorSimulationTest() throws Exception {
    final MockTime time = new MockTime();

    // Defaults. Tests may override these and do new puts as appropriate.
    maxChunkSize = random.nextInt(1024 * 1024) + 1;
    final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>();
    mockSelectorState.set(MockSelectorState.Good);
    VerifiableProperties vprops = new VerifiableProperties(getDefaultNonBlockingRouterProperties(true, LOCAL_DC));
    routerConfig = new RouterConfig(vprops);
    // include cloud backed colo
    mockClusterMap = new MockClusterMap(false, true, 9, 3, 3, false, true, LOCAL_DC);
    //mockClusterMap = new MockClusterMap(false, true, 0, 0, 3, false, true, LOCAL_DC);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    Assert.assertEquals("Local DC Name is same as the one we set.", LOCAL_DC, localDcName);

    final NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    mockServerLayout = new MockServerLayout(mockClusterMap);
    mockServers = mockServerLayout.getMockServers();

    CloudConfig cloudConfig = new CloudConfig(vprops);
    CloudDestinationFactory cloudDestinationFactory =
        Utils.getObj(cloudConfig.cloudDestinationFactoryClass, vprops, mockClusterMap.getMetricRegistry(),
            mockClusterMap);
    cloudDestination = cloudDestinationFactory.getCloudDestination();
    RequestHandlerPool requestHandlerPool =
        CloudRouterFactory.getRequestHandlerPool(vprops, mockClusterMap, cloudDestination, cloudConfig);

    Map<ReplicaType, NetworkClientFactory> childFactories = new EnumMap<>(ReplicaType.class);
    // requestHandlerPool and its thread pool handle the cloud blob operations.
    LocalNetworkClientFactory cloudClientFactory = new LocalNetworkClientFactory((LocalRequestResponseChannel) requestHandlerPool.getChannel(),
        new NetworkConfig(vprops), new NetworkMetrics(routerMetrics.getMetricRegistry()), time);
    childFactories.put(ReplicaType.CLOUD_BACKED, cloudClientFactory);

    MockNetworkClientFactory
        diskClientFactory = new MockNetworkClientFactory(vprops, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
        CHECKOUT_TIMEOUT_MS, mockServerLayout, time);
    childFactories.put(ReplicaType.DISK_BACKED, diskClientFactory);

    NetworkClientFactory networkClientFactory = new CompositeNetworkClientFactory(childFactories);
    router = new NonBlockingRouter(routerConfig, routerMetrics,
        networkClientFactory, new LoggingNotificationSystem(), mockClusterMap, null, null, null,
        new InMemAccountService(false, true), time, MockClusterMap.DEFAULT_PARTITION_CLASS);

    NetworkClient compNetworkClient = networkClientFactory.getNetworkClient();
    mockNetworkClient = new MockCompositeNetworkClient(compNetworkClient);
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
   * Do a put directly to the mock servers. This allows for blobs with malformed properties to be constructed.
   * @return the blob id
   * @throws Exception
   */
  private String doDirectPut() throws Exception {
    int blobSize = 4096;
    // direct put DataBlob
    BlobType blobType = BlobType.DataBlob;
    BlobProperties blobProperties = new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
        Utils.getRandomShort(random), Utils.getRandomShort(random), false, null, null, null);
    byte[] userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    byte[] putContent = new byte[blobSize];
    random.nextBytes(putContent);
    ByteBuf blobContent = PooledByteBufAllocator.DEFAULT.heapBuffer(blobSize);
    blobContent.writeBytes(putContent);

    List<PartitionId> writablePartitionIds = mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partitionId = writablePartitionIds.get(random.nextInt(writablePartitionIds.size()));
    BlobId blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), blobProperties.getAccountId(), blobProperties.getContainerId(),
        partitionId, blobProperties.isEncrypted(),
        blobType == BlobType.MetadataBlob ? BlobId.BlobDataType.METADATA : BlobId.BlobDataType.DATACHUNK);
    Iterator<MockServer> servers = partitionId.getReplicaIds()
        .stream()
        .map(ReplicaId::getDataNodeId)
        .map(dataNodeId -> mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort()))
        .iterator();

    ByteBuffer blobEncryptionKey = null;
    ByteBuffer userMetadataBuf = ByteBuffer.wrap(userMetadata);
    while (servers.hasNext()) {
      MockServer server = servers.next();
      PutRequest request =
          new PutRequest(random.nextInt(), "clientId", blobId, blobProperties, userMetadataBuf.duplicate(),
              blobContent.retainedDuplicate(), blobContent.readableBytes(), blobType,
              blobEncryptionKey == null ? null : blobEncryptionKey.duplicate());
      // Make sure we release the BoundedNettyByteBufReceive.
      server.send(request).release();
      request.release();
    }

    // send to Cloud destinations.
    PutRequest request =
        new PutRequest(random.nextInt(), "clientId", blobId, blobProperties, userMetadataBuf.duplicate(),
            blobContent.retainedDuplicate(), blobContent.readableBytes(), blobType,
            blobEncryptionKey == null ? null : blobEncryptionKey.duplicate());
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

    return blobId.getID();
  }

  @Test
  public void testGetBlobErrorSimulation() throws Exception {
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
      assertTrue("ErrorCode mismatch",
          ((RouterException) t).getErrorCode() == RouterErrorCode.BlobDoesNotExist || ((RouterException) t).getErrorCode() == RouterErrorCode.UnexpectedInternalError);
    }
  }

  @Test
  public void testGetBlobInfoErrorSimulation() throws Exception {
    String blobId = doDirectPut();

    mockServers.forEach(s -> s.setServerErrorForAllRequests(ServerErrorCode.Data_Corrupt));
    // although all disk colo will fail, cloud colo will return it successfully.
    router.getBlob(blobId, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build()).get();

    // inject error for cloud colo as well.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.ID_Not_Found);
    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build()).get();
      fail("Expecting exception");
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      assertTrue("Cause should be RouterException", t instanceof RouterException);
      // ServerErrorCode.Data_Corrupt will be transferred to RouterErrorCode.UnexpectedInternalError
      assertTrue("ErrorCode mismatch",
          ((RouterException) t).getErrorCode() == RouterErrorCode.BlobDoesNotExist || ((RouterException) t).getErrorCode() == RouterErrorCode.UnexpectedInternalError);
    }
  }

  @Test
  public void testTtlUpdateErrorSimulation() throws Exception {
    String blobId = doDirectPut();

    mockServers.forEach(s -> s.setServerErrorForAllRequests(ServerErrorCode.Blob_Expired));
    // although all disk colo will fail, cloud colo will return it successfully.
    router.updateBlobTtl(blobId, null, Utils.Infinite_Time).get();

    // inject error for cloud colo as well.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.TTL_Expired);
    try {
      router.updateBlobTtl(blobId, null, Utils.Infinite_Time).get();
      fail("Expecting exception");
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      assertTrue("Cause should be RouterException", t instanceof RouterException);
      // ServerErrorCode.Blob_Expired will be transferred to RouterErrorCode.BlobExpired
      assertSame("ErrorCode mismatch",
          ((RouterException) t).getErrorCode(), RouterErrorCode.BlobExpired);
    }
  }

  @Test
  public void testDeleteBlobErrorSimulation() throws Exception {
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
      assertSame("ErrorCode mismatch",
          ((RouterException) t).getErrorCode(), RouterErrorCode.UnexpectedInternalError);
    }
  }

  @Test
  public void testUndeleteBlobErrorSimulation() throws Exception {
    String blobId = doDirectPut();

    mockServers.forEach(s -> s.setServerErrorForAllRequests(ServerErrorCode.IO_Error));
    // although all disk colo will fail, cloud colo will return it successfully.
    // Jing TODO: Undelete is a test command. It is local only, right? Didn't see cross colo failover.
    //router.undeleteBlob(blobId, null).get();

    /*
    // inject error for cloud colo as well.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.Unknown_Error);
    try {
      router.undeleteBlob(blobId, null).get();
      fail("Expecting exception");
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      assertTrue("Cause should be RouterException", t instanceof RouterException);
      // ServerErrorCode.IO_Error or StoreErrorCodes.Unknown_Error will be transferred to RouterErrorCode.UnexpectedInternalError
      assertSame("ErrorCode mismatch",
          ((RouterException) t).getErrorCode(). RouterErrorCode.UnexpectedInternalError);
    }
    */
  }


  /**
   * Get the default {@link Properties} for the {@link NonBlockingRouter}.
   * @param excludeTimeout whether to exclude timed out request in Histogram.
   * @return the constructed {@link Properties}
   * @param routerDataCenter the local data center
   */
  private Properties getDefaultNonBlockingRouterProperties(boolean excludeTimeout, String routerDataCenter) {
    Properties properties = new Properties();

    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDataCenter);
    properties.setProperty("router.put.request.parallelism", Integer.toString(3));
    properties.setProperty("router.put.success.target", Integer.toString(2));
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxChunkSize));
    properties.setProperty("router.get.request.parallelism", Integer.toString(2));
    properties.setProperty("router.get.success.target", Integer.toString(1));
    properties.setProperty("router.get.operation.tracker.type", SimpleOperationTracker.class.getSimpleName());
    properties.setProperty("router.request.timeout.ms", Integer.toString(20));
    properties.setProperty("router.operation.tracker.exclude.timeout.enabled", Boolean.toString(excludeTimeout));
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

    properties.setProperty("kms.default.container.key", "B374A26A71490437AA024E4FADD5B497FDFF1A8EA6FF12F6FB65AF2720B59CCF");
    return properties;
  }
}
