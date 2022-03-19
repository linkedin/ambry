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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.InMemAccountService;
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
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.network.LocalNetworkClient;
import com.github.ambry.network.LocalNetworkClientFactory;
import com.github.ambry.network.LocalRequestResponseChannel;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.Response;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.TtlUpdateResponse;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.protocol.UndeleteResponse;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.router.CloudRouterFactory;
import com.github.ambry.router.RouterUtils;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.SystemTime;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Properties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Class to test the error simulation in {@link LatchBasedInMemoryCloudDestination}.
 */
public class InMemoryCloudDestinationErrorSimulationTest {
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final String LOCAL_DC = "DC3";

  private final int maxChunkSize;
  private final Random random = new Random();
  private final MockClusterMap mockClusterMap;
  private final LocalNetworkClient mockNetworkClient;
  private final LatchBasedInMemoryCloudDestination cloudDestination;

  private final RouterConfig routerConfig;

  // The partition id, replica, host name, port which is used in the test.
  private final PartitionId partitionId;
  private final ReplicaId replica;
  private final String hostname;
  private final Port port;

  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Initialize parameters common to all tests.
   * @throws Exception
   */
  public InMemoryCloudDestinationErrorSimulationTest() throws Exception {
    final MockTime time = new MockTime();

    maxChunkSize = random.nextInt(1024 * 1024) + 1;
    VerifiableProperties vprops = new VerifiableProperties(getDefaultProperties(true, LOCAL_DC));
    routerConfig = new RouterConfig(vprops);
    // include cloud backed colo
    mockClusterMap = new MockClusterMap(false, true, 9, 3, 3, false, true, LOCAL_DC);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    Assert.assertEquals("Local DC Name is same as the one we set.", LOCAL_DC, localDcName);

    CloudConfig cloudConfig = new CloudConfig(vprops);
    CloudDestinationFactory cloudDestinationFactory =
        Utils.getObj(cloudConfig.cloudDestinationFactoryClass, vprops, mockClusterMap.getMetricRegistry(),
            mockClusterMap);
    cloudDestination = (LatchBasedInMemoryCloudDestination) cloudDestinationFactory.getCloudDestination();

    AccountService accountService = new InMemAccountService(false, true);
    CloudRouterFactory cloudRouterFactory = new CloudRouterFactory(vprops, mockClusterMap,
        new LoggingNotificationSystem(), null, accountService);

    // requestHandlerPool and its thread pool handle the cloud blob operations.
    RequestHandlerPool requestHandlerPool =
        cloudRouterFactory.getRequestHandlerPool(vprops, mockClusterMap, cloudDestination, cloudConfig);

    LocalNetworkClientFactory cloudClientFactory =
        new LocalNetworkClientFactory((LocalRequestResponseChannel) requestHandlerPool.getChannel(),
            new NetworkConfig(vprops), new NetworkMetrics(new MetricRegistry()), time);
    mockNetworkClient = cloudClientFactory.getNetworkClient();

    List<PartitionId> writablePartitionIds =
        mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    partitionId = writablePartitionIds.get(random.nextInt(writablePartitionIds.size()));
    // Get the cloud replica.
    replica = partitionId.getReplicaIds().get(0);
    Assert.assertEquals("It should be a cloud backed replica.", replica.getReplicaType(), ReplicaType.CLOUD_BACKED);
    hostname = replica.getDataNodeId().getHostname();
    port = new Port(-1, PortType.PLAINTEXT);
  }

  /**
   * Submit all the requests that were handed over by the operation and wait until a response is received for every
   * one of them.
   * @param requestInfo the request handed over by the operation.
   * @return the response from the network client.
   */
  private ResponseInfo sendAndWaitForResponses(RequestInfo requestInfo) {
    List<RequestInfo> requestList = new ArrayList<>();
    requestList.add(requestInfo);

    int sendCount = requestList.size();
    List<ResponseInfo> responseList = new ArrayList<>();
    responseList.addAll(mockNetworkClient.sendAndPoll(requestList, Collections.emptySet(), 100));
    requestList.clear();
    while (responseList.size() < sendCount) {
      responseList.addAll(mockNetworkClient.sendAndPoll(requestList, Collections.emptySet(), 100));
    }
    Assert.assertEquals("Only one request and response", responseList.size(), 1);
    ResponseInfo responseInfo = responseList.get(0);
    return responseInfo;
  }

  /**
   * Do a put directly to the cloud mock servers.
   * @param partitionId partition id
   * @param expectedCode expected response error code
   * @return the blob id
   * @throws Exception
   */
  private BlobId doPut(PartitionId partitionId, ServerErrorCode expectedCode) throws Exception {
    int blobSize = 4096;
    // direct put DataBlob
    BlobType blobType = BlobType.DataBlob;
    BlobProperties blobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(random), Utils.getRandomShort(random), false, null, null, null);
    byte[] userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    byte[] putContent = new byte[blobSize];
    random.nextBytes(putContent);
    ByteBuf blobContent = PooledByteBufAllocator.DEFAULT.heapBuffer(blobSize);
    blobContent.writeBytes(putContent);

    BlobId blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), blobProperties.getAccountId(), blobProperties.getContainerId(),
        partitionId, blobProperties.isEncrypted(),
        blobType == BlobType.MetadataBlob ? BlobId.BlobDataType.METADATA : BlobId.BlobDataType.DATACHUNK);

    ByteBuffer userMetadataBuf = ByteBuffer.wrap(userMetadata);

    // send to Cloud destinations.
    PutRequest request =
        new PutRequest(random.nextInt(), "clientId", blobId, blobProperties, userMetadataBuf.duplicate(),
            blobContent.retainedDuplicate(), blobContent.readableBytes(), blobType, null);

    RequestInfo requestInfo = new RequestInfo(hostname, port, request, replica, null);
    ResponseInfo responseInfo = sendAndWaitForResponses(requestInfo);
    Assert.assertEquals("doPut should succeed.", responseInfo.getError(), null);
    //PutResponse response = PutResponse.readFrom(new NettyByteBufDataInputStream(responseInfo.content()));
    PutResponse response = (PutResponse) RouterUtils.mapToReceivedResponse((PutResponse) responseInfo.getResponse());
    Assert.assertEquals("The PutResponse is not expected.", expectedCode, response.getError());
    request.release();
    blobContent.release();

    return blobId;
  }

  /**
   * Do a put directly to the cloud mock servers.
   * @param partitionId partition id
   * @return the blob id
   * @throws Exception
   */
  private BlobId doPut(PartitionId partitionId) throws Exception {
    return doPut(partitionId, ServerErrorCode.No_Error);
  }

  /**
   * test error simulation for PutRequest
   * @throws Exception
   */
  @Test
  public void testPutRequestErrorSimulation() throws Exception {
    // inject error for cloud colo.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.IOError);

    BlobId blobId = doPut(partitionId, ServerErrorCode.IO_Error);
  }

  /**
   * test error simulation for GetBlobRequest
   * @throws Exception
   */
  @Test
  public void testGetBlobErrorSimulation() throws Exception {
    BlobId blobId = doPut(partitionId);

    ArrayList<BlobId> blobIdList = new ArrayList<BlobId>();
    blobIdList.add(blobId);
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, blobIdList);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest =
        new GetRequest(1234, "clientId", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
    RequestInfo requestInfo = new RequestInfo(hostname, port, getRequest, replica, null);
    ResponseInfo responseInfo = sendAndWaitForResponses(requestInfo);
    GetResponse response = responseInfo.getError() == null ? (GetResponse) RouterUtils.mapToReceivedResponse(
        (Response) responseInfo.getResponse()) : null;

    PartitionResponseInfo partitionResponseInfo = response.getPartitionResponseInfoList().get(0);
    Assert.assertEquals("GetRequest should succeed.", response.getError(), ServerErrorCode.No_Error);
    Assert.assertEquals("GetRequest partitionResponseInfo should succeed.", partitionResponseInfo.getErrorCode(),
        ServerErrorCode.No_Error);
    responseInfo.release();

    // inject error for cloud colo.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.ID_Not_Found);
    getRequest = new GetRequest(1234, "clientId", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
    requestInfo = new RequestInfo(hostname, port, getRequest, replica, null);
    responseInfo = sendAndWaitForResponses(requestInfo);
    response = responseInfo.getError() == null ? (GetResponse) RouterUtils.mapToReceivedResponse(
        (Response) responseInfo.getResponse()) : null;
    partitionResponseInfo = response.getPartitionResponseInfoList().get(0);
    Assert.assertEquals("GetRequest responseInfo should have no error.", response.getError(), ServerErrorCode.No_Error);
    Assert.assertEquals("GetRequest partitionResponseInfo should be Blob_Not_Found",
        partitionResponseInfo.getErrorCode(), ServerErrorCode.Blob_Not_Found);
    responseInfo.release();
  }

  /**
   * test error simulation for GetBlobInfoRequest
   * @throws Exception
   */
  @Test
  public void testGetBlobInfoErrorSimulation() throws Exception {
    BlobId blobId = doPut(partitionId);

    ArrayList<BlobId> blobIdList = new ArrayList<BlobId>();
    blobIdList.add(blobId);
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partitionId, blobIdList);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest =
        new GetRequest(1234, "clientId", MessageFormatFlags.BlobInfo, partitionRequestInfoList, GetOption.None);
    RequestInfo requestInfo = new RequestInfo(hostname, port, getRequest, replica, null);
    ResponseInfo responseInfo = sendAndWaitForResponses(requestInfo);
    GetResponse response = responseInfo.getError() == null ? (GetResponse) RouterUtils.mapToReceivedResponse(
        (Response) responseInfo.getResponse()) : null;
    PartitionResponseInfo partitionResponseInfo = response.getPartitionResponseInfoList().get(0);
    Assert.assertEquals("GetBlobInfo should succeed.", response.getError(), ServerErrorCode.No_Error);
    Assert.assertEquals("GetBlobInfo partitionResponseInfo should succeed.", partitionResponseInfo.getErrorCode(),
        ServerErrorCode.No_Error);
    responseInfo.release();

    // inject error for cloud colo.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.ID_Not_Found);
    getRequest =
        new GetRequest(1234, "clientId", MessageFormatFlags.BlobInfo, partitionRequestInfoList, GetOption.None);
    requestInfo = new RequestInfo(hostname, port, getRequest, replica, null);
    responseInfo = sendAndWaitForResponses(requestInfo);
    response = responseInfo.getError() == null ? (GetResponse) RouterUtils.mapToReceivedResponse(
        (Response) responseInfo.getResponse()) : null;
    partitionResponseInfo = response.getPartitionResponseInfoList().get(0);
    Assert.assertEquals("GetBlobInfo responseInfo should have no error.", response.getError(),
        ServerErrorCode.No_Error);
    Assert.assertEquals("GetBlobInfo partitionResponseInfo should be Blob_Not_Found",
        partitionResponseInfo.getErrorCode(), ServerErrorCode.Blob_Not_Found);
    responseInfo.release();
  }

  /**
   * test error simulation for TtlUpdateRequest
   * @throws Exception
   */
  @Test
  public void testTtlUpdateErrorSimulation() throws Exception {
    BlobId blobId = doPut(partitionId);

    TtlUpdateRequest ttlUpdateRequest =
        new TtlUpdateRequest(1234, "clientId", blobId, Utils.Infinite_Time, SystemTime.getInstance().milliseconds());
    RequestInfo requestInfo = new RequestInfo(hostname, port, ttlUpdateRequest, replica, null);
    ResponseInfo responseInfo = sendAndWaitForResponses(requestInfo);
    TtlUpdateResponse response =
        responseInfo.getError() == null ? (TtlUpdateResponse) RouterUtils.mapToReceivedResponse(
            (Response) responseInfo.getResponse()) : null;
    Assert.assertEquals("TtlUpdate should succeed.", response.getError(), ServerErrorCode.No_Error);
    response.release();

    // inject error for cloud colo.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.TTL_Expired);

    ttlUpdateRequest =
        new TtlUpdateRequest(1234, "clientId", blobId, Utils.Infinite_Time, SystemTime.getInstance().milliseconds());
    requestInfo = new RequestInfo(hostname, port, ttlUpdateRequest, replica, null);
    responseInfo = sendAndWaitForResponses(requestInfo);
    response = responseInfo.getError() == null ? ((TtlUpdateResponse) RouterUtils.mapToReceivedResponse(
        (Response) responseInfo.getResponse())) : null;
    Assert.assertEquals("TtlUpdate should return Blob_Expired.", response.getError(), ServerErrorCode.Blob_Expired);
    response.release();
  }

  /**
   * test error simulation for DeleteRequest
   * @throws Exception
   */
  @Test
  public void testDeleteBlobErrorSimulation() throws Exception {
    BlobId blobId = doPut(partitionId);

    DeleteRequest request = new DeleteRequest(1234, "clientId", blobId, SystemTime.getInstance().milliseconds());

    RequestInfo requestInfo = new RequestInfo(hostname, port, request, replica, null);
    ResponseInfo responseInfo = sendAndWaitForResponses(requestInfo);
    DeleteResponse response = responseInfo.getError() == null ? ((DeleteResponse) RouterUtils.mapToReceivedResponse(
        (Response) responseInfo.getResponse())) : null;
    Assert.assertEquals("DeleteBlob should succeed.", response.getError(), ServerErrorCode.No_Error);

    // inject error for cloud colo.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.Unknown_Error);

    request = new DeleteRequest(1234, "clientId", blobId, SystemTime.getInstance().milliseconds());
    requestInfo = new RequestInfo(hostname, port, request, replica, null);
    responseInfo = sendAndWaitForResponses(requestInfo);
    response = responseInfo.getError() == null ? ((DeleteResponse) RouterUtils.mapToReceivedResponse(
        (Response) responseInfo.getResponse())) : null;
    Assert.assertEquals("DeleteBlob should return Unknown_Error.", response.getError(), ServerErrorCode.Unknown_Error);
    response.release();
  }

  /**
   * test error simulation for UndeleteRequest
   * @throws Exception
   */
  @Test
  public void testUndeleteBlobErrorSimulation() throws Exception {
    BlobId blobId = doPut(partitionId);

    // Delete the blob first
    {
      DeleteRequest request = new DeleteRequest(1234, "clientId", blobId, SystemTime.getInstance().milliseconds());

      RequestInfo requestInfo = new RequestInfo(hostname, port, request, replica, null);
      ResponseInfo responseInfo = sendAndWaitForResponses(requestInfo);
      DeleteResponse response = responseInfo.getError() == null ? (DeleteResponse) RouterUtils.mapToReceivedResponse(
          (Response) responseInfo.getResponse()) : null;
      Assert.assertEquals("DeleteRequest should succeed.", response.getError(), ServerErrorCode.No_Error);
    }

    UndeleteRequest request = new UndeleteRequest(1234, "clientId", blobId, SystemTime.getInstance().milliseconds());
    RequestInfo requestInfo = new RequestInfo(hostname, port, request, replica, null);
    ResponseInfo responseInfo = sendAndWaitForResponses(requestInfo);
    UndeleteResponse response = responseInfo.getError() == null ? (UndeleteResponse) RouterUtils.mapToReceivedResponse(
        (UndeleteResponse) responseInfo.getResponse()) : null;
    Assert.assertEquals("UndeleteRequest should succeed.", response.getError(), ServerErrorCode.No_Error);
    response.release();

    // inject error for cloud colo.
    cloudDestination.setServerErrorForAllRequests(StoreErrorCodes.Unknown_Error);

    request = new UndeleteRequest(1234, "clientId", blobId, SystemTime.getInstance().milliseconds());
    requestInfo = new RequestInfo(hostname, port, request, replica, null);
    responseInfo = sendAndWaitForResponses(requestInfo);
    response = responseInfo.getError() == null ? (UndeleteResponse) RouterUtils.mapToReceivedResponse(
        (UndeleteResponse) responseInfo.getResponse()) : null;
    Assert.assertEquals("UndeleteRequest should return Unknown_Error.", response.getError(),
        ServerErrorCode.Unknown_Error);
    response.release();
  }

  /**
   * Get the default {@link Properties}.
   * @param excludeTimeout whether to exclude timed out request in Histogram.
   * @param routerDataCenter the local data center
   * @return the constructed {@link Properties}
   */
  private Properties getDefaultProperties(boolean excludeTimeout, String routerDataCenter) {
    Properties properties = new Properties();

    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDataCenter);
    properties.setProperty("router.put.request.parallelism", Integer.toString(3));
    properties.setProperty("router.put.success.target", Integer.toString(2));
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxChunkSize));
    properties.setProperty("router.get.request.parallelism", Integer.toString(2));
    properties.setProperty("router.get.success.target", Integer.toString(1));
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

    properties.setProperty("kms.default.container.key",
        "B374A26A71490437AA024E4FADD5B497FDFF1A8EA6FF12F6FB65AF2720B59CCF");
    return properties;
  }
}
