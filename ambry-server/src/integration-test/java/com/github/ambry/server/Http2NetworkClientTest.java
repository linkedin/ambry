/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobAll;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.network.http2.Http2NetworkClient;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.server.ServerTestUtil.*;
import static org.junit.Assert.*;


public class Http2NetworkClientTest {
  private static MockNotificationSystem notificationSystem;
  private static MockCluster http2Cluster;
  private static SSLConfig clientSSLConfig;

  @BeforeClass
  public static void initializeTests() throws Exception {
    Properties serverSSLProps;
    File trustStoreFile = File.createTempFile("truststore", ".jks");

    // Client
    Properties clientSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(clientSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile,
        "http2-client");
    TestSSLUtils.addHttp2Properties(clientSSLProps, SSLFactory.Mode.CLIENT, false);
    clientSSLConfig = new SSLConfig(new VerifiableProperties(clientSSLProps));

    // Server
    serverSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(serverSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile,
        "http2-server");
    TestSSLUtils.addHttp2Properties(serverSSLProps, SSLFactory.Mode.SERVER, false);
    http2Cluster = new MockCluster(serverSSLProps, false, SystemTime.getInstance(), 2, 1, 2);
    notificationSystem = new MockNotificationSystem(http2Cluster.getClusterMap());
    http2Cluster.initializeServers(notificationSystem);
    http2Cluster.startServers();
  }

  public Http2NetworkClientTest() {
  }

  @AfterClass
  public static void cleanup() throws IOException {
    if (http2Cluster != null) {
      http2Cluster.cleanup();
    }
  }

  @Test
  public void putGetTest() throws Exception {
    MockClusterMap clusterMap = http2Cluster.getClusterMap();
    DataNodeId dataNodeId = http2Cluster.getPrefetchDataNode();
    BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
    SSLFactory sslFactory = new NettySslHttp2Factory(clientSSLConfig);
    Http2NetworkClient networkClient = new Http2NetworkClient(new Http2ClientMetrics(new MetricRegistry()),
        new Http2ClientConfig(new VerifiableProperties(new Properties())), sslFactory);
    // Put a blob
    int blobSize = 1024 * 1024;
    byte[] usermetadata = new byte[1000];
    byte[] data = new byte[blobSize];
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);

    BlobProperties properties = new BlobProperties(blobSize, "serviceid1", accountId, containerId, false);
    TestUtils.RANDOM.nextBytes(usermetadata);
    TestUtils.RANDOM.nextBytes(data);
    List<? extends PartitionId> partitionIds =
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    BlobId blobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
        properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    // put blob 1
    PutRequest putRequest =
        new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata), Unpooled.wrappedBuffer(data),
            properties.getBlobSize(), BlobType.DataBlob, null);
    RequestInfo request =
        new RequestInfo(dataNodeId.getHostname(), new Port(dataNodeId.getHttp2Port(), PortType.HTTP2), putRequest,
            clusterMap.getReplicaIds(dataNodeId).get(0));
    List<ResponseInfo> responseInfos =
        networkClient.sendAndPoll(Collections.singletonList(request), new HashSet<>(), 300);
    long startTime = SystemTime.getInstance().milliseconds();
    while (responseInfos.size() == 0) {
      responseInfos = networkClient.sendAndPoll(Collections.EMPTY_LIST, new HashSet<>(), 300);
      if (SystemTime.getInstance().milliseconds() - startTime >= 6000) {
        fail("Network Client no reponse and timeout.");
      }
      Thread.sleep(30);
    }
    assertEquals("Should be only one response", 1, responseInfos.size());

    DataInputStream dis = new NettyByteBufDataInputStream(responseInfos.get(0).content());
    PutResponse putResponse = PutResponse.readFrom(dis);
    assertEquals("No error expected.", ServerErrorCode.No_Error, putResponse.getError());

    // Get the blob
    // get blob properties
    ArrayList<BlobId> ids = new ArrayList<BlobId>();
    MockPartitionId partition =
        (MockPartitionId) clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    ids.add(blobId1);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partition, ids);
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest =
        new GetRequest(1, "http2-clientid", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);

    request = new RequestInfo(dataNodeId.getHostname(), new Port(dataNodeId.getHttp2Port(), PortType.HTTP2), getRequest,
        clusterMap.getReplicaIds(dataNodeId).get(0));
    responseInfos = networkClient.sendAndPoll(Collections.singletonList(request), new HashSet<>(), 300);
    startTime = SystemTime.getInstance().milliseconds();
    while (responseInfos.size() == 0) {
      responseInfos = networkClient.sendAndPoll(Collections.EMPTY_LIST, new HashSet<>(), 300);
      if (SystemTime.getInstance().milliseconds() - startTime >= 3000) {
        fail("Network Client no response and timeout.");
      }
      Thread.sleep(30);
    }
    assertEquals("Should be only one response", 1, responseInfos.size());

    dis = new NettyByteBufDataInputStream(responseInfos.get(0).content());
    GetResponse resp = GetResponse.readFrom(dis, clusterMap);
    BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), blobIdFactory);
    // verify BlobProperties
    BlobProperties propertyOutput = blobAll.getBlobInfo().getBlobProperties();
    assertEquals(blobSize, propertyOutput.getBlobSize());
    assertEquals("serviceid1", propertyOutput.getServiceId());
    assertEquals("AccountId mismatch", accountId, propertyOutput.getAccountId());
    assertEquals("ContainerId mismatch", containerId, propertyOutput.getContainerId());
    // verify UserMetadata
    byte[] userMetadataOutput = blobAll.getBlobInfo().getUserMetadata();
    assertArrayEquals(usermetadata, userMetadataOutput);
    // verify content
    byte[] actualBlobData = getBlobDataAndRelease(blobAll.getBlobData());
    assertArrayEquals("Content mismatch.", data, actualBlobData);
  }

  @Test
  public void warmUpConnections() throws Exception {
    int connectionPerPort = 4;
    SSLFactory sslFactory = new NettySslHttp2Factory(clientSSLConfig);
    Properties properties = new Properties();
    properties.setProperty(Http2ClientConfig.HTTP2_MIN_CONNECTION_PER_PORT, Integer.toString(connectionPerPort));
    Http2ClientConfig http2ClientConfig = new Http2ClientConfig(new VerifiableProperties(properties));
    Http2NetworkClient networkClient =
        new Http2NetworkClient(new Http2ClientMetrics(new MetricRegistry()), http2ClientConfig, sslFactory);
    MockClusterMap clusterMap = http2Cluster.getClusterMap();
    assertEquals("Connection count is not expected", 0,
        networkClient.warmUpConnections(clusterMap.getDataNodeIds(), 0, 1000, new ArrayList<>()));

    assertEquals("Connection count is not expected", clusterMap.getDataNodeIds().size() * connectionPerPort / 2,
        networkClient.warmUpConnections(clusterMap.getDataNodeIds(), 50, 1000, new ArrayList<>()));

    assertEquals("Connection count is not expected", clusterMap.getDataNodeIds().size() * connectionPerPort,
        networkClient.warmUpConnections(clusterMap.getDataNodeIds(), 100, 1000, new ArrayList<>()));

    // All connection should be failed. Connection refused exceptions will be in test log.
    List<Port> ports = new ArrayList<>();
    ports.add(new Port(79, PortType.HTTP2));
    ports.add(new Port(78, PortType.PLAINTEXT));
    assertEquals("Connection count is not expected", 0,
        networkClient.warmUpConnections(Collections.singletonList(new MockDataNodeId(ports, null, "DC1")), 100, 1000,
            new ArrayList<>()));
  }
}
