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
package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SSLBlockingChannel;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.router.Callback;
import com.github.ambry.router.CopyingAsyncWritableChannel;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.NonBlockingRouterFactory;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.Offset;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSocketFactory;
import org.junit.Assert;

import static org.junit.Assert.*;


public final class ServerTestUtil {

  protected static void endToEndTest(Port targetPort, String routerDatacenter, String sslEnabledDatacenters,
      MockCluster cluster, SSLConfig clientSSLConfig, SSLSocketFactory clientSSLSocketFactory, Properties routerProps)
      throws InterruptedException, IOException, InstantiationException {
    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      byte[] usermetadata = new byte[1000];
      byte[] data = new byte[31870];
      BlobProperties properties = new BlobProperties(31870, "serviceid1");
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(data);
      List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
      BlobId blobId1 = new BlobId(partitionIds.get(0));
      BlobId blobId2 = new BlobId(partitionIds.get(0));
      BlobId blobId3 = new BlobId(partitionIds.get(0));
      BlobId blobId4 = new BlobId(partitionIds.get(0));
      // put blob 1
      PutRequest putRequest =
          new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      BlockingChannel channel =
          getBlockingChannelBasedOnPortType(targetPort, "localhost", clientSSLSocketFactory, clientSSLConfig);
      channel.connect();
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive().getInputStream();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response.getError());

      // put blob 2
      PutRequest putRequest2 =
          new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel.send(putRequest2);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      // put blob 3
      PutRequest putRequest3 =
          new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel.send(putRequest3);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());

      // put blob 4 that is expired
      BlobProperties propertiesExpired = new BlobProperties(31870, "serviceid1", "ownerid", "jpeg", false, 0);
      PutRequest putRequest4 =
          new PutRequest(1, "client1", blobId4, propertiesExpired, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel.send(putRequest4);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response4 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response4.getError());

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(blobId1);
      ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest1 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest1);
      InputStream stream = channel.receive().getInputStream();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        assertEquals(31870, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get blob properties with expired flag set
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(blobId1);
      partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      getRequest1 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
          GetOption.Include_Expired_Blobs);
      channel.send(getRequest1);
      stream = channel.receive().getInputStream();
      resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        assertEquals(31870, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get blob properties for expired blob
      // 1. With no flag
      ArrayList<BlobId> idsExpired = new ArrayList<BlobId>();
      MockPartitionId partitionExpired = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      idsExpired.add(blobId4);
      ArrayList<PartitionRequestInfo> partitionRequestInfoListExpired = new ArrayList<PartitionRequestInfo>();
      PartitionRequestInfo partitionRequestInfoExpired = new PartitionRequestInfo(partitionExpired, idsExpired);
      partitionRequestInfoListExpired.add(partitionRequestInfoExpired);
      GetRequest getRequestExpired =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoListExpired,
              GetOption.None);
      channel.send(getRequestExpired);
      InputStream streamExpired = channel.receive().getInputStream();
      GetResponse respExpired = GetResponse.readFrom(new DataInputStream(streamExpired), clusterMap);
      assertEquals(ServerErrorCode.Blob_Expired, respExpired.getPartitionResponseInfoList().get(0).getErrorCode());

      // 2. With Include_Expired flag
      idsExpired = new ArrayList<BlobId>();
      partitionExpired = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      idsExpired.add(blobId4);
      partitionRequestInfoListExpired = new ArrayList<PartitionRequestInfo>();
      partitionRequestInfoExpired = new PartitionRequestInfo(partitionExpired, idsExpired);
      partitionRequestInfoListExpired.add(partitionRequestInfoExpired);
      getRequestExpired =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoListExpired,
              GetOption.Include_Expired_Blobs);
      channel.send(getRequestExpired);
      streamExpired = channel.receive().getInputStream();
      respExpired = GetResponse.readFrom(new DataInputStream(streamExpired), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(respExpired.getInputStream());
        assertEquals(31870, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
        assertEquals("ownerid", propertyOutput.getOwnerId());
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get user metadata
      GetRequest getRequest2 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest2);
      stream = channel.receive().getInputStream();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
      } catch (MessageFormatException e) {
        assertEquals(false, true);
      }

      // get blob info
      GetRequest getRequest3 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobInfo, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest3);
      stream = channel.receive().getInputStream();
      GetResponse resp3 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      InputStream responseStream = resp3.getInputStream();
      // verify blob properties.
      BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(responseStream);
      assertEquals(31870, propertyOutput.getBlobSize());
      assertEquals("serviceid1", propertyOutput.getServiceId());
      // verify user metadata
      ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(responseStream);
      Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());

      // get blob data
      // Use router to get the blob
      Properties routerProperties = getRouterProps(routerDatacenter);
      routerProperties.putAll(routerProps);
      VerifiableProperties routerVerifiableProps = new VerifiableProperties(routerProperties);
      Router router = new NonBlockingRouterFactory(routerVerifiableProps, clusterMap, new MockNotificationSystem(9),
          getSSLFactoryIfRequired(routerVerifiableProps)).getRouter();
      checkBlobId(router, blobId1, data);
      router.close();

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(new BlobId(partition));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest4 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel.send(getRequest4);
      stream = channel.receive().getInputStream();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.Blob_Not_Found, resp4.getPartitionResponseInfoList().get(0).getErrorCode());
      channel.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  /**
   * Tests the case where a late PutRequest is sent to each server when it already has a record for the given BlobId.
   * The expected error from each server should be the given error.
   * @param blobId the {@link BlobId} of the blob to be put.
   * @param properties the {@link BlobProperties} of the blob to be put.
   * @param usermetadata the usermetadata of the blob to be put.
   * @param data the blob data of the blob to be put.
   * @param channelToDatanode1 the {@link BlockingChannel} to the Datanode1.
   * @param channelToDatanode2 the {@link BlockingChannel} to the Datanode2.
   * @param channelToDatanode3 the {@link BlockingChannel} to the Datanode3.
   * @param expectedErrorCode the {@link ServerErrorCode} that is expected from every Datanode.
   * @throws IOException
   */
  private static void testLatePutRequest(BlobId blobId, BlobProperties properties, byte[] usermetadata, byte[] data,
      BlockingChannel channelToDatanode1, BlockingChannel channelToDatanode2, BlockingChannel channelToDatanode3,
      ServerErrorCode expectedErrorCode) throws IOException {
    // Send put requests for an existing blobId for the exact blob to simulate a request arriving late.
    PutRequest latePutRequest1 =
        new PutRequest(1, "client1", blobId, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
            properties.getBlobSize(), BlobType.DataBlob);
    PutRequest latePutRequest2 =
        new PutRequest(1, "client2", blobId, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
            properties.getBlobSize(), BlobType.DataBlob);
    PutRequest latePutRequest3 =
        new PutRequest(1, "client3", blobId, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
            properties.getBlobSize(), BlobType.DataBlob);
    channelToDatanode1.send(latePutRequest1);
    InputStream putResponseStream = channelToDatanode1.receive().getInputStream();
    PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
    assertEquals(expectedErrorCode, response.getError());

    channelToDatanode2.send(latePutRequest2);
    putResponseStream = channelToDatanode2.receive().getInputStream();
    response = PutResponse.readFrom(new DataInputStream(putResponseStream));
    assertEquals(expectedErrorCode, response.getError());

    channelToDatanode3.send(latePutRequest3);
    putResponseStream = channelToDatanode3.receive().getInputStream();
    response = PutResponse.readFrom(new DataInputStream(putResponseStream));
    assertEquals(expectedErrorCode, response.getError());
  }

  protected static void endToEndReplicationWithMultiNodeMultiPartitionTest(int interestedDataNodePortNumber,
      Port dataNode1Port, Port dataNode2Port, Port dataNode3Port, MockCluster cluster, SSLConfig clientSSLConfig1,
      SSLConfig clientSSLConfig2, SSLConfig clientSSLConfig3, SSLSocketFactory clientSSLSocketFactory1,
      SSLSocketFactory clientSSLSocketFactory2, SSLSocketFactory clientSSLSocketFactory3,
      MockNotificationSystem notificationSystem) throws InterruptedException, IOException, InstantiationException {
    // interestedDataNodePortNumber is used to locate the datanode and hence has to be PlainTextPort
    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      List<AmbryServer> serverList = cluster.getServers();
      byte[] usermetadata = new byte[100];
      byte[] data = new byte[100];
      BlobProperties properties = new BlobProperties(100, "serviceid1");
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(data);

      // connect to all the servers
      BlockingChannel channel1 =
          getBlockingChannelBasedOnPortType(dataNode1Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);
      BlockingChannel channel2 =
          getBlockingChannelBasedOnPortType(dataNode2Port, "localhost", clientSSLSocketFactory2, clientSSLConfig2);
      BlockingChannel channel3 =
          getBlockingChannelBasedOnPortType(dataNode3Port, "localhost", clientSSLSocketFactory3, clientSSLConfig3);

      // put all the blobs to random servers
      channel1.connect();
      channel2.connect();
      channel3.connect();

      int noOfParallelThreads = 3;
      CountDownLatch latch = new CountDownLatch(noOfParallelThreads);
      List<DirectSender> runnables = new ArrayList<DirectSender>(noOfParallelThreads);
      BlockingChannel channel = null;
      for (int i = 0; i < noOfParallelThreads; i++) {
        if (i % noOfParallelThreads == 0) {
          channel = channel1;
        } else if (i % noOfParallelThreads == 1) {
          channel = channel2;
        } else if (i % noOfParallelThreads == 2) {
          channel = channel3;
        }
        DirectSender runnable = new DirectSender(cluster, channel, 50, data, usermetadata, properties, latch);
        runnables.add(runnable);
        Thread threadToRun = new Thread(runnable);
        threadToRun.start();
      }
      latch.await();

      // wait till replication can complete
      List<BlobId> blobIds = new ArrayList<BlobId>();
      for (int i = 0; i < runnables.size(); i++) {
        blobIds.addAll(runnables.get(i).getBlobIds());
      }
      for (BlobId blobId : blobIds) {
        notificationSystem.awaitBlobCreations(blobId.getID());
      }

      // Now that the blob is created and replicated, test the cases where a put request arrives for the same blob id
      // later than replication.
      testLatePutRequest(blobIds.get(0), properties, usermetadata, data, channel1, channel2, channel3,
          ServerErrorCode.No_Error);
      // Test the case where a put arrives with the same id as one in the server, but the blob is not identical.
      BlobProperties differentProperties = new BlobProperties(properties.getBlobSize(), properties.getServiceId());
      testLatePutRequest(blobIds.get(0), differentProperties, usermetadata, data, channel1, channel2, channel3,
          ServerErrorCode.Blob_Already_Exists);
      byte[] differentUsermetadata = Arrays.copyOf(usermetadata, usermetadata.length);
      differentUsermetadata[0] = (byte) ~differentUsermetadata[0];
      testLatePutRequest(blobIds.get(0), properties, differentUsermetadata, data, channel1, channel2, channel3,
          ServerErrorCode.Blob_Already_Exists);
      byte[] differentData = Arrays.copyOf(data, data.length);
      differentData[0] = (byte) ~differentData[0];
      testLatePutRequest(blobIds.get(0), properties, usermetadata, differentData, channel1, channel2, channel3,
          ServerErrorCode.Blob_Already_Exists);

      // verify blob properties, metadata and blob across all nodes
      for (int i = 0; i < 3; i++) {
        channel = null;
        if (i == 0) {
          channel = channel1;
        } else if (i == 1) {
          channel = channel2;
        } else if (i == 2) {
          channel = channel3;
        }

        ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
        for (int j = 0; j < blobIds.size(); j++) {
          ArrayList<BlobId> ids = new ArrayList<BlobId>();
          ids.add(blobIds.get(j));
          partitionRequestInfoList.clear();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                  GetOption.None);
          channel.send(getRequest);
          InputStream stream = channel.receive().getInputStream();
          GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            assertEquals(100, propertyOutput.getBlobSize());
            assertEquals("serviceid1", propertyOutput.getServiceId());
          } catch (MessageFormatException e) {
            Assert.fail();
          }

          // get user metadata
          ids.clear();
          ids.add(blobIds.get(j));
          partitionRequestInfoList.clear();
          partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
              GetOption.None);
          channel.send(getRequest);
          stream = channel.receive().getInputStream();
          resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
          } catch (MessageFormatException e) {
            e.printStackTrace();
            Assert.fail();
          }

          // get blob
          ids.clear();
          ids.add(blobIds.get(j));
          partitionRequestInfoList.clear();
          partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          channel.send(getRequest);
          stream = channel.receive().getInputStream();
          resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          //System.out.println("response from get " + resp.getError());
          try {
            BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobData.getSize()];
            int readsize = 0;
            while (readsize < blobData.getSize()) {
              readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
            }
            Assert.assertArrayEquals(data, blobout);
          } catch (MessageFormatException e) {
            e.printStackTrace();
            Assert.fail();
          }
        }
      }

      // delete random blobs, wait for replication and ensure it is deleted in all nodes

      Set<BlobId> blobsDeleted = new HashSet<BlobId>();
      Set<BlobId> blobsChecked = new HashSet<BlobId>();
      for (int i = 0; i < blobIds.size(); i++) {
        int j = new Random().nextInt(3);
        if (j == 0) {
          j = new Random().nextInt(3);
          if (j == 0) {
            channel = channel1;
          } else if (j == 1) {
            channel = channel2;
          } else if (j == 2) {
            channel = channel3;
          }
          DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", blobIds.get(i));
          channel.send(deleteRequest);
          InputStream deleteResponseStream = channel.receive().getInputStream();
          DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
          assertEquals(ServerErrorCode.No_Error, deleteResponse.getError());
          blobsDeleted.add(blobIds.get(i));
        }
      }

      Iterator<BlobId> iterator = blobsDeleted.iterator();
      ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      while (iterator.hasNext()) {
        BlobId deletedId = iterator.next();
        notificationSystem.awaitBlobDeletions(deletedId.getID());
        for (int j = 0; j < 3; j++) {
          if (j == 0) {
            channel = channel1;
          } else if (j == 1) {
            channel = channel2;
          } else if (j == 2) {
            channel = channel3;
          }

          ArrayList<BlobId> ids = new ArrayList<BlobId>();
          ids.add(deletedId);
          partitionRequestInfoList.clear();
          PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(deletedId.getPartition(), ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          GetRequest getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
          channel.send(getRequest);
          InputStream stream = channel.receive().getInputStream();
          GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          assertEquals(ServerErrorCode.Blob_Deleted, resp.getPartitionResponseInfoList().get(0).getErrorCode());
        }
      }

      // take a server down, clean up a mount path, start and ensure replication fixes it
      serverList.get(0).shutdown();
      serverList.get(0).awaitShutdown();

      MockDataNodeId dataNode = (MockDataNodeId) clusterMap.getDataNodeId("localhost", interestedDataNodePortNumber);
      System.out.println("Cleaning mount path " + dataNode.getMountPaths().get(0));
      for (ReplicaId replicaId : clusterMap.getReplicaIds(dataNode)) {
        if (replicaId.getMountPath().compareToIgnoreCase(dataNode.getMountPaths().get(0)) == 0) {
          System.out.println("Cleaning partition " + replicaId.getPartitionId());
        }
      }
      deleteFolderContent(new File(dataNode.getMountPaths().get(0)), false);
      for (int i = 0; i < blobIds.size(); i++) {
        for (ReplicaId replicaId : blobIds.get(i).getPartition().getReplicaIds()) {
          if (replicaId.getMountPath().compareToIgnoreCase(dataNode.getMountPaths().get(0)) == 0) {
            if (blobsDeleted.contains(blobIds.get(i))) {
              notificationSystem.decrementDeletedReplica(blobIds.get(i).getID(), dataNode.getHostname(),
                  dataNode.getPort());
            } else {
              notificationSystem.decrementCreatedReplica(blobIds.get(i).getID(), dataNode.getHostname(),
                  dataNode.getPort());
            }
          }
        }
      }
      serverList.get(0).startup();

      channel1.disconnect();
      channel1.connect();

      for (int j = 0; j < blobIds.size(); j++) {
        if (blobsDeleted.contains(blobIds.get(j))) {
          notificationSystem.awaitBlobDeletions(blobIds.get(j).getID());
        } else {
          notificationSystem.awaitBlobCreations(blobIds.get(j).getID());
        }
        ArrayList<BlobId> ids = new ArrayList<BlobId>();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        GetRequest getRequest =
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        InputStream stream = channel1.receive().getInputStream();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
        } else {
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            assertEquals(100, propertyOutput.getBlobSize());
            assertEquals("serviceid1", propertyOutput.getServiceId());
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get user metadata
        ids.clear();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
            GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
        } else {
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get blob
        ids.clear();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        //System.out.println("response from get " + resp.getError());
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
          blobsDeleted.remove(blobIds.get(j));
          blobsChecked.add(blobIds.get(j));
        } else {
          try {
            BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobData.getSize()];
            int readsize = 0;
            while (readsize < blobData.getSize()) {
              readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
            }
            Assert.assertArrayEquals(data, blobout);
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }
      }
      assertEquals(0, blobsDeleted.size());
      // take a server down, clean all contents, start and ensure replication fixes it
      serverList.get(0).shutdown();
      serverList.get(0).awaitShutdown();

      dataNode = (MockDataNodeId) clusterMap.getDataNodeId("localhost", interestedDataNodePortNumber);
      for (int i = 0; i < dataNode.getMountPaths().size(); i++) {
        System.out.println("Cleaning mount path " + dataNode.getMountPaths().get(i));
        for (ReplicaId replicaId : clusterMap.getReplicaIds(dataNode)) {
          if (replicaId.getMountPath().compareToIgnoreCase(dataNode.getMountPaths().get(i)) == 0) {
            System.out.println("Cleaning partition " + replicaId.getPartitionId());
          }
        }
        deleteFolderContent(new File(dataNode.getMountPaths().get(i)), false);
      }
      for (int i = 0; i < blobIds.size(); i++) {
        if (blobsChecked.contains(blobIds.get(i))) {
          notificationSystem.decrementDeletedReplica(blobIds.get(i).getID(), dataNode.getHostname(),
              dataNode.getPort());
        } else {
          notificationSystem.decrementCreatedReplica(blobIds.get(i).getID(), dataNode.getHostname(),
              dataNode.getPort());
        }
      }
      serverList.get(0).startup();

      channel1.disconnect();
      channel1.connect();

      for (int j = 0; j < blobIds.size(); j++) {
        if (blobsChecked.contains(blobIds.get(j))) {
          notificationSystem.awaitBlobDeletions(blobIds.get(j).getID());
        } else {
          notificationSystem.awaitBlobCreations(blobIds.get(j).getID());
        }
        ArrayList<BlobId> ids = new ArrayList<BlobId>();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        GetRequest getRequest =
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        InputStream stream = channel1.receive().getInputStream();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
        } else {
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            assertEquals(100, propertyOutput.getBlobSize());
            assertEquals("serviceid1", propertyOutput.getServiceId());
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get user metadata
        ids.clear();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
            GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
        } else {
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }

        // get blob
        ids.clear();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        //System.out.println("response from get " + resp.getError());
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
          blobsChecked.remove(blobIds.get(j));
        } else {
          try {
            BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobData.getSize()];
            int readsize = 0;
            while (readsize < blobData.getSize()) {
              readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
            }
            Assert.assertArrayEquals(data, blobout);
          } catch (MessageFormatException e) {
            Assert.fail();
          }
        }
      }
      assertEquals(0, blobsChecked.size());

      channel1.disconnect();
      channel2.disconnect();
      channel3.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  protected static void endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest(String sourceDatacenter,
      String sslEnabledDatacenters, PortType portType, MockCluster cluster, MockNotificationSystem notificationSystem,
      Properties routerProps) throws Exception {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", sourceDatacenter);
    props.setProperty("router.put.request.parallelism", "1");
    props.setProperty("router.put.success.target", "1");
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", sourceDatacenter);
    props.setProperty("clustermap.host.name", "localhost");
    props.putAll(routerProps);
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    Router router = new NonBlockingRouterFactory(verifiableProperties, cluster.getClusterMap(), notificationSystem,
        getSSLFactoryIfRequired(verifiableProperties)).getRouter();
    int numberOfRequestsToSend = 15;
    int numberOfVerifierThreads = 3;
    final LinkedBlockingQueue<Payload> payloadQueue = new LinkedBlockingQueue<Payload>();
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>(null);
    final CountDownLatch callbackLatch = new CountDownLatch(numberOfRequestsToSend);
    List<Future<String>> putFutures = new ArrayList<>(numberOfRequestsToSend);
    for (int i = 0; i < numberOfRequestsToSend; i++) {
      int size = new Random().nextInt(5000);
      final BlobProperties properties =
          new BlobProperties(size, "service1", "owner id check", "image/jpeg", false, Utils.Infinite_Time);
      final byte[] metadata = new byte[new Random().nextInt(1000)];
      final byte[] blob = new byte[size];
      new Random().nextBytes(metadata);
      new Random().nextBytes(blob);
      Future<String> future =
          router.putBlob(properties, metadata, new ByteBufferReadableStreamChannel(ByteBuffer.wrap(blob)),
              new Callback<String>() {
                @Override
                public void onCompletion(String result, Exception exception) {
                  if (exception == null) {
                    payloadQueue.add(new Payload(properties, metadata, blob, result));
                  } else {
                    exceptionRef.set(exception);
                  }
                  callbackLatch.countDown();
                }
              });
      putFutures.add(future);
    }
    for (Future<String> future : putFutures) {
      future.get(1, TimeUnit.SECONDS);
    }
    assertTrue("Did not receive all callbacks in time", callbackLatch.await(1, TimeUnit.SECONDS));
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
    assertEquals("Did not put expected number of blobs", numberOfRequestsToSend, payloadQueue.size());
    Properties sslProps = new Properties();
    sslProps.putAll(routerProps);
    sslProps.setProperty("clustermap.ssl.enabled.datacenters", sslEnabledDatacenters);
    sslProps.setProperty("clustermap.cluster.name", "test");
    sslProps.setProperty("clustermap.datacenter.name", sourceDatacenter);
    sslProps.setProperty("clustermap.host.name", "localhost");
    VerifiableProperties vProps = new VerifiableProperties(sslProps);
    ConnectionPool connectionPool =
        new BlockingChannelConnectionPool(new ConnectionPoolConfig(new VerifiableProperties(new Properties())),
            new SSLConfig(vProps), new ClusterMapConfig(vProps), new MetricRegistry());
    CountDownLatch verifierLatch = new CountDownLatch(numberOfVerifierThreads);
    AtomicInteger totalRequests = new AtomicInteger(numberOfRequestsToSend);
    AtomicInteger verifiedRequests = new AtomicInteger(0);
    AtomicBoolean cancelTest = new AtomicBoolean(false);
    for (int i = 0; i < numberOfVerifierThreads; i++) {
      Thread thread = new Thread(
          new Verifier(payloadQueue, verifierLatch, totalRequests, verifiedRequests, cluster.getClusterMap(),
              cancelTest, portType, connectionPool, notificationSystem));
      thread.start();
    }
    verifierLatch.await();

    assertEquals(totalRequests.get(), verifiedRequests.get());
    router.close();
    connectionPool.shutdown();
  }

  protected static void endToEndReplicationWithMultiNodeSinglePartitionTest(String routerDatacenter,
      String sslEnabledDatacenters, int interestedDataNodePortNumber, Port dataNode1Port, Port dataNode2Port,
      Port dataNode3Port, MockCluster cluster, SSLConfig clientSSLConfig1, SSLSocketFactory clientSSLSocketFactory1,
      MockNotificationSystem notificationSystem, Properties routerProps)
      throws InterruptedException, IOException, InstantiationException {
    // interestedDataNodePortNumber is used to locate the datanode and hence has to be PlainText port
    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      byte[] usermetadata = new byte[1000];
      byte[] data = new byte[1000];
      BlobProperties properties = new BlobProperties(1000, "serviceid1");
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(data);
      PartitionId partition = clusterMap.getWritablePartitionIds().get(0);
      BlobId blobId1 = new BlobId(partition);
      BlobId blobId2 = new BlobId(partition);
      BlobId blobId3 = new BlobId(partition);
      BlobId blobId4 = new BlobId(partition);
      BlobId blobId5 = new BlobId(partition);
      BlobId blobId6 = new BlobId(partition);
      BlobId blobId7 = new BlobId(partition);
      BlobId blobId8 = new BlobId(partition);
      BlobId blobId9 = new BlobId(partition);
      BlobId blobId10 = new BlobId(partition);
      BlobId blobId11 = new BlobId(partition);

      // put blob 1
      PutRequest putRequest =
          new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      BlockingChannel channel1 =
          getBlockingChannelBasedOnPortType(dataNode1Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);
      BlockingChannel channel2 =
          getBlockingChannelBasedOnPortType(dataNode2Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);
      BlockingChannel channel3 =
          getBlockingChannelBasedOnPortType(dataNode3Port, "localhost", clientSSLSocketFactory1, clientSSLConfig1);

      channel1.connect();
      channel2.connect();
      channel3.connect();
      channel1.send(putRequest);
      InputStream putResponseStream = channel1.receive().getInputStream();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response.getError());
      // put blob 2
      PutRequest putRequest2 =
          new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());
      // put blob 3
      PutRequest putRequest3 =
          new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());

      // put blob 4
      putRequest =
          new PutRequest(1, "client1", blobId4, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel1.send(putRequest);
      putResponseStream = channel1.receive().getInputStream();
      response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response.getError());

      // put blob 5
      putRequest2 =
          new PutRequest(1, "client1", blobId5, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      // put blob 6
      putRequest3 =
          new PutRequest(1, "client1", blobId6, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());
      // wait till replication can complete
      notificationSystem.awaitBlobCreations(blobId1.getID());
      notificationSystem.awaitBlobCreations(blobId2.getID());
      notificationSystem.awaitBlobCreations(blobId3.getID());
      notificationSystem.awaitBlobCreations(blobId4.getID());
      notificationSystem.awaitBlobCreations(blobId5.getID());
      notificationSystem.awaitBlobCreations(blobId6.getID());

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId mockPartitionId = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(blobId3);
      ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(mockPartitionId, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest1 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel2.send(getRequest1);
      InputStream stream = channel2.receive().getInputStream();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.No_Error, resp1.getError());
      assertEquals(ServerErrorCode.No_Error, resp1.getPartitionResponseInfoList().get(0).getErrorCode());
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        assertEquals(1000, propertyOutput.getBlobSize());
        assertEquals("serviceid1", propertyOutput.getServiceId());
      } catch (MessageFormatException e) {
        Assert.fail();
      }
      // get user metadata
      ids.clear();
      ids.add(blobId2);
      GetRequest getRequest2 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList, GetOption.None);
      channel1.send(getRequest2);
      stream = channel1.receive().getInputStream();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.No_Error, resp2.getError());
      assertEquals(ServerErrorCode.No_Error, resp2.getPartitionResponseInfoList().get(0).getErrorCode());
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(usermetadata, userMetadataOutput.array());
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get blob
      ids.clear();
      ids.add(blobId1);
      GetRequest getRequest3 =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      channel3.send(getRequest3);
      stream = channel3.receive().getInputStream();
      GetResponse resp3 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      //System.out.println("response from get " + resp3.getError());
      try {
        BlobData blobData = MessageFormatRecord.deserializeBlob(resp3.getInputStream());
        byte[] blobout = new byte[(int) blobData.getSize()];
        int readsize = 0;
        while (readsize < blobData.getSize()) {
          readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
        }
        Assert.assertArrayEquals(data, blobout);
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // get blob data
      // Use router to get the blob
      Properties routerProperties = getRouterProps(routerDatacenter);
      routerProperties.putAll(routerProps);
      VerifiableProperties routerVerifiableProperties = new VerifiableProperties(routerProperties);
      Router router = new NonBlockingRouterFactory(routerVerifiableProperties, clusterMap, notificationSystem,
          getSSLFactoryIfRequired(routerVerifiableProperties)).getRouter();
      checkBlobId(router, blobId1, data);
      checkBlobId(router, blobId2, data);
      checkBlobId(router, blobId3, data);
      checkBlobId(router, blobId4, data);
      checkBlobId(router, blobId5, data);
      checkBlobId(router, blobId6, data);

      router.close();

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      mockPartitionId = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(new BlobId(mockPartitionId));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(mockPartitionId, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest4 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
      channel3.send(getRequest4);
      stream = channel3.receive().getInputStream();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.No_Error, resp4.getError());
      assertEquals(ServerErrorCode.Blob_Not_Found, resp4.getPartitionResponseInfoList().get(0).getErrorCode());

      // delete a blob and ensure it is propagated
      DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", blobId1);
      channel1.send(deleteRequest);
      InputStream deleteResponseStream = channel1.receive().getInputStream();
      DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
      assertEquals(ServerErrorCode.No_Error, deleteResponse.getError());

      notificationSystem.awaitBlobDeletions(blobId1.getID());
      ids = new ArrayList<BlobId>();
      ids.add(blobId1);
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest5 =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
      channel3.send(getRequest5);
      stream = channel3.receive().getInputStream();
      GetResponse resp5 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      assertEquals(ServerErrorCode.No_Error, resp5.getError());
      assertEquals(ServerErrorCode.Blob_Deleted, resp5.getPartitionResponseInfoList().get(0).getErrorCode());

      // get the data node to inspect replication tokens on
      DataNodeId dataNodeId = clusterMap.getDataNodeId("localhost", interestedDataNodePortNumber);
      // read the replica file and check correctness
      // The token offset value of 13074 was derived as followed:
      // - Up to this point we have done 6 puts and 1 delete
      // - Each put takes up 2179 bytes in the log (1000 data, 1000 user metadata, 179 ambry metadata)
      // - Each delete takes up 97 bytes in the log
      // - The offset stored in the token will be the position of the last entry in the log (the delete, in this case)
      // - Thus, it will be at the end of the 6 puts: 6 * 2179 = 13074
      checkReplicaTokens(clusterMap, dataNodeId, 13074, "0");

      // Shut down server 1
      cluster.getServers().get(0).shutdown();
      cluster.getServers().get(0).awaitShutdown();
      // Add more data to server 2 and server 3. Recover server 1 and ensure it is completely replicated
      // put blob 7
      putRequest2 =
          new PutRequest(1, "client1", blobId7, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      // put blob 8
      putRequest3 =
          new PutRequest(1, "client1", blobId8, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());

      // put blob 9
      putRequest2 =
          new PutRequest(1, "client1", blobId9, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      // put blob 10
      putRequest3 =
          new PutRequest(1, "client1", blobId10, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response3.getError());

      // put blob 11
      putRequest2 =
          new PutRequest(1, "client1", blobId11, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
              properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      assertEquals(ServerErrorCode.No_Error, response2.getError());

      cluster.getServers().get(0).startup();
      // wait for server to recover
      notificationSystem.awaitBlobCreations(blobId7.getID());
      notificationSystem.awaitBlobCreations(blobId8.getID());
      notificationSystem.awaitBlobCreations(blobId9.getID());
      notificationSystem.awaitBlobCreations(blobId10.getID());
      notificationSystem.awaitBlobCreations(blobId11.getID());
      channel1.disconnect();
      channel1.connect();

      // check all ids exist on server 1
      // get blob
      try {
        checkBlobContent(clusterMap, blobId2, channel1, data);
        checkBlobContent(clusterMap, blobId3, channel1, data);
        checkBlobContent(clusterMap, blobId4, channel1, data);
        checkBlobContent(clusterMap, blobId5, channel1, data);
        checkBlobContent(clusterMap, blobId6, channel1, data);
        checkBlobContent(clusterMap, blobId7, channel1, data);
        checkBlobContent(clusterMap, blobId8, channel1, data);
        checkBlobContent(clusterMap, blobId9, channel1, data);
        checkBlobContent(clusterMap, blobId10, channel1, data);
        checkBlobContent(clusterMap, blobId11, channel1, data);
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      // Shutdown server 1. Remove all its data from all mount path. Recover server 1 and ensure node is built
      cluster.getServers().get(0).shutdown();
      cluster.getServers().get(0).awaitShutdown();

      File mountFile = new File(clusterMap.getReplicaIds(dataNodeId).get(0).getMountPath());
      for (File toDelete : mountFile.listFiles()) {
        deleteFolderContent(toDelete, true);
      }
      notificationSystem.decrementCreatedReplica(blobId2.getID(), dataNodeId.getHostname(), dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobId3.getID(), dataNodeId.getHostname(), dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobId4.getID(), dataNodeId.getHostname(), dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobId5.getID(), dataNodeId.getHostname(), dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobId6.getID(), dataNodeId.getHostname(), dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobId7.getID(), dataNodeId.getHostname(), dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobId8.getID(), dataNodeId.getHostname(), dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobId9.getID(), dataNodeId.getHostname(), dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobId10.getID(), dataNodeId.getHostname(), dataNodeId.getPort());
      notificationSystem.decrementCreatedReplica(blobId11.getID(), dataNodeId.getHostname(), dataNodeId.getPort());

      cluster.getServers().get(0).startup();
      notificationSystem.awaitBlobCreations(blobId2.getID());
      notificationSystem.awaitBlobCreations(blobId3.getID());
      notificationSystem.awaitBlobCreations(blobId4.getID());
      notificationSystem.awaitBlobCreations(blobId5.getID());
      notificationSystem.awaitBlobCreations(blobId6.getID());
      notificationSystem.awaitBlobCreations(blobId7.getID());
      notificationSystem.awaitBlobCreations(blobId8.getID());
      notificationSystem.awaitBlobCreations(blobId9.getID());
      notificationSystem.awaitBlobCreations(blobId10.getID());
      notificationSystem.awaitBlobCreations(blobId11.getID());

      channel1.disconnect();
      channel1.connect();

      // check all ids exist on server 1
      // get blob
      try {
        checkBlobContent(clusterMap, blobId2, channel1, data);
        checkBlobContent(clusterMap, blobId3, channel1, data);
        checkBlobContent(clusterMap, blobId4, channel1, data);
        checkBlobContent(clusterMap, blobId5, channel1, data);
        checkBlobContent(clusterMap, blobId6, channel1, data);
        checkBlobContent(clusterMap, blobId7, channel1, data);
        checkBlobContent(clusterMap, blobId8, channel1, data);
        checkBlobContent(clusterMap, blobId9, channel1, data);
        checkBlobContent(clusterMap, blobId10, channel1, data);
        checkBlobContent(clusterMap, blobId11, channel1, data);
      } catch (MessageFormatException e) {
        Assert.fail();
      }

      channel1.disconnect();
      channel2.disconnect();
      channel3.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  /**
   * Repeatedly check the replication token file until a certain offset value on all nodes on a certain
   * partition is found.  Fail if {@code numTries} is exceeded or a token offset larger than the target
   * is found.
   * @param clusterMap the cluster map that contains the data node to inspect
   * @param dataNodeId the data node to inspect
   * @param targetOffset the token offset to look for in the {@code targetPartition}
   * @param targetPartition the name of the partition to look for the {@code targetOffset}
   * @throws Exception
   */
  private static void checkReplicaTokens(MockClusterMap clusterMap, DataNodeId dataNodeId, long targetOffset,
      String targetPartition) throws Exception {
    List<String> mountPaths = ((MockDataNodeId) dataNodeId).getMountPaths();

    // we should have an entry for each partition - remote replica pair
    Set<String> completeSetToCheck = new HashSet<>();
    List<ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
    int numRemoteNodes = 0;
    for (ReplicaId replicaId : replicaIds) {
      List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
      if (replicaId.getPartitionId().isEqual(targetPartition)) {
        numRemoteNodes = peerReplicas.size();
      }
      for (ReplicaId peerReplica : peerReplicas) {
        completeSetToCheck.add(replicaId.getPartitionId().toString() + peerReplica.getDataNodeId().getHostname()
            + peerReplica.getDataNodeId().getPort());
      }
    }

    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    FindTokenFactory factory = Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);

    int numTries = 4;
    boolean foundTarget = false;
    while (!foundTarget && numTries > 0) {
      Thread.sleep(5000);
      numTries--;
      Set<String> setToCheck = new HashSet<String>(completeSetToCheck);
      int numFound = 0;
      for (String mountPath : mountPaths) {
        File replicaTokenFile = new File(mountPath, "replicaTokens");
        if (replicaTokenFile.exists()) {
          CrcInputStream crcStream = new CrcInputStream(new FileInputStream(replicaTokenFile));
          DataInputStream dataInputStream = new DataInputStream(crcStream);
          try {
            short version = dataInputStream.readShort();
            assertEquals(0, version);

            System.out.println("setToCheck" + setToCheck.size());
            while (dataInputStream.available() > 8) {
              // read partition id
              PartitionId partitionId = clusterMap.getPartitionIdFromStream(dataInputStream);
              // read remote node host name
              String hostname = Utils.readIntString(dataInputStream);
              // read remote replica path
              Utils.readIntString(dataInputStream);

              // read remote port
              int port = dataInputStream.readInt();
              Assert.assertTrue(setToCheck.contains(partitionId.toString() + hostname + port));
              setToCheck.remove(partitionId.toString() + hostname + port);
              // read total bytes read from local store
              dataInputStream.readLong();
              // read replica token
              StoreFindToken token = (StoreFindToken) factory.getFindToken(dataInputStream);
              System.out.println(
                  "partitionId " + partitionId + " hostname " + hostname + " port " + port + " token " + token);
              Offset endTokenOffset = token.getOffset();
              long parsedToken = endTokenOffset == null ? -1 : endTokenOffset.getOffset();
              System.out.println("The parsed token is " + parsedToken);
              if (partitionId.isEqual(targetPartition)) {
                Assert.assertFalse("Parsed offset must not be larger than target value: " + targetOffset,
                    parsedToken > targetOffset);
                if (parsedToken == targetOffset) {
                  numFound++;
                }
              } else {
                assertEquals("Tokens should remain at -1 offsets on unmodified partitions", -1, parsedToken);
              }
            }
            long crc = crcStream.getValue();
            assertEquals(crc, dataInputStream.readLong());
          } catch (IOException e) {
            Assert.fail();
          } finally {
            dataInputStream.close();
          }
        }
      }
      if (numFound == numRemoteNodes) {
        foundTarget = true;
      }
    }
    if (!foundTarget) {
      Assert.fail("Could not find target token offset: " + targetOffset);
    }
  }

  private static void checkBlobId(Router router, BlobId blobId, byte[] data) throws Exception {
    GetBlobResult result = router.getBlob(blobId.getID(), new GetBlobOptionsBuilder().build()).get(1, TimeUnit.SECONDS);
    ReadableStreamChannel blob = result.getBlobDataChannel();
    assertEquals("Size does not match that of data", data.length,
        result.getBlobInfo().getBlobProperties().getBlobSize());
    CopyingAsyncWritableChannel channel = new CopyingAsyncWritableChannel();
    blob.readInto(channel, null).get(1, TimeUnit.SECONDS);
    Assert.assertArrayEquals(data, channel.getData());
  }

  private static void checkBlobContent(MockClusterMap clusterMap, BlobId blobId, BlockingChannel channel,
      byte[] dataToCheck) throws IOException, MessageFormatException {
    ArrayList<BlobId> listIds = new ArrayList<BlobId>();
    listIds.add(blobId);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfoList.clear();
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), listIds);
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest3 =
        new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
    channel.send(getRequest3);
    InputStream stream = channel.receive().getInputStream();
    GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
    assertEquals(ServerErrorCode.No_Error, resp.getError());
    BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
    byte[] blobout = new byte[(int) blobData.getSize()];
    int readsize = 0;
    while (readsize < blobData.getSize()) {
      readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
    }
    Assert.assertArrayEquals(dataToCheck, blobout);
  }

  private static Properties getRouterProps(String routerDatacenter) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDatacenter);
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", routerDatacenter);
    properties.setProperty("clustermap.host.name", "localhost");
    return properties;
  }

  private static void deleteFolderContent(File folder, boolean deleteParentFolder) {
    File[] files = folder.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteFolderContent(f, true);
        } else {
          f.delete();
        }
      }
    }
    if (deleteParentFolder) {
      folder.delete();
    }
  }

  /**
   * Returns BlockingChannel or SSLBlockingChannel depending on whether the port type is PlainText or SSL
   * for the given targetPort
   * @param targetPort upon which connection has to be established
   * @param hostName upon which connection has to be established
   * @return BlockingChannel
   */
  protected static BlockingChannel getBlockingChannelBasedOnPortType(Port targetPort, String hostName,
      SSLSocketFactory sslSocketFactory, SSLConfig sslConfig) {
    BlockingChannel channel = null;
    if (targetPort.getPortType() == PortType.PLAINTEXT) {
      channel = new BlockingChannel(hostName, targetPort.getPort(), 10000, 10000, 10000, 2000);
    } else if (targetPort.getPortType() == PortType.SSL) {
      channel = new SSLBlockingChannel(hostName, targetPort.getPort(), new MetricRegistry(), 10000, 10000, 10000, 4000,
          sslSocketFactory, sslConfig);
    }
    return channel;
  }

  /**
   * Create an {@link SSLFactory} if there are SSL enabled datacenters in the properties
   * @param verifiableProperties the {@link VerifiableProperties} to use.
   * @return an {@link SSLFactory}, or {@code null}, if no {@link SSLFactory} is required.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  static SSLFactory getSSLFactoryIfRequired(VerifiableProperties verifiableProperties)
      throws GeneralSecurityException, IOException {
    boolean requiresSSL = new ClusterMapConfig(verifiableProperties).clusterMapSslEnabledDatacenters.length() > 0;
    return requiresSSL ? new SSLFactory(new SSLConfig(verifiableProperties)) : null;
  }
}
