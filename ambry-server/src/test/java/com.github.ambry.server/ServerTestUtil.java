package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.coordinator.OperationContext;
import com.github.ambry.coordinator.PutOperation;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobOutput;
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
import com.github.ambry.protocol.GetOptions;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLSocketFactory;
import org.junit.Assert;


class HelperCoordinator extends AmbryCoordinator {
  public HelperCoordinator(VerifiableProperties properties, MockClusterMap clusterMap) {
    super(properties, clusterMap);
  }

  @Override
  public String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata, InputStream blobStream)
      throws CoordinatorException {
    PartitionId partitionId = getPartitionForPut();
    BlobId blobId = new BlobId(partitionId);
    OperationContext oc = getOperationContext();
    PutOperation putOperation =
        new PutOperation(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs, blobProperties,
            userMetadata, blobStream, 1, 1);
    putOperation.execute();

    notificationSystem.onBlobCreated(blobId.getID(), blobProperties, userMetadata.array());
    return blobId.getID();
  }
}

/**
 *
 */
public final class ServerTestUtil {

  protected static void endToEndTest(Port targetPort, String coordinatorDatacenter, String sslEnabledDatacenters,
      MockCluster cluster, SSLConfig clientSSLConfig, SSLSocketFactory clientSSLSocketFactory,
      Properties coordinatorProps)
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
      PutRequest putRequest = new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      BlockingChannel channel =
          getBlockingChannelBasedOnPortType(targetPort, "localhost", clientSSLSocketFactory, clientSSLConfig);
      channel.connect();
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive().getInputStream();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);

      // put blob 2
      PutRequest putRequest2 = new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel.send(putRequest2);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 3
      PutRequest putRequest3 = new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel.send(putRequest3);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 4 that is expired
      BlobProperties propertiesExpired = new BlobProperties(31870, "serviceid1", "ownerid", "jpeg", false, 0);
      PutRequest putRequest4 = new PutRequest(1, "client1", blobId4, propertiesExpired, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel.send(putRequest4);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response4 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response4.getError(), ServerErrorCode.No_Error);

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(blobId1);
      ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest1 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOptions.None);
      channel.send(getRequest1);
      InputStream stream = channel.receive().getInputStream();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), 31870);
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      // get blob properties with expired flag set
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(blobId1);
      partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      getRequest1 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
          GetOptions.Include_Expired_Blobs);
      channel.send(getRequest1);
      stream = channel.receive().getInputStream();
      resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), 31870);
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
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
              GetOptions.None);
      channel.send(getRequestExpired);
      InputStream streamExpired = channel.receive().getInputStream();
      GetResponse respExpired = GetResponse.readFrom(new DataInputStream(streamExpired), clusterMap);
      Assert
          .assertEquals(respExpired.getPartitionResponseInfoList().get(0).getErrorCode(), ServerErrorCode.Blob_Expired);

      // 2. With Include_Expired flag
      idsExpired = new ArrayList<BlobId>();
      partitionExpired = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      idsExpired.add(blobId4);
      partitionRequestInfoListExpired = new ArrayList<PartitionRequestInfo>();
      partitionRequestInfoExpired = new PartitionRequestInfo(partitionExpired, idsExpired);
      partitionRequestInfoListExpired.add(partitionRequestInfoExpired);
      getRequestExpired =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoListExpired,
              GetOptions.Include_Expired_Blobs);
      channel.send(getRequestExpired);
      streamExpired = channel.receive().getInputStream();
      respExpired = GetResponse.readFrom(new DataInputStream(streamExpired), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(respExpired.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), 31870);
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
        Assert.assertEquals(propertyOutput.getOwnerId(), "ownerid");
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      // get user metadata
      GetRequest getRequest2 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
              GetOptions.None);
      channel.send(getRequest2);
      stream = channel.receive().getInputStream();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      try {
        // get blob data
        // Use coordinator to get the blob
        Properties coordinatorProperties = getCoordinatorProperties(coordinatorDatacenter, sslEnabledDatacenters);
        coordinatorProperties.putAll(coordinatorProps);
        Coordinator coordinator = new HelperCoordinator(new VerifiableProperties(coordinatorProperties), clusterMap);
        BlobOutput output = coordinator.getBlob(blobId1.getID());
        Assert.assertEquals(output.getSize(), 31870);
        byte[] dataOutputStream = new byte[(int) output.getSize()];
        output.getStream().read(dataOutputStream);
        Assert.assertArrayEquals(dataOutputStream, data);
        coordinator.close();
      } catch (CoordinatorException e) {
        e.printStackTrace();
        Assert.assertEquals(false, true);
      }

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(new BlobId(partition));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest4 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOptions.None);
      channel.send(getRequest4);
      stream = channel.receive().getInputStream();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp4.getPartitionResponseInfoList().get(0).getErrorCode(), ServerErrorCode.Blob_Not_Found);
      channel.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(true, false);
    }
  }

  protected static void endToEndReplicationWithMultiNodeMultiPartitionTest(int interestedDataNodePortNumber,
      Port dataNode1Port, Port dataNode2Port, Port dataNode3Port, MockCluster cluster, SSLConfig clientSSLConfig1,
      SSLConfig clientSSLConfig2, SSLConfig clientSSLConfig3, SSLSocketFactory clientSSLSocketFactory1,
      SSLSocketFactory clientSSLSocketFactory2, SSLSocketFactory clientSSLSocketFactory3,
      MockNotificationSystem notificationSystem)
      throws InterruptedException, IOException, InstantiationException {
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
                  GetOptions.None);
          channel.send(getRequest);
          InputStream stream = channel.receive().getInputStream();
          GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            Assert.assertEquals(propertyOutput.getBlobSize(), 100);
            Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }

          // get user metadata
          ids.clear();
          ids.add(blobIds.get(j));
          partitionRequestInfoList.clear();
          partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
              GetOptions.None);
          channel.send(getRequest);
          stream = channel.receive().getInputStream();
          resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
          } catch (MessageFormatException e) {
            e.printStackTrace();
            Assert.assertEquals(false, true);
          }

          // get blob
          ids.clear();
          ids.add(blobIds.get(j));
          partitionRequestInfoList.clear();
          partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
          partitionRequestInfoList.add(partitionRequestInfo);
          getRequest =
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.None);
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
            Assert.assertArrayEquals(blobout, data);
          } catch (MessageFormatException e) {
            e.printStackTrace();
            Assert.assertEquals(false, true);
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
          Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);
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
              new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.None);
          channel.send(getRequest);
          InputStream stream = channel.receive().getInputStream();
          GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
          Assert.assertEquals(resp.getPartitionResponseInfoList().get(0).getErrorCode(), ServerErrorCode.Blob_Deleted);
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
      int totalblobs = 0;
      for (int i = 0; i < blobIds.size(); i++) {
        for (ReplicaId replicaId : blobIds.get(i).getPartition().getReplicaIds()) {
          if (replicaId.getMountPath().compareToIgnoreCase(dataNode.getMountPaths().get(0)) == 0) {
            if (blobsDeleted.contains(blobIds.get(i))) {
              notificationSystem.decrementDeletedReplica(blobIds.get(i).getID());
            } else {
              totalblobs++;
              notificationSystem.decrementCreatedReplica(blobIds.get(i).getID());
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
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                GetOptions.None);
        channel1.send(getRequest);
        InputStream stream = channel1.receive().getInputStream();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
        } else {
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            Assert.assertEquals(propertyOutput.getBlobSize(), 100);
            Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }

        // get user metadata
        ids.clear();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
            GetOptions.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsDeleted.contains(blobIds.get(j)));
        } else {
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }

        // get blob
        ids.clear();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.None);
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
            Assert.assertArrayEquals(blobout, data);
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }
      }
      Assert.assertEquals(blobsDeleted.size(), 0);
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
          notificationSystem.decrementDeletedReplica(blobIds.get(i).getID());
        } else {
          notificationSystem.decrementCreatedReplica(blobIds.get(i).getID());
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
            new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                GetOptions.None);
        channel1.send(getRequest);
        InputStream stream = channel1.receive().getInputStream();
        GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
        } else {
          try {
            BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
            Assert.assertEquals(propertyOutput.getBlobSize(), 100);
            Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }

        // get user metadata
        ids.clear();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
            GetOptions.None);
        channel1.send(getRequest);
        stream = channel1.receive().getInputStream();
        resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
        if (resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Deleted
            || resp.getPartitionResponseInfoList().get(0).getErrorCode() == ServerErrorCode.Blob_Not_Found) {
          Assert.assertTrue(blobsChecked.contains(blobIds.get(j)));
        } else {
          try {
            ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
            Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }

        // get blob
        ids.clear();
        ids.add(blobIds.get(j));
        partitionRequestInfoList.clear();
        partitionRequestInfo = new PartitionRequestInfo(blobIds.get(j).getPartition(), ids);
        partitionRequestInfoList.add(partitionRequestInfo);
        getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.None);
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
            Assert.assertArrayEquals(blobout, data);
          } catch (MessageFormatException e) {
            Assert.assertEquals(false, true);
          }
        }
      }
      Assert.assertEquals(blobsChecked.size(), 0);

      channel1.disconnect();
      channel2.disconnect();
      channel3.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  protected static void endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest(String sourceDatacenter,
      String sslEnabledDatacenters, PortType portType, MockCluster cluster, MockNotificationSystem notificationSystem,
      Properties coordinatorProps)
      throws Exception {
    Properties props = new Properties();
    props.setProperty("coordinator.hostname", "localhost");
    props.setProperty("coordinator.datacenter.name", sourceDatacenter);
    props.putAll(coordinatorProps);
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    Coordinator coordinator = new HelperCoordinator(verifiableProperties, cluster.getClusterMap());
    Thread[] senderThreads = new Thread[3];
    LinkedBlockingQueue<Payload> payloadQueue = new LinkedBlockingQueue<Payload>();
    int numberOfSenderThreads = 3;
    int numberOfVerifierThreads = 3;
    CountDownLatch senderLatch = new CountDownLatch(numberOfSenderThreads);
    int numberOfRequestsToSendPerThread = 5;
    for (int i = 0; i < numberOfSenderThreads; i++) {
      senderThreads[i] =
          new Thread(new CoordinatorSender(payloadQueue, senderLatch, numberOfRequestsToSendPerThread, coordinator));
      senderThreads[i].start();
    }
    senderLatch.await();

    if (payloadQueue.size() != numberOfRequestsToSendPerThread * numberOfSenderThreads) {
      // Failed during putBlob
      throw new IllegalStateException();
    }

    Properties sslProps = new Properties();
    sslProps.putAll(coordinatorProps);
    sslProps.setProperty("ssl.enabled.datacenters", sslEnabledDatacenters);
    ConnectionPool connectionPool =
        new BlockingChannelConnectionPool(new ConnectionPoolConfig(new VerifiableProperties(new Properties())),
            new SSLConfig(new VerifiableProperties(sslProps)), new MetricRegistry());
    CountDownLatch verifierLatch = new CountDownLatch(numberOfVerifierThreads);
    AtomicInteger totalRequests = new AtomicInteger(numberOfRequestsToSendPerThread * numberOfSenderThreads);
    AtomicInteger verifiedRequests = new AtomicInteger(0);
    AtomicBoolean cancelTest = new AtomicBoolean(false);
    for (int i = 0; i < numberOfVerifierThreads; i++) {
      Thread thread = new Thread(
          new Verifier(payloadQueue, verifierLatch, totalRequests, verifiedRequests, cluster.getClusterMap(),
              cancelTest, portType, connectionPool, notificationSystem));
      thread.start();
    }
    verifierLatch.await();

    Assert.assertEquals(totalRequests.get(), verifiedRequests.get());
    coordinator.close();
    connectionPool.shutdown();
  }

  protected static void endToEndReplicationWithMultiNodeSinglePartitionTest(String coordinatorDatacenter,
      String sslEnabledDatacenters, int interestedDataNodePortNumber, Port dataNode1Port, Port dataNode2Port,
      Port dataNode3Port, MockCluster cluster, SSLConfig clientSSLConfig1, SSLSocketFactory clientSSLSocketFactory1,
      MockNotificationSystem notificationSystem, Properties coordinatorProps)
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
      PutRequest putRequest = new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
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
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);
      // put blob 2
      PutRequest putRequest2 = new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);
      // put blob 3
      PutRequest putRequest3 = new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 4
      putRequest = new PutRequest(1, "client1", blobId4, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel1.send(putRequest);
      putResponseStream = channel1.receive().getInputStream();
      response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);

      // put blob 5
      putRequest2 = new PutRequest(1, "client1", blobId5, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 6
      putRequest3 = new PutRequest(1, "client1", blobId6, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);
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
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOptions.None);
      channel2.send(getRequest1);
      InputStream stream = channel2.receive().getInputStream();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp1.getError(), ServerErrorCode.No_Error);
      Assert.assertEquals(resp1.getPartitionResponseInfoList().get(0).getErrorCode(), ServerErrorCode.No_Error);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), 1000);
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }
      // get user metadata
      ids.clear();
      ids.add(blobId2);
      GetRequest getRequest2 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
              GetOptions.None);
      channel1.send(getRequest2);
      stream = channel1.receive().getInputStream();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp2.getError(), ServerErrorCode.No_Error);
      Assert.assertEquals(resp2.getPartitionResponseInfoList().get(0).getErrorCode(), ServerErrorCode.No_Error);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      // get blob
      ids.clear();
      ids.add(blobId1);
      GetRequest getRequest3 =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.None);
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
        Assert.assertArrayEquals(blobout, data);
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      try {
        // get blob data
        // Use coordinator to get the blob
        Properties coordinatorProperties = getCoordinatorProperties(coordinatorDatacenter, sslEnabledDatacenters);
        coordinatorProperties.putAll(coordinatorProps);
        Coordinator coordinator = new HelperCoordinator(new VerifiableProperties(coordinatorProperties), clusterMap);
        checkBlobId(coordinator, blobId1, data);
        checkBlobId(coordinator, blobId2, data);
        checkBlobId(coordinator, blobId3, data);
        checkBlobId(coordinator, blobId4, data);
        checkBlobId(coordinator, blobId5, data);
        checkBlobId(coordinator, blobId6, data);

        coordinator.close();
      } catch (CoordinatorException e) {
        e.printStackTrace();
        Assert.assertEquals(false, true);
      }

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      mockPartitionId = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);
      ids.add(new BlobId(mockPartitionId));
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(mockPartitionId, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest4 =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOptions.None);
      channel3.send(getRequest4);
      stream = channel3.receive().getInputStream();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp4.getError(), ServerErrorCode.No_Error);
      Assert.assertEquals(resp4.getPartitionResponseInfoList().get(0).getErrorCode(), ServerErrorCode.Blob_Not_Found);

      // delete a blob and ensure it is propagated
      DeleteRequest deleteRequest = new DeleteRequest(1, "reptest", blobId1);
      channel1.send(deleteRequest);
      InputStream deleteResponseStream = channel1.receive().getInputStream();
      DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
      Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

      notificationSystem.awaitBlobDeletions(blobId1.getID());
      ids = new ArrayList<BlobId>();
      ids.add(blobId1);
      partitionRequestInfoList.clear();
      partitionRequestInfo = new PartitionRequestInfo(partition, ids);
      partitionRequestInfoList.add(partitionRequestInfo);
      GetRequest getRequest5 =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.None);
      channel3.send(getRequest5);
      stream = channel3.receive().getInputStream();
      GetResponse resp5 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp5.getError(), ServerErrorCode.No_Error);
      Assert.assertEquals(resp5.getPartitionResponseInfoList().get(0).getErrorCode(), ServerErrorCode.Blob_Deleted);

      // persist and restore to check state
      cluster.getServers().get(0).shutdown();
      cluster.getServers().get(0).awaitShutdown();
      // read the replica file and check correctness
      DataNodeId dataNodeId = clusterMap.getDataNodeId("localhost", interestedDataNodePortNumber);
      List<String> mountPaths = ((MockDataNodeId) dataNodeId).getMountPaths();
      Set<String> setToCheck = new HashSet<String>();

      // we should have an entry for each partition - remote replica pair
      List<ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
      for (ReplicaId replicaId : replicaIds) {
        List<ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
        for (ReplicaId peerReplica : peerReplicas) {
          setToCheck.add(replicaId.getPartitionId().toString() +
              peerReplica.getDataNodeId().getHostname() +
              peerReplica.getDataNodeId().getPort());
        }
      }
      for (String mountPath : mountPaths) {
        File replicaTokenFile = new File(mountPath, "replicaTokens");
        if (replicaTokenFile.exists()) {
          CrcInputStream crcStream = new CrcInputStream(new FileInputStream(replicaTokenFile));
          DataInputStream dataInputStream = new DataInputStream(crcStream);
          try {
            short version = dataInputStream.readShort();
            Assert.assertEquals(version, 0);
            StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
            FindTokenFactory factory = Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);

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
              FindToken token = factory.getFindToken(dataInputStream);
              System.out.println(
                  "partitionId " + partitionId + " hostname " + hostname + " port " + port + " token " + token);
              ByteBuffer bytebufferToken = ByteBuffer.wrap(token.toBytes());
              Assert.assertEquals(bytebufferToken.getShort(), 0);
              int size = bytebufferToken.getInt();
              bytebufferToken.position(bytebufferToken.position() + size);
              long parsedToken = bytebufferToken.getLong();
              System.out.println("The parsed token is " + parsedToken);
              Assert.assertTrue(parsedToken == -1 || parsedToken == 13062);
            }
            long crc = crcStream.getValue();
            Assert.assertEquals(crc, dataInputStream.readLong());
          } catch (IOException e) {
            Assert.assertTrue(false);
          } finally {
            dataInputStream.close();
          }
        } else {
          Assert.assertTrue(false);
        }
      }
      // Add more data to server 2 and server 3. Recover server 1 and ensure it is completely replicated
      // put blob 7
      putRequest2 = new PutRequest(1, "client1", blobId7, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 8
      putRequest3 = new PutRequest(1, "client1", blobId8, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 9
      putRequest2 = new PutRequest(1, "client1", blobId9, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 10
      putRequest3 = new PutRequest(1, "client1", blobId10, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 11
      putRequest2 = new PutRequest(1, "client1", blobId11, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)), properties.getBlobSize(), BlobType.DataBlob);
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

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
        Assert.assertFalse(true);
      }

      // Shutdown server 1. Remove all its data from all mount path. Recover server 1 and ensure node is built
      cluster.getServers().get(0).shutdown();
      cluster.getServers().get(0).awaitShutdown();

      File mountFile = new File(clusterMap.getReplicaIds(dataNodeId).get(0).getMountPath());
      for (File toDelete : mountFile.listFiles()) {
        deleteFolderContent(toDelete, true);
      }
      notificationSystem.decrementCreatedReplica(blobId2.getID());
      notificationSystem.decrementCreatedReplica(blobId3.getID());
      notificationSystem.decrementCreatedReplica(blobId4.getID());
      notificationSystem.decrementCreatedReplica(blobId5.getID());
      notificationSystem.decrementCreatedReplica(blobId6.getID());
      notificationSystem.decrementCreatedReplica(blobId7.getID());
      notificationSystem.decrementCreatedReplica(blobId8.getID());
      notificationSystem.decrementCreatedReplica(blobId9.getID());
      notificationSystem.decrementCreatedReplica(blobId10.getID());
      notificationSystem.decrementCreatedReplica(blobId11.getID());

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
        Assert.assertFalse(true);
      }

      channel1.disconnect();
      channel2.disconnect();
      channel3.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  private static void checkBlobId(Coordinator coordinator, BlobId blobId, byte[] data)
      throws CoordinatorException, IOException {
    BlobOutput output = coordinator.getBlob(blobId.getID());
    Assert.assertEquals(output.getSize(), 1000);
    byte[] dataOutputStream = new byte[(int) output.getSize()];
    output.getStream().read(dataOutputStream);
    Assert.assertArrayEquals(dataOutputStream, data);
  }

  private static void checkBlobContent(MockClusterMap clusterMap, BlobId blobId, BlockingChannel channel,
      byte[] dataToCheck)
      throws IOException, MessageFormatException {
    ArrayList<BlobId> listIds = new ArrayList<BlobId>();
    listIds.add(blobId);
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfoList.clear();
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), listIds);
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest3 =
        new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.None);
    channel.send(getRequest3);
    InputStream stream = channel.receive().getInputStream();
    GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
    Assert.assertEquals(resp.getError(), ServerErrorCode.No_Error);
    BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
    byte[] blobout = new byte[(int) blobData.getSize()];
    int readsize = 0;
    while (readsize < blobData.getSize()) {
      readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
    }
    Assert.assertArrayEquals(blobout, dataToCheck);
  }

  private static Properties getCoordinatorProperties(String coordinatorDatacenter, String sslEnabledDatacenters) {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", coordinatorDatacenter);
    properties.setProperty("ssl.enabled.datacenters", sslEnabledDatacenters);
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
}
