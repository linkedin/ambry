package com.github.ambry.server;

import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BlockingChannel;
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
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;


public class ServerTest {

  private MockCluster cluster;
  private MockNotificationSystem notificationSystem;

  public ServerTest()
      throws InterruptedException, IOException, StoreException, InstantiationException {
    notificationSystem = new MockNotificationSystem(9);
  }

  @After
  public void cleanup() {
    long start = System.currentTimeMillis();
    // cleanup appears to hang sometimes. And, it sometimes takes a long time. Printing some info until cleanup is fast
    // and reliable.
    System.out.println("About to invoke cluster.cleanup()");
    cluster.cleanup();
    System.out.println("cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Test
  public void startStopTest()
      throws IOException, InstantiationException {
    // do nothing
    cluster = new MockCluster(notificationSystem, false, "", "", "");
  }

  @Test
  public void endToEndTest()
      throws InterruptedException, IOException, InstantiationException {
    endToEndTest(new Port(64422, PortType.PLAINTEXT), false, "DC1", "", "", "");
  }

  @Test
  public void endToEndSSLTest()
      throws InterruptedException, IOException, InstantiationException {
    endToEndTest(new Port(54422, PortType.SSL), true, "DC1", "DC2", "DC1", "DC1,DC2");
  }

  private void endToEndTest(Port targetPort, boolean enableSSLPorts, String coordinatorDatacenter,
      String sslEnabledDatacentersForDC1, String sslEnabledDatacentersForDC2, String sslEnabledDatacentersForDC3)
      throws InterruptedException, IOException, InstantiationException {
    cluster =
        new MockCluster(notificationSystem, enableSSLPorts, sslEnabledDatacentersForDC1, sslEnabledDatacentersForDC2,
            sslEnabledDatacentersForDC3);

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
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      BlockingChannel channel = null;
      if (targetPort.getPortType() == PortType.SSL) {
        channel = new SSLBlockingChannel("localhost", targetPort.getPort(), 10000, 10000, 10000, 2000);
      } else {
        channel = new BlockingChannel("localhost", targetPort.getPort(), 10000, 10000, 10000, 2000);
      }
      channel.connect();
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive().getInputStream();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);

      // put blob 2
      PutRequest putRequest2 = new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel.send(putRequest2);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 3
      PutRequest putRequest3 = new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel.send(putRequest3);
      putResponseStream = channel.receive().getInputStream();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 4 that is expired
      BlobProperties propertiesExpired = new BlobProperties(31870, "serviceid1", "ownerid", "jpeg", false, 0);
      PutRequest putRequest4 = new PutRequest(1, "client1", blobId4, propertiesExpired, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
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
        Coordinator coordinator = null;
        if (targetPort.getPortType() == PortType.SSL) {
          coordinator = new AmbryCoordinator(getCoordinatorProperties(coordinatorDatacenter), clusterMap);
        } else {
          coordinator = new AmbryCoordinator(getCoordinatorProperties(""), clusterMap);
        }
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

  void ensureCleanupTokenCatchesUp(String path, MockClusterMap clusterMap, long expectedTokenValue)
      throws Exception {
    final int TIMEOUT = 10000;
    File cleanupTokenFile = new File(path, "cleanuptoken");
    FindToken endToken;
    long parsedTokenValue = -1;

    long endTime = SystemTime.getInstance().milliseconds() + TIMEOUT;
    do {
      if (cleanupTokenFile.exists()) {
        CrcInputStream crcStream = new CrcInputStream(new FileInputStream(cleanupTokenFile));
        DataInputStream stream = new DataInputStream(crcStream);
        try {
          short version = stream.readShort();
          Assert.assertEquals(version, 0);
          StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
          FindTokenFactory factory = Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);

          factory.getFindToken(stream);
          endToken = factory.getFindToken(stream);

          ByteBuffer bytebufferToken = ByteBuffer.wrap(endToken.toBytes());
          Assert.assertEquals(bytebufferToken.getShort(), 0);
          int size = bytebufferToken.getInt();
          bytebufferToken.position(bytebufferToken.position() + size);
          parsedTokenValue = bytebufferToken.getLong();
          long crc = crcStream.getValue();
          Assert.assertEquals(crc, stream.readLong());
          Thread.sleep(1000);
        } finally {
          stream.close();
        }
      }
    } while (SystemTime.getInstance().milliseconds() < endTime && parsedTokenValue < expectedTokenValue);
    Assert.assertEquals(expectedTokenValue, parsedTokenValue);
  }

  @Test
  public void endToEndTestHardDeletes()
      throws Exception {
    cluster = new MockCluster(notificationSystem, true, "", "", "");
    MockClusterMap clusterMap = cluster.getClusterMap();
    ArrayList<byte[]> usermetadata = new ArrayList<byte[]>(9);
    ArrayList<byte[]> data = new ArrayList<byte[]>(9);
    for (int i = 0; i < 9; i++) {
      usermetadata.add(new byte[1000 + i]);
      data.add(new byte[31870 + i]);
      new Random().nextBytes(usermetadata.get(i));
      new Random().nextBytes(data.get(i));
    }

    ArrayList<BlobProperties> properties = new ArrayList<BlobProperties>(9);
    properties.add(new BlobProperties(31870, "serviceid1"));
    properties.add(new BlobProperties(31871, "serviceid1"));
    properties.add(new BlobProperties(31872, "serviceid1"));
    properties.add(new BlobProperties(31873, "serviceid1", "ownerid", "jpeg", false, 0));
    properties.add(new BlobProperties(31874, "serviceid1"));
    properties.add(new BlobProperties(31875, "serviceid1", "ownerid", "jpeg", false, 0));
    properties.add(new BlobProperties(31876, "serviceid1"));
    properties.add(new BlobProperties(31877, "serviceid1"));
    properties.add(new BlobProperties(31878, "serviceid1"));

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    ArrayList<BlobId> blobIdList = new ArrayList<BlobId>(9);
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));
    blobIdList.add(new BlobId(partitionIds.get(0)));

    // put blob 0
    PutRequest putRequest0 =
        new PutRequest(1, "client1", blobIdList.get(0), properties.get(0), ByteBuffer.wrap(usermetadata.get(0)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(0))));
    BlockingChannel channel = new BlockingChannel("localhost", 64422, 10000, 10000, 10000, 2000);
    channel.connect();
    channel.send(putRequest0);
    InputStream putResponseStream = channel.receive().getInputStream();
    PutResponse response0 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response0.getError(), ServerErrorCode.No_Error);

    // put blob 1
    PutRequest putRequest1 =
        new PutRequest(1, "client1", blobIdList.get(1), properties.get(1), ByteBuffer.wrap(usermetadata.get(1)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(1))));
    channel.send(putRequest1);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response1 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response1.getError(), ServerErrorCode.No_Error);

    // put blob 2
    PutRequest putRequest2 =
        new PutRequest(1, "client1", blobIdList.get(2), properties.get(2), ByteBuffer.wrap(usermetadata.get(2)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(2))));
    channel.send(putRequest2);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

    // put blob 3 that is expired
    PutRequest putRequest3 =
        new PutRequest(1, "client1", blobIdList.get(3), properties.get(3), ByteBuffer.wrap(usermetadata.get(3)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(3))));
    channel.send(putRequest3);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

    // put blob 4
    PutRequest putRequest4 =
        new PutRequest(1, "client1", blobIdList.get(4), properties.get(4), ByteBuffer.wrap(usermetadata.get(4)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(4))));
    channel.send(putRequest4);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response4 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response4.getError(), ServerErrorCode.No_Error);

    // put blob 5 that is expired
    PutRequest putRequest5 =
        new PutRequest(1, "client1", blobIdList.get(5), properties.get(5), ByteBuffer.wrap(usermetadata.get(5)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(5))));
    channel.send(putRequest5);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response5 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response5.getError(), ServerErrorCode.No_Error);

    notificationSystem.awaitBlobCreations(blobIdList.get(0).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(1).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(2).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(4).getID());

    // delete blob 1
    DeleteRequest deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(1));
    channel.send(deleteRequest);
    InputStream deleteResponseStream = channel.receive().getInputStream();
    DeleteResponse deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    byte[] zeroedMetadata = new byte[usermetadata.get(1).length];
    usermetadata.set(1, zeroedMetadata);
    byte[] zeroedData = new byte[data.get(1).length];
    data.set(1, zeroedData);

    // delete blob 4
    deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(4));
    channel.send(deleteRequest);
    deleteResponseStream = channel.receive().getInputStream();
    deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    zeroedMetadata = new byte[usermetadata.get(4).length];
    usermetadata.set(4, zeroedMetadata);
    zeroedData = new byte[data.get(4).length];
    data.set(4, zeroedData);

    notificationSystem.awaitBlobDeletions(blobIdList.get(1).getID());
    notificationSystem.awaitBlobDeletions(blobIdList.get(4).getID());

    ensureCleanupTokenCatchesUp(partitionIds.get(0).getReplicaIds().get(0).getReplicaPath(), clusterMap, 198431);
    ensureCleanupTokenCatchesUp(partitionIds.get(0).getReplicaIds().get(1).getReplicaPath(), clusterMap, 132299);
    ensureCleanupTokenCatchesUp(partitionIds.get(0).getReplicaIds().get(2).getReplicaPath(), clusterMap, 132299);

    MockPartitionId partition = (MockPartitionId) clusterMap.getWritablePartitionIds().get(0);

    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    ArrayList<BlobId> ids = new ArrayList<BlobId>();
    for (int i = 0; i < 6; i++) {
      ids.add(blobIdList.get(i));
    }

    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(partition, ids);
    partitionRequestInfoList.add(partitionRequestInfo);

    try {
      GetRequest getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
              GetOptions.Include_All);
      channel.send(getRequest);
      InputStream stream = channel.receive().getInputStream();
      GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);

      for (int i = 0; i < 6; i++) {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), properties.get(i).getBlobSize());
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      }

      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
          GetOptions.Include_All);
      channel.send(getRequest);
      stream = channel.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);

      for (int i = 0; i < 6; i++) {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata.get(i));
      }

      getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.Include_All);
      channel.send(getRequest);
      stream = channel.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);

      for (int i = 0; i < 6; i++) {
        BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
        Assert.assertEquals(blobOutput.getSize(), properties.get(i).getBlobSize());
        byte[] dataOutput = new byte[(int) blobOutput.getSize()];
        blobOutput.getStream().read(dataOutput);
        Assert.assertArrayEquals(dataOutput, data.get(i));
      }
    } catch (Exception e) {
      Assert.assertEquals(false, true);
    }

    // put blob 6
    PutRequest putRequest6 =
        new PutRequest(1, "client1", blobIdList.get(6), properties.get(6), ByteBuffer.wrap(usermetadata.get(6)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(6))));
    channel.send(putRequest6);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response6 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response6.getError(), ServerErrorCode.No_Error);

    // put blob 7
    PutRequest putRequest7 =
        new PutRequest(1, "client1", blobIdList.get(7), properties.get(7), ByteBuffer.wrap(usermetadata.get(7)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(7))));
    channel.send(putRequest7);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response7 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response7.getError(), ServerErrorCode.No_Error);

    // put blob 8
    PutRequest putRequest8 =
        new PutRequest(1, "client1", blobIdList.get(8), properties.get(8), ByteBuffer.wrap(usermetadata.get(8)),
            new ByteBufferInputStream(ByteBuffer.wrap(data.get(8))));
    channel.send(putRequest8);
    putResponseStream = channel.receive().getInputStream();
    PutResponse response9 = PutResponse.readFrom(new DataInputStream(putResponseStream));
    Assert.assertEquals(response9.getError(), ServerErrorCode.No_Error);

    notificationSystem.awaitBlobCreations(blobIdList.get(6).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(7).getID());
    notificationSystem.awaitBlobCreations(blobIdList.get(8).getID());
    // Do more deletes

    // delete blob 3 that is expired.
    deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(3));
    channel.send(deleteRequest);
    deleteResponseStream = channel.receive().getInputStream();
    deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    zeroedMetadata = new byte[usermetadata.get(3).length];
    usermetadata.set(3, zeroedMetadata);
    zeroedData = new byte[data.get(3).length];
    data.set(3, zeroedData);

    // delete blob 0
    deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(0));
    channel.send(deleteRequest);
    deleteResponseStream = channel.receive().getInputStream();
    deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    zeroedMetadata = new byte[usermetadata.get(0).length];
    usermetadata.set(0, zeroedMetadata);
    zeroedData = new byte[data.get(0).length];
    data.set(0, zeroedData);

    // delete blob 6.
    deleteRequest = new DeleteRequest(1, "client1", blobIdList.get(6));
    channel.send(deleteRequest);
    deleteResponseStream = channel.receive().getInputStream();
    deleteResponse = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    Assert.assertEquals(deleteResponse.getError(), ServerErrorCode.No_Error);

    zeroedMetadata = new byte[usermetadata.get(6).length];
    usermetadata.set(6, zeroedMetadata);
    zeroedData = new byte[data.get(6).length];
    data.set(6, zeroedData);

    notificationSystem.awaitBlobDeletions(blobIdList.get(0).getID());
    notificationSystem.awaitBlobDeletions(blobIdList.get(6).getID());

    ensureCleanupTokenCatchesUp(partitionIds.get(0).getReplicaIds().get(0).getReplicaPath(), clusterMap, 297905);
    ensureCleanupTokenCatchesUp(partitionIds.get(0).getReplicaIds().get(1).getReplicaPath(), clusterMap, 231676);
    ensureCleanupTokenCatchesUp(partitionIds.get(0).getReplicaIds().get(2).getReplicaPath(), clusterMap, 231676);

    partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    partitionRequestInfo = new PartitionRequestInfo(partition, blobIdList);
    partitionRequestInfoList.add(partitionRequestInfo);

    try {
      GetRequest getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
              GetOptions.Include_All);
      channel.send(getRequest);
      InputStream stream = channel.receive().getInputStream();
      GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);

      for (int i = 0; i < 9; i++) {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), properties.get(i).getBlobSize());
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      }

      getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
          GetOptions.Include_All);
      channel.send(getRequest);
      stream = channel.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);

      for (int i = 0; i < 9; i++) {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata.get(i));
      }

      getRequest =
          new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.Include_All);
      channel.send(getRequest);
      stream = channel.receive().getInputStream();
      resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);

      for (int i = 0; i < 9; i++) {
        BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
        Assert.assertEquals(blobOutput.getSize(), properties.get(i).getBlobSize());
        byte[] dataOutput = new byte[(int) blobOutput.getSize()];
        blobOutput.getStream().read(dataOutput);
        Assert.assertArrayEquals(dataOutput, data.get(i));
      }
    } catch (MessageFormatException e) {
      e.printStackTrace();
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void endToEndReplicationWithMultiNodeSinglePartitionTest()
      throws InterruptedException, IOException, InstantiationException {
    endToEndReplicationWithMultiNodeSinglePartitionTest("", new Port(64422, PortType.PLAINTEXT),
        new Port(64423, PortType.PLAINTEXT), new Port(64424, PortType.PLAINTEXT), false, "", "", "");
  }

  @Test
  public void endToEndSSLReplicationWithMultiNodeSinglePartitionTest()
      throws InterruptedException, IOException, InstantiationException {
    endToEndReplicationWithMultiNodeSinglePartitionTest("DC2,DC3", new Port(64422, PortType.PLAINTEXT),
        new Port(54423, PortType.SSL), new Port(54424, PortType.SSL), true, "DC2,DC3", "DC3", "DC2");
  }

  private void endToEndReplicationWithMultiNodeSinglePartitionTest(String sslEnabledDatacenters, Port sourcePort,
      Port targetPort1, Port targetPort2, boolean enableSSLPorts, String sslEnabledDatacentersForDC1,
      String sslEnabledDatacentersForDC2, String sslEnabledDatacentersForDC3)
      throws InterruptedException, IOException, InstantiationException {
    // sourceNode is used to locate the datanode and hence has to be PlainText port
    cluster =
        new MockCluster(notificationSystem, enableSSLPorts, sslEnabledDatacentersForDC1, sslEnabledDatacentersForDC2,
            sslEnabledDatacentersForDC3);
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
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      BlockingChannel channel1 = null;
      BlockingChannel channel2 = null;
      BlockingChannel channel3 = null;

      if (sourcePort.getPortType() == PortType.SSL) {
        channel1 = new BlockingChannel("localhost", sourcePort.getPort(), 10000, 10000, 10000, 2000);
      } else {
        channel1 = new SSLBlockingChannel("localhost", sourcePort.getPort(), 10000, 10000, 10000, 2000);
      }

      if (targetPort1.getPortType() == PortType.SSL) {
        channel2 = new BlockingChannel("localhost", targetPort1.getPort(), 10000, 10000, 10000, 2000);
      } else {
        channel2 = new SSLBlockingChannel("localhost", targetPort1.getPort(), 10000, 10000, 10000, 2000);
      }

      if (targetPort2.getPortType() == PortType.SSL) {
        channel3 = new BlockingChannel("localhost", targetPort2.getPort(), 10000, 10000, 10000, 2000);
      } else {
        channel3 = new SSLBlockingChannel("localhost", targetPort2.getPort(), 10000, 10000, 10000, 2000);
      }

      channel1.connect();
      channel2.connect();
      channel3.connect();
      channel1.send(putRequest);
      InputStream putResponseStream = channel1.receive().getInputStream();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);
      // put blob 2
      PutRequest putRequest2 = new PutRequest(1, "client1", blobId2, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);
      // put blob 3
      PutRequest putRequest3 = new PutRequest(1, "client1", blobId3, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 4
      putRequest = new PutRequest(1, "client1", blobId4, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel1.send(putRequest);
      putResponseStream = channel1.receive().getInputStream();
      response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);

      // put blob 5
      putRequest2 = new PutRequest(1, "client1", blobId5, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 6
      putRequest3 = new PutRequest(1, "client1", blobId6, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
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
        BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp3.getInputStream());
        byte[] blobout = new byte[(int) blobOutput.getSize()];
        int readsize = 0;
        while (readsize < blobOutput.getSize()) {
          readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
        }
        Assert.assertArrayEquals(blobout, data);
      } catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      try {
        // get blob data
        // Use coordinator to get the blob
        Coordinator coordinator = new AmbryCoordinator(getCoordinatorProperties(sslEnabledDatacenters), clusterMap);
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
      DataNodeId dataNodeId = clusterMap.getDataNodeId("localhost", sourcePort.getPort());
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
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 8
      putRequest3 = new PutRequest(1, "client1", blobId8, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 9
      putRequest2 = new PutRequest(1, "client1", blobId9, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel2.send(putRequest2);
      putResponseStream = channel2.receive().getInputStream();
      response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 10
      putRequest3 = new PutRequest(1, "client1", blobId10, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
      channel3.send(putRequest3);
      putResponseStream = channel3.receive().getInputStream();
      response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // put blob 11
      putRequest2 = new PutRequest(1, "client1", blobId11, properties, ByteBuffer.wrap(usermetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(data)));
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
        checkBlobContent(blobId2, channel1, data);
        checkBlobContent(blobId3, channel1, data);
        checkBlobContent(blobId4, channel1, data);
        checkBlobContent(blobId5, channel1, data);
        checkBlobContent(blobId6, channel1, data);
        checkBlobContent(blobId7, channel1, data);
        checkBlobContent(blobId8, channel1, data);
        checkBlobContent(blobId9, channel1, data);
        checkBlobContent(blobId10, channel1, data);
        checkBlobContent(blobId11, channel1, data);
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
        checkBlobContent(blobId2, channel1, data);
        checkBlobContent(blobId3, channel1, data);
        checkBlobContent(blobId4, channel1, data);
        checkBlobContent(blobId5, channel1, data);
        checkBlobContent(blobId6, channel1, data);
        checkBlobContent(blobId7, channel1, data);
        checkBlobContent(blobId8, channel1, data);
        checkBlobContent(blobId9, channel1, data);
        checkBlobContent(blobId10, channel1, data);
        checkBlobContent(blobId11, channel1, data);
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

  class PutRequestRunnable implements Runnable {

    BlockingChannel channel;
    List<BlobId> blobIds;
    byte[] data;
    byte[] usermetadata;
    BlobProperties blobProperties;
    CountDownLatch latch;

    private PutRequestRunnable(BlockingChannel channel, int totalBlobsToPut, byte[] data, byte[] usermetadata,
        BlobProperties blobProperties, CountDownLatch latch) {
      MockClusterMap clusterMap = cluster.getClusterMap();
      this.channel = channel;
      blobIds = new ArrayList<BlobId>(totalBlobsToPut);
      List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
      for (int i = 0; i < totalBlobsToPut; i++) {
        int partitionIndex = new Random().nextInt(partitionIds.size());
        BlobId blobId = new BlobId(partitionIds.get(partitionIndex));
        blobIds.add(blobId);
      }
      this.data = data;
      this.usermetadata = usermetadata;
      this.blobProperties = blobProperties;
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < blobIds.size(); i++) {
          PutRequest putRequest =
              new PutRequest(1, "client1", blobIds.get(i), blobProperties, ByteBuffer.wrap(usermetadata),
                  new ByteBufferInputStream(ByteBuffer.wrap(data)));

          channel.send(putRequest);
          InputStream putResponseStream = channel.receive().getInputStream();
          PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
          Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);
        }
      } catch (Exception e) {
        Assert.assertTrue(false);
      } finally {
        latch.countDown();
      }
    }

    List<BlobId> getBlobIds() {
      return blobIds;
    }
  }

  @Test
  public void endToEndReplicationWithMultiNodeMultiPartitionTest()
      throws InterruptedException, IOException, InstantiationException {
    endToEndReplicationWithMultiNodeMultiPartitionTest(new Port(64422, PortType.PLAINTEXT),
        new Port(64423, PortType.PLAINTEXT), new Port(64424, PortType.PLAINTEXT), false, "", "", "");
  }

  @Test
  public void endToEndSSLReplicationWithMultiNodeMultiPartitionTest()
      throws InterruptedException, IOException, InstantiationException {
    endToEndReplicationWithMultiNodeMultiPartitionTest(new Port(64422, PortType.PLAINTEXT),
        new Port(54423, PortType.SSL), new Port(54424, PortType.SSL), true, "DC1,DC2", "DC1,DC2", "DC1,DC2");
  }

  private void endToEndReplicationWithMultiNodeMultiPartitionTest(Port sourcePort, Port targetPort1, Port targetPort2,
      boolean enableSSLPorts, String sslEnabledDatacentersForDC1, String sslEnabledDatacentersForDC2,
      String sslEnabledDatacentersForDC3)
      throws InterruptedException, IOException, InstantiationException {
    // sourceNode is used to locate the datanode and hence has to be PlainTextPort
    cluster =
        new MockCluster(notificationSystem, enableSSLPorts, sslEnabledDatacentersForDC1, sslEnabledDatacentersForDC2,
            sslEnabledDatacentersForDC3);

    try {
      MockClusterMap clusterMap = cluster.getClusterMap();
      List<AmbryServer> serverList = cluster.getServers();
      byte[] usermetadata = new byte[100];
      byte[] data = new byte[100];
      BlobProperties properties = new BlobProperties(100, "serviceid1");
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(data);

      // connect to all the servers
      BlockingChannel channel1 = null;
      BlockingChannel channel2 = null;
      BlockingChannel channel3 = null;

      if (sourcePort.getPortType() == PortType.SSL) {
        channel1 = new BlockingChannel("localhost", sourcePort.getPort(), 10000, 10000, 10000, 2000);
      } else {
        channel1 = new SSLBlockingChannel("localhost", sourcePort.getPort(), 10000, 10000, 10000, 2000);
      }

      if (targetPort1.getPortType() == PortType.SSL) {
        channel2 = new BlockingChannel("localhost", targetPort1.getPort(), 10000, 10000, 10000, 2000);
      } else {
        channel2 = new SSLBlockingChannel("localhost", targetPort1.getPort(), 10000, 10000, 10000, 2000);
      }

      if (targetPort2.getPortType() == PortType.SSL) {
        channel3 = new BlockingChannel("localhost", targetPort2.getPort(), 10000, 10000, 10000, 2000);
      } else {
        channel3 = new SSLBlockingChannel("localhost", targetPort2.getPort(), 10000, 10000, 10000, 2000);
      }

      // put all the blobs to random servers

      channel1.connect();
      channel2.connect();
      channel3.connect();

      int noOfParallelThreads = 3;
      CountDownLatch latch = new CountDownLatch(noOfParallelThreads);
      List<PutRequestRunnable> runnables = new ArrayList<PutRequestRunnable>(noOfParallelThreads);
      BlockingChannel channel = null;
      for (int i = 0; i < noOfParallelThreads; i++) {
        if (i % noOfParallelThreads == 0) {
          channel = channel1;
        } else if (i % noOfParallelThreads == 1) {
          channel = channel2;
        } else if (i % noOfParallelThreads == 2) {
          channel = channel3;
        }
        PutRequestRunnable runnable = new PutRequestRunnable(channel, 50, data, usermetadata, properties, latch);
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
            BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobOutput.getSize()];
            int readsize = 0;
            while (readsize < blobOutput.getSize()) {
              readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
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

      MockDataNodeId dataNode = (MockDataNodeId) clusterMap.getDataNodeId("localhost", sourcePort.getPort());
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
            BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobOutput.getSize()];
            int readsize = 0;
            while (readsize < blobOutput.getSize()) {
              readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
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

      dataNode = (MockDataNodeId) clusterMap.getDataNodeId("localhost", sourcePort.getPort());
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
            BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
            byte[] blobout = new byte[(int) blobOutput.getSize()];
            int readsize = 0;
            while (readsize < blobOutput.getSize()) {
              readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
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

  class Payload {
    public byte[] blob;
    public byte[] metadata;
    public BlobProperties blobProperties;
    public String blobId;

    public Payload(BlobProperties blobProperties, byte[] metadata, byte[] blob, String blobId) {
      this.blobProperties = blobProperties;
      this.metadata = metadata;
      this.blob = blob;
      this.blobId = blobId;
    }
  }

  class Sender implements Runnable {

    BlockingQueue<Payload> blockingQueue;
    CountDownLatch completedLatch;
    int numberOfRequests;
    Coordinator coordinator;

    public Sender(LinkedBlockingQueue<Payload> blockingQueue, CountDownLatch completedLatch, int numberOfRequests,
        Coordinator coordinator) {
      this.blockingQueue = blockingQueue;
      this.completedLatch = completedLatch;
      this.numberOfRequests = numberOfRequests;
      this.coordinator = coordinator;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < numberOfRequests; i++) {
          int size = new Random().nextInt(5000);
          BlobProperties properties = new BlobProperties(size, "service1", "owner id check", "image/jpeg", false);
          byte[] metadata = new byte[new Random().nextInt(1000)];
          byte[] blob = new byte[size];
          new Random().nextBytes(metadata);
          new Random().nextBytes(blob);
          String blobId = coordinator.putBlob(properties, ByteBuffer.wrap(metadata), new ByteArrayInputStream(blob));
          blockingQueue.put(new Payload(properties, metadata, blob, blobId));
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        completedLatch.countDown();
      }
    }
  }

  class Verifier implements Runnable {

    BlockingQueue<Payload> blockingQueue;
    CountDownLatch completedLatch;
    AtomicInteger totalRequests;
    AtomicInteger requestsVerified;
    MockClusterMap clusterMap;
    AtomicBoolean cancelTest;

    public Verifier(BlockingQueue<Payload> blockingQueue, CountDownLatch completedLatch, AtomicInteger totalRequests,
        AtomicInteger requestsVerified, MockClusterMap clusterMap, AtomicBoolean cancelTest) {
      this.blockingQueue = blockingQueue;
      this.completedLatch = completedLatch;
      this.totalRequests = totalRequests;
      this.requestsVerified = requestsVerified;
      this.clusterMap = clusterMap;
      this.cancelTest = cancelTest;
    }

    @Override
    public void run() {
      try {
        ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
        while (requestsVerified.get() != totalRequests.get() && !cancelTest.get()) {
          Payload payload = blockingQueue.poll(1000, TimeUnit.MILLISECONDS);
          if (payload != null) {
            notificationSystem.awaitBlobCreations(payload.blobId);
            for (MockDataNodeId dataNodeId : clusterMap.getDataNodes()) {
              BlockingChannel channel1 =
                  new BlockingChannel(dataNodeId.getHostname(), dataNodeId.getPort(), 10000, 10000, 10000, 2000);
              channel1.connect();
              ArrayList<BlobId> ids = new ArrayList<BlobId>();
              ids.add(new BlobId(payload.blobId, clusterMap));
              partitionRequestInfoList.clear();
              PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(ids.get(0).getPartition(), ids);
              partitionRequestInfoList.add(partitionRequestInfo);
              GetRequest getRequest =
                  new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                      GetOptions.None);
              channel1.send(getRequest);
              InputStream stream = channel1.receive().getInputStream();
              GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println(dataNodeId.getHostname() + " " + dataNodeId.getPort() + " " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
                  if (propertyOutput.getBlobSize() != payload.blobProperties.getBlobSize()) {
                    System.out.println("blob size not matching " + " expected " +
                        payload.blobProperties.getBlobSize() + " actual " + propertyOutput.getBlobSize());
                    throw new IllegalStateException();
                  }
                  if (!propertyOutput.getServiceId().equals(payload.blobProperties.getServiceId())) {
                    System.out.println("service id not matching " + " expected " +
                        payload.blobProperties.getServiceId() + " actual " + propertyOutput.getBlobSize());
                    throw new IllegalStateException();
                  }
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              // get user metadata
              ids.clear();
              ids.add(new BlobId(payload.blobId, clusterMap));
              partitionRequestInfoList.clear();
              partitionRequestInfo = new PartitionRequestInfo(ids.get(0).getPartition(), ids);
              partitionRequestInfoList.add(partitionRequestInfo);
              getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
                  GetOptions.None);
              channel1.send(getRequest);
              stream = channel1.receive().getInputStream();
              resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println("Error after get user metadata " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
                  if (userMetadataOutput.compareTo(ByteBuffer.wrap(payload.metadata)) != 0) {
                    throw new IllegalStateException();
                  }
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              // get blob
              ids.clear();
              ids.add(new BlobId(payload.blobId, clusterMap));
              partitionRequestInfoList.clear();
              partitionRequestInfo = new PartitionRequestInfo(ids.get(0).getPartition(), ids);
              partitionRequestInfoList.add(partitionRequestInfo);
              getRequest =
                  new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOptions.None);
              channel1.send(getRequest);
              stream = channel1.receive().getInputStream();
              resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              //System.out.println("response from get " + resp.getError());
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println("Error after get blob " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
                  byte[] blobout = new byte[(int) blobOutput.getSize()];
                  int readsize = 0;
                  while (readsize < blobOutput.getSize()) {
                    readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
                  }
                  if (ByteBuffer.wrap(blobout).compareTo(ByteBuffer.wrap(payload.blob)) != 0) {
                    throw new IllegalStateException();
                  }
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              channel1.disconnect();
            }

            requestsVerified.incrementAndGet();
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        cancelTest.set(true);
      } finally {
        completedLatch.countDown();
      }
    }
  }

  @Test
  public void endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest()
      throws InterruptedException, IOException, InstantiationException {
    endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest("DC1", false, "", "", "");
  }

  @Test
  public void endToEndSSLReplicationWithMultiNodeMultiPartitionMultiDCTest()
      throws InterruptedException, IOException, InstantiationException {
    endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest("DC1", true, "DC2,DC3", "DC2,DC3", "DC2,DC3");
  }

  private void endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest(String sourceDatacenter,
      boolean enableSSLPorts, String sslEnabledDatacenter1, String sslEnabledDatacenter2, String sslEnabledDatacenter3)
      throws InterruptedException, IOException, InstantiationException {
    cluster = new MockCluster(notificationSystem, enableSSLPorts, sslEnabledDatacenter1, sslEnabledDatacenter2,
        sslEnabledDatacenter3);
    Properties props = new Properties();
    props.setProperty("coordinator.hostname", "localhost");
    props.setProperty("coordinator.datacenter.name", sourceDatacenter);
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    Coordinator coordinator = new AmbryCoordinator(verifiableProperties, cluster.getClusterMap());
    Thread[] senderThreads = new Thread[3];
    LinkedBlockingQueue<Payload> blockingQueue = new LinkedBlockingQueue<Payload>();
    int numberOfSenderThreads = 3;
    int numberOfVerifierThreads = 3;
    CountDownLatch senderLatch = new CountDownLatch(numberOfSenderThreads);
    int numberOfRequestsToSendPerThread = 3;
    for (int i = 0; i < numberOfSenderThreads; i++) {
      senderThreads[i] =
          new Thread(new Sender(blockingQueue, senderLatch, numberOfRequestsToSendPerThread, coordinator));
      senderThreads[i].start();
    }
    senderLatch.await();

    if (blockingQueue.size() != numberOfRequestsToSendPerThread * numberOfSenderThreads) {
      // Failed during putBlob
      throw new IllegalStateException();
    }

    CountDownLatch verifierLatch = new CountDownLatch(numberOfVerifierThreads);
    AtomicInteger totalRequests = new AtomicInteger(numberOfRequestsToSendPerThread * numberOfSenderThreads);
    AtomicInteger verifiedRequests = new AtomicInteger(0);
    AtomicBoolean cancelTest = new AtomicBoolean(false);
    for (int i = 0; i < numberOfVerifierThreads; i++) {
      Thread thread = new Thread(
          new Verifier(blockingQueue, verifierLatch, totalRequests, verifiedRequests, cluster.getClusterMap(),
              cancelTest));
      thread.start();
    }
    verifierLatch.await();

    Assert.assertEquals(totalRequests.get(), verifiedRequests.get());
    coordinator.close();
  }

  private void checkBlobId(Coordinator coordinator, BlobId blobId, byte[] data)
      throws CoordinatorException, IOException {
    BlobOutput output = coordinator.getBlob(blobId.getID());
    Assert.assertEquals(output.getSize(), 1000);
    byte[] dataOutputStream = new byte[(int) output.getSize()];
    output.getStream().read(dataOutputStream);
    Assert.assertArrayEquals(dataOutputStream, data);
  }

  private void checkBlobContent(BlobId blobId, BlockingChannel channel, byte[] dataToCheck)
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
    GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), cluster.getClusterMap());
    Assert.assertEquals(resp.getError(), ServerErrorCode.No_Error);
    BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(resp.getInputStream());
    byte[] blobout = new byte[(int) blobOutput.getSize()];
    int readsize = 0;
    while (readsize < blobOutput.getSize()) {
      readsize += blobOutput.getStream().read(blobout, readsize, (int) blobOutput.getSize() - readsize);
    }
    Assert.assertArrayEquals(blobout, dataToCheck);
  }

  private VerifiableProperties getCoordinatorProperties(String sslEnabledDatacenters) {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "DC1");
    properties.setProperty("coordinator.ssl.enabled.datacenters", sslEnabledDatacenters);
    return new VerifiableProperties(properties);
  }

  public static void deleteFolderContent(File folder, boolean deleteParentFolder) {
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
}
