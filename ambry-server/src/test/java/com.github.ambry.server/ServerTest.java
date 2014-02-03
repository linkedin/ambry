package com.github.ambry.server;

import com.github.ambry.clustermap.*;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.messageformat.*;
import com.github.ambry.shared.*;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.ByteBufferInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.List;

public class ServerTest {

  private List<AmbryServer> serverList = null;
  private MockClusterMap clusterMap = null;

  public ServerTest() throws InterruptedException, IOException, StoreException, InstantiationException {

    clusterMap = new MockClusterMap();
    serverList = new ArrayList<AmbryServer>();
    DataNodeId dataNodeId = clusterMap.getDataNodeId("localhost", 6667);
    for (ReplicaId replicaId : clusterMap.getReplicaIds(dataNodeId)) {
      Properties props = new Properties();
      props.setProperty("host.name", "localhost");
      props.setProperty("port", Integer.toString(replicaId.getDataNodeId().getPort()));
      VerifiableProperties propverify = new VerifiableProperties(props);
      AmbryServer server = new AmbryServer(propverify, clusterMap);
      server.startup();
      serverList.add(server);
    }
  }

  @After
  public void cleanup() {
    for (AmbryServer server : serverList)
      server.shutdown();
    clusterMap.cleanup();
  }

  @Test
  public void EndToEndTest() throws InterruptedException, IOException {

    try {
      byte[] usermetadata = new byte[1000];
      byte[] data = new byte[31870];
      BlobProperties properties = new BlobProperties(31870, "serviceid1");
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(data);
      BlobId blobId1 = new BlobId(new MockPartitionId(null));
      BlobId blobId2 = new BlobId(new MockPartitionId(null));
      BlobId blobId3 = new BlobId(new MockPartitionId(null));
      // put blob 1
      PutRequest putRequest = new PutRequest(1,
                                             "client1",
                                             blobId1,
                                             properties, ByteBuffer.wrap(usermetadata),
                                             new ByteBufferInputStream(ByteBuffer.wrap(data))
      );
      BlockingChannel channel = new BlockingChannel("localhost", 6667, 10000, 10000, 10000);
      channel.connect();
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);

      // put blob 2
      PutRequest putRequest2 = new PutRequest(1,
                                              "client1",
                                              blobId2,
                                              properties, ByteBuffer.wrap(usermetadata),
                                              new ByteBufferInputStream(ByteBuffer.wrap(data))
      );
      channel.send(putRequest2);
      putResponseStream = channel.receive();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 3
      PutRequest putRequest3 = new PutRequest(1,
                                              "client1",
                                              blobId3,
                                              properties, ByteBuffer.wrap(usermetadata),
                                              new ByteBufferInputStream(ByteBuffer.wrap(data))
      );
      channel.send(putRequest3);
      putResponseStream = channel.receive();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId partition = new MockPartitionId(null);
      ids.add(blobId1);
      GetRequest getRequest1 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partition, ids);
      channel.send(getRequest1);
      InputStream stream = channel.receive();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), 31870);
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      // get user metadata
      GetRequest getRequest2 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partition, ids);
      channel.send(getRequest2);
      stream = channel.receive();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      try {
        // get blob data
        // Use coordinator to get the blob
        Coordinator coordinator = new AmbryCoordinator(getCoordinatorProperties(), clusterMap);
        coordinator.start();
        BlobOutput output = coordinator.getBlob(blobId1.toString());
        Assert.assertEquals(output.getSize(), 31870);
        byte[] dataOutputStream = new byte[(int)output.getSize()];
        output.getStream().read(dataOutputStream);
        Assert.assertArrayEquals(dataOutputStream, data);
        coordinator.shutdown();
      }
      catch (CoordinatorException e) {
        e.printStackTrace();
        Assert.assertEquals(false, true);
      }

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      partition = new MockPartitionId(null);
      ids.add(new BlobId(partition));
      GetRequest getRequest4 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partition, ids);
      channel.send(getRequest4);
      stream = channel.receive();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp4.getError(), ServerErrorCode.Blob_Not_Found);
      channel.disconnect();
    }
    catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(true, false);
    }
  }

  @Test
  public void EndToEndReplicationTest() throws InterruptedException, IOException {

    try {
      byte[] usermetadata = new byte[1000];
      byte[] data = new byte[31870];
      BlobProperties properties = new BlobProperties(31870, "serviceid1");
      new Random().nextBytes(usermetadata);
      new Random().nextBytes(data);
      BlobId blobId1 = new BlobId(new MockPartitionId(null));
      BlobId blobId2 = new BlobId(new MockPartitionId(null));
      BlobId blobId3 = new BlobId(new MockPartitionId(null));
      // put blob 1
      PutRequest putRequest = new PutRequest(1,
              "client1",
              blobId1,
              properties, ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(data))
      );
      BlockingChannel channel = new BlockingChannel("localhost", 6667, 10000, 10000, 10000);
      channel.connect();
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response.getError(), ServerErrorCode.No_Error);

      // put blob 2
      PutRequest putRequest2 = new PutRequest(1,
              "client1",
              blobId2,
              properties, ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(data))
      );
      channel.send(putRequest2);
      putResponseStream = channel.receive();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response2.getError(), ServerErrorCode.No_Error);

      // put blob 3
      PutRequest putRequest3 = new PutRequest(1,
              "client1",
              blobId3,
              properties, ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(data))
      );
      channel.send(putRequest3);
      putResponseStream = channel.receive();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));
      Assert.assertEquals(response3.getError(), ServerErrorCode.No_Error);

      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId partition = new MockPartitionId(null);
      ids.add(blobId1);
      GetRequest getRequest1 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partition, ids);
      channel.send(getRequest1);
      InputStream stream = channel.receive();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), 31870);
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      // get user metadata
      GetRequest getRequest2 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partition, ids);
      channel.send(getRequest2);
      stream = channel.receive();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(false, true);
      }

      try {
        // get blob data
        // Use coordinator to get the blob
        Coordinator coordinator = new AmbryCoordinator(getCoordinatorProperties(), clusterMap);
        coordinator.start();
        BlobOutput output = coordinator.getBlob(blobId1.toString());
        Assert.assertEquals(output.getSize(), 31870);
        byte[] dataOutputStream = new byte[(int)output.getSize()];
        output.getStream().read(dataOutputStream);
        Assert.assertArrayEquals(dataOutputStream, data);
        coordinator.shutdown();
      }
      catch (CoordinatorException e) {
        e.printStackTrace();
        Assert.assertEquals(false, true);
      }

      // fetch blob that does not exist
      // get blob properties
      ids = new ArrayList<BlobId>();
      partition = new MockPartitionId(null);
      ids.add(new BlobId(partition));
      GetRequest getRequest4 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partition, ids);
      channel.send(getRequest4);
      stream = channel.receive();
      GetResponse resp4 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      Assert.assertEquals(resp4.getError(), ServerErrorCode.Blob_Not_Found);
      channel.disconnect();
    }
    catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(true, false);
    }
  }

  public VerifiableProperties getCoordinatorProperties() {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "Datacenter");
    return new VerifiableProperties(properties);
  }
}
