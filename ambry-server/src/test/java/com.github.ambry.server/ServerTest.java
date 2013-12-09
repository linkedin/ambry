package com.github.ambry.server;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.BlobNotFoundException;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.messageformat.*;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.BlobId;
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

public class ServerTest {

  private AmbryServer server = null;
  private MockClusterMap clusterMap = null;

  public ServerTest() throws InterruptedException, IOException, StoreException, InstantiationException {

    clusterMap = new MockClusterMap();
    Properties props = new Properties();
    VerifiableProperties propverify = new VerifiableProperties(props);
    server = new AmbryServer(propverify, clusterMap);
    server.startup();
  }

  @After
  public void cleanup() {
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
      BlobId blobId1 = new BlobId(new MockPartitionId());
      BlobId blobId2 = new BlobId(new MockPartitionId());
      BlobId blobId3 = new BlobId(new MockPartitionId());
      // put blob 1
      PutRequest putRequest = new PutRequest(1,
                                             "client1",
                                             blobId1,
                                             ByteBuffer.wrap(usermetadata),
                                             new ByteBufferInputStream(ByteBuffer.wrap(data)),
                                             properties);
      BlockingChannel channel = new BlockingChannel("localhost", 6667, 10000, 10000, 10000);
      channel.connect();
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));

      // put blob 2
      PutRequest putRequest2 = new PutRequest(1,
                                              "client1",
                                              blobId2,
                                              ByteBuffer.wrap(usermetadata),
                                              new ByteBufferInputStream(ByteBuffer.wrap(data)),
                                              properties);
      channel.send(putRequest2);
      putResponseStream = channel.receive();
      PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));

      // put blob 3
      PutRequest putRequest3 = new PutRequest(1,
                                              "client1",
                                              blobId3,
                                              ByteBuffer.wrap(usermetadata),
                                              new ByteBufferInputStream(ByteBuffer.wrap(data)),
                                              properties);
      channel.send(putRequest3);
      putResponseStream = channel.receive();
      PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));


      // get blob properties
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      MockPartitionId partition = new MockPartitionId();
      ids.add(blobId1);
      GetRequest getRequest1 = new GetRequest(partition, 1, "clientid2", MessageFormatFlags.BlobProperties, ids);
      channel.send(getRequest1);
      InputStream stream = channel.receive();
      GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        BlobProperties propertyOutput = MessageFormat.deserializeBlobProperties(resp1.getInputStream());
        Assert.assertEquals(propertyOutput.getBlobSize(), 31870);
        Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(e.getErrorCode(), MessageFormatErrorCodes.Data_Corrupt);
      }

      // get user metadata
      GetRequest getRequest2 = new GetRequest(partition, 1, "clientid2", MessageFormatFlags.UserMetadata, ids);
      channel.send(getRequest2);
      stream = channel.receive();
      GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
      try {
        ByteBuffer userMetadataOutput = MessageFormat.deserializeMetadata(resp2.getInputStream());
        Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(e.getErrorCode(), MessageFormatErrorCodes.Data_Corrupt);
      }
      channel.disconnect();


      try {
        // get blob data
        // Use coordinator to get the blob
        Coordinator coordinator = new AmbryCoordinator(clusterMap);
        BlobOutput output = coordinator.getBlob(blobId1.toString());
        Assert.assertEquals(output.getSize(), 31870);
        byte[] dataOutputStream = new byte[(int)output.getSize()];
        output.getStream().read(dataOutputStream);
        Assert.assertArrayEquals(dataOutputStream, data);
        coordinator.shutdown();
      }
      catch (BlobNotFoundException e) {
        Assert.assertEquals(false, true);
      }
    }
    catch (Exception e) {
      Assert.assertEquals(true, false);
    }
  }
}