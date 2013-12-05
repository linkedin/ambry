package com.github.ambry.server;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.BlobNotFoundException;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.DataCorruptException;
import com.github.ambry.messageformat.MessageFormat;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.*;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.ByteBufferInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class ServerTest {

  private AmbryServer server = null;

  /**
   * Create a temporary file
   */
  File tempFile() throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  public ServerTest() throws InterruptedException, IOException, StoreException {
    File tempFile = tempFile();
    File indexFile = new File(tempFile.getParent());
    for (File c : indexFile.listFiles())
      c.delete();
    File logFile = new File(tempFile.getParent(), "log_current");
    RandomAccessFile randomFile = new RandomAccessFile(logFile, "rw");
    // preallocate file
    randomFile.setLength(100000);

    Properties props = new Properties();
    props.setProperty("store.data.dir", tempFile.getParent());
    VerifiableProperties propverify = new VerifiableProperties(props);
    server = new AmbryServer(propverify);
    server.startup();
  }

  @After
  public void cleanup() {
    server.shutdown();
  }

  @Test
  public void EndToEndTest() throws InterruptedException, IOException {

    byte[] usermetadata = new byte[1000];
    byte[] data = new byte[31870];
    BlobProperties properties = new BlobProperties(31870, "serviceid1");
    new Random().nextBytes(usermetadata);
    new Random().nextBytes(data);
    // put blob 1
    PutRequest putRequest = new PutRequest(1, 1, "client1", new BlobId("id1"), ByteBuffer.wrap(usermetadata),
            new ByteBufferInputStream(ByteBuffer.wrap(data)), properties);
    BlockingChannel channel = new BlockingChannel("localhost", 6667, 10000, 10000, 10000);
    channel.connect();
    channel.send(putRequest);
    InputStream putResponseStream = channel.receive();
    PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));

    PutRequest putRequest2 = new PutRequest(1, 1, "client1", new BlobId("id2"), ByteBuffer.wrap(usermetadata),
            new ByteBufferInputStream(ByteBuffer.wrap(data)), properties);
    channel.send(putRequest2);
    putResponseStream = channel.receive();
    PutResponse response2 = PutResponse.readFrom(new DataInputStream(putResponseStream));

    PutRequest putRequest3 = new PutRequest(1, 1, "client1", new BlobId("id3"), ByteBuffer.wrap(usermetadata),
            new ByteBufferInputStream(ByteBuffer.wrap(data)), properties);
    channel.send(putRequest3);
    putResponseStream = channel.receive();
    PutResponse response3 = PutResponse.readFrom(new DataInputStream(putResponseStream));


    // get blob properties
    ArrayList<BlobId> ids = new ArrayList<BlobId>();
    ids.add(new BlobId("id1"));
    GetRequest getRequest1 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, 1, ids);
    channel.send(getRequest1);
    InputStream stream = channel.receive();
    GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream));
    try {
      BlobProperties propertyOutput = MessageFormat.deserializeBlobProperties(resp1.getInputStream());
      Assert.assertEquals(propertyOutput.getBlobSize(), 31870);
      Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
    }
    catch (DataCorruptException e) {
      Assert.assertEquals(false, true);
    }

    // get user metadata
    GetRequest getRequest2 = new GetRequest(1, "clientid2", MessageFormatFlags.UserMetadata, 1, ids);
    channel.send(getRequest2);
    stream = channel.receive();
    GetResponse resp2 = GetResponse.readFrom(new DataInputStream(stream));
    try {
      ByteBuffer userMetadataOutput = MessageFormat.deserializeMetadata(resp2.getInputStream());
      Assert.assertArrayEquals(userMetadataOutput.array(), usermetadata);
    }
    catch (DataCorruptException e) {
      Assert.assertEquals(false, true);
    }
    channel.disconnect();


    try {
      // get blob data
      // Use coordinator to get the blob
      Coordinator coordinator = new AmbryCoordinator("localhost", 6667);
      BlobOutput output = coordinator.getBlob("id1");
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
}