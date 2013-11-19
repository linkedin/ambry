package com.github.ambry.server;

import com.github.ambry.config.VerifiableProperties;
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
import java.net.SocketException;
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
    RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
    // preallocate file
    randomFile.setLength(5000);
    File indexFile = new File(tempFile.getParent(), "index_current");
    indexFile.delete();
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
  public void EndToEndTest() throws SocketException, InterruptedException, IOException {

    byte[] usermetadata = new byte[1000];
    byte[] data = new byte[2000];
    BlobProperties properties = new BlobProperties(2000, "serviceid1");
    new Random().nextBytes(usermetadata);
    new Random().nextBytes(data);
    // put blob
    PutRequest putRequest = new PutRequest(1, 1, "client1", new BlobId("id1"), ByteBuffer.wrap(usermetadata),
            new ByteBufferInputStream(ByteBuffer.wrap(data)), properties);
    BlockingChannel channel = new BlockingChannel("localhost", 6667, 10000, 10000, 10000);
    channel.connect();
    channel.send(putRequest);
    InputStream putResponseStream = channel.receive();
    PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));

    // get blob
    ArrayList<BlobId> ids = new ArrayList<BlobId>();
    ids.add(new BlobId("id1"));
    GetRequest getRequest1 = new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, 1, ids);
    channel.send(getRequest1);
    InputStream stream = channel.receive();
    GetResponse resp1 = GetResponse.readFrom(new DataInputStream(stream));
    try {
      BlobProperties propertyOutput = MessageFormat.deserializeBlobProperties(resp1.getInputStream());
      Assert.assertEquals(propertyOutput.getBlobSize(), 2000);
      Assert.assertEquals(propertyOutput.getServiceId(), "serviceid1");
    }
    catch (DataCorruptException e) {
      Assert.assertEquals(false, true);
    }

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

    GetRequest getRequest3 = new GetRequest(1, "clientid2", MessageFormatFlags.Data, 1, ids);
    channel.send(getRequest3);
    stream = channel.receive();
    GetResponse resp3 = GetResponse.readFrom(new DataInputStream(stream));
    try {
      ByteBufferInputStream dataOutput = MessageFormat.deserializeData(resp3.getInputStream());
      byte[] dataOutputStream = new byte[2000];
      dataOutput.read(dataOutputStream);
      Assert.assertArrayEquals(dataOutputStream, data);
    }
    catch (DataCorruptException e) {
      Assert.assertEquals(false, true);
    }
  }
}