package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.utils.ByteBufferInputStream;
import org.junit.After;
import org.junit.Test;

import java.nio.BufferUnderflowException;
import org.junit.Assert;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.net.*;
import java.io.*;

public class SocketServerTest {

  private SocketServer server = null;

  public SocketServerTest() throws InterruptedException, IOException {
    Properties props = new Properties();
    VerifiableProperties propverify = new VerifiableProperties(props);
    NetworkConfig config = new NetworkConfig(propverify);
    server = new SocketServer(config);
    server.start();
  }

  @After
  public void cleanup() {
    server.shutdown();
  }

  @Test
  public void simpleRequest() throws UnknownHostException, IOException, InterruptedException {
    int correlationId = 1;
    byte [] bufdata = new byte[10];
    new Random().nextBytes(bufdata);
    ByteBuffer bufstream = ByteBuffer.wrap(bufdata);
    ByteBufferInputStream stream = new ByteBufferInputStream(bufstream);
    byte[] bufmetadata = new byte[5];
    new Random().nextBytes(bufmetadata);

    PutRequest emptyRequest =
            new PutRequest(0, correlationId, "test", new BlobId("1234"), ByteBuffer.wrap(bufmetadata), stream, new BlobProperties(10, "id"));
    BlockingChannel channel = new BlockingChannel("localhost", server.getPort(), 10000, 10000, 1000);
    channel.connect();
    channel.send(emptyRequest);
    RequestResponseChannel requestResponseChannel = server.getRequestResponseChannel();
    Request request = requestResponseChannel.receiveRequest();
    DataInputStream requestStream = new DataInputStream(request.getInputStream());
    Assert.assertEquals(requestStream.readShort(), 0); // read type
    PutRequest requestFromNetwork = PutRequest.readFrom(requestStream);
    Assert.assertEquals(1, requestFromNetwork.getVersionId());
    Assert.assertEquals(0, requestFromNetwork.getPartitionId());
    Assert.assertEquals(correlationId, requestFromNetwork.getCorrelationId());
    Assert.assertEquals("test", requestFromNetwork.getClientId());
    Assert.assertEquals("1234", new String(requestFromNetwork.getBlobId().toBytes().array()));
    Assert.assertArrayEquals(bufmetadata, requestFromNetwork.getUsermetadata().array());
    Assert.assertEquals(10, requestFromNetwork.getDataSize());
    Assert.assertEquals("id", requestFromNetwork.getBlobProperties().getServiceId());
    InputStream streamFromNetwork = requestFromNetwork.getData();
    for (int i = 0; i < 10; i++)
    {
      Assert.assertEquals(bufdata[i], (byte)streamFromNetwork.read());
    }
    try {
      streamFromNetwork.read();
      Assert.assertTrue(false);
    }
    catch (BufferUnderflowException e) {
      Assert.assertTrue(true);
    }

    // send response back and ensure response is received
    PutResponse response = new PutResponse(1, "clientid1", (short)2);
    requestResponseChannel.sendResponse(response, request);
    InputStream streamResponse = channel.receive();
    PutResponse responseReplay = PutResponse.readFrom(new DataInputStream(streamResponse));
    Assert.assertEquals(responseReplay.getCorrelationId(), 1);
    Assert.assertEquals(responseReplay.getVersionId(), 1);
    Assert.assertEquals(responseReplay.getError(), 2);
  }

  /**
   * Choose a number of random available ports
   */
   ArrayList<Integer> choosePorts(int count) throws IOException {
     ArrayList<Integer> sockets = new ArrayList<Integer>();
     for(int i = 0; i < count; i++) {
       ServerSocket socket = new ServerSocket(0);
       sockets.add(socket.getLocalPort());
       socket.close();
     }
     return sockets;
   }

   /**
    * Choose an available port
    */
    public int choosePort() throws IOException {
      return choosePorts(1).get(0);
    }
}
