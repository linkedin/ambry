package com.github.ambry;

import com.ambry.shared.PutRequest;
import com.ambry.shared.PutResponse;
import org.junit.After;
import org.junit.Test;

import java.nio.BufferUnderflowException;
import org.junit.Assert;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.net.*;
import java.io.*;

public class SocketServerTest {

  private SocketServer server = null;

  public SocketServerTest() throws InterruptedException, IOException {
    server = new SocketServer(null, choosePort(), 1, 50, 300000, 300000, 50);
    server.start();
  }

  private Socket connect() throws UnknownHostException, IOException {
    return new Socket("localhost", server.getPort());
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
            new PutRequest((short)1,0, correlationId, "test", "1234", ByteBuffer.wrap(bufmetadata), stream, 10);
    BlockingChannel channel = new BlockingChannel("localhost", server.getPort(), 10000, 10000, 1000);
    channel.connect();
    channel.send(emptyRequest);
    RequestResponseChannel requestResponseChannel = server.getRequestResponseChannel();
    Request request = requestResponseChannel.receiveRequest();
    DataInputStream requestStream = new DataInputStream(request.getInputStream());
    PutRequest requestFromNetwork = PutRequest.readFrom(requestStream);
    Assert.assertEquals(1, requestFromNetwork.getVersionId());
    Assert.assertEquals(0, requestFromNetwork.getLogicalVolumeId());
    Assert.assertEquals(correlationId, requestFromNetwork.getCorrelationId());
    Assert.assertEquals("test", requestFromNetwork.getClientId());
    Assert.assertEquals("1234", requestFromNetwork.getBlobId());
    Assert.assertArrayEquals(bufmetadata, requestFromNetwork.getMetadata().array());
    InputStream streamFromNetwork = requestFromNetwork.getData();
    for (int i = 0; i < 10; i++)
    {
      Assert.assertEquals(bufdata[i], streamFromNetwork.read());
    }
    try {
      streamFromNetwork.read();
      Assert.assertTrue(false);
    }
    catch (BufferUnderflowException e) {
      Assert.assertTrue(true);
    }

    // send response back and ensure response is received
    PutResponse response = new PutResponse((short)0, 1, (short)2);
    requestResponseChannel.sendResponse(new SocketServerResponse(request,response));
    InputStream streamResponse = channel.receive();
    PutResponse responseReplay = PutResponse.readFrom(new DataInputStream(streamResponse));
    Assert.assertEquals(responseReplay.getCorrelationId(), 1);
    Assert.assertEquals(responseReplay.getVersionId(), 0);
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
