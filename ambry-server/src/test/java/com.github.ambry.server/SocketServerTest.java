package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.Request;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.SocketServer;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.utils.ByteBufferInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class SocketServerTest {

  private SocketServer server = null;

  public SocketServerTest() throws InterruptedException, IOException {
    Properties props = new Properties();
    VerifiableProperties propverify = new VerifiableProperties(props);
    NetworkConfig config = new NetworkConfig(propverify);
    server = new SocketServer(config, new MetricRegistry());
    server.start();
  }

  @After
  public void cleanup() {
    server.shutdown();
  }

  @Test
  public void simpleRequest() throws IOException, InterruptedException {
    MockClusterMap map = null;
    int correlationId = 1;
    byte[] bufdata = new byte[10];
    new Random().nextBytes(bufdata);
    ByteBuffer bufstream = ByteBuffer.wrap(bufdata);
    ByteBufferInputStream stream = new ByteBufferInputStream(bufstream);
    byte[] bufmetadata = new byte[5];
    new Random().nextBytes(bufmetadata);

    PutRequest emptyRequest = new PutRequest(correlationId,
                                             "test",
                                             new BlobId(new MockPartitionId(null)),
                                             new BlobProperties(10, "id"), ByteBuffer.wrap(bufmetadata), stream
    );

    BlockingChannel channel = new BlockingChannel("localhost", server.getPort(), 10000, 10000, 1000);
    channel.connect();
    channel.send(emptyRequest);
    RequestResponseChannel requestResponseChannel = server.getRequestResponseChannel();
    Request request = requestResponseChannel.receiveRequest();
    DataInputStream requestStream = new DataInputStream(request.getInputStream());
    Assert.assertEquals(requestStream.readShort(), 0); // read type
    map = new MockClusterMap();
    PutRequest requestFromNetwork = PutRequest.readFrom(requestStream, map);
    Assert.assertEquals(1, requestFromNetwork.getVersionId());
    Assert.assertEquals(correlationId, requestFromNetwork.getCorrelationId());
    Assert.assertEquals("test", requestFromNetwork.getClientId());
    ByteBuffer partition = ByteBuffer.allocate(8);
    partition.putLong(1);
    Assert.assertArrayEquals(partition.array(), requestFromNetwork.getBlobId().getPartition().getBytes());
    Assert.assertArrayEquals(bufmetadata, requestFromNetwork.getUsermetadata().array());
    Assert.assertEquals(10, requestFromNetwork.getDataSize());
    Assert.assertEquals("id", requestFromNetwork.getBlobProperties().getServiceId());
    InputStream streamFromNetwork = requestFromNetwork.getData();
    for (int i = 0; i < 10; i++) {
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
    PutResponse response = new PutResponse(1, "clientid1", ServerErrorCode.IO_Error);
    requestResponseChannel.sendResponse(response, request, null);
    InputStream streamResponse = channel.receive();
    PutResponse responseReplay = PutResponse.readFrom(new DataInputStream(streamResponse));
    Assert.assertEquals(responseReplay.getCorrelationId(), 1);
    Assert.assertEquals(responseReplay.getVersionId(), 1);
    Assert.assertEquals(responseReplay.getError(), ServerErrorCode.IO_Error);
  }

  /**
   * Choose a number of random available ports
   */
  ArrayList<Integer> choosePorts(int count) throws IOException {
    ArrayList<Integer> sockets = new ArrayList<Integer>();
    for (int i = 0; i < count; i++) {
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
