package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.Request;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.SocketServer;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.utils.ByteBufferInputStream;
import java.util.List;
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

  public SocketServerTest()
      throws InterruptedException, IOException {
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
  public void simpleRequest()
      throws IOException, InterruptedException {
    MockClusterMap map = new MockClusterMap();
    int correlationId = 1;
    byte[] bufdata = new byte[10];
    new Random().nextBytes(bufdata);
    ByteBuffer bufstream = ByteBuffer.wrap(bufdata);
    ByteBufferInputStream stream = new ByteBufferInputStream(bufstream);
    byte[] bufmetadata = new byte[5];
    new Random().nextBytes(bufmetadata);

    List<PartitionId> partitionIds = map.getWritablePartitionIds();
    PutRequest emptyRequest =
        new PutRequest(correlationId, "test", new BlobId(partitionIds.get(0)), new BlobProperties(10, "id"),
            ByteBuffer.wrap(bufmetadata), stream);

    BlockingChannel channel = new BlockingChannel("localhost", server.getPort(), 10000, 10000, 1000);
    channel.connect();
    channel.send(emptyRequest);
    RequestResponseChannel requestResponseChannel = server.getRequestResponseChannel();
    Request request = requestResponseChannel.receiveRequest();
    DataInputStream requestStream = new DataInputStream(request.getInputStream());
    Assert.assertEquals(requestStream.readShort(), 0); // read type

    PutRequest requestFromNetwork = PutRequest.readFrom(requestStream, map);
    Assert.assertEquals(1, requestFromNetwork.getVersionId());
    Assert.assertEquals(correlationId, requestFromNetwork.getCorrelationId());
    Assert.assertEquals("test", requestFromNetwork.getClientId());
    ByteBuffer partition = ByteBuffer.allocate(10);
    partition.putShort((short) 1);
    partition.putLong(0);
    Assert.assertArrayEquals(partition.array(), requestFromNetwork.getBlobId().getPartition().getBytes());
    Assert.assertArrayEquals(bufmetadata, requestFromNetwork.getUsermetadata().array());
    Assert.assertEquals(10, requestFromNetwork.getDataSize());
    Assert.assertEquals("id", requestFromNetwork.getBlobProperties().getServiceId());
    InputStream streamFromNetwork = requestFromNetwork.getData();
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(bufdata[i], (byte) streamFromNetwork.read());
    }
    int read = streamFromNetwork.read();
    Assert.assertTrue(read == -1);

    // send response back and ensure response is received
    PutResponse response = new PutResponse(1, "clientid1", ServerErrorCode.IO_Error);
    requestResponseChannel.sendResponse(response, request, null);
    InputStream streamResponse = channel.receive().getInputStream();
    PutResponse responseReplay = PutResponse.readFrom(new DataInputStream(streamResponse));
    Assert.assertEquals(responseReplay.getCorrelationId(), 1);
    Assert.assertEquals(responseReplay.getVersionId(), 1);
    Assert.assertEquals(responseReplay.getError(), ServerErrorCode.IO_Error);
  }

  /**
   * Choose a number of random available ports
   */
  ArrayList<Integer> choosePorts(int count)
      throws IOException {
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
  public int choosePort()
      throws IOException {
    return choosePorts(1).get(0);
  }
}
