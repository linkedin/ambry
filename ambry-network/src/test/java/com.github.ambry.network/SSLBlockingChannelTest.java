package com.github.ambry.network;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class SSLBlockingChannelTest {
  private static SSLFactory sslFactory;
  private static SSLSocketFactory sslSocketFactory;
  private static EchoServer sslEchoServer;
  private static String hostName = "localhost";
  private static int sslPort = 18284;

  @BeforeClass
  public static void onceExecutedBeforeAll()
      throws Exception {
    sslFactory = TestUtils.createSSLFactory();
    SSLContext sslContext = sslFactory.createSSLContext();
    sslSocketFactory = sslContext.getSocketFactory();
    sslEchoServer = new EchoServer(sslFactory, sslPort);
    sslEchoServer.start();
  }

  @AfterClass
  public static void onceExecutedAfterAll()
      throws Exception {
    int serverExceptionCount = sslEchoServer.getExceptionCount();
    assertEquals(serverExceptionCount, 0);
    sslEchoServer.close();
  }

  @Before
  public void setup()
      throws Exception {
  }

  @After
  public void teardown()
      throws Exception {
  }

  @Test
  public void testSendAndReceive()
      throws Exception {
    BlockingChannel channel = new SSLBlockingChannel(hostName, sslPort, 10000, 10000, 10000, 2000, sslSocketFactory);
    sendAndReceive(channel);
    channel.disconnect();
  }

  @Test
  public void testRenegotiation() throws Exception {
    BlockingChannel channel = new SSLBlockingChannel(hostName, sslPort, 10000, 10000, 10000, 2000, sslSocketFactory);
    sendAndReceive(channel);
    sslEchoServer.renegotiate();
    sendAndReceive(channel);
    channel.disconnect();
  }

  @Test
  public void testWrongPortConnection()
      throws Exception {
    BlockingChannel channel =
        new SSLBlockingChannel(hostName, sslPort + 1, 10000, 10000, 10000, 2000, sslSocketFactory);
    try {
      // send request
      channel.connect();
      fail("should have thrown!");
    } catch (IOException e) {
      assertEquals(e.getMessage(), "Connection refused");
    }
  }

  private void sendAndReceive(BlockingChannel channel) throws Exception {
    long blobSize = 1028;
    byte[] bytesToSend = new byte[(int) blobSize];
    new Random().nextBytes(bytesToSend);
    ByteBuffer byteBufferToSend = ByteBuffer.wrap(bytesToSend);
    byteBufferToSend.putLong(0, blobSize);
    BoundedByteBufferSend bufferToSend = new BoundedByteBufferSend(byteBufferToSend);
    // send request
    channel.connect();
    channel.send(bufferToSend);
    // receive response
    InputStream streamResponse = channel.receive().getInputStream();
    DataInputStream input = new DataInputStream(streamResponse);
    byte[] bytesReceived = new byte[(int) blobSize - 8];
    input.readFully(bytesReceived);
    for (int i = 0; i < blobSize - 8; i++) {
      Assert.assertEquals(bytesToSend[8 + i], bytesReceived[i]);
    }
  }
}
