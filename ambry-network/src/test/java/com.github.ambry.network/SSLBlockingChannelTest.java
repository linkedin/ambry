package com.github.ambry.network;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;


public class SSLBlockingChannelTest {
  private SSLFactory sslFactory;
  private SSLSocketFactory sslSocketFactory;
  private String hostName = "localhost";
  private int sslPort = 18284;
  private SSLBlockingEchoServer sslEchoServer;

  @Before
  public void setup()
      throws Exception {
    sslFactory = TestUtils.createSSLFactory();
    SSLContext sslContext = sslFactory.createSSLContext();
    sslSocketFactory = sslContext.getSocketFactory();
    sslEchoServer = new SSLBlockingEchoServer(sslFactory, sslPort);
    sslEchoServer.start();
  }

  @After
  public void teardown()
      throws Exception {
    sslEchoServer.close();
  }

  @Test
  public void testSendAndReceive() {
    long blobSize = 1028;
    BlockingChannel channel = new SSLBlockingChannel(hostName, sslPort, 10000, 10000, 10000, 2000, sslSocketFactory);
    try {
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
      channel.disconnect();
    } catch (IOException e) {
      fail("Unexpected error in testing SSLBlockingChannel");
      e.printStackTrace();
    }
  }
}
