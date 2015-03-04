package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.Properties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test for the blocking channel connection pool
 */
public class BlockingChannelConnectionPoolTest {

  private SocketServer server = null;

  public BlockingChannelConnectionPoolTest()
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
  public void testBlockingChannelInfo()
      throws InterruptedException {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.host", "5");
    BlockingChannelInfo channelInfo =
        new BlockingChannelInfo(new ConnectionPoolConfig(new VerifiableProperties(props)), "127.0.0.1", 6667,
            new MetricRegistry());
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
    BlockingChannel blockingChannel = null;
    try {
      blockingChannel = channelInfo.getBlockingChannel(1000);
    } catch (ConnectionPoolTimeoutException e) {
      Assert.assertTrue(false);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 1);
    channelInfo.addBlockingChannel(blockingChannel);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
  }

  @Test
  public void testBlockingChannelConnectionPool() {

  }
}
