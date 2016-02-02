package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestResponseHandler;
import com.github.ambry.utils.MockTime;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


class MockRequestResponseHandler extends RequestResponseHandler {
  List<String> connectionIds;
  int index;

  MockRequestResponseHandler()
      throws IOException {
    super(null, new NetworkConfig(new VerifiableProperties(new Properties())), new NetworkMetrics(new MetricRegistry()),
        null, new MockTime());
    connectionIds = new ArrayList<String>();
    index = 0;
  }

  @Override
  public void start() {
  }

  @Override
  public String connect(String host, Port port)
      throws IOException {
    String connId = host + port + index++;
    connectionIds.add(connId);
    return connId;
  }

  public int count() {
    return connectionIds.size();
  }

  public List<String> getConnectionIds() {
    return connectionIds;
  }
}
