/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.DefaultObjectNameFactory;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;
import java.util.Properties;
import java.util.function.Function;
import org.junit.Test;

import static org.mockito.Mockito.*;


/**
 * Test logic in {@link AmbryServer}.
 */
public class AmbryServerTest {

  /**
   * Test starting and shutting down the server with a custom {@link JmxReporter} factory.
   * @throws Exception
   */
  @Test
  public void testAmbryServerWithReporterFactory() throws Exception {
    ClusterAgentsFactory clusterAgentsFactory = new MockClusterAgentsFactory(false, false, 1, 1, 1);
    ObjectNameFactory spyObjectNameFactory = spy(new DefaultObjectNameFactory());
    Function<MetricRegistry, JmxReporter> reporterFactory =
        reporter -> JmxReporter.forRegistry(reporter).createsObjectNamesWith(spyObjectNameFactory).build();

    DataNodeId dataNodeId = clusterAgentsFactory.getClusterMap().getDataNodeIds().get(0);

    Properties props = new Properties();
    props.setProperty("host.name", dataNodeId.getHostname());
    props.setProperty("port", Integer.toString(dataNodeId.getPort()));
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", dataNodeId.getHostname());

    AmbryServer ambryServer =
        new AmbryServer(new VerifiableProperties(props), clusterAgentsFactory, null, new LoggingNotificationSystem(),
            SystemTime.getInstance(), reporterFactory);
    ambryServer.startup();
    verify(spyObjectNameFactory, atLeastOnce()).createName(anyString(), anyString(), anyString());
    ambryServer.shutdown();
  }
}
