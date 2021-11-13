/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
 */
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.DefaultObjectNameFactory;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Function;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Mockito.*;


/**
 * Test of the VCR Server.
 */
public class VcrServerTest {

  private static MockClusterAgentsFactory mockClusterAgentsFactory;
  private static MockClusterMap mockClusterMap;
  private static NotificationSystem notificationSystem;

  @BeforeClass
  public static void setup() throws Exception {
    mockClusterAgentsFactory = new MockClusterAgentsFactory(false, true, 1, 1, 2);
    mockClusterMap = mockClusterAgentsFactory.getClusterMap();
    notificationSystem = mock(NotificationSystem.class);
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    mockClusterMap.cleanup();
  }

  /**
   * Bring up the VCR server and then shut it down with {@link StaticVcrClusterParticipant}.
   * @throws Exception
   */
  @Test
  public void testVCRServerWithStaticCluster() throws Exception {
    VerifiableProperties verifiableProperties = getStaticClusterVcrProps();
    VcrServer vcrServer = new VcrServer(verifiableProperties, mockClusterAgentsFactory, notificationSystem, null);
    vcrServer.startup();
    Assert.assertNull("Expected null compactor", vcrServer.getVcrReplicationManager().getCloudStorageCompactor());
    vcrServer.shutdown();
  }

  /**
   * Bring up the VCR server and then shut it down with {@link StaticVcrClusterParticipant} and a custom {@link JmxReporter}
   * factory.
   * @throws Exception
   */
  @Test
  public void testVCRServerWithReporterFactory() throws Exception {
    VerifiableProperties verifiableProperties = getStaticClusterVcrProps();
    ObjectNameFactory spyObjectNameFactory = spy(new DefaultObjectNameFactory());
    Function<MetricRegistry, JmxReporter> reporterFactory =
        reporter -> JmxReporter.forRegistry(reporter).createsObjectNamesWith(spyObjectNameFactory).build();
    VcrServer vcrServer =
        new VcrServer(verifiableProperties, mockClusterAgentsFactory, notificationSystem, reporterFactory);
    vcrServer.startup();
    // check that the custom ObjectNameFactory specified in reporterFactory was used.
    verify(spyObjectNameFactory, atLeastOnce()).createName(anyString(), anyString(), anyString());
    vcrServer.shutdown();
  }

  /**
   * Bring up the VCR server and then shut it down with {@link HelixVcrClusterParticipant}.
   * @throws Exception
   */
  @Test
  public void testVCRServerWithHelixCluster() throws Exception {
    Properties serverSSLProps = new Properties();
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    TestSSLUtils.addSSLProperties(serverSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server");
    TestSSLUtils.addHttp2Properties(serverSSLProps, SSLFactory.Mode.SERVER, true);
    int zkPort = 31999;
    String zkConnectString = "localhost:" + zkPort;
    String vcrClusterName = "vcrTestCluster";
    TestUtils.ZkInfo zkInfo = new TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), "DC1", (byte) 1, zkPort, true);
    HelixControllerManager helixControllerManager =
        VcrTestUtil.populateZkInfoAndStartController(zkConnectString, vcrClusterName, mockClusterMap);
    Properties props =
        VcrTestUtil.createVcrProperties("DC1", vcrClusterName, zkConnectString, 12300, 12400, 12500, serverSSLProps);
    CloudDestinationFactory cloudDestinationFactory = new LatchBasedInMemoryCloudDestinationFactory(
        new LatchBasedInMemoryCloudDestination(Collections.emptyList(), mockClusterMap));
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    VcrServer vcrServer =
        new VcrServer(verifiableProperties, mockClusterAgentsFactory, notificationSystem, cloudDestinationFactory,
            null);
    vcrServer.startup();
    Assert.assertNotNull("Expected compactor", vcrServer.getVcrReplicationManager().getCloudStorageCompactor());
    vcrServer.shutdown();
    helixControllerManager.syncStop();
    zkInfo.shutdown();
  }

  /**
   * @return {@link VerifiableProperties} to start a VCR with a static cluster.
   */
  private VerifiableProperties getStaticClusterVcrProps() throws Exception {

    Properties serverSSLProps = new Properties();
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    TestSSLUtils.addSSLProperties(serverSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server");
    TestSSLUtils.addHttp2Properties(serverSSLProps, SSLFactory.Mode.SERVER, true);
    Properties props =
        VcrTestUtil.createVcrProperties("DC1", "vcrClusterName", "", 12300, 12400, 12500, serverSSLProps);
    props.setProperty(CloudConfig.VCR_ASSIGNED_PARTITIONS, "0,1");
    props.setProperty(CloudConfig.VCR_CLUSTER_AGENTS_FACTORY_CLASS, StaticVcrClusterAgentsFactory.class.getName());
    // Run this one with compaction disabled
    props.setProperty(CloudConfig.CLOUD_BLOB_COMPACTION_ENABLED, "false");
    props.setProperty(CloudConfig.CLOUD_DESTINATION_FACTORY_CLASS,
        "com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory");
    props.setProperty("clustermap.enable.http2.replication", "true");
    props.setProperty("server.security.service.factory", "com.github.ambry.cloud.AmbryVcrSecurityServiceFactory");
    return new VerifiableProperties(props);
  }
}
