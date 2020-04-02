/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.CompositeNetworkClientFactory;
import com.github.ambry.network.LocalNetworkClientFactory;
import com.github.ambry.network.LocalRequestResponseChannel;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.utils.Utils;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


/**
 * Class to test the {@link NonBlockingRouter} returned by {@link CloudRouterFactory}.
 */
@RunWith(Parameterized.class)
public class CloudRouterTest extends NonBlockingRouterTest {

  /**
   * Running for both regular and encrypted blobs, and versions 2 and 3 of MetadataContent
   * @return an array with all four different choices
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false, MessageFormatRecord.Metadata_Content_Version_V2},
        {false, MessageFormatRecord.Metadata_Content_Version_V3},
        {true, MessageFormatRecord.Metadata_Content_Version_V2},
        {true, MessageFormatRecord.Metadata_Content_Version_V3}});
  }

  /**
   * Initialize parameters common to all tests.
   * @param testEncryption {@code true} to test with encryption enabled. {@code false} otherwise
   * @throws Exception
   */
  public CloudRouterTest(boolean testEncryption, int metadataContentVersion) throws Exception {
    super(testEncryption, metadataContentVersion, true);
  }

  @After
  public void after() {
    // Any single test failure should not impact others.
    if (router != null && router.isOpen()) {
      router.close();
    }
    Assert.assertEquals("Current operations count should be 0", 0, NonBlockingRouter.currentOperationsCount.get());
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link NonBlockingRouter}.
   * @return the created VerifiableProperties instance.
   */
  @Override
  protected Properties getNonBlockingRouterProperties(String routerDataCenter) {
    Properties properties = super.getNonBlockingRouterProperties(routerDataCenter);
    properties.setProperty("clustermap.port", "1666");
    properties.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    properties.setProperty("clustermap.resolve.hostnames", "false");
    properties.setProperty(CloudConfig.CLOUD_DESTINATION_FACTORY_CLASS,
        LatchBasedInMemoryCloudDestinationFactory.class.getName());
    properties.setProperty(CloudConfig.VCR_MIN_TTL_DAYS, "0");
    return properties;
  }

  /**
   * Construct {@link Properties} and {@link MockServerLayout} and initialize and set the
   * router with them.
   */
  @Override
  protected void setRouter() throws Exception {
    setRouter(getNonBlockingRouterProperties("DC1"), new MockServerLayout(mockClusterMap),
        new LoggingNotificationSystem());
  }

  /**
   * Initialize and set the router with the given {@link Properties} and {@link MockServerLayout}
   * @param props the {@link Properties}
   * @param notificationSystem the {@link NotificationSystem} to use.
   */
  @Override
  protected void setRouter(Properties props, MockServerLayout mockServerLayout, NotificationSystem notificationSystem)
      throws Exception {
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
    CloudDestinationFactory cloudDestinationFactory =
        Utils.getObj(cloudConfig.cloudDestinationFactoryClass, verifiableProperties,
            mockClusterMap.getMetricRegistry());
    CloudDestination cloudDestination = cloudDestinationFactory.getCloudDestination();
    RequestHandlerPool requestHandlerPool =
        CloudRouterFactory.getRequestHandlerPool(verifiableProperties, mockClusterMap, cloudDestination, cloudConfig);

    Map<ReplicaType, NetworkClientFactory> childFactories = new EnumMap<>(ReplicaType.class);
    childFactories.put(ReplicaType.CLOUD_BACKED,
        new LocalNetworkClientFactory((LocalRequestResponseChannel) requestHandlerPool.getChannel(),
            new NetworkConfig(verifiableProperties), new NetworkMetrics(routerMetrics.getMetricRegistry()), mockTime));
    childFactories.put(ReplicaType.DISK_BACKED,
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime));
    NetworkClientFactory networkClientFactory = new CompositeNetworkClientFactory(childFactories);

    router =
        new NonBlockingRouter(routerConfig, routerMetrics, networkClientFactory, notificationSystem, mockClusterMap,
            kms, cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS);
    router.addResourceToClose(requestHandlerPool);
  }

  /**
   * Test the {@link CloudRouterFactory}
   */
  @Test
  public void testCloudRouterFactory() throws Exception {
    Properties props = getNonBlockingRouterProperties("NotInClusterMap");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    try {
      router = (NonBlockingRouter) new CloudRouterFactory(verifiableProperties, mockClusterMap,
          new LoggingNotificationSystem(), null, accountService).getRouter();
      Assert.fail("NonBlockingRouterFactory instantiation should have failed because the router datacenter is not in "
          + "the cluster map");
    } catch (IllegalStateException e) {
    }
    props = getNonBlockingRouterProperties("DC1");
    verifiableProperties = new VerifiableProperties((props));
    router = (NonBlockingRouter) new CloudRouterFactory(verifiableProperties, mockClusterMap,
        new LoggingNotificationSystem(), null, accountService).getRouter();
    assertExpectedThreadCounts(2, 1);
    router.close();
    assertExpectedThreadCounts(0, 0);
  }

  /**
   * Test Router with a single scaling unit.
   */
  @Test
  public void testRouterBasic() throws Exception {
    super.testRouterBasic();
  }

  /**
   * Test behavior with various null inputs to router methods.
   * @throws Exception
   */
  @Test
  public void testNullArguments() throws Exception {
    super.testNullArguments();
  }

  /**
   * Test router put operation in a scenario where there are no partitions available.
   */
  @Test
  public void testRouterPartitionsUnavailable() throws Exception {
    super.testRouterPartitionsUnavailable();
  }

  /**
   * Test to ensure that for simple blob deletions, no additional background delete operations
   * are initiated.
   */
  @Test
  public void testSimpleBlobDelete() throws Exception {
    super.testSimpleBlobDelete();
  }

  /**
   * Tests basic TTL update for simple (one chunk) blobs
   * @throws Exception
   */
  @Test
  public void testSimpleBlobTtlUpdate() throws Exception {
    doTtlUpdateTest(1);
  }

  /**
   * Tests basic TTL update for composite (multiple chunk) blobs
   * @throws Exception
   */
  @Test
  public void testCompositeBlobTtlUpdate() throws Exception {
    doTtlUpdateTest(4);
  }

  /**
   * Test that stitched blobs are usable by the other router methods.
   * @throws Exception
   */
  @Test
  public void testStitchGetUpdateDelete() throws Exception {
    super.testStitchGetUpdateDelete();
  }

  /**
   * Test that multiple scaling units can be instantiated, exercised and closed.
   */
  @Test
  public void testMultipleScalingUnit() throws Exception {
    super.testMultipleScalingUnit();
  }

  // Remaining tests not applicable to cloud router
  @Override
  @Ignore
  public void testNonBlockingRouterFactory() throws Exception {
  }

  @Override
  @Ignore
  public void testRouterNoPartitionInLocalDC() throws Exception {
  }

  @Override
  @Ignore
  public void testRequestResponseHandlerThreadExitFlow() throws Exception {
  }

  @Override
  @Ignore
  public void testUnsuccessfulPutDataChunkDelete() throws Exception {
  }

  @Override
  @Ignore
  public void testCompositeBlobDataChunksDelete() throws Exception {
  }

  @Override
  @Ignore
  public void testBlobTtlUpdateErrors() throws Exception {
  }

  @Override
  @Ignore
  public void testBadCallbackForUpdateTtl() throws Exception {
  }

  @Override
  @Ignore
  public void testWarmUpConnectionFailureHandling() throws Exception {
  }

  @Override
  @Ignore
  public void testResponseHandling() throws Exception {
  }
}
