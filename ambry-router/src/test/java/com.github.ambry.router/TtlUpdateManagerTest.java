/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static com.github.ambry.router.RouterTestHelpers.*;


/**
 * Tests for {@link TtlUpdateManager}
 */
public class TtlUpdateManagerTest {
  private static final int DEFAULT_PARALLELISM = 3;
  private static final int DEFAULT_SUCCESS_TARGET = 2;
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;

  private final AtomicReference<MockSelectorState> mockSelectorState;
  private final MockClusterMap clusterMap;
  private final MockServerLayout serverLayout;
  private final NonBlockingRouter router;
  private final MockTime time = new MockTime();
  private final PartitionId partition;
  private final BlobId blobId;

  BlobProperties putBlobProperties;

  /**
   * Sets up all required components including a blob.
   * @throws IOException
   */
  public TtlUpdateManagerTest() throws IOException {
    mockSelectorState = new AtomicReference<>(MockSelectorState.Good);
    clusterMap = new MockClusterMap();
    serverLayout = new MockServerLayout(clusterMap);
    VerifiableProperties vProps =
        new VerifiableProperties(getNonBlockingRouterProperties(DEFAULT_SUCCESS_TARGET, DEFAULT_PARALLELISM));
    RouterConfig routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(clusterMap),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, time), new LoggingNotificationSystem(), clusterMap, null, null, null,
        new InMemAccountService(false, true), time, MockClusterMap.DEFAULT_PARTITION_CLASS);
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    partition = partitionIds.get(ThreadLocalRandom.current().nextInt(partitionIds.size()));
    blobId =
        new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), partition, false);
  }

  /**
   * Test the case how a {@link DeleteManager} acts when a router is closed, and when there are inflight
   * operations. Setting servers to not respond any requests, so {@link DeleteOperation} can be "in flight".
   */
  @Test
  public void routerClosedDuringOperationTest() throws Exception {
    serverLayout.getMockServers().forEach(mockServer -> mockServer.setShouldRespond(false));
    testWithErrorCodes(Collections.singletonMap(ServerErrorCode.No_Error, 9), serverLayout,
        RouterErrorCode.RouterClosed, expectedError -> {
          TestCallback<Void> callback = new TestCallback<>();
          Future<Void> future = router.updateBlobTtl(blobId.getID(), null, Utils.Infinite_Time, callback);
          router.close();
          assertFailureAndCheckErrorCode(future, callback, expectedError);
        });
  }

  /**
   * Generates {@link Properties} that includes initial configuration.
   *
   * @return Properties
   */
  private Properties getNonBlockingRouterProperties(int successTarget, int parallelism) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.ttl.update.success.target", Integer.toString(successTarget));
    properties.setProperty("router.ttl.update.request.parallelism", Integer.toString(parallelism));
    return properties;
  }
}
