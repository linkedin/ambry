/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.frontend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.junit.Test;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.AccountServiceFactory;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.BlobPayloadTransferListener;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.InMemoryRouterFactory;
import com.github.ambry.router.Router;
import com.github.ambry.utils.TestUtils;


public class BlobPayloadTransferIntegrationTest {
  private static final int TIMEOUT_SECONDS = 10;
  private static final short ACCOUNT_ID = 101;
  private static final Container CONTAINER =
      new ContainerBuilder((short) 2, "transfer-container", Container.ContainerStatus.ACTIVE, "", ACCOUNT_ID).build();
  private static final Account ACCOUNT = new AccountBuilder(ACCOUNT_ID, "transfer-account", Account.AccountStatus.ACTIVE)
      .addOrUpdateContainer(CONTAINER)
      .build();
  private static volatile BlobPayloadTransferListener factoryListener;

  @Test
  public void testPostNotifiesConfiguredListener() throws Exception {
    BlobPayloadTransferListener listener = mock(BlobPayloadTransferListener.class);
    CountDownLatch transferCompleted = new CountDownLatch(1);
    doAnswer(invocation -> {
      transferCompleted.countDown();
      return null;
    }).when(listener).onTransferComplete(any(), any(), any(), anyLong());
    factoryListener = listener;

    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port;
    byte[] payload = {1, 2, 3, 4};
    try (MockClusterMap clusterMap = new MockClusterMap()) {
      try (ServerSocket socket = new ServerSocket(0, 1, loopback)) {
        port = socket.getLocalPort();
      }
      RestServer server = new RestServer(serverProperties(port), clusterMap, new LoggingNotificationSystem(), null);
      try {
        server.start();
        FullHttpRequest request =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", Unpooled.wrappedBuffer(payload));
        try (NettyClient client = new NettyClient(loopback.getHostAddress(), port, null)) {
          request.headers().set(RestUtils.Headers.SERVICE_ID, "blob-transfer-test");
          request.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
          request.headers().set(RestUtils.Headers.TARGET_ACCOUNT_NAME, ACCOUNT.getName());
          request.headers().set(RestUtils.Headers.TARGET_CONTAINER_NAME, CONTAINER.getName());
          HttpUtil.setContentLength(request, payload.length);
          NettyClient.ResponseParts response =
              client.sendRequest(request, null, null).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
          try {
            HttpObject firstPart = response.queue.peek();
            assertTrue("Expected an HTTP response", firstPart instanceof HttpResponse);
            HttpResponse headers = (HttpResponse) firstPart;
            assertEquals(HttpResponseStatus.CREATED, headers.status());
            assertNotNull("Expected a generated blob ID", headers.headers().get(HttpHeaderNames.LOCATION));
          } finally {
            response.queue.forEach(ReferenceCountUtil::release);
          }
          assertTrue("POST returned 201, but the transfer listener was not notified",
              transferCompleted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
          if (request.refCnt() > 0) {
            request.release();
          }
        }
      } finally {
        server.shutdown();
      }
      verify(listener).onTransferComplete(BlobPayloadTransferListener.Direction.UPLOAD, ACCOUNT, CONTAINER,
          payload.length);
      verifyNoMoreInteractions(listener);
    } finally {
      factoryListener = null;
    }
  }

  private static VerifiableProperties serverProperties(int port) {
    Properties properties = new Properties();
    properties.setProperty("rest.server.rest.request.service.factory", ListeningFrontendFactory.class.getName());
    properties.setProperty("rest.server.router.factory", InMemoryRouterFactory.class.getName());
    properties.setProperty("rest.server.account.service.factory", TransferAccountServiceFactory.class.getName());
    properties.setProperty(NettyConfig.NETTY_SERVER_PORT, Integer.toString(port));
    properties.setProperty("clustermap.cluster.name", "test-cluster");
    properties.setProperty("clustermap.datacenter.name", "localDc");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    CommonTestUtils.populateRequiredRouterProps(properties);
    return new VerifiableProperties(properties);
  }

  public static class ListeningFrontendFactory extends FrontendRestRequestServiceFactory {
    public ListeningFrontendFactory(VerifiableProperties properties, ClusterMap clusterMap, Router router,
        AccountService accountService) {
      super(properties, clusterMap, router, accountService, factoryListener);
    }
  }

  public static class TransferAccountServiceFactory implements AccountServiceFactory {
    private final InMemAccountService accountService = new InMemAccountService(false, false);

    public TransferAccountServiceFactory(VerifiableProperties properties, MetricRegistry registry)
        throws AccountServiceException {
      accountService.updateAccounts(Collections.singletonList(ACCOUNT));
    }

    @Override
    public AccountService getAccountService() {
      return accountService;
    }
  }
}
