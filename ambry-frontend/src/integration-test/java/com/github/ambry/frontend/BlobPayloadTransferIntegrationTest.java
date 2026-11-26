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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.codahale.metrics.MetricRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
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
import com.github.ambry.rest.BlobPayloadTransferListener.Direction;
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
  private final BlobPayloadTransferListener listener = mock(BlobPayloadTransferListener.class);
  private final InetAddress loopback = InetAddress.getLoopbackAddress();
  private MockClusterMap clusterMap;
  private RestServer server;
  private NettyClient client;

  @Before
  public void setUp() throws Exception {
    clusterMap = new MockClusterMap();
    int port;
    try (ServerSocket socket = new ServerSocket(0, 1, loopback)) {
      port = socket.getLocalPort();
    }
    factoryListener = listener;
    try {
      server = new RestServer(serverProperties(port), clusterMap, new LoggingNotificationSystem(), null);
    } finally {
      factoryListener = null;
    }
    server.start();
    client = new NettyClient(loopback.getHostAddress(), port, null);
  }

  @After
  public void tearDown() {
    try {
      shutdownServer();
    } finally {
      if (clusterMap != null) {
        clusterMap.close();
      }
    }
  }

  @Test
  public void testPostNotifiesConfiguredListener() throws Exception {
    CountDownLatch uploadCompleted = watchTransfer(Direction.UPLOAD);
    byte[] payload = {1, 2, 3, 4};
    postBlob(payload);
    assertTrue("POST returned 201, but the transfer listener was not notified",
        uploadCompleted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    shutdownServer();
    verify(listener).onTransferComplete(Direction.UPLOAD, ACCOUNT, CONTAINER, payload.length);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testRangeGetNotifiesConfiguredListenerForReturnedBytes() throws Exception {
    CountDownLatch uploadCompleted = watchTransfer(Direction.UPLOAD);
    CountDownLatch downloadCompleted = watchTransfer(Direction.DOWNLOAD);
    byte[] payload = {1, 2, 3, 4};
    byte[] expected = {2, 3};
    String blobId = postBlob(payload);
    assertTrue("POST returned 201, but the transfer listener was not notified",
        uploadCompleted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, blobId);
    request.headers().set(HttpHeaderNames.RANGE, "bytes=1-2");
    NettyClient.ResponseParts response = sendRequest(request);
    try {
      HttpResponse headers = responseHeaders(response);
      assertEquals(HttpResponseStatus.PARTIAL_CONTENT, headers.status());
      assertEquals("bytes 1-2/4", headers.headers().get(HttpHeaderNames.CONTENT_RANGE));
      assertEquals(expected.length, HttpUtil.getContentLength(headers));
      assertResponseBody(response, expected);
    } finally {
      response.queue.forEach(ReferenceCountUtil::release);
    }
    assertTrue("Range GET returned 206, but the transfer listener was not notified",
        downloadCompleted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    shutdownServer();
    verify(listener).onTransferComplete(Direction.UPLOAD, ACCOUNT, CONTAINER, payload.length);
    verify(listener).onTransferComplete(Direction.DOWNLOAD, ACCOUNT, CONTAINER, expected.length);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testFullChunkedGetNotifiesConfiguredListener() throws Exception {
    CountDownLatch uploadCompleted = watchTransfer(Direction.UPLOAD);
    CountDownLatch downloadCompleted = watchTransfer(Direction.DOWNLOAD);
    int chunkedPayloadSize = 20_000;
    byte[] payload = TestUtils.getRandomBytes(chunkedPayloadSize);
    String blobId = postBlob(payload);
    assertTrue("POST returned 201, but the transfer listener was not notified",
        uploadCompleted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, blobId);
    NettyClient.ResponseParts response = sendRequest(request);
    try {
      HttpResponse headers = responseHeaders(response);
      assertEquals(HttpResponseStatus.OK, headers.status());
      assertTrue("Expected chunked transfer encoding", HttpUtil.isTransferEncodingChunked(headers));
      assertFalse("Chunked response must not have Content-Length", HttpUtil.isContentLengthSet(headers));
      assertResponseBody(response, payload);
    } finally {
      response.queue.forEach(ReferenceCountUtil::release);
    }
    assertTrue("Full GET returned 200, but the transfer listener was not notified",
        downloadCompleted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    shutdownServer();
    verify(listener).onTransferComplete(Direction.UPLOAD, ACCOUNT, CONTAINER, payload.length);
    verify(listener).onTransferComplete(Direction.DOWNLOAD, ACCOUNT, CONTAINER, payload.length);
    verifyNoMoreInteractions(listener);
  }

  private static void assertResponseBody(NettyClient.ResponseParts response, byte[] expected) {
    byte[] actual = new byte[expected.length];
    int offset = 0;
    for (HttpObject part : response.queue) {
      if (part instanceof HttpContent) {
        ByteBuf content = ((HttpContent) part).content();
        int length = content.readableBytes();
        assertTrue("Response body exceeds the expected length", length <= actual.length - offset);
        content.getBytes(content.readerIndex(), actual, offset, length);
        offset += length;
      }
    }
    assertEquals("Unexpected response body length", expected.length, offset);
    assertArrayEquals(expected, actual);
  }

  private CountDownLatch watchTransfer(Direction direction) {
    CountDownLatch transferCompleted = new CountDownLatch(1);
    doAnswer(invocation -> {
      transferCompleted.countDown();
      return null;
    }).when(listener).onTransferComplete(eq(direction), any(), any(), anyLong());
    return transferCompleted;
  }

  private String postBlob(byte[] payload) throws Exception {
    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", Unpooled.wrappedBuffer(payload));
    request.headers().set(RestUtils.Headers.SERVICE_ID, "blob-transfer-test");
    request.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
    request.headers().set(RestUtils.Headers.TARGET_ACCOUNT_NAME, ACCOUNT.getName());
    request.headers().set(RestUtils.Headers.TARGET_CONTAINER_NAME, CONTAINER.getName());
    HttpUtil.setContentLength(request, payload.length);
    NettyClient.ResponseParts response = sendRequest(request);
    try {
      HttpResponse headers = responseHeaders(response);
      assertEquals(HttpResponseStatus.CREATED, headers.status());
      String blobId = headers.headers().get(HttpHeaderNames.LOCATION);
      assertNotNull("Expected a generated blob ID", blobId);
      return blobId;
    } finally {
      response.queue.forEach(ReferenceCountUtil::release);
    }
  }

  private NettyClient.ResponseParts sendRequest(FullHttpRequest request) throws Exception {
    try {
      return client.sendRequest(request, null, null).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      client.close();
      throw e;
    } finally {
      if (request.refCnt() > 0) {
        request.release();
      }
    }
  }

  private static HttpResponse responseHeaders(NettyClient.ResponseParts response) {
    HttpObject firstPart = response.queue.peek();
    assertTrue("Expected an HTTP response", firstPart instanceof HttpResponse);
    return (HttpResponse) firstPart;
  }

  private void shutdownServer() {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      if (server != null) {
        RestServer startedServer = server;
        server = null;
        startedServer.shutdown();
      }
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
