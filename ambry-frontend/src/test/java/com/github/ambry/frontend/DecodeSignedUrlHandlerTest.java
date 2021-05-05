/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.NettyMetrics;
import com.github.ambry.rest.NettyRequest;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link DecodeSignedUrlHandler}.
 */
public class DecodeSignedUrlHandlerTest {
  private static final String ENDPOINT = "https://example.com";
  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();
  private static final Account REF_ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount();
  private static final Container REF_CONTAINER = REF_ACCOUNT.getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID);
  private static final ClusterMap CLUSTER_MAP;
  private final BlobId testBlobId;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private final FrontendTestSecurityServiceFactory securityServiceFactory = new FrontendTestSecurityServiceFactory();
  private final UrlSigningService urlSigningService;
  private final FrontendTestIdConverterFactory idConverterFactory = new FrontendTestIdConverterFactory();
  private final GetSignedUrlHandler getSignedUrlHandler;
  private final DecodeSignedUrlHandler handler;
  private final Time time = new MockTime();

  public DecodeSignedUrlHandlerTest() {
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
    FrontendConfig config = new FrontendConfig(new VerifiableProperties(new Properties()));
    AccountAndContainerInjector accountAndContainerInjector =
        new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, config);
    urlSigningService =
        new AmbryUrlSigningService(ENDPOINT, ENDPOINT, 60 * 60 * 24, 1024 * 1024, 60 * 60 * 24 * 30, 60 * 60 * 24,
            4 * 1024 * 1024, time);
    getSignedUrlHandler = new GetSignedUrlHandler(urlSigningService, securityServiceFactory.getSecurityService(),
        idConverterFactory.getIdConverter(), accountAndContainerInjector, metrics, CLUSTER_MAP);
    testBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, REF_ACCOUNT.getId(), REF_CONTAINER.getId(),
        CLUSTER_MAP.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    handler = new DecodeSignedUrlHandler(securityServiceFactory.getSecurityService(), metrics);
  }

  @Test
  public void handlePostSignedUrlTest() throws Exception {
    RestRequest restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    restRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse("/signedUrl", Collections.emptyMap(), Collections.emptyList(), "Ambry-test"));
    restRequest.setArg(RestUtils.Headers.URL_TYPE, RestMethod.POST.name());
    restRequest.setArg(RestUtils.Headers.URL_TTL, String.valueOf(60 * 60 * 24));
    restRequest.setArg(RestUtils.Headers.CHUNK_UPLOAD, String.valueOf(true));
    restRequest.setArg(RestUtils.Headers.TARGET_ACCOUNT_NAME, REF_ACCOUNT.getName());
    restRequest.setArg(RestUtils.Headers.TARGET_CONTAINER_NAME, REF_CONTAINER.getName());
    restRequest.setArg(RestUtils.Headers.AMBRY_CONTENT_TYPE, "text/plain");
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> future = new FutureResult<>();
    getSignedUrlHandler.handle(restRequest, restResponseChannel, future::done);
    future.get(1, TimeUnit.SECONDS);
    String signedUrl = (String) restResponseChannel.getHeader(RestUtils.Headers.SIGNED_URL);

    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, signedUrl);
    Channel channel = new EmbeddedChannel();
    NettyRequest nettyRequest =
        new NettyRequest(httpRequest, channel, new NettyMetrics(new MetricRegistry()), Collections.EMPTY_SET);
    restResponseChannel = new MockRestResponseChannel();
    future = new FutureResult<>();
    handler.handle(nettyRequest, restResponseChannel, future::done);
    future.get(1, TimeUnit.SECONDS);
    Assert.assertEquals(restResponseChannel.getHeader(RestUtils.Headers.URL_TYPE), RestMethod.POST.name());
    // AmbryUrlSigningService.Link_EXPIRY_TIME
    Assert.assertEquals(restResponseChannel.getHeader("et"), String.valueOf(time.seconds() + 60 * 60 * 24));
    Assert.assertEquals(restResponseChannel.getHeader(RestUtils.Headers.CHUNK_UPLOAD), String.valueOf(true));
    Assert.assertEquals(restResponseChannel.getHeader(RestUtils.Headers.MAX_UPLOAD_SIZE),
        String.valueOf(4 * 1024 * 1024));
    Assert.assertEquals(restResponseChannel.getHeader(RestUtils.Headers.TTL), String.valueOf(60 * 60 * 24));
    Assert.assertEquals(restResponseChannel.getHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME), REF_ACCOUNT.getName());
    Assert.assertEquals(restResponseChannel.getHeader(RestUtils.Headers.TARGET_CONTAINER_NAME),
        REF_CONTAINER.getName());
    Assert.assertEquals(restResponseChannel.getHeader(RestUtils.Headers.AMBRY_CONTENT_TYPE), "text/plain");
  }

  @Test
  public void handleGetSignedUrlTest() throws Exception {
    RestRequest restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    restRequest.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse("/signedUrl", Collections.emptyMap(), Collections.emptyList(), "Ambry-test"));
    restRequest.setArg(RestUtils.Headers.URL_TYPE, RestMethod.GET.name());
    restRequest.setArg(RestUtils.Headers.URL_TTL, String.valueOf(60 * 60 * 24));
    restRequest.setArg(RestUtils.Headers.BLOB_ID, testBlobId.getID());
    idConverterFactory.translation = testBlobId.getID();
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<ReadableStreamChannel> future = new FutureResult<>();
    getSignedUrlHandler.handle(restRequest, restResponseChannel, future::done);
    future.get(1, TimeUnit.SECONDS);
    String signedUrl = (String) restResponseChannel.getHeader(RestUtils.Headers.SIGNED_URL);

    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, signedUrl);
    Channel channel = new EmbeddedChannel();
    NettyRequest nettyRequest =
        new NettyRequest(httpRequest, channel, new NettyMetrics(new MetricRegistry()), Collections.EMPTY_SET);
    restResponseChannel = new MockRestResponseChannel();
    future = new FutureResult<>();
    handler.handle(nettyRequest, restResponseChannel, future::done);
    future.get(1, TimeUnit.SECONDS);
    Assert.assertEquals(restResponseChannel.getHeader(RestUtils.Headers.URL_TYPE), RestMethod.GET.name());
    // AmbryUrlSigningService.Link_EXPIRY_TIME
    Assert.assertEquals(restResponseChannel.getHeader("et"), String.valueOf(time.seconds() + 60 * 60 * 24));
    Assert.assertEquals(restResponseChannel.getHeader(RestUtils.Headers.BLOB_ID), testBlobId.getID());
  }
}
