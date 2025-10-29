/*
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;
import com.github.ambry.commons.InMemNamedBlobDbFactory;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.s3.S3BatchDeleteHandler;
import com.github.ambry.frontend.s3.S3MessagePayload;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobDbFactory;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.TestUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static com.github.ambry.frontend.s3.S3Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class S3BatchDeleteHandlerTest {
  private static final InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, true);
  private static final String CONTENT_TYPE = "text/plain";
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String KEY_NAME = "key-success";
  private static final String KEY_NAME_2 = "key-name-2";
  private static final String KEY_NAME_3 = "key-success-2";
  private final Account account;
  private final Container container;
  private FrontendConfig frontendConfig;
  private NamedBlobPutHandler namedBlobPutHandler;
  private S3BatchDeleteHandler s3BatchDeleteHandler;
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();


  public S3BatchDeleteHandlerTest() throws Exception {
    account = ACCOUNT_SERVICE.createAndAddRandomAccount();
    container = new ContainerBuilder().setName("container-a")
        .setId((short) 10)
        .setParentAccountId(account.getId())
        .setStatus(Container.ContainerStatus.ACTIVE)
        .setNamedBlobMode(Container.NamedBlobMode.OPTIONAL)
        .build();
    account.updateContainerMap(Collections.singletonList(container));
    setup();
    performPutOperation(KEY_NAME, CONTENT_TYPE, container, account);
    performPutOperation(KEY_NAME_2, CONTENT_TYPE, container, account);
    performPutOperation(KEY_NAME_3, KEY_NAME, container, account);
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  @Test
  public void deleteObjectTest() throws Exception {
    String uri = String.format("/s3/%s/%s", account.getName(), container.getName());
    // tests one correct delete and one error
    String xmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">" +
        "<Object>" +
        "<Key>key-success</Key>" +
        "</Object>" +
        "<Object>" +
        "<Key>key-error</Key>" +
        "</Object>" +
        "<Object>" +
        "<Key>key-error2</Key>" +
        "</Object>" +
        "<Object>" +
        "<Key>key-success-2</Key>" +
        "</Object>" +
        "</Delete>";

    byte[] xmlBytes = xmlBody.getBytes("UTF-8");
    RestRequest request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, uri, new JSONObject(), new LinkedList<>(Arrays.asList(ByteBuffer.wrap(xmlBytes), null)));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3BatchDeleteHandler.handle(request, restResponseChannel, futureResult::done);
    ReadableStreamChannel readableStreamChannel = futureResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    // verify correct XML via result value
    String result = new String(byteBuffer.array(), StandardCharsets.UTF_8);
    XmlMapper xmlMapper = new XmlMapper();
    S3MessagePayload.DeleteResult response =
        xmlMapper.readValue(byteBuffer.array(), S3MessagePayload.DeleteResult.class);
    assertEquals(response.getDeleted().get(0).getKey(), KEY_NAME);
    assertEquals(response.getErrors().get(0).toString(), new S3MessagePayload.S3ErrorObject("key-error","NotFound").toString());
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
  }

  @Test
  public void testBufferReleaseInDeserializeRequest() throws Exception {
    // Arrange
    S3BatchDeleteHandler handler = new S3BatchDeleteHandler(null, null);
    RetainingAsyncWritableChannel channel = mock(RetainingAsyncWritableChannel.class);
    ByteBuf mockByteBuf = mock(ByteBuf.class);

    String validXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">" +
        "<Object>" +
        "<Key>key-success</Key>" +
        "</Object>" +
        "<Object>" +
        "<Key>key-error</Key>" +
        "</Object>" +
        "<Object>" +
        "<Key>key-error2</Key>" +
        "</Object>" +
        "<Object>" +
        "<Key>key-success-2</Key>" +
        "</Object>" +
        "</Delete>";

    byte[] xmlBytes = validXml.getBytes();

    when(channel.consumeContentAsByteBuf()).thenReturn(mockByteBuf);
    when(mockByteBuf.readableBytes()).thenReturn(xmlBytes.length);
    doAnswer(invocation -> {
      byte[] buffer = invocation.getArgument(0);
      System.arraycopy(xmlBytes, 0, buffer, 0, xmlBytes.length);
      return null;
    }).when(mockByteBuf).readBytes(Mockito.any(byte[].class));

    S3MessagePayload.S3BatchDeleteObjects result = handler.deserializeRequest(channel);

    assertNotNull(result);
    verify(mockByteBuf, times(1)).release(); // Verify that release() was called
  }

  @Test
  public void testDeserializeRequestWithRealChannel() throws Exception {
    // Arrange
    String validXml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">"
            + "<Object>" + "<Key>key-success</Key>" + "</Object>" + "<Object>" + "<Key>key-error</Key>" + "</Object>"
            + "</Delete>";

    ByteBuf byteBuf = Unpooled.wrappedBuffer(validXml.getBytes());
    RetainingAsyncWritableChannel channel = new RetainingAsyncWritableChannel();
    channel.write(byteBuf, (result, exception) -> {
      if (exception != null) {
        fail("Failed to write to channel: " + exception.getMessage());
      }
    });

    S3BatchDeleteHandler handler = new S3BatchDeleteHandler(null, null);

    // Act
    S3MessagePayload.S3BatchDeleteObjects result = handler.deserializeRequest(channel);

    // Assert
    assertNotNull(result);
    assertEquals(2, result.getObjects().size());
    assertEquals("key-success", result.getObjects().get(0).getKey());
    assertEquals("key-error", result.getObjects().get(1).getKey());
  }


  @Test
  public void malformedXMLRequestTest() throws Exception {
    String uri = String.format("/s3/%s/%s", account.getName(), container.getName());
    // tests one correct delete and one error
    String xmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
    "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">" +
    "<Object>" +
    "<Key>key-success</Key>" +
    "</Object>" +
    "<Object>" +
    "<Key>key-error</Key>" +
    "</Object>" +
    "<Object>" +
    "<Key>key-error2</Key>" +
    "</Object>" +
    "<Object>" +
    "<Key>key-success-2</Key>" +
    "<Object>" +  // <-- Unclosed Object tag
    "</Delete>";

    byte[] xmlBytes = xmlBody.getBytes("UTF-8");
    RestRequest request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, uri, new JSONObject(), new LinkedList<>(Arrays.asList(ByteBuffer.wrap(xmlBytes), null)));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3BatchDeleteHandler.handle(request, restResponseChannel, futureResult::done);
    ReadableStreamChannel readableStreamChannel = futureResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    // verify correct XML via result value
    String result = new String(byteBuffer.array(), StandardCharsets.UTF_8);
    XmlMapper xmlMapper = new XmlMapper();
    S3MessagePayload.Error response =
        xmlMapper.readValue(byteBuffer.array(), S3MessagePayload.Error.class);
    assertEquals(response.getMessage(), ERR_MALFORMED_REQUEST_BODY_MESSAGE);
    assertEquals(response.getCode(), ERR_MALFORMED_REQUEST_BODY_CODE);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
  }

  @Test
  public void emptyRequestTest() throws Exception {
    String uri = String.format("/s3/%s/%s", account.getName(), container.getName());
    // tests one correct delete and one error
    String xmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">\n" + "</Delete>";

    byte[] xmlBytes = xmlBody.getBytes("UTF-8");
    RestRequest request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, uri, new JSONObject(), new LinkedList<>(Arrays.asList(ByteBuffer.wrap(xmlBytes), null)));
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3BatchDeleteHandler.handle(request, restResponseChannel, futureResult::done);
    ReadableStreamChannel readableStreamChannel = futureResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    // verify correct XML via result value
    String result = new String(byteBuffer.array(), StandardCharsets.UTF_8);
    XmlMapper xmlMapper = new XmlMapper();
    S3MessagePayload.Error response =
        xmlMapper.readValue(byteBuffer.array(), S3MessagePayload.Error.class);
    assertEquals(response.getMessage(), ERR_MALFORMED_REQUEST_BODY_MESSAGE);
    assertEquals(response.getCode(), ERR_MALFORMED_REQUEST_BODY_CODE);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
  }

  @Test
  public void exceedMaxBatchSizeTest() throws Exception {
    String uri = String.format("/s3/%s/%s", account.getName(), container.getName());
    String xmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">";

    // Add 1001 <Object> elements
    for (int i = 1; i <= 1001; i++) {
      xmlBody += "<Object>" +
          "<Key>key-" + i + "</Key>" +
          "</Object>";
    }
    xmlBody += "</Delete>";
    byte[] xmlBytes = xmlBody.getBytes("UTF-8");
    RestRequest request =
        FrontendRestRequestServiceTest.createRestRequest(RestMethod.POST, uri, new JSONObject(),
            new LinkedList<>(Arrays.asList(ByteBuffer.wrap(xmlBytes), null)));

    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<>();
    s3BatchDeleteHandler.handle(request, restResponseChannel, futureResult::done);
    ReadableStreamChannel readableStreamChannel = futureResult.get();
    ByteBuffer byteBuffer = ((ByteBufferReadableStreamChannel) readableStreamChannel).getContent();
    // Verify result value
    String result = new String(byteBuffer.array(), StandardCharsets.UTF_8);
    XmlMapper xmlMapper = new XmlMapper();
    S3MessagePayload.Error response =
        xmlMapper.readValue(byteBuffer.array(), S3MessagePayload.Error.class);
    assertEquals(response.getMessage(), ERR_MALFORMED_REQUEST_BODY_MESSAGE);
    assertEquals(response.getCode(), ERR_MALFORMED_REQUEST_BODY_CODE);
    assertEquals("Mismatch on status", ResponseStatus.Ok, restResponseChannel.getStatus());
  }

  private void setup() throws Exception {
    Properties properties = new Properties();
    CommonTestUtils.populateRequiredRouterProps(properties);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    frontendConfig = new FrontendConfig(verifiableProperties);
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry(), frontendConfig);
    AccountAndContainerInjector injector = new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, frontendConfig);
    IdSigningService idSigningService = new AmbryIdSigningService();
    AmbrySecurityServiceFactory securityServiceFactory =
        new AmbrySecurityServiceFactory(verifiableProperties, new MockClusterMap(), ACCOUNT_SERVICE, null,
            idSigningService, injector, QuotaTestUtils.createDummyQuotaManager());
    SecurityService securityService = securityServiceFactory.getSecurityService();
    NamedBlobDbFactory namedBlobDbFactory =
        new InMemNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), ACCOUNT_SERVICE);
    NamedBlobDb namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry(), idSigningService, namedBlobDb);
    InMemoryRouter router = new InMemoryRouter(verifiableProperties, new MockClusterMap(), ambryIdConverterFactory);
    namedBlobPutHandler =
        new NamedBlobPutHandler(securityService, idSigningService, router, injector, frontendConfig, metrics, CLUSTER_NAME,
            QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE, null);
    DeleteBlobHandler deleteBlobHandler =
        new DeleteBlobHandler(router, securityService, ambryIdConverterFactory.getIdConverter(), injector, metrics,
            new MockClusterMap(), QuotaTestUtils.createDummyQuotaManager(), ACCOUNT_SERVICE, null);
    s3BatchDeleteHandler = new S3BatchDeleteHandler(deleteBlobHandler, metrics);
  }

  private void performPutOperation(String keyName, String contentType, Container container, Account account) throws Exception {
    String requestPath = String.format("/named/%s/%s/%s", account.getName(), container.getName(), keyName);
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, TestUtils.TTL_SECS, container.isCacheable(),
        "test-app", contentType, "tester", null, null, null);

    byte[] content = TestUtils.getRandomBytes(1024);

    RestRequest request = FrontendRestRequestServiceTest.createRestRequest(RestMethod.PUT, requestPath, headers,
        new LinkedList<>(Arrays.asList(ByteBuffer.wrap(content), null)));

    request.setArg(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse(request, frontendConfig.pathPrefixesToRemove, CLUSTER_NAME));

    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> putResult = new FutureResult<>();
    namedBlobPutHandler.handle(request, restResponseChannel, putResult::done);
    putResult.get();
  }
}

