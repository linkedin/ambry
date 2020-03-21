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
package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit tests for {@link PublicAccessLogHandler}
 */
public class PublicAccessLogHandlerTest {
  private MockPublicAccessLogger publicAccessLogger;
  private static final String REQUEST_HEADERS =
      HttpHeaderNames.HOST + "," + HttpHeaderNames.CONTENT_LENGTH + ",x-ambry-content-type";
  private static final String RESPONSE_HEADERS =
      EchoMethodHandler.RESPONSE_HEADER_KEY_1 + "," + EchoMethodHandler.RESPONSE_HEADER_KEY_2;
  private static final String NOT_LOGGED_HEADER_KEY = "headerKey";
  private static final X509Certificate PEER_CERT;
  private static final SslContext SSL_CONTEXT;

  static {
    try {
      PEER_CERT = new SelfSignedCertificate().cert();
      SelfSignedCertificate localCert = new SelfSignedCertificate();
      SSL_CONTEXT = SslContextBuilder.forServer(localCert.certificate(), localCert.privateKey())
          .clientAuth(ClientAuth.REQUIRE)
          .build();
    } catch (CertificateException | SSLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Sets up the mock public access logger that {@link PublicAccessLogHandler} can use.
   */
  public PublicAccessLogHandlerTest() {
    publicAccessLogger = new MockPublicAccessLogger(REQUEST_HEADERS.split(","), RESPONSE_HEADERS.split(","));
  }

  /**
   * Tests for the common case request handling flow.
   * @throws Exception
   */
  @Test
  public void requestHandleWithGoodInputTest() throws Exception {
    doRequestHandleTest(HttpMethod.POST, "POST", false, false);
    doRequestHandleTest(HttpMethod.GET, "GET", false, false);
    doRequestHandleTest(HttpMethod.DELETE, "DELETE", false, false);
    // SSL enabled
    doRequestHandleTest(HttpMethod.POST, "POST", false, true);
    doRequestHandleTest(HttpMethod.GET, "GET", false, true);
    doRequestHandleTest(HttpMethod.DELETE, "DELETE", false, true);
  }

  /**
   * Tests for multiple requests with keep alive.
   * @throws Exception
   */
  @Test
  public void requestHandleWithGoodInputTestWithKeepAlive() throws Exception {
    doRequestHandleWithKeepAliveTest(HttpMethod.POST, "POST", false);
    doRequestHandleWithKeepAliveTest(HttpMethod.GET, "GET", false);
    doRequestHandleWithKeepAliveTest(HttpMethod.DELETE, "DELETE", false);
    // SSL enabled
    doRequestHandleWithKeepAliveTest(HttpMethod.POST, "POST", true);
    doRequestHandleWithKeepAliveTest(HttpMethod.GET, "GET", true);
    doRequestHandleWithKeepAliveTest(HttpMethod.DELETE, "DELETE", true);
  }

  /**
   * Tests two successive request without completing first request
   * @throws Exception
   */
  @Test
  public void requestHandleWithTwoSuccessiveRequest() throws Exception {
    doRequestHandleWithMultipleRequest(HttpMethod.POST, "POST", false);
    doRequestHandleWithMultipleRequest(HttpMethod.GET, "GET", false);
    doRequestHandleWithMultipleRequest(HttpMethod.DELETE, "DELETE", false);
    // SSL enabled
    doRequestHandleWithMultipleRequest(HttpMethod.POST, "POST", true);
    doRequestHandleWithMultipleRequest(HttpMethod.GET, "GET", true);
    doRequestHandleWithMultipleRequest(HttpMethod.DELETE, "DELETE", true);
  }

  /**
   * Tests for the request handling flow for close
   * @throws Exception
   */
  @Test
  public void requestHandleOnCloseTest() throws Exception {
    doRequestHandleTest(HttpMethod.POST, EchoMethodHandler.CLOSE_URI, true, false);
    doRequestHandleTest(HttpMethod.GET, EchoMethodHandler.CLOSE_URI, true, false);
    doRequestHandleTest(HttpMethod.DELETE, EchoMethodHandler.CLOSE_URI, true, false);
    // SSL enabled
    doRequestHandleTest(HttpMethod.POST, EchoMethodHandler.CLOSE_URI, true, true);
    doRequestHandleTest(HttpMethod.GET, EchoMethodHandler.CLOSE_URI, true, true);
    doRequestHandleTest(HttpMethod.DELETE, EchoMethodHandler.CLOSE_URI, true, true);
  }

  /**
   * Tests for the request handling flow on disconnect
   * @throws Exception
   */
  @Test
  public void requestHandleOnDisconnectTest() throws Exception {
    // disonnecting the embedded channel, calls close of PubliAccessLogRequestHandler
    doRequestHandleTest(HttpMethod.POST, EchoMethodHandler.DISCONNECT_URI, true, false);
    doRequestHandleTest(HttpMethod.GET, EchoMethodHandler.DISCONNECT_URI, true, false);
    doRequestHandleTest(HttpMethod.DELETE, EchoMethodHandler.DISCONNECT_URI, true, false);
    // SSL enabled
    doRequestHandleTest(HttpMethod.POST, EchoMethodHandler.DISCONNECT_URI, true, true);
    doRequestHandleTest(HttpMethod.GET, EchoMethodHandler.DISCONNECT_URI, true, true);
    doRequestHandleTest(HttpMethod.DELETE, EchoMethodHandler.DISCONNECT_URI, true, true);
  }

  /**
   * Tests for the request handling flow with transfer encoding chunked
   */
  @Test
  public void requestHandleWithChunkedResponse() throws Exception {
    doRequestHandleWithChunkedResponse(false);
    // SSL enabled
    doRequestHandleWithChunkedResponse(true);
  }

  // requestHandleTest() helpers

  /**
   * Does a test to see that request handling results in expected entries in public access log
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param uri Uri to be used during the request
   * @param testErrorCase true if error case has to be tested, false otherwise
   * @param useSSL {@code true} to test SSL logging.
   * @throws Exception
   */
  private void doRequestHandleTest(HttpMethod httpMethod, String uri, boolean testErrorCase, boolean useSSL)
      throws Exception {
    EmbeddedChannel channel = createChannel(useSSL);
    List<HttpHeaders> httpHeadersList = getHeadersList();
    for (HttpHeaders headers : httpHeadersList) {
      HttpRequest request = RestTestUtils.createRequest(httpMethod, uri, headers);
      HttpUtil.setKeepAlive(request, true);
      sendRequestCheckResponse(channel, request, uri, headers, testErrorCase, false, useSSL);
      if (!testErrorCase) {
        Assert.assertTrue("Channel should not be closed ", channel.isOpen());
      } else {
        Assert.assertFalse("Channel should have been closed ", channel.isOpen());
        channel = createChannel(useSSL);
      }
    }
    channel.close();
  }

  /**
   * Does a test to see that two consecutive request handling results in expected entries in public access log
   * with keep alive
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param uri Uri to be used during the request
   * @param useSSL {@code true} to test SSL logging.
   * @throws Exception
   */
  private void doRequestHandleWithKeepAliveTest(HttpMethod httpMethod, String uri, boolean useSSL) throws Exception {
    EmbeddedChannel channel = createChannel(useSSL);
    // contains one logged request header
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaderNames.CONTENT_LENGTH, new Random().nextLong());

    HttpRequest request = RestTestUtils.createRequest(httpMethod, uri, headers);
    HttpUtil.setKeepAlive(request, true);
    sendRequestCheckResponse(channel, request, uri, headers, false, false, useSSL);
    Assert.assertTrue("Channel should not be closed ", channel.isOpen());

    // contains one logged and not logged header
    headers = new DefaultHttpHeaders();
    headers.add(NOT_LOGGED_HEADER_KEY + "1", "headerValue1");
    headers.add(HttpHeaderNames.CONTENT_LENGTH, new Random().nextLong());

    request = RestTestUtils.createRequest(httpMethod, uri, headers);
    HttpUtil.setKeepAlive(request, true);
    sendRequestCheckResponse(channel, request, uri, headers, false, false, useSSL);
    Assert.assertTrue("Channel should not be closed ", channel.isOpen());
    channel.close();
  }

  /**
   * Does a test to see that two consecutive requests without sending last http content for first request fails
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param uri Uri to be used during the request
   * @param useSSL {@code true} to test SSL logging.
   * @throws Exception
   */
  private void doRequestHandleWithMultipleRequest(HttpMethod httpMethod, String uri, boolean useSSL) throws Exception {
    EmbeddedChannel channel = createChannel(useSSL);
    // contains one logged request header
    HttpHeaders headers1 = new DefaultHttpHeaders();
    headers1.add(HttpHeaderNames.CONTENT_LENGTH, new Random().nextLong());

    HttpRequest request = RestTestUtils.createRequest(httpMethod, uri, headers1);
    HttpUtil.setKeepAlive(request, true);
    channel.writeInbound(request);

    // contains one logged and not logged header
    HttpHeaders headers2 = new DefaultHttpHeaders();
    headers2.add(NOT_LOGGED_HEADER_KEY + "1", "headerValue1");
    headers2.add(HttpHeaderNames.CONTENT_LENGTH, new Random().nextLong());
    // sending another request w/o sending last http content
    request = RestTestUtils.createRequest(httpMethod, uri, headers2);
    HttpUtil.setKeepAlive(request, true);
    sendRequestCheckResponse(channel, request, uri, headers2, false, false, useSSL);
    Assert.assertTrue("Channel should not be closed ", channel.isOpen());

    // verify that headers from first request is not found in public access log
    String lastLogEntry = publicAccessLogger.getLastPublicAccessLogEntry();
    // verify request headers
    verifyPublicAccessLogEntryForRequestHeaders(lastLogEntry, headers1, request.method(), false);

    channel.close();
  }

  /**
   * Sends the provided {@code httpRequest} and verifies that the response is as expected.
   * @param channel the {@link EmbeddedChannel} to send the request over.
   * @param httpRequest the {@link HttpRequest} that has to be sent
   * @param uri, Uri to be used for the request
   * @param headers {@link HttpHeaders} that is set in the request to be used for verification purposes
   * @param testErrorCase true if error case has to be tested, false otherwise
   * @param sslUsed true if SSL was used for this request.
   */
  private void sendRequestCheckResponse(EmbeddedChannel channel, HttpRequest httpRequest, String uri,
      HttpHeaders headers, boolean testErrorCase, boolean chunkedResponse, boolean sslUsed) throws Exception {
    channel.writeInbound(httpRequest);
    if (uri.equals(EchoMethodHandler.DISCONNECT_URI)) {
      channel.disconnect();
    } else {
      channel.writeInbound(new DefaultLastHttpContent());
    }
    String lastLogEntry = publicAccessLogger.getLastPublicAccessLogEntry();
    // verify remote host, http method and uri
    String subString = testErrorCase ? "Error" : "Info" + ":embedded" + " " + httpRequest.method() + " " + uri;
    Assert.assertTrue("Public Access log entry doesn't have expected remote host/method/uri ",
        lastLogEntry.startsWith(subString));
    // verify SSL-related info
    subString = "SSL ([used=" + sslUsed + "]";
    if (sslUsed) {
      subString += ", [principal=" + PEER_CERT.getSubjectX500Principal() + "]";
      subString += ", [san=" + PEER_CERT.getSubjectAlternativeNames() + "]";
    }
    subString += ")";
    Assert.assertTrue("Public Access log entry doesn't have SSL info set correctly", lastLogEntry.contains(subString));
    // verify request headers
    verifyPublicAccessLogEntryForRequestHeaders(lastLogEntry, headers, httpRequest.method(), true);

    // verify response
    subString = "Response (";
    for (String responseHeader : RESPONSE_HEADERS.split(",")) {
      if (headers.contains(responseHeader)) {
        subString += "[" + responseHeader + "=" + headers.get(responseHeader) + "] ";
      }
    }
    subString += "[isChunked=" + chunkedResponse + "]), status=" + HttpResponseStatus.OK.code();

    if (!testErrorCase) {
      Assert.assertTrue("Public Access log entry doesn't have response set correctly",
          lastLogEntry.contains(subString));
    } else {
      Assert.assertTrue("Public Access log entry doesn't have error set correctly ",
          lastLogEntry.contains(": Channel closed while request in progress."));
    }
  }

  /**
   * Does a test for the request handling flow with transfer encoding chunked
   */
  private void doRequestHandleWithChunkedResponse(boolean useSSL) throws Exception {
    EmbeddedChannel channel = createChannel(useSSL);
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(EchoMethodHandler.IS_CHUNKED, "true");
    HttpRequest request = RestTestUtils.createRequest(HttpMethod.POST, "POST", headers);
    HttpUtil.setKeepAlive(request, true);
    sendRequestCheckResponse(channel, request, "POST", headers, false, true, useSSL);
    Assert.assertTrue("Channel should not be closed ", channel.isOpen());
    channel.close();
  }

  // helpers
  // general

  /**
   * Creates an {@link EmbeddedChannel} that incorporates an instance of {@link PublicAccessLogHandler}
   * and {@link EchoMethodHandler}.
   * @param useSSL {@code true} to add an {@link SslHandler} to the pipeline.
   * @return an {@link EmbeddedChannel} that incorporates an instance of {@link PublicAccessLogHandler}
   *         and {@link EchoMethodHandler}, and an {@link SslHandler} if needed.
   */
  private EmbeddedChannel createChannel(boolean useSSL) {
    EmbeddedChannel channel = new EmbeddedChannel();
    if (useSSL) {
      SSLEngine sslEngine = SSL_CONTEXT.newEngine(channel.alloc());
      // HttpRequests pass through the SslHandler without a handshake (it only operates on ByteBuffers) so we have
      // to mock certain methods of SSLEngine and SSLSession to ensure that we can test certificate logging.
      SSLEngine mockSSLEngine =
          new MockSSLEngine(sslEngine, new MockSSLSession(sslEngine.getSession(), new Certificate[]{PEER_CERT}));
      channel.pipeline().addLast(new SslHandler(mockSSLEngine));
    }
    channel.pipeline()
        .addLast(new PublicAccessLogHandler(publicAccessLogger, new NettyMetrics(new MetricRegistry())))
        .addLast(new EchoMethodHandler());
    return channel;
  }

  /**
   * Prepares a list of HttpHeaders for test purposes
   * @return
   */
  private List<HttpHeaders> getHeadersList() {
    List<HttpHeaders> headersList = new ArrayList<HttpHeaders>();

    // contains one logged request header
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaderNames.CONTENT_TYPE, "content-type1");
    headersList.add(headers);

    // contains one logged and not logged header
    headers = new DefaultHttpHeaders();
    headers.add(NOT_LOGGED_HEADER_KEY + "1", "headerValue1");
    headers.add(HttpHeaderNames.CONTENT_TYPE, "content-type2");
    headersList.add(headers);

    // contains all not logged headers
    headers = new DefaultHttpHeaders();
    headers.add(NOT_LOGGED_HEADER_KEY + "1", "headerValue1");
    headers.add(NOT_LOGGED_HEADER_KEY + "2", "headerValue2");
    headersList.add(headers);

    // contains all the logged headers
    headers = new DefaultHttpHeaders();
    headers.add(HttpHeaderNames.HOST, "host1");
    headers.add(RestUtils.Headers.CONTENT_TYPE, "content-type3");
    headers.add(EchoMethodHandler.RESPONSE_HEADER_KEY_1, "responseHeaderValue1");
    headers.add(EchoMethodHandler.RESPONSE_HEADER_KEY_2, "responseHeaderValue1");
    headersList.add(headers);
    return headersList;
  }

  /**
   * Verifies either the expected request headers are found or not found (based on the parameter passed) in the
   * public access log entry
   * @param logEntry the public access log entry
   * @param headers expected headers
   * @param httpMethod HttpMethod type
   * @param expected, true if the headers are expected, false otherwise
   */
  private void verifyPublicAccessLogEntryForRequestHeaders(String logEntry, HttpHeaders headers, HttpMethod httpMethod,
      boolean expected) {
    Iterator<Map.Entry<String, String>> itr = headers.iteratorAsString();
    while (itr.hasNext()) {
      Map.Entry<String, String> entry = itr.next();
      if (!entry.getKey().startsWith(NOT_LOGGED_HEADER_KEY) && !entry.getKey()
          .startsWith(EchoMethodHandler.RESPONSE_HEADER_KEY_PREFIX)) {
        if (httpMethod == HttpMethod.GET && !entry.getKey().equalsIgnoreCase(HttpHeaderNames.CONTENT_TYPE.toString())) {
          String subString = "[" + entry.getKey() + "=" + entry.getValue() + "]";
          boolean actual = logEntry.contains(subString);
          if (expected) {
            Assert.assertTrue("Public Access log entry does not have expected header " + entry.getKey(), actual);
          } else {
            Assert.assertFalse("Public Access log entry has unexpected header " + entry.getKey(), actual);
          }
        }
      }
    }
  }
}
