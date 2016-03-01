package com.github.ambry.rest;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit tests for {@link PublicAccessLogRequestHandler}
 */
public class PublicAccessLogRequestHandlerTest {
  private final MockPublicAccessLogger publicAccessLogger;
  private String requestHeaders = "Host,Content-Length,x-ambry-content-type";
  private String responseHeaders = "responseHeaderKey1,responseHeaderKey2";
  private static final String NOT_LOGGED_HEADER_KEY = "headerKey";
  private static final String DISCONNECT_URI = "disconnect";
  private static final String CLOSE_URI = "close";

  /**
   * Sets up the mock public access logger that {@link PublicAccessLogRequestHandler} can use.
   */
  public PublicAccessLogRequestHandlerTest() {
    publicAccessLogger = new MockPublicAccessLogger(requestHeaders.split(","), responseHeaders.split(","));
  }

  /**
   * Tests for the common case request handling flow.
   * @throws IOException
   */
  @Test
  public void requestHandleWithGoodInputTest()
      throws IOException {
    doRequestHandleTest(HttpMethod.POST, "POST", false);
    doRequestHandleTest(HttpMethod.GET, "GET", false);
    doRequestHandleTest(HttpMethod.DELETE, "DELETE", false);
  }

  /**
   * Tests for the request handling flow for close
   * @throws IOException
   */
  @Test
  public void requestHandleOnCloseTest()
      throws IOException {
    doRequestHandleTest(HttpMethod.POST, CLOSE_URI, true);
    doRequestHandleTest(HttpMethod.GET, CLOSE_URI, true);
    doRequestHandleTest(HttpMethod.DELETE, CLOSE_URI, true);
  }

  /**
   * Tests for the request handling flow on disconnect
   * @throws IOException
   */
  @Test
  public void requestHandleOnDisconnectTest()
      throws IOException {
    // disonnecting the embedded channel, calls close of PubliAccessLogRequestHandler
    doRequestHandleTest(HttpMethod.POST, DISCONNECT_URI, true);
    doRequestHandleTest(HttpMethod.GET, DISCONNECT_URI, true);
    doRequestHandleTest(HttpMethod.DELETE, DISCONNECT_URI, true);
  }

  // requestHandleTest() helpers

  /**
   * Does a test to see that request handling results in expected entries in public access log
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param uri Uri to be used during the request
   * @param testErrorCase true if error case has to be tested, false otherwise
   * @throws IOException
   */
  private void doRequestHandleTest(HttpMethod httpMethod, String uri, boolean testErrorCase)
      throws IOException {
    EmbeddedChannel channel = createChannel();
    List<HttpHeaders> httpHeadersList = getHeadersList();
    for (HttpHeaders headers : httpHeadersList) {
      HttpRequest request = RestTestUtils.createRequest(httpMethod, uri, headers);
      HttpHeaders.setKeepAlive(request, true);
      sendRequestCheckResponse(channel, request, uri, headers, testErrorCase);
      if (!testErrorCase) {
        Assert.assertTrue("Channel should not be closed ", channel.isOpen());
      } else {
        Assert.assertFalse("Channel should have been closed ", channel.isOpen());
        channel = createChannel();
      }
    }
    channel.close();
  }

  /**
   * Sends the provided {@code httpRequest} and verifies that the response is as expected.
   * @param channel the {@link EmbeddedChannel} to send the request over.
   * @param httpRequest the {@link HttpRequest} that has to be sent
   * @param uri, Uri to be used for the request
   * @param headers {@link HttpHeaders} that is set in the request to be used for verification purposes
   * @param testErrorCase true if error case has to be tested, false otherwise
   */
  private void sendRequestCheckResponse(EmbeddedChannel channel, HttpRequest httpRequest, String uri,
      HttpHeaders headers, boolean testErrorCase) {
    channel.writeInbound(httpRequest);
    if (uri.equals(DISCONNECT_URI)) {
      channel.disconnect();
    } else {
      channel.writeInbound(new DefaultLastHttpContent());
    }
    String lastLogEntry = publicAccessLogger.getLastPublicAccessLogEntry();

    // verify remote host, http method and uri
    String subString = testErrorCase ? "Error" : "Info" + ":embedded" + " " + httpRequest.getMethod() + " " + uri;
    Assert.assertTrue("Public Access log entry doesn't have expected remote host/method/uri ",
        lastLogEntry.startsWith(subString));
    // verify request headers
    verifyPublicAccessLogEntryForHeaders(lastLogEntry, headers, httpRequest.getMethod());

    // verify response
    subString = "Response (";
    for(String responseHeader : responseHeaders.split(",")){
      if(headers.contains(responseHeader)){
        subString += "["+responseHeader+"="+headers.get(responseHeader)+"] ";
      }
    }
    subString += "[isChunked=false]), status=" + HttpResponseStatus.OK.code();

    if (!testErrorCase) {
      Assert
          .assertTrue("Public Access log entry doesn't have response set correctly", lastLogEntry.contains(subString));
    } else {
      Assert.assertTrue("Public Access log entry doesn't have error set correctly ",
          lastLogEntry.contains(": Channel closed while request in progress."));
    }
  }

  // helpers
  // general

  /**
   * Creates an {@link EmbeddedChannel} that incorporates an instance of {@link PublicAccessLogRequestHandler}
   * and {@link EchoMethodHandler}.
   * @return an {@link EmbeddedChannel} that incorporates an instance of {@link PublicAccessLogRequestHandler}
   * nad {@link EchoMethodHandler}.
   */
  private EmbeddedChannel createChannel() {
    return new EmbeddedChannel(new PublicAccessLogRequestHandler(publicAccessLogger), new EchoMethodHandler());
  }

  /**
   * Prepares a list of HttpHeaders for test purposes
   * @return
   */
  private List<HttpHeaders> getHeadersList() {
    List<HttpHeaders> headersList = new ArrayList<HttpHeaders>();

    // contains one logged request header
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextLong());
    headersList.add(headers);

    // contains one logged and not logged header
    headers = new DefaultHttpHeaders();
    headers.add(NOT_LOGGED_HEADER_KEY + "1", "headerValue1");
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextLong());
    headersList.add(headers);

    // contains all not logged headers
    headers = new DefaultHttpHeaders();
    headers.add(NOT_LOGGED_HEADER_KEY + "1", "headerValue1");
    headers.add(NOT_LOGGED_HEADER_KEY + "2", "headerValue2");
    headersList.add(headers);

    // contains all the logged headers
    headers = new DefaultHttpHeaders();
    headers.add(HttpHeaders.Names.HOST, "host1");
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextLong());
    headers.add(RestUtils.Headers.CONTENT_TYPE, "content-type1");
    headers.add(EchoMethodHandler.responseHeaderKey1, "responseHeaderValue1");
    headers.add(EchoMethodHandler.responseHeaderKey2, "responseHeaderValue1");
    headersList.add(headers);
    return headersList;
  }

  /**
   * Verifies either the expected headers are found or not found (based on the parameter passed) in the public access log entry
   * @param logEntry the public access log entry
   * @param headers expected headers
   * @param httpMethod HttpMethod type
   */
  private void verifyPublicAccessLogEntryForHeaders(String logEntry, HttpHeaders headers, HttpMethod httpMethod) {
    Iterator<Map.Entry<String, String>> itr = headers.iterator();
    while (itr.hasNext()) {
      Map.Entry<String, String> entry = itr.next();
      if (!entry.getKey().startsWith(NOT_LOGGED_HEADER_KEY)) {
        if (httpMethod == HttpMethod.GET && !entry.getKey().equals(HttpHeaders.Names.CONTENT_TYPE)) {
          String subString = "[" + entry.getKey() + "=" + entry.getValue() + "]";
          Assert.assertTrue("Public Access log entry does not have expected header", logEntry.contains(subString));
        }
      }
    }
  }
}
