package com.github.ambry.rest;

import com.github.ambry.utils.UtilsTest;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


public class PublicAccessLogRequestHandlerTest {
  private MockPublicAccessLogger publicAccessLogger = null;
  private String requestHeaders =
      "Host,Referer,User-Agent,Content-Length,x-ambry-content-type,x-ambry-owner-id,x-ambry-ttl,x-ambry-private,x-ambry-service-id,X-Forwarded-For";
  private String responseHeaders = "Location,x-ambry-blob-size";
  private final String invalidHeaderKeyPrefix = "headerKey";
  private final String disconnectUri = "disconnect";

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
    doRequestHandleTest(HttpMethod.GET, MockBlobStorageService.ECHO_REST_METHOD, false);
    doRequestHandleTest(HttpMethod.DELETE, "DELETE", false);
  }

  /**
   * Tests for the request handling flow for close
   * @throws IOException
   */
  @Test
  public void requestHandleOnCloseTest()
      throws IOException {
    doRequestHandleTest(HttpMethod.POST, "close", true);
    doRequestHandleTest(HttpMethod.GET, "close", true);
    doRequestHandleTest(HttpMethod.DELETE, "close", true);
  }

  /**
   * Tests for the request handling flow on disconnect
   * @throws IOException
   */
  @Test
  public void requestHandleOnDisconnectTest()
      throws IOException {
    // disonnecting the embedded channel, calls close of PubliAccessLogRequestHandler
    doRequestHandleTest(HttpMethod.POST, disconnectUri, true);
    doRequestHandleTest(HttpMethod.GET, disconnectUri, true);
    doRequestHandleTest(HttpMethod.DELETE, disconnectUri, true);
  }

  // requestHandleWithGoodInputTest() helpers

  /**
   * Does a test to see that request handling results in expected entries in public access log
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param uri Uri to be used during the request
   * @throws IOException
   */
  private void doRequestHandleTest(HttpMethod httpMethod, String uri, boolean testErrorCase)
      throws IOException {
    EmbeddedChannel channel = createChannel();
    Random random = new Random();
    List<HttpHeaders> httpHeadersList = getHeadersList();
    for (HttpHeaders headers : httpHeadersList) {
      String location = UtilsTest.getRandomString(60);
      int size = random.nextInt(10000);
      // for POST, the EchoMethodHandler sets location and blob size(in response) based on the values from the request headers
      if (httpMethod == HttpMethod.POST) {
        headers.add(RestUtils.Headers.LOCATION, location);
        headers.add(RestUtils.Headers.BLOB_SIZE, size);
      }
      channel.writeInbound(RestTestUtils.createRequest(httpMethod, uri, headers));
      if (uri.equals(disconnectUri)) {
        channel.disconnect();
      } else {
        channel.writeInbound(new DefaultLastHttpContent());
      }
      String lastLogEntry = publicAccessLogger.getLastPublicAccessLogEntry();

      // verify remote host, http method and uri
      String subString = testErrorCase ? "Error" : "Info" + ":embedded" + " " + httpMethod + " " + uri;
      Assert.assertTrue("Public Access log entry doesn't have expected remote host/method/uri ",
          lastLogEntry.startsWith(subString));
      // verify headers
      verifyPublicAccessLogEntryForHeaders(lastLogEntry, headers, httpMethod);

      // verify response
      subString = "Response (";
      if (httpMethod == HttpMethod.GET) {
        subString += "[isChunked=false])" + "," + " status=" + HttpResponseStatus.OK.code();
      } else if (httpMethod == HttpMethod.DELETE) {
        subString += "[isChunked=false])" + "," + " status=" + HttpResponseStatus.ACCEPTED.code();
      } else if (httpMethod == HttpMethod.POST) {
        subString +=
            "[" + RestUtils.Headers.LOCATION + "=" + location + "] [" + RestUtils.Headers.BLOB_SIZE + "=" + size
                + "] [isChunked=false])" + "," + " status=" + HttpResponseStatus.CREATED.code();
      }

      if (!testErrorCase) {
        Assert.assertTrue("Public Access log entry doesn't have response set correctly",
            lastLogEntry.contains(subString));
      } else {
        Assert.assertTrue("Public Access log entry doesn't have error set correctly ",
            lastLogEntry.contains(": Channel closed while request in progress."));
      }
      if (testErrorCase) {
        // during error cases, the channel would have been closed
        channel = createChannel();
      }
    }
    channel.close();
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

    // contains one valid request header
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextLong());
    headersList.add(headers);

    // contains one valid and one invalid header
    headers = new DefaultHttpHeaders();
    headers.add(invalidHeaderKeyPrefix + "1", "headerValue1");
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextLong());
    headersList.add(headers);

    // contains all invalid headers
    headers = new DefaultHttpHeaders();
    headers.add(invalidHeaderKeyPrefix + "1", "headerVxalue1");
    headers.add(invalidHeaderKeyPrefix + "2", "headerValue2");
    headersList.add(headers);

    // contains all the expected headers
    headers = new DefaultHttpHeaders();
    headers.add(HttpHeaders.Names.HOST, "host1");
    headers.add(HttpHeaders.Names.REFERER, "referrer1");
    headers.add(HttpHeaders.Names.USER_AGENT, "user-agent1");
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextLong());
    headers.add(RestUtils.Headers.CONTENT_TYPE, "content-type1");
    headers.add(RestUtils.Headers.OWNER_ID, "ownerId1");
    headers.add(RestUtils.Headers.TTL, "ttl1");
    headers.add(RestUtils.Headers.PRIVATE, "private1");
    headers.add(RestUtils.Headers.SERVICE_ID, "serviceId1");
    headers.add(RestUtils.Headers.X_FORWARDED_FOR, "x-forwarded-for");
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
      if (!entry.getKey().startsWith(invalidHeaderKeyPrefix)) {
        if (httpMethod == HttpMethod.GET && !entry.getKey().equals(HttpHeaders.Names.CONTENT_TYPE)) {
          String subString = "[" + entry.getKey() + "=" + entry.getValue() + "]";
          Assert.assertTrue("Public Access log entry does not have expected header", logEntry.contains(subString));
        }
      }
    }
  }
}
