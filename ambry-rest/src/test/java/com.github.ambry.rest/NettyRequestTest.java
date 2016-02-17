package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link NettyRequest}.
 */
public class NettyRequestTest {

  /**
   * Tests conversion of {@link HttpRequest} to {@link NettyRequest} given good input.
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void conversionWithGoodInputTest()
      throws IOException, RestServiceException {
    // headers
    HttpHeaders headers = new DefaultHttpHeaders(false);
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextInt(Integer.MAX_VALUE));
    headers.add("headerKey", "headerValue1");
    headers.add("headerKey", "headerValue2");
    headers.add("overLoadedKey", "headerOverloadedValue");
    headers.add("paramNoValueInUriButValueInHeader", "paramValueInHeader");
    headers.add("headerNoValue", (Object) null);
    headers.add("headerNoValueButValueInUri", (Object) null);

    // params
    Map<String, List<String>> params = new HashMap<String, List<String>>();
    List<String> values = new ArrayList<String>(2);
    values.add("paramValue1");
    values.add("paramValue2");
    params.put("paramKey", values);
    values = new ArrayList<String>(1);
    values.add("paramOverloadedValue");
    params.put("overLoadedKey", values);
    values = new ArrayList<String>(1);
    values.add("headerValueInUri");
    params.put("headerNoValueButValueInUri", values);
    params.put("paramNoValue", null);
    params.put("paramNoValueInUriButValueInHeader", null);

    StringBuilder uriAttachmentBuilder = new StringBuilder("?");
    for (Map.Entry<String, List<String>> param : params.entrySet()) {
      if (param.getValue() != null) {
        for (String value : param.getValue()) {
          uriAttachmentBuilder.append(param.getKey()).append("=").append(value).append("&");
        }
      } else {
        uriAttachmentBuilder.append(param.getKey()).append("&");
      }
    }
    uriAttachmentBuilder.deleteCharAt(uriAttachmentBuilder.length() - 1);
    String uriAttachment = uriAttachmentBuilder.toString();

    NettyRequest nettyRequest;
    String uri;

    uri = "/GET" + uriAttachment;
    nettyRequest = createNettyRequest(HttpMethod.GET, uri, headers);
    validateRequest(nettyRequest, RestMethod.GET, uri, headers, params);
    closeRequestAndValidate(nettyRequest);

    uri = "/POST" + uriAttachment;
    nettyRequest = createNettyRequest(HttpMethod.POST, uri, headers);
    validateRequest(nettyRequest, RestMethod.POST, uri, headers, params);
    closeRequestAndValidate(nettyRequest);

    uri = "/DELETE" + uriAttachment;
    nettyRequest = createNettyRequest(HttpMethod.DELETE, uri, headers);
    validateRequest(nettyRequest, RestMethod.DELETE, uri, headers, params);
    closeRequestAndValidate(nettyRequest);

    uri = "/HEAD" + uriAttachment;
    nettyRequest = createNettyRequest(HttpMethod.HEAD, uri, headers);
    validateRequest(nettyRequest, RestMethod.HEAD, uri, headers, params);
    closeRequestAndValidate(nettyRequest);
  }

  /**
   * Tests conversion of {@link HttpRequest} to {@link NettyRequest} given bad input (i.e. checks for the correct
   * exception and {@link RestServiceErrorCode} if any).
   * @throws RestServiceException
   */
  @Test
  public void conversionWithBadInputTest()
      throws RestServiceException {
    // HttpRequest null.
    try {
      new NettyRequest(null, new NettyMetrics(new MetricRegistry()));
      fail("Provided null HttpRequest to NettyRequest, yet it did not fail");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    // unknown http method
    try {
      createNettyRequest(HttpMethod.TRACE, "/", null);
      fail("Unknown http method was supplied to NettyRequest. It should have failed to construct");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", e.getErrorCode(), RestServiceErrorCode.UnsupportedHttpMethod);
    }
  }

  /**
   * Tests for behavior of multiple operations after {@link NettyRequest#close()} has been called. Some should be ok to
   * do and some should throw exceptions.
   * @throws Exception
   */
  @Test
  public void operationsAfterCloseTest()
      throws Exception {
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null);
    closeRequestAndValidate(nettyRequest);

    // operations that should be ok to do (does not include all operations).
    nettyRequest.close();

    // operations that will throw exceptions.
    AsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    try {
      nettyRequest.readInto(writeChannel, callback).get();
      fail("Request channel has been closed, so read should have thrown ClosedChannelException");
    } catch (ExecutionException e) {
      Exception exception = getRootCause(e);
      assertTrue("Exception is not ClosedChannelException", exception instanceof ClosedChannelException);
      assertEquals("Exceptions of callback and future differ", exception.getMessage(), callback.exception.getMessage());
    }

    try {
      byte[] content = getRandomBytes(1024);
      nettyRequest.addContent(new DefaultLastHttpContent(Unpooled.wrappedBuffer(content)));
      fail("Request channel has been closed, so addContent() should have thrown ClosedChannelException");
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
    }
  }

  /**
   * Tests {@link NettyRequest#addContent(HttpContent)} and
   * {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} by creating a {@link NettyRequest}, adding a few
   * pieces of content to it and then reading from it to match the stream with the added content.
   * <p/>
   * The read happens at different points of time w.r.t content addition (before, during, after).
   * @throws Exception
   */
  @Test
  public void contentAddAndReadTest()
      throws Exception {
    // start reading before content added
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null);
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    ByteBuffer content = generateContent(httpContents);
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    Future<Long> future = nettyRequest.readInto(writeChannel, callback);

    for (HttpContent httpContent : httpContents) {
      nettyRequest.addContent(httpContent);
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    readAndVerify(content.limit(), writeChannel, content);
    verifyRefCnts(httpContents);
    writeChannel.close();
    if (callback.exception != null) {
      throw callback.exception;
    }
    long futureBytesRead = future.get();
    assertEquals("Total bytes read does not match (callback)", content.limit(), callback.bytesRead);
    assertEquals("Total bytes read does not match (future)", content.limit(), futureBytesRead);
    closeRequestAndValidate(nettyRequest);

    // start reading in the middle of content add
    nettyRequest = createNettyRequest(HttpMethod.POST, "/", null);
    httpContents = new ArrayList<HttpContent>();
    content = generateContent(httpContents);
    writeChannel = new ByteBufferAsyncWritableChannel();
    callback = new ReadIntoCallback();

    // add content initially
    int bytesToVerify = 0;
    int addedCount = 0;
    for (; addedCount < httpContents.size() / 2; addedCount++) {
      HttpContent httpContent = httpContents.get(addedCount);
      bytesToVerify += httpContent.content().readableBytes();
      nettyRequest.addContent(httpContent);
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    future = nettyRequest.readInto(writeChannel, callback);
    readAndVerify(bytesToVerify, writeChannel, content);

    // add some more content
    bytesToVerify = 0;
    for (; addedCount < httpContents.size(); addedCount++) {
      HttpContent httpContent = httpContents.get(addedCount);
      bytesToVerify += httpContent.content().readableBytes();
      nettyRequest.addContent(httpContent);
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    readAndVerify(bytesToVerify, writeChannel, content);
    verifyRefCnts(httpContents);
    writeChannel.close();
    if (callback.exception != null) {
      throw callback.exception;
    }
    futureBytesRead = future.get();
    assertEquals("Total bytes read does not match (callback)", content.limit(), callback.bytesRead);
    assertEquals("Total bytes read does not match (future)", content.limit(), futureBytesRead);
    closeRequestAndValidate(nettyRequest);

    // start reading after all content added
    nettyRequest = createNettyRequest(HttpMethod.POST, "/", null);
    httpContents = new ArrayList<HttpContent>();
    content = generateContent(httpContents);
    writeChannel = new ByteBufferAsyncWritableChannel();
    callback = new ReadIntoCallback();

    for (HttpContent httpContent : httpContents) {
      nettyRequest.addContent(httpContent);
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    future = nettyRequest.readInto(writeChannel, callback);
    readAndVerify(content.limit(), writeChannel, content);
    verifyRefCnts(httpContents);
    writeChannel.close();
    if (callback.exception != null) {
      throw callback.exception;
    }
    futureBytesRead = future.get();
    assertEquals("Total bytes read does not match (callback)", content.limit(), callback.bytesRead);
    assertEquals("Total bytes read does not match (future)", content.limit(), futureBytesRead);
    closeRequestAndValidate(nettyRequest);
  }

  /**
   * Tests exception scenarios of {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} and behavior of
   * {@link NettyRequest} when {@link AsyncWritableChannel} instances fail.
   * @throws InterruptedException
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void readIntoExceptionsTest()
      throws InterruptedException, IOException, RestServiceException {
    // try to call readInto twice.
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null);
    AsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    nettyRequest.readInto(writeChannel, null);

    try {
      nettyRequest.readInto(writeChannel, null);
      fail("Calling readInto twice should have failed");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }

    // write into a channel that throws exceptions
    // non RuntimeException
    nettyRequest = createNettyRequest(HttpMethod.POST, "/", null);
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    assertTrue("Not enough content has been generated", httpContents.size() > 2);
    String expectedMsg = "@@expectedMsg@@";
    Exception exception = new Exception(expectedMsg);
    writeChannel = new BadAsyncWritableChannel(exception);
    ReadIntoCallback callback = new ReadIntoCallback();

    // add content initially
    int addedCount = 0;
    for (; addedCount < httpContents.size() / 2; addedCount++) {
      HttpContent httpContent = httpContents.get(addedCount);
      nettyRequest.addContent(httpContent);
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    Future<Long> future = nettyRequest.readInto(writeChannel, callback);

    // add some more content
    for (; addedCount < httpContents.size(); addedCount++) {
      HttpContent httpContent = httpContents.get(addedCount);
      nettyRequest.addContent(httpContent);
    }

    writeChannel.close();
    verifyRefCnts(httpContents);
    assertNotNull("Exception was not piped correctly", callback.exception);
    assertEquals("Exception message mismatch (callback)", expectedMsg, callback.exception.getMessage());
    try {
      future.get();
      fail("Future should have thrown exception");
    } catch (ExecutionException e) {
      assertEquals("Exception message mismatch (future)", expectedMsg, getRootCause(e).getMessage());
    }
    closeRequestAndValidate(nettyRequest);

    // RuntimeException
    // during readInto
    nettyRequest = createNettyRequest(HttpMethod.POST, "/", null);
    httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    exception = new IllegalStateException(expectedMsg);
    writeChannel = new BadAsyncWritableChannel(exception);
    callback = new ReadIntoCallback();

    for (HttpContent httpContent : httpContents) {
      nettyRequest.addContent(httpContent);
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    try {
      nettyRequest.readInto(writeChannel, callback);
      fail("readInto did not throw expected exception");
    } catch (Exception e) {
      assertEquals("Exception caught does not match expected exception", expectedMsg, e.getMessage());
    }
    writeChannel.close();
    closeRequestAndValidate(nettyRequest);
    verifyRefCnts(httpContents);

    // after readInto
    nettyRequest = createNettyRequest(HttpMethod.POST, "/", null);
    httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    exception = new IllegalStateException(expectedMsg);
    writeChannel = new BadAsyncWritableChannel(exception);
    callback = new ReadIntoCallback();

    nettyRequest.readInto(writeChannel, callback);
    // add content
    HttpContent httpContent = httpContents.get(1);
    try {
      nettyRequest.addContent(httpContent);
      fail("addContent did not throw expected exception");
    } catch (Exception e) {
      assertEquals("Exception caught does not match expected exception", expectedMsg, e.getMessage());
    }
    writeChannel.close();
    closeRequestAndValidate(nettyRequest);
    verifyRefCnts(httpContents);
  }

  /**
   * Tests that {@link NettyRequest#close()} leaves any added {@link HttpContent} the way it was before it was added.
   * (i.e no reference count changes).
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void closeTest()
      throws IOException, RestServiceException {
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null);
    Queue<HttpContent> httpContents = new LinkedBlockingQueue<HttpContent>();
    for (int i = 0; i < 5; i++) {
      ByteBuffer content = ByteBuffer.wrap(getRandomBytes(1024));
      HttpContent httpContent = new DefaultHttpContent(Unpooled.wrappedBuffer(content));
      nettyRequest.addContent(httpContent);
      httpContents.add(httpContent);
    }
    closeRequestAndValidate(nettyRequest);
    while (httpContents.peek() != null) {
      assertEquals("Reference count of http content has changed", 1, httpContents.poll().refCnt());
    }
  }

  /**
   * Tests different state transitions that can happen with {@link NettyRequest#addContent(HttpContent)} for GET
   * requests. Some transitions are valid and some should necessarily throw exceptions.
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void addContentForGetTest()
      throws IOException, RestServiceException {
    byte[] content = getRandomBytes(16);
    // adding non LastHttpContent to nettyRequest
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.GET, "/", null);
    try {
      nettyRequest.addContent(new DefaultHttpContent(Unpooled.wrappedBuffer(content)));
      fail("GET requests should not accept non-LastHTTPContent");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    }

    // adding LastHttpContent with some content to nettyRequest
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", null);
    try {
      nettyRequest.addContent(new DefaultLastHttpContent(Unpooled.wrappedBuffer(content)));
      fail("GET requests should not accept actual content in LastHTTPContent");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    }

    // should accept LastHttpContent just fine.
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", null);
    nettyRequest.addContent(new DefaultLastHttpContent());

    // should not accept LastHttpContent after close
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", null);
    nettyRequest.close();
    try {
      nettyRequest.addContent(new DefaultLastHttpContent());
      fail("Request channel has been closed, so addContent() should have thrown ClosedChannelException");
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
    }
  }

  @Test
  public void keepAliveTest()
      throws RestServiceException {
    NettyRequest request = createNettyRequest(HttpMethod.GET, "/", null);
    // by default, keep-alive is true for HTTP 1.1
    assertTrue("Keep-alive not as expected", request.isKeepAlive());

    HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request = createNettyRequest(HttpMethod.GET, "/", headers);
    assertTrue("Keep-alive not as expected", request.isKeepAlive());

    headers = new DefaultHttpHeaders();
    headers.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
    request = createNettyRequest(HttpMethod.GET, "/", headers);
    assertFalse("Keep-alive not as expected", request.isKeepAlive());
  }

  /**
   * Tests the {@link NettyRequest#getSize()} function to see that it respects priorities.
   * @throws RestServiceException
   */
  @Test
  public void sizeTest()
      throws RestServiceException {
    // no length headers provided.
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.GET, "/", null);
    assertEquals("Size not as expected", 0, nettyRequest.getSize());

    // deliberate mismatch to check priorities.
    int xAmbryBlobSize = 20;
    int contentLength = 10;

    // Content-Length header set
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, contentLength);
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", headers);
    assertEquals("Size not as expected", contentLength, nettyRequest.getSize());

    // xAmbryBlobSize set
    headers = new DefaultHttpHeaders();
    headers.add(RestUtils.Headers.BLOB_SIZE, xAmbryBlobSize);
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", headers);
    assertEquals("Size not as expected", xAmbryBlobSize, nettyRequest.getSize());

    // both set
    headers = new DefaultHttpHeaders();
    headers.add(RestUtils.Headers.BLOB_SIZE, xAmbryBlobSize);
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, contentLength);
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", headers);
    assertEquals("Size not as expected", xAmbryBlobSize, nettyRequest.getSize());
  }

  /**
   * Does any left over tests for {@link ContentWriteCallback}
   */
  @Test
  public void contentWriteCallbackTests() {
    ReadIntoCallback readIntoCallback = new ReadIntoCallback();
    ReadIntoCallbackWrapper wrapper = new ReadIntoCallbackWrapper(readIntoCallback);
    ContentWriteCallback callback = new ContentWriteCallback(null, true, wrapper);
    long bytesRead = new Random().nextInt(Integer.MAX_VALUE);
    // there should be no problem even though httpContent is null.
    callback.onCompletion(bytesRead, null);
    assertEquals("Bytes read does not match", bytesRead, readIntoCallback.bytesRead);
  }

  // helpers
  // general

  /**
   * Creates a {@link NettyRequest} with the given parameters.
   * @param httpMethod the {@link HttpMethod} desired.
   * @param uri the URI desired.
   * @param headers {@link HttpHeaders} that need to be a part of the request.
   * @return {@link NettyRequest} encapsulating a {@link HttpRequest} with the given parameters.
   * @throws RestServiceException if the {@code httpMethod} is not recognized by {@link NettyRequest}.
   */
  private NettyRequest createNettyRequest(HttpMethod httpMethod, String uri, HttpHeaders headers)
      throws RestServiceException {
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri, false);
    if (headers != null) {
      httpRequest.headers().set(headers);
    }
    return new NettyRequest(httpRequest, new NettyMetrics(metricRegistry));
  }

  /**
   * Closes the provided {@code restRequest} and validates that it is actually closed.
   * @param restRequest the {@link RestRequest} that needs to be closed and validated.
   * @throws IOException if there is an I/O error while closing the {@code restRequest}.
   */
  private void closeRequestAndValidate(RestRequest restRequest)
      throws IOException {
    restRequest.close();
    assertFalse("Request channel is not closed", restRequest.isOpen());
  }

  /**
   * Gets random bytes of length {@code size}
   * @param size the length of random bytes required.
   * @return a byte array of length {@code size} with random bytes.
   */
  private byte[] getRandomBytes(int size) {
    byte[] bytes = new byte[size];
    new Random().nextBytes(bytes);
    return bytes;
  }

  /**
   * Gets the root cause for {@code e}.
   * @param e the {@link Exception} whose root cause is required.
   * @return the root cause for {@code e}.
   */
  private Exception getRootCause(Exception e) {
    Exception exception = e;
    while (exception.getCause() != null) {
      exception = (Exception) exception.getCause();
    }
    return exception;
  }

  // conversionWithGoodInputTest() helpers

  /**
   * Validates the various expected properties of the provided {@code nettyRequest}.
   * @param nettyRequest the {@link NettyRequest} that needs to be validated.
   * @param restMethod the expected {@link RestMethod} in {@code nettyRequest}.
   * @param uri the expected URI in {@code nettyRequest}.
   * @param headers the {@link HttpHeaders} passed with the request that need to be in {@link NettyRequest#getArgs()}.
   * @param params the parameters passed with the request that need to be in {@link NettyRequest#getArgs()}.
   */
  private void validateRequest(NettyRequest nettyRequest, RestMethod restMethod, String uri, HttpHeaders headers,
      Map<String, List<String>> params) {
    long contentLength = headers.contains(HttpHeaders.Names.CONTENT_LENGTH) ? Long
        .parseLong(headers.get(HttpHeaders.Names.CONTENT_LENGTH)) : 0;
    assertTrue("Request channel is not open", nettyRequest.isOpen());
    assertEquals("Mismatch in content length", contentLength, nettyRequest.getSize());
    assertEquals("Mismatch in rest method", restMethod, nettyRequest.getRestMethod());
    assertEquals("Mismatch in path", uri.substring(0, uri.indexOf("?")), nettyRequest.getPath());
    assertEquals("Mismatch in uri", uri, nettyRequest.getUri());

    Map<String, List<String>> receivedArgs = new HashMap<String, List<String>>();
    for (Map.Entry<String, Object> e : nettyRequest.getArgs().entrySet()) {
      if (!receivedArgs.containsKey(e.getKey())) {
        receivedArgs.put(e.getKey(), new LinkedList<String>());
      }
      if (e.getValue() != null) {
        List<String> values =
            Arrays.asList(e.getValue().toString().split(NettyRequest.MULTIPLE_HEADER_VALUE_DELIMITER));
        receivedArgs.get(e.getKey()).addAll(values);
      }
    }
    Map<String, Integer> keyValueCount = new HashMap<String, Integer>();
    for (Map.Entry<String, List<String>> param : params.entrySet()) {
      assertTrue("Did not find key: " + param.getKey(), receivedArgs.containsKey(param.getKey()));
      if (!keyValueCount.containsKey(param.getKey())) {
        keyValueCount.put(param.getKey(), 0);
      }

      if (param.getValue() != null) {
        boolean containsAllValues = receivedArgs.get(param.getKey()).containsAll(param.getValue());
        assertTrue("Did not find all values expected for key: " + param.getKey(), containsAllValues);
        keyValueCount.put(param.getKey(), keyValueCount.get(param.getKey()) + param.getValue().size());
      }
    }

    for (Map.Entry<String, String> e : headers) {
      assertTrue("Did not find key: " + e.getKey(), receivedArgs.containsKey(e.getKey()));
      if (!keyValueCount.containsKey(e.getKey())) {
        keyValueCount.put(e.getKey(), 0);
      }
      if (headers.get(e.getKey()) != null) {
        assertTrue("Did not find value '" + e.getValue() + "' expected for key: '" + e.getKey() + "'",
            receivedArgs.get(e.getKey()).contains(e.getValue()));
        keyValueCount.put(e.getKey(), keyValueCount.get(e.getKey()) + 1);
      }
    }

    assertEquals("Number of args does not match", keyValueCount.size(), receivedArgs.size());
    for (Map.Entry<String, Integer> e : keyValueCount.entrySet()) {
      assertEquals("Value count for key " + e.getKey() + " does not match", e.getValue().intValue(),
          receivedArgs.get(e.getKey()).size());
    }
  }

  // contentAddAndReadTest() and readIntoExceptionsTest() helpers

  /**
   * Generates random content and fills it up (in parts) in {@code httpContents}.
   * @param httpContents the {@link List<HttpContent>} that will contain all the content in parts.
   * @return the whole content as a {@link ByteBuffer} - serves as a source of truth.
   */
  private ByteBuffer generateContent(List<HttpContent> httpContents) {
    byte[] contentBytes = getRandomBytes(10240);
    for (int addedContentCount = 0; addedContentCount < 9; addedContentCount++) {
      HttpContent httpContent =
          new DefaultHttpContent(Unpooled.wrappedBuffer(contentBytes, addedContentCount * 1024, 1024));
      httpContents.add(httpContent);
    }
    httpContents.add(new DefaultLastHttpContent(Unpooled.wrappedBuffer(contentBytes, 9 * 1024, 1024)));
    return ByteBuffer.wrap(contentBytes);
  }

  /**
   * Verifies that the reference counts of {@code httpContents} is undisturbed after all operations.
   * @param httpContents the {@link List<HttpContent>} of contents whose reference counts need to checked.
   */
  private void verifyRefCnts(List<HttpContent> httpContents) {
    for (HttpContent httpContent : httpContents) {
      assertEquals("Reference count of http content has changed", 1, httpContent.refCnt());
    }
  }

  /**
   * Reads from the provided {@code writeChannel} and verifies that the bytes received match the original content
   * provided through {@code content}.
   * @param readLengthDesired desired length of bytes to read.
   * @param writeChannel the {@link ByteBufferAsyncWritableChannel} to read from.
   * @param content the original content that serves as the source of truth.
   * @throws InterruptedException
   */
  private void readAndVerify(int readLengthDesired, ByteBufferAsyncWritableChannel writeChannel, ByteBuffer content)
      throws InterruptedException {
    int bytesRead = 0;
    while (bytesRead < readLengthDesired) {
      ByteBuffer recvdContent = writeChannel.getNextChunk();
      while (recvdContent.hasRemaining()) {
        assertEquals("Unexpected byte", content.get(), recvdContent.get());
        bytesRead++;
      }
      writeChannel.resolveChunk(recvdContent, null);
    }
  }
}

/**
 * Callback for all read operations on {@link NettyRequest}.
 */
class ReadIntoCallback implements Callback<Long> {
  public volatile long bytesRead;
  public volatile Exception exception;
  private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

  @Override
  public void onCompletion(Long result, Exception exception) {
    if (callbackInvoked.compareAndSet(false, true)) {
      bytesRead = result;
      this.exception = exception;
    } else {
      this.exception = new IllegalStateException("Callback invoked more than once");
    }
  }
}

/**
 * Used to test for {@link NettyRequest} behavior when a {@link AsyncWritableChannel} throws exceptions.
 */
class BadAsyncWritableChannel implements AsyncWritableChannel {
  private final Exception exceptionToThrow;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);

  /**
   * Creates an instance of BadAsyncWritableChannel that throws {@code exceptionToThrow} on write.
   * @param exceptionToThrow the {@link Exception} to throw on write.
   */
  public BadAsyncWritableChannel(Exception exceptionToThrow) {
    this.exceptionToThrow = exceptionToThrow;
  }

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    if (exceptionToThrow instanceof RuntimeException) {
      throw (RuntimeException) exceptionToThrow;
    } else {
      return markFutureInvokeCallback(callback, 0, exceptionToThrow);
    }
  }

  @Override
  public boolean isOpen() {
    return isOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    isOpen.set(false);
  }

  /**
   * Creates and marks a future as done and invoked the callback with paramaters {@code totalBytesWritten} and
   * {@code Exception}.
   * @param callback the {@link Callback} to invoke.
   * @param totalBytesWritten the number of bytes successfully written.
   * @param exception the {@link Exception} that occurred if any.
   * @return the {@link Future} that will contain the result of the operation.
   */
  private Future<Long> markFutureInvokeCallback(Callback<Long> callback, long totalBytesWritten, Exception exception) {
    FutureResult<Long> futureResult = new FutureResult<Long>();
    futureResult.done(totalBytesWritten, exception);
    if (callback != null) {
      callback.onCompletion(totalBytesWritten, exception);
    }
    return futureResult;
  }
}
