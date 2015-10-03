package com.github.ambry.rest;

import com.github.ambry.utils.ByteBufferChannel;
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
import java.nio.channels.WritableByteChannel;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link NettyRequest}.
 */
public class NettyRequestTest {

  /**
   * Tests conversion of {@link HttpRequest} to {@link NettyRequest} given good input.
   * @throws RestServiceException
   */
  @Test
  public void conversionWithGoodInputTest()
      throws IOException, RestServiceException {
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add("headerKey", "headerValue");
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextLong());
    String PARAM_KEY = "paramKey";
    String PARAM_VALUE = "paramValue";
    String uriAttachment = "?" + PARAM_KEY + "=" + PARAM_VALUE;

    NettyRequest nettyRequest;
    String uri;

    uri = "/GET" + uriAttachment;
    nettyRequest = createNettyRequest(HttpMethod.GET, uri, headers);
    validateRequest(nettyRequest, RestMethod.GET, uri, headers, PARAM_KEY, PARAM_VALUE);
    closeRequestAndValidate(nettyRequest);

    uri = "/POST" + uriAttachment;
    nettyRequest = createNettyRequest(HttpMethod.POST, uri, headers);
    validateRequest(nettyRequest, RestMethod.POST, uri, headers, PARAM_KEY, PARAM_VALUE);
    closeRequestAndValidate(nettyRequest);

    uri = "/DELETE" + uriAttachment;
    nettyRequest = createNettyRequest(HttpMethod.DELETE, uri, headers);
    validateRequest(nettyRequest, RestMethod.DELETE, uri, headers, PARAM_KEY, PARAM_VALUE);
    closeRequestAndValidate(nettyRequest);

    uri = "/HEAD" + uriAttachment;
    nettyRequest = createNettyRequest(HttpMethod.HEAD, uri, headers);
    validateRequest(nettyRequest, RestMethod.HEAD, uri, headers, PARAM_KEY, PARAM_VALUE);
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
    // null input.
    try {
      new NettyRequest(null);
      fail("Provided null input to NettyRequest, yet it did not fail");
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
   * @throws IOException
   */
  @Test
  public void operationsAfterCloseTest()
      throws RestServiceException, IOException {
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.GET, "/", null);
    closeRequestAndValidate(nettyRequest);

    // operations that should be ok to do (does not include all operations).
    nettyRequest.close();
    nettyRequest.retain();
    nettyRequest.release();

    // operations that will throw exceptions.
    try {
      nettyRequest.read(new ByteBufferChannel(ByteBuffer.allocate(0)));
      fail("Request channel has been closed, so read should have thrown ClosedChannelException");
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
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
   * Tests {@link NettyRequest#addContent(HttpContent)} and {@link NettyRequest#read(WritableByteChannel)} by creating a
   * NettyRequest, adding a few pieces of content to it and then reading from it to match the stream with the added
   * content. The read happens in different random sizes.
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void contentAddAndReadTest()
      throws IOException, RestServiceException {
    final Random randLength = new Random();
    final int CONTENT_LENGTH = 1024;
    int addedContentCount = 0;
    int bytesRead = 0;
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.GET, "/", null);
    Queue<ByteBuffer> contents = new LinkedBlockingQueue<ByteBuffer>();
    Queue<HttpContent> httpContents = new LinkedBlockingQueue<HttpContent>();

    // read before adding content.
    readAndVerify(CONTENT_LENGTH, 0, nettyRequest, contents, httpContents);

    // add content.
    for (; addedContentCount < 5; addedContentCount++) {
      ByteBuffer content = ByteBuffer.wrap(getRandomBytes(CONTENT_LENGTH));
      HttpContent httpContent = new DefaultHttpContent(Unpooled.wrappedBuffer(content));
      nettyRequest.addContent(httpContent);
      contents.add(content);
      httpContents.add(httpContent);
    }

    // read nothing
    readAndVerify(0, 0, nettyRequest, contents, httpContents);
    // read CONTENT_LENGTH size bytes.
    readAndVerify(CONTENT_LENGTH, CONTENT_LENGTH, nettyRequest, contents, httpContents);
    bytesRead += CONTENT_LENGTH;
    // read [0, CONTENT_LENGTH] bytes until we run out of data.
    while (bytesRead < addedContentCount * CONTENT_LENGTH) {
      // make sure to include CONTENT_LENGTH too - rand.nextInt(n) is [0,n)
      int readLengthDesired = randLength.nextInt(CONTENT_LENGTH + 1);
      // we might read lesser than expected because we are out of content.
      int readLengthExpected = Math.min(readLengthDesired, addedContentCount * CONTENT_LENGTH - bytesRead);
      readAndVerify(readLengthDesired, readLengthExpected, nettyRequest, contents, httpContents);
      bytesRead += readLengthExpected;
    }

    // add some more content
    for (; addedContentCount < 25; addedContentCount++) {
      ByteBuffer content = ByteBuffer.wrap(getRandomBytes(CONTENT_LENGTH));
      HttpContent httpContent = new DefaultHttpContent(Unpooled.wrappedBuffer(content));
      nettyRequest.addContent(httpContent);
      contents.add(content);
      httpContents.add(httpContent);
    }
    // add an end marker
    nettyRequest.addContent(new DefaultLastHttpContent());
    // read [CONTENT_LENGTH + 1, 4 * CONTENT_LENGTH] bytes until we run out of data.
    while (bytesRead < addedContentCount * CONTENT_LENGTH) {
      // make sure to exclude CONTENT_LENGTH but include 4 * CONTENT_LENGTH.
      int readLengthDesired = CONTENT_LENGTH + 1 + randLength.nextInt(3 * CONTENT_LENGTH);
      // we might read lesser than expected because we are out of content.
      int readLengthExpected = Math.min(readLengthDesired, addedContentCount * CONTENT_LENGTH - bytesRead);
      // make sure we don't read when there is no more available because we don't want a -1 return from NettyRequest yet
      if (readLengthExpected != 0) {
        readAndVerify(readLengthDesired, readLengthExpected, nettyRequest, contents, httpContents);
        bytesRead += readLengthExpected;
      }
    }

    // make sure that the stream has ended.
    readAndVerify(CONTENT_LENGTH, -1, nettyRequest, contents, httpContents);
    closeRequestAndValidate(nettyRequest);
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
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.GET, "/", null);
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
   * Tests instantiation of {@link NettyContent} on bad input.
   */
  @Test
  public void nettyContentBadInputTest() {
    try {
      new NettyContent(null);
      fail("Constructor of NettyContent should have thrown IllegalArgumentException on null input");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }
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
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri);
    if (headers != null) {
      httpRequest.headers().set(headers);
    }
    return new NettyRequest(httpRequest);
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

  // conversionWithGoodInputTest() helpers

  /**
   * Validates the various expected properties of the provided {@code nettyRequest}.
   * @param nettyRequest the {@link NettyRequest} that needs to be validated.
   * @param restMethod the expected {@link RestMethod} in {@code nettyRequest}.
   * @param uri the expected URI in {@code nettyRequest}.
   * @param headers the expected {@link HttpHeaders} in {@code nettyRequest}.
   * @param key the expected URI parameter key in {@code nettyRequest}.
   * @param value the expected value of the parameter {@code key} in the URI in {@code nettyRequest}.
   */
  private void validateRequest(NettyRequest nettyRequest, RestMethod restMethod, String uri, HttpHeaders headers,
      String key, String value) {
    assertTrue("Request channel is not open", nettyRequest.isOpen());
    // TODO: need header check once headers are supported in NettyRequest.
    long contentLength = headers.contains(HttpHeaders.Names.CONTENT_LENGTH) ? Long
        .parseLong(headers.get(HttpHeaders.Names.CONTENT_LENGTH)) : 0;
    assertEquals("Mismatch in content length", contentLength, nettyRequest.getSize());
    assertEquals("Mismatch in rest method", restMethod, nettyRequest.getRestMethod());
    assertEquals("Mismatch in path", uri.substring(0, uri.indexOf("?")), nettyRequest.getPath());
    assertEquals("Mismatch in uri", uri, nettyRequest.getUri());
    assertEquals("Mismatch in argument value", value, nettyRequest.getArgs().get(key).get(0));
  }

  // contentAddAndReadTest() helpers

  /**
   * Reads from the provided {@code nettyRequest} and verifies the bytes received against the original content provided
   * through {@code contents}. Also checks that reference counts of the http content added inside {@code nettyRequest}
   * remain unchanged once read of the content is finished.
   * @param readLengthDesired desired length of bytes to read.
   * @param readLengthExpected expected length of read bytes. This can be different from {@code readLengthDesired}
   *                           because there might not be enough data (yet) to read that many bytes.
   * @param nettyRequest the {@link NettyRequest} to read from.
   * @param contents the array of original content used to create {@link HttpContent}.
   * @param httpContents the array of the {@link HttpContent} added to {@code nettyRequest}.
   * @throws IOException if there was an I/O error reading from {@code nettyRequest}.
   */
  private void readAndVerify(int readLengthDesired, int readLengthExpected, NettyRequest nettyRequest,
      Queue<ByteBuffer> contents, Queue<HttpContent> httpContents)
      throws IOException {
    ByteBuffer contentBuffer = ByteBuffer.allocate(readLengthDesired);
    WritableByteChannel channel = new ByteBufferChannel(contentBuffer);
    assertEquals("Did not read expected size", readLengthExpected, nettyRequest.read(channel));

    ByteBuffer originalContent = ByteBuffer.allocate(readLengthDesired);
    int bytesCopied = 0;
    while (bytesCopied < readLengthExpected) {
      ByteBuffer head = contents.peek();
      int bytesToCopy = Math.min(originalContent.remaining(), head.remaining());
      for (int i = 0; i < bytesToCopy; i++) {
        originalContent.put(head.get());
      }
      bytesCopied += bytesToCopy;
      if (bytesCopied < readLengthDesired) {
        contents.poll();
        assertEquals("Reference count of http content has changed", 1, httpContents.poll().refCnt());
      }
    }
    assertArrayEquals("Content does not match", originalContent.array(), contentBuffer.array());
  }
}
