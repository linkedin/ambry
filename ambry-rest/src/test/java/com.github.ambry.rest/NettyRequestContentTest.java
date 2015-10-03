package com.github.ambry.rest;

import com.github.ambry.utils.ByteBufferChannel;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link NettyRequestContent}.
 */
public class NettyRequestContentTest {

  /**
   * This tests conversion of {@link HttpContent} to {@link NettyRequestContent} given good input.
   * @throws IOException
   */
  @Test
  public void conversionWithGoodInputTest()
      throws IOException {
    byte[] content = getRandomBytes(1024);
    NettyRequestContent nettyContent;
    boolean isLast = false;

    nettyContent = createNettyContentAndValidate(new DefaultHttpContent(Unpooled.wrappedBuffer(content)));
    validateContent(nettyContent, content, isLast);
    closeContentAndValidate(nettyContent);

    isLast = true;
    nettyContent = createNettyContentAndValidate(new DefaultLastHttpContent(Unpooled.wrappedBuffer(content)));
    validateContent(nettyContent, content, isLast);
    closeContentAndValidate(nettyContent);

    nettyContent = createNettyContentAndValidate(new DefaultLastHttpContent());
    validateContent(nettyContent, new byte[0], isLast);
    closeContentAndValidate(nettyContent);
  }

  /**
   * This tests conversion of {@link HttpContent} to {@link NettyRequestContent} given bad bad input (i.e. checks for
   * the correct exception and {@link RestServiceErrorCode} if any).
   */
  @Test
  public void conversionWithBadInputTest() {
    // null input.
    try {
      new NettyRequestContent(null);
      fail("Provided null input to NettyRequestContent, yet it did not fail");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }
  }

  /**
   * Tests for behavior of multiple operations after {@link NettyRequestContent#close()} has been called. Some should be
   * ok to do and some should throw exceptions.
   * @throws IOException
   */
  @Test
  public void operationsAfterCloseTest()
      throws IOException {
    byte[] content = getRandomBytes(1024);
    NettyRequestContent nettyContent =
        createNettyContentAndValidate(new DefaultHttpContent(Unpooled.wrappedBuffer(content)));
    closeContentAndValidate(nettyContent);

    // operations that should be ok to do (does not include all operations).
    nettyContent.close();
    // behavioral test of these functions in another test.
    nettyContent.release();
    nettyContent.retain();

    // operations that will throw exceptions.
    try {
      nettyContent.read(new ByteBufferChannel(ByteBuffer.allocate(0)));
      fail("Content channel has been closed, so read should have thrown ClosedChannelException");
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
    }
  }

  /**
   * Tests {@link NettyRequestContent#retain()} and {@link NettyRequestContent#release()} and makes sure that the
   * reference counts of the underlying {@link HttpContent} are updated correctly. Also tests their behavior after
   * {@link NettyRequestContent#close()} has been called.
   * @throws IOException
   */
  @Test
  public void retainReleaseTest()
      throws IOException {
    HttpContent httpContent = new DefaultHttpContent(Unpooled.wrappedBuffer(getRandomBytes(1024)));
    normalBehaviorTest(httpContent);
    behaviorAfterCloseTest(httpContent);
  }

  // helpers
  // general

  /**
   * Creates an instance of {@link NettyRequestContent} given {@code content}.
   * @param content the {@link HttpContent} that the {@link NettyRequestContent} should wrap.
   * @return a {@link NettyRequestContent} wrapping the {@code content}.
   */
  private NettyRequestContent createNettyContentAndValidate(HttpContent content) {
    NettyRequestContent nettyContent = new NettyRequestContent(content);
    assertTrue("Content channel is not open", nettyContent.isOpen());
    return nettyContent;
  }

  /**
   * Closes the provided {@code restRequestContent} and verifies that it has been closed.
   * @param restRequestContent the {@link NettyRequestContent} that has to be closed and verified.
   * @throws IOException
   */
  private void closeContentAndValidate(RestRequestContent restRequestContent)
      throws IOException {
    restRequestContent.close();
    assertFalse("Content channel is not closed", restRequestContent.isOpen());
  }

  /**
   * Gets a byte array of length {@code size} with random bytes.
   * @param size the required length of the random byte array.
   * @return a byte array of length {@code size} with random bytes.
   */
  private byte[] getRandomBytes(int size) {
    byte[] bytes = new byte[size];
    new Random().nextBytes(bytes);
    return bytes;
  }

  // conversionWithGoodInputTest() helpers

  /**
   * Validates the properties given {@code nettyContent} by comparing them to the provided original {@code content} and
   * {@code isLast} status.
   * @param nettyContent the {@link NettyRequestContent} that needs to be validated.
   * @param content the original byte array that was used to form the {@link HttpContent} and subsequently the
   *                  {@link NettyRequestContent}.
   * @param isLast the original boolean input for whether this is the past piece of content.
   * @throws IOException
   */
  private void validateContent(NettyRequestContent nettyContent, byte[] content, boolean isLast)
      throws IOException {
    long contentSize = nettyContent.getSize();
    assertEquals("Content size mismatch", content.length, contentSize);
    ByteBuffer readBuffer = ByteBuffer.allocate((int) contentSize);
    nettyContent.read(new ByteBufferChannel(readBuffer));
    assertArrayEquals("Content mismatch", content, readBuffer.array());
    assertEquals("Last status mismatch", isLast, nettyContent.isLast());
  }

  // retainReleaseTest() helpers

  /**
   * Tests normal behavior of {@link NettyRequestContent#retain()} and {@link NettyRequestContent#release()} by calling
   * them multiple times and verifying that the reference count of the underlying {@link HttpContent} is updated
   * correctly.
   * @param httpContent the {@link HttpContent} that needs to be used to construct a {@link NettyRequestContent}.
   * @throws IOException
   */
  private void normalBehaviorTest(HttpContent httpContent)
      throws IOException {
    int startRefCount = httpContent.refCnt();
    NettyRequestContent nettyContent = createNettyContentAndValidate(httpContent);
    int retainReleaseCount = 5;

    for (int i = 0; i < retainReleaseCount; i++) {
      nettyContent.retain();
      assertEquals("Ref count is not as expected", i + 1, httpContent.refCnt() - startRefCount);
    }

    for (int i = retainReleaseCount; i > 0; i--) {
      nettyContent.release();
      assertEquals("Ref count is not as expected", i - 1, httpContent.refCnt() - startRefCount);
    }

    assertEquals("Ref count not equal to what it was when retain-release cycle was started", startRefCount,
        httpContent.refCnt());

    // release some more. No errors and refCount does not change.
    for (int i = retainReleaseCount; i > 0; i--) {
      nettyContent.release();
      assertEquals("Ref count is not as expected", startRefCount, httpContent.refCnt());
    }
    closeContentAndValidate(nettyContent);
  }

  /**
   * Tests behavior of {@link NettyRequestContent#retain()} and {@link NettyRequestContent#release()} after
   * {@link NettyRequestContent#close()} has been called.
   * @param httpContent the {@link HttpContent} that needs to be used to construct a {@link NettyRequestContent}.
   * @throws IOException
   */
  private void behaviorAfterCloseTest(HttpContent httpContent)
      throws IOException {
    int startRefCount = httpContent.refCnt();
    NettyRequestContent nettyContent = createNettyContentAndValidate(httpContent);
    int retainReleaseCount = 5;

    for (int i = 0; i < retainReleaseCount; i++) {
      nettyContent.retain();
      assertEquals("Ref count is not as expected", i + 1, httpContent.refCnt() - startRefCount);
    }
    closeContentAndValidate(nettyContent);
    assertEquals("Ref count not equal to what it was when retain-release cycle was started", startRefCount,
        httpContent.refCnt());

    // retain a few times. No errors and no effect
    for (int i = 0; i < retainReleaseCount; i++) {
      nettyContent.retain();
      assertEquals("Ref count is not as expected", startRefCount, httpContent.refCnt());
    }

    // release a few times. No errors and no effect
    for (int i = retainReleaseCount; i > 0; i--) {
      nettyContent.release();
      assertEquals("Ref count is not as expected", startRefCount, httpContent.refCnt());
    }
  }
}
