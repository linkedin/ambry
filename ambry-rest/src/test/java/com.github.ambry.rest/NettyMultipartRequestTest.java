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
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.CopyingAsyncWritableChannel;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import io.netty.handler.codec.http.multipart.MemoryFileUpload;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link NettyMultipartRequest}.
 */
public class NettyMultipartRequestTest {

  private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
  private static final NettyMetrics NETTY_METRICS = new NettyMetrics(METRIC_REGISTRY);
  private static final int DEFAULT_WATERMARK;

  static {
    DEFAULT_WATERMARK = new NettyConfig(new VerifiableProperties(new Properties())).nettyServerRequestBufferWatermark;
    RestRequestMetricsTracker.setDefaults(METRIC_REGISTRY);
  }

  public NettyMultipartRequestTest() {
    NettyRequest.bufferWatermark = DEFAULT_WATERMARK;
  }

  /**
   * Tests instantiation of {@link NettyMultipartRequest} with different {@link HttpMethod} types.
   * </p>
   * Only {@link HttpMethod#POST} should succeed.
   * @throws RestServiceException
   */
  @Test
  public void instantiationTest() throws RestServiceException {
    // POST will succeed.
    NettyRequest.bufferWatermark = 1;
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
    MockChannel channel = new MockChannel();
    int expectedVal = channel.config().getMaxMessagesPerRead();
    NettyMultipartRequest request = new NettyMultipartRequest(httpRequest, channel, NETTY_METRICS);
    assertTrue("Auto-read should not have been changed", channel.config().isAutoRead());
    assertEquals("Max messages per read should not have changed", expectedVal,
        channel.config().getMaxMessagesPerRead());
    closeRequestAndValidate(request);

    // Methods that will fail. Can include other methods, but these should be enough.
    HttpMethod[] methods = {HttpMethod.GET, HttpMethod.DELETE, HttpMethod.HEAD};
    for (HttpMethod method : methods) {
      httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, "/");
      try {
        new NettyMultipartRequest(httpRequest, new MockChannel(), NETTY_METRICS);
        fail("Creation of NettyMultipartRequest should have failed for " + method);
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }
  }

  /**
   * Tests that multipart requests are decoded successfully and verifies that the decoded data matches the source data.
   * Request kinds tested:
   * 1. Request without content.
   * 2. Request without a {@link RestUtils.MultipartPost#BLOB_PART} but with other parts.
   * 3. Request with a {@link RestUtils.MultipartPost#BLOB_PART} and with other parts.
   * @throws Exception
   */
  @Test
  public void multipartRequestDecodeTest() throws Exception {
    String[] digestAlgorithms = {"", "MD5", "SHA-1", "SHA-256"};
    for (String digestAlgorithm : digestAlgorithms) {
      // request without content
      doMultipartDecodeTest(0, null, digestAlgorithm);

      final int BLOB_PART_SIZE = 1024;
      // number of parts including the Blob
      final int NUM_TOTAL_PARTS = 5;
      Random random = new Random();
      InMemoryFile[] files = new InMemoryFile[NUM_TOTAL_PARTS];
      for (int i = 0; i < NUM_TOTAL_PARTS; i++) {
        files[i] =
            new InMemoryFile("part-" + i, ByteBuffer.wrap(RestTestUtils.getRandomBytes(random.nextInt(128) + 128)));
      }

      // request without blob (but has other parts)
      doMultipartDecodeTest(0, files, digestAlgorithm);

      // request with blob and other parts
      files[NUM_TOTAL_PARTS - 1] = new InMemoryFile(RestUtils.MultipartPost.BLOB_PART,
          ByteBuffer.wrap(RestTestUtils.getRandomBytes(BLOB_PART_SIZE)));
      doMultipartDecodeTest(BLOB_PART_SIZE, files, digestAlgorithm);
    }
  }

  /**
   * Tests that reference counts are correct when a {@link NettyMultipartRequest} is closed without being read.
   * @throws Exception
   */
  @Test
  public void refCountsAfterCloseTest() throws Exception {
    NettyMultipartRequest requestCloseBeforePrepare = createRequest(null, null);
    NettyMultipartRequest requestCloseAfterPrepare = createRequest(null, null);
    List<HttpContent> httpContents = new ArrayList<HttpContent>(5);
    for (int i = 0; i < 5; i++) {
      HttpContent httpContent = new DefaultHttpContent(Unpooled.wrappedBuffer(RestTestUtils.getRandomBytes(10)));
      requestCloseBeforePrepare.addContent(httpContent);
      requestCloseAfterPrepare.addContent(httpContent);
      assertEquals("Reference count is not as expected", 3, httpContent.refCnt());
      httpContents.add(httpContent);
    }
    closeRequestAndValidate(requestCloseBeforePrepare);
    requestCloseAfterPrepare.prepare();
    closeRequestAndValidate(requestCloseAfterPrepare);
    for (HttpContent httpContent : httpContents) {
      assertEquals("Reference count is not as expected", 1, httpContent.refCnt());
    }
  }

  /**
   * Tests the expected behavior of operations after {@link NettyMultipartRequest#close()} has been called.
   * @throws Exception
   */
  @Test
  public void operationsAfterCloseTest() throws Exception {
    NettyMultipartRequest request = createRequest(null, null);
    request.prepare();
    closeRequestAndValidate(request);
    // close should be idempotent.
    request.close();

    try {
      request.readInto(new ByteBufferAsyncWritableChannel(), null).get();
      fail("Reading should have failed because request is closed");
    } catch (ExecutionException e) {
      assertEquals("Unexpected exception", ClosedChannelException.class, Utils.getRootCause(e).getClass());
    }

    try {
      request.prepare();
      fail("Preparing should have failed because request is closed");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.RequestChannelClosed, e.getErrorCode());
    }

    try {
      request.addContent(new DefaultHttpContent(Unpooled.wrappedBuffer(RestTestUtils.getRandomBytes(10))));
      fail("Content addition should have failed because request is closed");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.RequestChannelClosed, e.getErrorCode());
    }
  }

  /**
   * Tests exception scenarios of {@link NettyMultipartRequest#readInto(AsyncWritableChannel, Callback)}.
   * @throws Exception
   */
  @Test
  public void readIntoExceptionsTest() throws Exception {
    // most tests are in NettyRequest. Adding tests for differing code in NettyMultipartRequest
    // try to call readInto twice.
    NettyMultipartRequest request = createRequest(null, null);
    AsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    request.prepare();
    request.readInto(writeChannel, null);
    try {
      request.readInto(writeChannel, null);
      fail("Calling readInto twice should have failed");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    } finally {
      closeRequestAndValidate(request);
    }

    // call readInto when not ready for read.
    request = createRequest(null, null);
    writeChannel = new ByteBufferAsyncWritableChannel();
    try {
      request.readInto(writeChannel, null);
      fail("Calling readInto without calling prepare() should have failed");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    } finally {
      closeRequestAndValidate(request);
    }
  }

  /**
   * Tests different scenarios with {@link NettyMultipartRequest#prepare()}.
   * Currently tests:
   * 1. Idempotency of {@link NettyMultipartRequest#prepare()}.
   * 2. Exception scenarios of {@link NettyMultipartRequest#prepare()}.
   * @throws Exception
   */
  @Test
  public void prepareTest() throws Exception {
    // prepare half baked data
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
    HttpPostRequestEncoder encoder = createEncoder(httpRequest, null);
    NettyMultipartRequest request =
        new NettyMultipartRequest(encoder.finalizeRequest(), new MockChannel(), NETTY_METRICS);
    assertTrue("Request channel is not open", request.isOpen());
    // insert random data
    HttpContent httpContent = new DefaultHttpContent(Unpooled.wrappedBuffer(RestTestUtils.getRandomBytes(10)));
    request.addContent(httpContent);
    // prepare should fail
    try {
      request.prepare();
      fail("Preparing request should have failed");
    } catch (HttpPostRequestDecoder.NotEnoughDataDecoderException e) {
      assertEquals("Reference count is not as expected", 1, httpContent.refCnt());
    } finally {
      closeRequestAndValidate(request);
    }

    // more than one blob part
    HttpHeaders httpHeaders = new DefaultHttpHeaders();
    httpHeaders.set(RestUtils.Headers.BLOB_SIZE, 256);
    InMemoryFile[] files = new InMemoryFile[2];
    files[0] = new InMemoryFile(RestUtils.MultipartPost.BLOB_PART, ByteBuffer.wrap(RestTestUtils.getRandomBytes(256)));
    files[1] = new InMemoryFile(RestUtils.MultipartPost.BLOB_PART, ByteBuffer.wrap(RestTestUtils.getRandomBytes(256)));
    request = createRequest(httpHeaders, files);
    assertEquals("Request size does not match", 256, request.getSize());
    try {
      request.prepare();
      fail("Prepare should have failed because there was more than one " + RestUtils.MultipartPost.BLOB_PART);
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.MalformedRequest, e.getErrorCode());
    } finally {
      closeRequestAndValidate(request);
    }

    // more than one part named "part-1"
    files = new InMemoryFile[2];
    files[0] = new InMemoryFile("Part-1", ByteBuffer.wrap(RestTestUtils.getRandomBytes(256)));
    files[1] = new InMemoryFile("Part-1", ByteBuffer.wrap(RestTestUtils.getRandomBytes(256)));
    request = createRequest(null, files);
    try {
      request.prepare();
      fail("Prepare should have failed because there was more than one part named Part-1");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.MalformedRequest, e.getErrorCode());
    } finally {
      closeRequestAndValidate(request);
    }

    // size of blob does not match the advertized size
    httpHeaders = new DefaultHttpHeaders();
    httpHeaders.set(RestUtils.Headers.BLOB_SIZE, 256);
    files = new InMemoryFile[1];
    files[0] = new InMemoryFile(RestUtils.MultipartPost.BLOB_PART, ByteBuffer.wrap(RestTestUtils.getRandomBytes(128)));
    request = createRequest(httpHeaders, files);
    try {
      request.prepare();
      fail("Prepare should have failed because there was more than one " + RestUtils.MultipartPost.BLOB_PART);
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadRequest, e.getErrorCode());
    } finally {
      closeRequestAndValidate(request);
    }

    // non fileupload (file attribute present)
    httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
    HttpHeaders.setHeader(httpRequest, RestUtils.Headers.BLOB_SIZE, 256);
    files = new InMemoryFile[1];
    files[0] = new InMemoryFile(RestUtils.MultipartPost.BLOB_PART, ByteBuffer.wrap(RestTestUtils.getRandomBytes(256)));
    encoder = createEncoder(httpRequest, files);
    encoder.addBodyAttribute("dummyKey", "dummyValue");
    request = new NettyMultipartRequest(encoder.finalizeRequest(), new MockChannel(), NETTY_METRICS);
    assertTrue("Request channel is not open", request.isOpen());
    while (!encoder.isEndOfInput()) {
      // Sending null for ctx because the encoder is OK with that.
      request.addContent(encoder.readChunk(null));
    }
    try {
      request.prepare();
      fail("Prepare should have failed because there was non fileupload");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadRequest, e.getErrorCode());
    } finally {
      closeRequestAndValidate(request);
    }
  }

  // helpers
  // general

  /**
   * Creates a {@link NettyMultipartRequest} with the given {@code headers} and {@code parts}.
   * @param headers the {@link HttpHeaders} that need to be added to the request.
   * @param parts the files that will form the parts of the request.
   * @return a {@link NettyMultipartRequest} containing all the {@code headers} and {@code parts}.
   * @throws Exception
   */
  private NettyMultipartRequest createRequest(HttpHeaders headers, InMemoryFile[] parts) throws Exception {
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
    if (headers != null) {
      httpRequest.headers().set(headers);
    }
    HttpPostRequestEncoder encoder = createEncoder(httpRequest, parts);
    NettyMultipartRequest request =
        new NettyMultipartRequest(encoder.finalizeRequest(), new MockChannel(), NETTY_METRICS);
    assertTrue("Request channel is not open", request.isOpen());
    while (!encoder.isEndOfInput()) {
      // Sending null for ctx because the encoder is OK with that.
      request.addContent(encoder.readChunk(null));
    }
    return request;
  }

  /**
   * Creates a {@link HttpPostRequestEncoder} that encodes the given {@code request} and {@code parts}.
   * @param request the {@link HttpRequest} containing headers and other metadata about the request.
   * @param parts the {@link InMemoryFile}s that will form the parts of the request.
   * @return a {@link HttpPostRequestEncoder} that can encode the {@code request} and {@code parts}.
   * @throws HttpPostRequestEncoder.ErrorDataEncoderException
   * @throws IOException
   */
  private HttpPostRequestEncoder createEncoder(HttpRequest request, InMemoryFile[] parts)
      throws HttpPostRequestEncoder.ErrorDataEncoderException, IOException {
    HttpDataFactory httpDataFactory = new DefaultHttpDataFactory(false);
    HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(httpDataFactory, request, true);
    if (parts != null) {
      for (InMemoryFile part : parts) {
        FileUpload fileUpload =
            new MemoryFileUpload(part.name, part.name, "application/octet-stream", "", Charset.forName("UTF-8"),
                part.content.remaining());
        fileUpload.setContent(Unpooled.wrappedBuffer(part.content));
        encoder.addBodyHttpData(fileUpload);
      }
    }
    return encoder;
  }

  /**
   * Closes the provided {@code request} and validates that it is actually closed.
   * @param request the {@link NettyMultipartRequest} that needs to be closed and validated.
   */
  private void closeRequestAndValidate(NettyMultipartRequest request) {
    request.close();
    assertFalse("Request channel is not closed", request.isOpen());
  }

  // multipartRequestDecodeTest() helpers.

  /**
   * Does a multipart decode test.
   * 1. Creates a {@link NettyMultipartRequest}.
   * 2. Adds the {@link HttpRequest} and {@link HttpContent} generated by encoding the {@code files} as a multipart
   * request to the {@link NettyMultipartRequest}.
   * 3. Reads data from the {@link NettyMultipartRequest} via read operations and
   * {@link NettyMultipartRequest#getArgs()} and verifies them against the source data ({@code files}).
   * @param expectedRequestSize the value expected on a call to {@link NettyMultipartRequest#getSize()}.
   * @param files the {@link InMemoryFile}s that form the parts of the multipart request.
   * @param digestAlgorithm the digest algorithm to use. Can be empty or {@code null} if digest checking is not
   *                        required.
   * @throws Exception
   */
  private void doMultipartDecodeTest(int expectedRequestSize, InMemoryFile[] files, String digestAlgorithm)
      throws Exception {
    HttpHeaders httpHeaders = new DefaultHttpHeaders();
    httpHeaders.set(RestUtils.Headers.BLOB_SIZE, expectedRequestSize);
    NettyMultipartRequest request = createRequest(httpHeaders, files);
    assertEquals("Request size does not match", expectedRequestSize, request.getSize());
    request.prepare();

    CopyingAsyncWritableChannel asyncWritableChannel;
    byte[] readOutput;
    Map<String, Object> args = request.getArgs();
    ByteBuffer blobData = ByteBuffer.allocate(0);
    byte[] wholeDigest = null;
    if (files != null) {
      for (InMemoryFile file : files) {
        if (file.name.equals(RestUtils.MultipartPost.BLOB_PART)) {
          blobData = file.content;
          if (digestAlgorithm != null && !digestAlgorithm.isEmpty()) {
            MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
            digest.update(blobData);
            wholeDigest = digest.digest();
            blobData.rewind();
            request.setDigestAlgorithm(digestAlgorithm);
          }
        } else {
          Object value = args.get(file.name);
          assertNotNull("Request does not contain " + file, value);
          assertTrue("Argument value is not ByteBuffer", value instanceof ByteBuffer);
          readOutput = new byte[((ByteBuffer) value).remaining()];
          ((ByteBuffer) value).get(readOutput);
          assertArrayEquals(file.name + " content does not match", file.content.array(), readOutput);
        }
      }
    }
    asyncWritableChannel = new CopyingAsyncWritableChannel(expectedRequestSize);
    request.readInto(asyncWritableChannel, null).get();
    readOutput = asyncWritableChannel.getData();
    assertArrayEquals(RestUtils.MultipartPost.BLOB_PART + " content does not match", blobData.array(), readOutput);
    assertArrayEquals("Part by part digest should match digest of whole", wholeDigest, request.getDigest());
    closeRequestAndValidate(request);
  }

  /**
   * In memory representation of content.
   */
  private class InMemoryFile {
    public final String name;
    public final ByteBuffer content;

    public InMemoryFile(String name, ByteBuffer content) {
      this.name = name;
      this.content = content;
    }
  }
}
