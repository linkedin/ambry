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

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostMultipartRequestDecoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An extension of {@link NettyRequest} that can handle multipart requests.
 * <p/>
 * However, unlike {@link NettyRequest} and because of the limitations of multipart decoding offered by Netty, this does
 * not allow on-demand streaming. The full content of the request is held in memory and decoded on {@link #prepare()}.
 * Do not call {@link #prepare()} in I/O bound threads as decoding is a costly operation.
 * </p>
 * Multipart decoding also creates copies of the data. This affects latency and increases memory pressure.
 */
class NettyMultipartRequest extends NettyRequest {
  private final Queue<HttpContent> rawRequestContents = new LinkedBlockingQueue<>();
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final long maxSizeAllowedInBytes;
  //For Multipart request, bytes received in http content can be different from blob bytes received.
  private final AtomicLong blobBytesReceived = new AtomicLong(0);
  private boolean readyForRead = false;
  private boolean hasBlob = false;

  /**
   * Wraps the {@code request} in a NettyMultipartRequest so that other layers can understand the request.
   * @param request the {@link HttpRequest} that needs to be wrapped.
   * @param channel the {@link Channel} over which the {@code request} has been received.
   * @param nettyMetrics the {@link NettyMetrics} instance to use.
   * @param blacklistedQueryParams the set of query params that should not be exposed via {@link #getArgs()}.
   * @param maxSizeAllowedInBytes the cap on the size of the request. If the size of the request goes beyond this size, then
   *                        it will be discarded.
   * @throws IllegalArgumentException if {@code request} is null or if the HTTP method defined in {@code request} is
   *                                    anything other than POST.
   * @throws RestServiceException if the HTTP method defined in {@code request} is not recognized as a
   *                                {@link RestMethod}.
   */
  NettyMultipartRequest(HttpRequest request, Channel channel, NettyMetrics nettyMetrics,
      Set<String> blacklistedQueryParams, long maxSizeAllowedInBytes) throws RestServiceException {
    super(request, channel, nettyMetrics, blacklistedQueryParams);
    // reset auto read state.
    channel.config().setRecvByteBufAllocator(savedAllocator);
    setAutoRead(true);
    if (!getRestMethod().equals(RestMethod.POST) && !getRestMethod().equals(RestMethod.PUT)) {
      throw new IllegalArgumentException("NettyMultipartRequest cannot be created for " + getRestMethod());
    }
    this.maxSizeAllowedInBytes = maxSizeAllowedInBytes;
  }

  @Override
  public void close() {
    super.close();
    logger.trace("Closing NettyMultipartRequest with {} raw content chunks unread", rawRequestContents.size());
    HttpContent content = rawRequestContents.poll();
    while (content != null) {
      ReferenceCountUtil.release(content);
      content = rawRequestContents.poll();
    }
  }

  /**
   * {@inheritDoc}
   * @param asyncWritableChannel the {@link AsyncWritableChannel} to read the data into.
   * @param callback the {@link Callback} that will be invoked either when all the data in the channel has been emptied
   *                 into the {@code asyncWritableChannel} or if there is an exception in doing so. This can be null.
   * @return the {@link Future} that will eventually contain the result of the operation.
   * @throws IllegalStateException if an attempt is made to read the channel before calling {@link #prepare()} or if
   *                                this function is called more than once.
   */
  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    if (callbackWrapper != null) {
      throw new IllegalStateException("ReadableStreamChannel cannot be read more than once");
    } else if (!readyForRead) {
      throw new IllegalStateException("The channel cannot be read yet");
    }
    callbackWrapper = new ReadIntoCallbackWrapper(callback);
    if (!isOpen()) {
      nettyMetrics.multipartRequestAlreadyClosedError.inc();
      callbackWrapper.invokeCallback(new ClosedChannelException());
    }
    HttpContent content = requestContents.poll();
    while (content != null) {
      try {
        writeContent(asyncWritableChannel, callbackWrapper, content);
      } finally {
        ReferenceCountUtil.release(content);
      }
      content = requestContents.poll();
    }
    return callbackWrapper.futureResult;
  }

  /**
   * Adds content that will be decoded on a call to {@link #prepare()}.
   * </p>
   * All content has to be added before {@link #prepare()} can be called.
   * @param httpContent the {@link HttpContent} that needs to be added.
   * @throws RestServiceException if request channel has been closed.
   */
  @Override
  public void addContent(HttpContent httpContent) throws RestServiceException {
    if (!isOpen()) {
      nettyMetrics.multipartRequestAlreadyClosedError.inc();
      throw new RestServiceException("The request has been closed and is not accepting content",
          RestServiceErrorCode.RequestChannelClosed);
    } else {
      long bytesReceivedTillNow = bytesReceived.addAndGet(httpContent.content().readableBytes());
      if (bytesReceivedTillNow > maxSizeAllowedInBytes) {
        throw new RestServiceException("Request is larger than allowed size", RestServiceErrorCode.RequestTooLarge);
      }
      rawRequestContents.add(ReferenceCountUtil.retain(httpContent));
    }
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Prepares the request for reading by decoding all the content added via {@link #addContent(HttpContent)}.
   * @throws RestServiceException if request channel is closed or if the request could not be decoded/prepared.
   */
  @Override
  public void prepare() throws RestServiceException {
    if (!isOpen()) {
      nettyMetrics.multipartRequestAlreadyClosedError.inc();
      throw new RestServiceException("Request is closed", RestServiceErrorCode.RequestChannelClosed);
    } else if (!readyForRead) {
      // make sure data is held in memory.
      HttpDataFactory httpDataFactory = new DefaultHttpDataFactory(false);
      HttpPostMultipartRequestDecoder postRequestDecoder =
          new HttpPostMultipartRequestDecoder(httpDataFactory, request);
      try {
        HttpContent httpContent = rawRequestContents.poll();
        while (httpContent != null) {
          try {
            // if the request is also an instance of HttpContent, the HttpPostMultipartRequestDecoder does the offer
            // automatically at the time of construction. We should not add it again.
            if (httpContent != request) {
              postRequestDecoder.offer(httpContent);
            }
          } finally {
            ReferenceCountUtil.release(httpContent);
          }
          httpContent = rawRequestContents.poll();
        }
        for (InterfaceHttpData part : postRequestDecoder.getBodyHttpDatas()) {
          processPart(part);
        }
        requestContents.add(LastHttpContent.EMPTY_LAST_CONTENT);
        readyForRead = true;
      } catch (HttpPostRequestDecoder.ErrorDataDecoderException e) {
        nettyMetrics.multipartRequestDecodeError.inc();
        throw new RestServiceException("There was an error decoding the request", e,
            RestServiceErrorCode.MalformedRequest);
      } finally {
        postRequestDecoder.destroy();
      }
    }
  }

  /**
   * Processes a single decoded part in a multipart request. Exposes the data in the part either through the channel
   * itself (if it is the blob part) or via {@link #getArgs()}.
   * @param part the {@link InterfaceHttpData} that needs to be processed.
   * @throws RestServiceException if the request channel is closed, if there is more than one part of the same name, if
   *                              the size obtained from the headers does not match the actual size of the blob part or
   *                              if {@code part} is not of the expected type ({@link FileUpload}).
   */
  private void processPart(InterfaceHttpData part) throws RestServiceException {
    if (part.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
      FileUpload fileUpload = (FileUpload) part;
      if (fileUpload.getName().equals(RestUtils.MultipartPost.BLOB_PART)) {
        // this is actual data.
        if (hasBlob) {
          nettyMetrics.repeatedPartsError.inc();
          throw new RestServiceException("Request has more than one " + RestUtils.MultipartPost.BLOB_PART,
              RestServiceErrorCode.BadRequest);
        } else {
          hasBlob = true;
          if (getSize() != -1 && fileUpload.length() != getSize()) {
            nettyMetrics.multipartRequestSizeMismatchError.inc();
            throw new RestServiceException(
                "Request size [" + fileUpload.length() + "] does not match Content-Length [" + getSize() + "]",
                RestServiceErrorCode.BadRequest);
          } else {
            contentLock.lock();
            try {
              if (isOpen()) {
                requestContents.add(new DefaultHttpContent(ReferenceCountUtil.retain(fileUpload.content())));
                blobBytesReceived.set(fileUpload.content().capacity());
              } else {
                nettyMetrics.multipartRequestAlreadyClosedError.inc();
                throw new RestServiceException("Request is closed", RestServiceErrorCode.RequestChannelClosed);
              }
            } finally {
              contentLock.unlock();
            }
          }
        }
      } else {
        // this is any kind of data. (For ambry, this will be user metadata).
        // TODO: find a configurable way of rejecting unexpected file parts.
        String name = fileUpload.getName();
        if (allArgs.containsKey(name)) {
          nettyMetrics.repeatedPartsError.inc();
          throw new RestServiceException("Request already has a component named " + name,
              RestServiceErrorCode.BadRequest);
        } else {
          ByteBuffer buffer = ByteBuffer.allocate(fileUpload.content().readableBytes());
          // TODO: Possible optimization - Upgrade ByteBufferReadableStreamChannel to take a list of ByteBuffer. This
          // TODO: will avoid the copy.
          fileUpload.content().readBytes(buffer);
          buffer.flip();
          allArgs.put(name, buffer);
        }
      }
    } else {
      nettyMetrics.unsupportedPartError.inc();
      throw new RestServiceException("Unexpected HTTP data", RestServiceErrorCode.BadRequest);
    }
  }

  @Override
  public long getBlobBytesReceived() {
    return blobBytesReceived.get();
  }
}
