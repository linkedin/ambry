package com.github.ambry.rest;

import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
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
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
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
public class NettyMultipartRequest extends NettyRequest {
  private final Queue<HttpContent> rawRequestContents = new LinkedBlockingQueue<HttpContent>();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private boolean readyForRead = false;
  private boolean hasBlob = false;

  /**
   * Wraps the {@code request} in a NettyMultipartRequest so that other layers can understand the request.
   * @param request the {@link HttpRequest} that needs to be wrapped.
   * @param nettyMetrics the {@link NettyMetrics} instance to use.
   * @throws IllegalArgumentException if {@code request} is null or if the HTTP method defined in {@code request} is
   *                                    anything other than POST.
   * @throws RestServiceException if the HTTP method defined in {@code request} is not recognized as a
   *                                {@link RestMethod}.
   */
  public NettyMultipartRequest(HttpRequest request, NettyMetrics nettyMetrics)
      throws RestServiceException {
    super(request, nettyMetrics);
    if (!getRestMethod().equals(RestMethod.POST)) {
      throw new IllegalArgumentException("NettyMultipartRequest cannot be created for " + getRestMethod());
    }
  }

  @Override
  public void close() {
    super.close();
    logger
        .trace("Closing NettyMultipartRequest {} with {} raw content chunks unread", getUri(), requestContents.size());
    while (rawRequestContents.peek() != null) {
      ReferenceCountUtil.release(rawRequestContents.poll());
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
    } else if (isOpen() && !readyForRead) {
      throw new IllegalStateException("The channel cannot be read yet");
    }
    callbackWrapper = new ReadIntoCallbackWrapper(callback);
    if (!isOpen()) {
      nettyMetrics.multipartRequestAlreadyClosedError.inc();
      callbackWrapper.invokeCallback(new ClosedChannelException());
    }
    while (requestContents.peek() != null) {
      writeContent(asyncWritableChannel, callbackWrapper, requestContents.peek());
      ReferenceCountUtil.release(requestContents.poll());
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
  public void addContent(HttpContent httpContent)
      throws RestServiceException {
    if (!isOpen()) {
      nettyMetrics.multipartRequestAlreadyClosedError.inc();
      throw new RestServiceException("The request has been closed and is not accepting content",
          RestServiceErrorCode.RequestChannelClosed);
    } else {
      rawRequestContents.add(httpContent);
      ReferenceCountUtil.retain(httpContent);
    }
  }

  /**
   * Prepares the request for reading by decoding all the content added via {@link #addContent(HttpContent)}.
   * @throws RestServiceException if request channel has been closed or if the request could not be decoded/processed.
   */
  public void prepare()
      throws RestServiceException {
    if (!isOpen()) {
      nettyMetrics.multipartRequestAlreadyClosedError.inc();
      throw new RestServiceException("Request is closed", RestServiceErrorCode.RequestChannelClosed);
    } else if (!readyForRead) {
      // make sure data does *not* go to disk. It should be held in memory.
      HttpDataFactory httpDataFactory = new DefaultHttpDataFactory(false);
      HttpPostMultipartRequestDecoder postRequestDecoder =
          new HttpPostMultipartRequestDecoder(httpDataFactory, request);
      try {
        HttpContent httpContent = rawRequestContents.peek();
        while (httpContent != null) {
          postRequestDecoder.offer(httpContent);
          ReferenceCountUtil.release(rawRequestContents.poll());
          httpContent = rawRequestContents.peek();
        }
        for (InterfaceHttpData part : postRequestDecoder.getBodyHttpDatas()) {
          processPart(part);
        }
        allArgsReadOnly = Collections.unmodifiableMap(allArgs);
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
  private void processPart(InterfaceHttpData part)
      throws RestServiceException {
    if (part.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
      FileUpload fileUpload = (FileUpload) part;
      if (fileUpload.getName().equals(RestUtils.MultipartPost.Blob_Part)) {
        // this is actual data.
        if (hasBlob) {
          nettyMetrics.repeatedPartsError.inc();
          throw new RestServiceException("Request has more than one " + RestUtils.MultipartPost.Blob_Part,
              RestServiceErrorCode.BadRequest);
        } else {
          hasBlob = true;
          if (getSize() > 0 && fileUpload.length() != getSize()) {
            nettyMetrics.multipartRequestSizeMismatchError.inc();
            throw new RestServiceException(
                "Request size [" + fileUpload.length() + "] does not match Content-Length [" + getSize() + "]",
                RestServiceErrorCode.BadRequest);
          } else {
            contentLock.lock();
            try {
              if (isOpen()) {
                ReferenceCountUtil.retain(fileUpload.content());
                requestContents.add(new DefaultHttpContent(fileUpload.content()));
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
          ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(buffer);
          allArgs.put(name, channel);
        }
      }
    } else {
      nettyMetrics.unsupportedPartError.inc();
      throw new RestServiceException("Unexpected HTTP data", RestServiceErrorCode.BadRequest);
    }
  }
}
