package com.github.ambry.rest;

import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link RestRequest}.
 * <p/>
 * Just a wrapper over {@link HttpRequest}.
 */
class NettyRequest implements RestRequest {
  private final QueryStringDecoder query;
  private final HttpRequest request;
  private final RestMethod restMethod;
  private final Map<String, List<String>> args;

  private final ReentrantLock contentLock = new ReentrantLock();
  private final List<NettyContent> requestContents = new LinkedList<NettyContent>();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final AtomicBoolean streamEnded = new AtomicBoolean(false);

  /**
   * Wraps the {@code request} in an implementation of {@link RestRequest} so that other layers can understand the
   * request.
   * @param request the {@link HttpRequest} that needs to be wrapped.
   * @throws IllegalArgumentException if {@code request} is null.
   * @throws RestServiceException if the HTTP method defined in {@code request} is not recognized as a
   *                                {@link RestMethod}.
   */
  public NettyRequest(HttpRequest request)
      throws RestServiceException {
    if (request == null) {
      throw new IllegalArgumentException("Received null HttpRequest");
    }
    this.request = request;
    this.query = new QueryStringDecoder(request.getUri());
    HttpMethod httpMethod = request.getMethod();
    if (httpMethod == HttpMethod.GET) {
      restMethod = RestMethod.GET;
    } else if (httpMethod == HttpMethod.POST) {
      restMethod = RestMethod.POST;
    } else if (httpMethod == HttpMethod.DELETE) {
      restMethod = RestMethod.DELETE;
    } else if (httpMethod == HttpMethod.HEAD) {
      restMethod = RestMethod.HEAD;
    } else {
      throw new RestServiceException("http method not supported: " + httpMethod,
          RestServiceErrorCode.UnsupportedHttpMethod);
    }

    Map<String, List<String>> allArgs = new HashMap<String, List<String>>();
    allArgs.putAll(query.parameters());
    for (Map.Entry<String, String> e : request.headers()) {
      if (!allArgs.containsKey(e.getKey())) {
        allArgs.put(e.getKey(), new LinkedList<String>());
      }
      allArgs.get(e.getKey()).add(e.getValue());
    }
    args = Collections.unmodifiableMap(allArgs);
  }

  @Override
  public String getUri() {
    return request.getUri();
  }

  @Override
  public String getPath() {
    return query.path();
  }

  @Override
  public RestMethod getRestMethod() {
    return restMethod;
  }

  @Override
  public Map<String, List<String>> getArgs() {
    return args;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    if (channelOpen.compareAndSet(true, false)) {
      try {
        contentLock.lock();
        Iterator<NettyContent> nettyContentIterator = requestContents.iterator();
        while (nettyContentIterator.hasNext()) {
          nettyContentIterator.next().release();
          nettyContentIterator.remove();
        }
        // no need to call release() because Netty request objects are not reference counted.
      } finally {
        contentLock.unlock();
      }
    }
  }

  /**
   * Only prints the request metadata (URI, path, HTTP method etc) as a string. Does not print the content.
   * @return the request metadata (URI, path, HTTP method etc) as a String. Content is *not* included.
   */
  @Override
  public String toString() {
    return request.toString();
  }

  /**
   * Returns the ambry specific content length header ({@link RestConstants.Headers#Blob_Size}. If there is no such
   * header, returns length in the "Content-Length" header. If there is no such header, tries to infer content size. If
   * that cannot be done, returns 0.
   * <p/>
   * This function does not individually count the bytes in the content (it is not possible) so the bytes received may
   * actually be different if the stream is buggy or the client made a mistake. Do *not* treat this as fully accurate.
   * @return the size of content as defined in headers. Might not be actual length of content if the stream is buggy.
   */
  @Override
  public long getSize() {
    long contentLength;
    if (HttpHeaders.getHeader(request, RestConstants.Headers.Blob_Size, null) != null) {
      contentLength = Long.parseLong(HttpHeaders.getHeader(request, RestConstants.Headers.Blob_Size));
    } else {
      contentLength = HttpHeaders.getContentLength(request, 0);
    }
    return contentLength;
  }

  @Override
  public int read(WritableByteChannel channel)
      throws IOException {
    int bytesWritten = streamEnded.get() ? -1 : 0;
    if (!channelOpen.get()) {
      throw new ClosedChannelException();
    } else if (!streamEnded.get()) {
      try {
        contentLock.lock();
        // We read from the NettyContent at the head of the list until :-
        // 1. The writable channel can hold no more data or there is no more data immediately available - while loop
        //      ends.
        // 2. The NettyContent runs out of content - remove it from the head of the list and start reading from new head
        //      if it is available.
        // Content may be added at any time and it is not necessary that the list have any elements at the time of
        // reading. Read returns -1 only when the a NettyContent with isLast() true is read. If stream has not ended and
        // there is no content in the list, we return 0.
        // Cases to consider:
        // 1. Writable channel can consume no content (nothing to do. Should return 0).
        // 2. Writable channel can consume data limited to one NettyContent (might rollover).
        //      a. There is content available in the NettyContent at the head of the list (don't rollover).
        //      b. Request content stream ended when we tried to read from the NettyContent at the head of the list (end
        //          of stream).
        //      b. There is no content available right now in the NettyContent at the head of the list but it has not
        //          finished its content (don't rollover).
        //      c. There is no content available in the NettyContent at the head of the list because it just finished
        //          its content (rollover).
        //            i. More NettyContent available in the list (continue read).
        //            ii. No more NettyContent in the list currently (cannot continue read).
        // 3. Writable channel can consume data across NettyContents (will rollover).
        //      a. More NettyContent is available in the list (continue read).
        //      b. Request content stream has not ended but more NettyContent is not available in the list (cannot
        //          continue read).
        //      c. Request content stream has ended (end of stream).
        int currentBytesWritten = requestContents.size() > 0 ? requestContents.get(0).read(channel) : 0;
        while (currentBytesWritten != 0) {
          if (currentBytesWritten == -1) {
            NettyContent nettyContent = requestContents.remove(0);
            nettyContent.release();
            streamEnded.set(nettyContent.isLast());
          } else {
            bytesWritten += currentBytesWritten;
          }

          currentBytesWritten = 0;
          if (!streamEnded.get() && requestContents.size() > 0) {
            currentBytesWritten = requestContents.get(0).read(channel);
          }
        }
      } finally {
        contentLock.unlock();
      }
    }
    return bytesWritten;
  }

  /**
   * Adds some content in the form of {@link HttpContent} to this RestRequest. This content will be available to read
   * through the read operations.
   * @throws IllegalStateException if content is being added when it is not expected (GET, DELETE, HEAD).
   * @throws ClosedChannelException if request channel has been closed.
   * @throws IllegalArgumentException if {@code httpContent} is null.
   */
  public void addContent(HttpContent httpContent)
      throws ClosedChannelException {
    if (!RestMethod.POST.equals(getRestMethod()) && (!(httpContent instanceof LastHttpContent)
        || httpContent.content().readableBytes() > 0)) {
      throw new IllegalStateException("There is no content expected for " + getRestMethod());
    } else {
      NettyContent nettyContent = new NettyContent(httpContent);
      try {
        contentLock.lock();
        if (!isOpen()) {
          throw new ClosedChannelException();
        }
        requestContents.add(nettyContent);
        nettyContent.retain();
      } finally {
        contentLock.unlock();
      }
    }
  }
}

/**
 * Just a wrapper over {@link HttpContent} that helps convert the data inside into a form that can be used to write into
 * a {@link WritableByteChannel}.
 */
class NettyContent {

  private final HttpContent content;
  private final ByteBuffer contentBuffer;
  private final boolean isLast;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Wraps the {@code content} so that is easier to read.
   * @param content the {@link HttpContent} that needs to be wrapped.
   * @throws IllegalArgumentException if {@code content} is null.
   */
  public NettyContent(HttpContent content) {
    if (content == null) {
      throw new IllegalArgumentException("Received null HttpContent");
    } else if (content.content().nioBufferCount() > 0) {
      // not a copy.
      contentBuffer = content.content().nioBuffer();
      this.content = content;
    } else {
      // this will not happen (looking at current implementations of ByteBuf in Netty), but if it does, we cannot avoid
      // a copy (or we can introduce a read(GatheringByteChannel) method in ReadableStreamChannel if required).
      logger.warn("Http content had to be copied because ByteBuf did not have a backing ByteBuffer");
      contentBuffer = ByteBuffer.allocate(content.content().capacity());
      content.content().readBytes(contentBuffer);
      // no need to retain content since we have a copy.
      this.content = null;
    }
    // LastHttpContent in the end marker in netty http world.
    isLast = content instanceof LastHttpContent;
  }

  /**
   * Used to check if this is the last chunk of a particular request.
   * @return whether this is the last chunk.
   */
  public boolean isLast() {
    return isLast;
  }

  /**
   * Writes the underlying content into the provided {@code channel}. Returns number of bytes written. If end of stream
   * has been reached, returns -1.
   * @param channel the {@link WritableByteChannel} to write data into.
   * @return the number of bytes written. Can be 0. Returns -1 if there is no more data to read.
   * @throws IOException if there was an I/O error while writing to the channel.
   */
  public int read(WritableByteChannel channel)
      throws IOException {
    int bytesWritten = -1;
    if (contentBuffer.hasRemaining()) {
      bytesWritten = channel.write(contentBuffer);
    }
    return bytesWritten;
  }

  /**
   * If required, increase the reference count of the underlying {@link HttpContent} so that the it is not lost to
   * recycling before processing is complete.
   */
  public void retain() {
    if (content != null) {
      ReferenceCountUtil.retain(content);
    }
  }

  /**
   * If required,, decrease the reference count of the underlying {@link HttpContent} so that it can be recycled, clean
   * up any resources and do work that needs to be done at the end of the lifecycle.
   */
  public void release() {
    if (content != null) {
      ReferenceCountUtil.release(content);
    }
  }
}
