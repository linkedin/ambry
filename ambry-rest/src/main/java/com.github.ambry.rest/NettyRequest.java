package com.github.ambry.rest;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Netty specific implementation of {@link RestRequest}.
 * <p/>
 * Just a wrapper over {@link HttpRequest}.
 */
class NettyRequest implements RestRequest {
  private final QueryStringDecoder query;
  private final HttpRequest request;
  private final RestMethod restMethod;

  private final ReentrantLock contentLock = new ReentrantLock();
  private final List<RestRequestContent> requestContents = new LinkedList<RestRequestContent>();
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
    // TODO: return headers also.
    return query.parameters();
  }

  @Override
  public void addContent(RestRequestContent restRequestContent)
      throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    try {
      contentLock.lock();
      requestContents.add(restRequestContent);
      restRequestContent.retain();
    } finally {
      contentLock.unlock();
    }
  }

  @Override
  public void retain() {
    //nothing to do
  }

  @Override
  public void release() {
    //nothing to do
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
        Iterator<RestRequestContent> requestContentIterator = requestContents.iterator();
        while (requestContentIterator.hasNext()) {
          requestContentIterator.next().close();
          requestContentIterator.remove();
        }
        // no need to release() because Netty request objects are not reference counted.
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
   * Returns whatever is available as a part of the "content-length" header. If there is no such header, tries to infer
   * content size. If that cannot be done, returns 0.
   * <p/>
   * This function does not individually count the bytes in the content (it is not possible) so the bytes received may
   * actually be different if the stream is buggy or the client made a mistake. Do *not* treat this as fully accurate.
   * @return the size of content as defined in the "content-length" header. Might not be actual length of content if
   *          the stream is buggy.
   */
  @Override
  public long getSize() {
    return HttpHeaders.getContentLength(request, 0);
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
        // We read from the RestRequestContent at the head of the list until :-
        // 1. The writable channel can hold no more data or there is no more data immediately available - while loop
        //      ends.
        // 2. The RestRequestContent runs out of content - remove it from the head of the list and start reading from
        //      new head if it is available.
        // Content may be added at any time and it is not necessary that the list have any elements at the time of
        // reading. Read returns -1 only when the a RestRequestContent with isLast() true is read. If stream has not
        // ended and there is no content in the list, we return 0.
        // Cases to consider:
        // 1. Writable channel can consume no content (nothing to do. Should return 0).
        // 2. Writable channel can consume data limited to one RestRequestContent (might rollover).
        //      a. There is content available in the RestRequestContent at the head of the list (don't rollover).
        //      b. Request content stream ended when we tried to read from the RestRequestContent at the head of the
        //          list (end of stream).
        //      b. There is no content available right now in the RestRequestContent at the head of the list but it has
        //          not finished its content (don't rollover).
        //      c. There is no content available in the RestRequestContent at the head of the list because it
        //          just finished its content (rollover).
        //            i. More RestRequestContent available in the list (continue read).
        //            ii. No more RestRequestContent in the list currently (cannot continue read).
        // 3. Writable channel can consume data across RestRequestContents (will rollover).
        //      a. More RestRequestContent is available in the list (continue read).
        //      b. Request content stream has not ended but more RestRequestContent is not available in the list (cannot
        //          continue read).
        //      c. Request content stream has ended (end of stream).
        int currentBytesWritten = requestContents.size() > 0 ? requestContents.get(0).read(channel) : 0;
        while (currentBytesWritten != 0) {
          if (currentBytesWritten == -1) {
            RestRequestContent restRequestContent = requestContents.remove(0);
            restRequestContent.close();
            streamEnded.set(restRequestContent.isLast());
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
}
