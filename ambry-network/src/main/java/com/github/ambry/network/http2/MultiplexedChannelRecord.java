/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * Modifications copyright (C) 2020 <Linkedin/zzmao>
 */
package com.github.ambry.network.http2;

import com.github.ambry.commons.NettyUtils;
import com.github.ambry.utils.SystemTime;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains a {@link Future} for the actual socket channel and tracks available stream channels.
 */
public class MultiplexedChannelRecord {
  private static final Logger log = LoggerFactory.getLogger(MultiplexedChannelRecord.class);

  private final Channel parentChannel;
  private final int maxConcurrentStreams;
  private final Long allowedIdleTimeInMs;
  private final int maxContentLength;

  private final AtomicInteger numOfAvailableStreams;
  private volatile long lastReserveAttemptTimeMillis;

  // Only read or write in the connection.eventLoop()
  private final Map<ChannelId, Http2StreamChannel> streamChannels = new HashMap<>();
  private ScheduledFuture<?> closeIfIdleTask;

  // Only write in the connection.eventLoop()
  private volatile RecordState state = RecordState.OPEN;

  private volatile int lastStreamId;

  MultiplexedChannelRecord(Channel parentChannel, int maxConcurrentStreams, Long allowedIdleTimeInMs,
      int maxContentLength) {
    this.parentChannel = parentChannel;
    this.maxConcurrentStreams = maxConcurrentStreams;
    this.numOfAvailableStreams = new AtomicInteger(maxConcurrentStreams);
    this.allowedIdleTimeInMs = allowedIdleTimeInMs;
    this.maxContentLength = maxContentLength;
  }

  AtomicInteger getNumOfAvailableStreams() {
    return numOfAvailableStreams;
  }

  boolean acquireStream(Promise<Channel> promise) {
    if (claimStream()) {
      releaseClaimOnFailure(promise);
      acquireClaimedStream(promise);
      return true;
    }
    log.warn("No available streams on this connection: {}", parentChannel.remoteAddress());
    return false;
  }

  void acquireClaimedStream(Promise<Channel> promise) {
    NettyUtils.doInEventLoop(parentChannel.eventLoop(), () -> {
      if (state != RecordState.OPEN) {
        String message;
        // GOAWAY
        if (state == RecordState.CLOSED_TO_NEW) {
          message = String.format("Connection %s received GOAWAY with Last Stream ID %d. Unable to open new "
              + "streams on this connection.", parentChannel, lastStreamId);
        } else {
          message = String.format("Connection %s was closed while acquiring new stream.", parentChannel);
        }
        log.warn(message);
        promise.setFailure(new IOException(message));
        return;
      }

      Future<Http2StreamChannel> streamFuture =
          new Http2StreamChannelBootstrap(parentChannel).open();
      // handler are added when stream is returned to claimer.
      streamFuture.addListener((GenericFutureListener<Future<Http2StreamChannel>>) future -> {
        NettyUtils.warnIfNotInEventLoop(parentChannel.eventLoop());

        if (!future.isSuccess()) {
          promise.setFailure(future.cause());
          return;
        }

        Http2StreamChannel channel = future.getNow();
        streamChannels.put(channel.id(), channel);
        promise.setSuccess(channel);

        if (closeIfIdleTask == null && allowedIdleTimeInMs != null && allowedIdleTimeInMs > 0) {
          enableCloseIfIdleTask();
        }
      });
    }, promise);
  }

  private void enableCloseIfIdleTask() {
    log.info("enableCloseIfIdleTask is enabled.");
    NettyUtils.warnIfNotInEventLoop(parentChannel.eventLoop());

    // Don't poll more frequently than 800 ms. Being overly-conservative is okay. Blowing up our CPU is not.
    long taskFrequencyMillis = Math.max(allowedIdleTimeInMs, 800);

    closeIfIdleTask = parentChannel.eventLoop()
        .scheduleAtFixedRate(this::closeIfIdle, taskFrequencyMillis, taskFrequencyMillis, TimeUnit.MILLISECONDS);
    parentChannel.closeFuture().addListener(f -> closeIfIdleTask.cancel(false));
  }

  private void releaseClaimOnFailure(Promise<Channel> promise) {
    try {
      promise.addListener(f -> {
        if (!promise.isSuccess()) {
          releaseClaim();
        }
      });
    } catch (Throwable e) {
      releaseClaim();
      throw e;
    }
  }

  private void releaseClaim() {
    if (numOfAvailableStreams.incrementAndGet() > maxConcurrentStreams) {
      assert false;
      log.warn("Child channel count was caught attempting to be increased over max concurrency. "
          + "Please report this issue to the AWS SDK for Java team.");
      numOfAvailableStreams.decrementAndGet();
    }
  }

  /**
   * Handle a {@link Http2GoAwayFrame} on this connection, preventing new streams from being created on it, and closing any
   * streams newer than the last-stream-id on the go-away frame. The GOAWAY frame (type=0x7) is used to initiate shutdown
   * of a connection or to signal serious error conditions. GOAWAY allows an endpoint to gracefully stop accepting new
   * streams while still finishing processing of previously established streams.
   * This enables administrative actions, like server maintenance.
   */
  void handleGoAway(int lastStreamId, GoAwayException exception) {
    NettyUtils.doInEventLoop(parentChannel.eventLoop(), () -> {
      this.lastStreamId = lastStreamId;

      if (state == RecordState.CLOSED) {
        return;
      }

      if (state == RecordState.OPEN) {
        state = RecordState.CLOSED_TO_NEW;
      }

      // Create a copy of the children to close, because fireExceptionCaught may remove from the childChannels.
      List<Http2StreamChannel> childrenToClose = new ArrayList<>(streamChannels.values());
      childrenToClose.stream()
          .filter(cc -> cc.stream().id() > lastStreamId)
          .forEach(cc -> cc.pipeline().fireExceptionCaught(exception));
    });
  }

  /**
   * Close all registered child channels, and prohibit new streams from being created on this connection.
   */
  void closeChildChannels() {
    closeAndExecuteOnChildChannels(ChannelOutboundInvoker::close);
  }

  /**
   * Delivers the exception to all registered child channels, and prohibits new streams being created on this connection.
   */
  void closeChildChannels(Throwable t) {
    closeAndExecuteOnChildChannels(ch -> ch.pipeline().fireExceptionCaught(t));
  }

  private void closeAndExecuteOnChildChannels(Consumer<Channel> childChannelConsumer) {
    NettyUtils.doInEventLoop(parentChannel.eventLoop(), () -> {
      if (state == RecordState.CLOSED) {
        return;
      }
      state = RecordState.CLOSED;

      // Create a copy of the children, because they may be modified by the consumer.
      List<Http2StreamChannel> childrenToClose = new ArrayList<>(streamChannels.values());
      for (Channel childChannel : childrenToClose) {
        childChannelConsumer.accept(childChannel);
      }
    });
  }

  void closeAndReleaseChild(Channel childChannel) {
    childChannel.close();
    NettyUtils.doInEventLoop(parentChannel.eventLoop(), () -> {
      streamChannels.remove(childChannel.id());
      releaseClaim();
    });
  }

  private void closeIfIdle() {
    NettyUtils.warnIfNotInEventLoop(parentChannel.eventLoop());

    // Don't close if we have child channels.
    if (!streamChannels.isEmpty()) {
      return;
    }

    // Don't close if there have been any reserves attempted since the idle connection time.
    long nonVolatileLastReserveAttemptTimeMillis = lastReserveAttemptTimeMillis;
    if (nonVolatileLastReserveAttemptTimeMillis > System.currentTimeMillis() - allowedIdleTimeInMs) {
      return;
    }

    // Cut off new streams from being acquired from this connection by setting the number of available channels to 0.
    // This write may fail if a reservation has happened since we checked the lastReserveAttemptTime.
    if (!numOfAvailableStreams.compareAndSet(maxConcurrentStreams, 0)) {
      return;
    }

    // If we've been closed, no need to shut down.
    if (state != RecordState.OPEN) {
      return;
    }

    log.debug("Connection " + parentChannel + " has been idle for " + (System.currentTimeMillis()
        - nonVolatileLastReserveAttemptTimeMillis) + "ms and will be shut down.");

    // Mark ourselves as closed
    state = RecordState.CLOSED;

    // Start the shutdown process by closing the connection (which should be noticed by the connection pool)
    parentChannel.close();
  }

  public Channel getParentChannel() {
    return parentChannel;
  }

  // Return true if we are not run out of streams.
  private boolean claimStream() {
    lastReserveAttemptTimeMillis = System.currentTimeMillis();
    for (int attempt = 0; attempt < 5; ++attempt) {

      if (state != RecordState.OPEN) {
        return false;
      }

      int currentlyAvailable = numOfAvailableStreams.get();

      if (currentlyAvailable <= 0) {
        return false;
      }
      if (numOfAvailableStreams.compareAndSet(currentlyAvailable, currentlyAvailable - 1)) {
        return true;
      }
    }

    return false;
  }

  boolean canBeClosedAndReleased() {
    return state != RecordState.OPEN && numOfAvailableStreams.get() == maxConcurrentStreams;
  }

  private enum RecordState {
    /**
     * The connection is open and new streams may be acquired from it, if they are available.
     */
    OPEN,

    /**
     * The connection is open, but new streams may not be acquired from it. This occurs when a connection is being
     * shut down (e.g. after it has received a GOAWAY frame), but all streams haven't been closed yet.
     */
    CLOSED_TO_NEW,

    /**
     * The connection is closed and new streams may not be acquired from it.
     */
    CLOSED
  }
}


