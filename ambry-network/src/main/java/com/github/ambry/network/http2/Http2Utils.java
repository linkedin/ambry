/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network.http2;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a utility class used by HTTP2.
 */
public class Http2Utils {

  private static final Logger logger = LoggerFactory.getLogger(Http2ClientResponseHandler.class);

  static void releaseAndCloseStreamChannel(Channel streamChannel) {
    logger.info("Stream channel is being closed. Stream: {}, Parent: {}", streamChannel, streamChannel.parent());
    streamChannel.attr(Http2NetworkClient.REQUEST_INFO).set(null);
    streamChannel.parent()
        .attr(Http2MultiplexedChannelPool.HTTP2_MULTIPLEXED_CHANNEL_POOL)
        .get()
        .release(streamChannel);
  }
}
