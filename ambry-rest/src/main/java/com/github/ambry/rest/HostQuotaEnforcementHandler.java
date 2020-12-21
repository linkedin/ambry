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
package com.github.ambry.rest;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.EnforcementRecommendation;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaMode;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler that enforces host level quota before processing requests.
 */
@ChannelHandler.Sharable
public class HostQuotaEnforcementHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(HostQuotaEnforcementHandler.class);
  private final static byte[] TOO_MANY_REQUESTS = "TOO_MANY_REQUESTS".getBytes();
  private final NettyMetrics metrics;
  private final QuotaManager quotaManager;
  private final QuotaConfig quotaConfig;
  private FullHttpResponse response;

  /**
   * Constructor for {@link HostQuotaEnforcementHandler}.
   * @param nettyMetrics {@link NettyMetrics} object.
   * @param quotaManager {@link QuotaManager} object.
   * @param quotaConfig {@link QuotaConfig} object.
   */
  public HostQuotaEnforcementHandler(NettyMetrics nettyMetrics, QuotaManager quotaManager, QuotaConfig quotaConfig) {
    this.metrics = nettyMetrics;
    this.quotaManager = quotaManager;
    this.quotaConfig = quotaConfig;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    logger.trace("Doing host level checks on channel {}", ctx.name());
    List<EnforcementRecommendation> enforcementRecommendations = new ArrayList<>();
    if (quotaConfig.hostQuotaThrottlingEnabled && quotaManager.shouldThrottleOnHost(enforcementRecommendations)) {
      if (quotaConfig.quotaThrottlingMode == QuotaMode.THROTTLING) {
        // TODO Add a detailed message and possible a retry after header
        response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.TOO_MANY_REQUESTS,
            Unpooled.wrappedBuffer(TOO_MANY_REQUESTS));
        HttpUtil.setKeepAlive(response, false);
        HttpUtil.setContentLength(response, TOO_MANY_REQUESTS.length);
        ctx.write(response);
      } else {
        // TODO send feedback to user in terms of response headers.
        // TODO log metrics internally
        logger.warn("HostQuotaEnforcementHandler recommends throttling due to high system load");
      }
    }
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
  }
}
