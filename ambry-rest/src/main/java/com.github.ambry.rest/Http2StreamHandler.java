package com.github.ambry.rest;

import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.stream.ChunkedWriteHandler;


public class Http2StreamHandler extends ChannelInboundHandlerAdapter {

  private NettyMetrics nettyMetrics;
  private NettyConfig nettyConfig;
  private PerformanceConfig performanceConfig;
  private RestRequestHandler requestHandler;

  public Http2StreamHandler(NettyMetrics nettyMetrics, NettyConfig nettyConfig, PerformanceConfig performanceConfig,
      RestRequestHandler requestHandler) {
    this.nettyMetrics = nettyMetrics;
    this.nettyConfig = nettyConfig;
    this.performanceConfig = performanceConfig;
    this.requestHandler = requestHandler;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    ctx.pipeline().addLast(new Http2StreamFrameToHttpObjectCodec(true));
    // for safe writing of chunks for responses
    ctx.pipeline().addLast("chunker", new ChunkedWriteHandler());
    ctx.pipeline().addLast(new NettyMessageProcessor(nettyMetrics, nettyConfig, performanceConfig, requestHandler));
  }
}
