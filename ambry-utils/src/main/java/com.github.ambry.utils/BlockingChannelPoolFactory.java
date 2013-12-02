package com.github.ambry.utils;

/**
 *  Factory to create BlockingChannelPool.
 */
public interface BlockingChannelPoolFactory {
  public BlockingChannelPool getBlockingChannelPool();
}
