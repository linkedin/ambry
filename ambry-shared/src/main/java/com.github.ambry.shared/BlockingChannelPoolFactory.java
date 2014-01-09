package com.github.ambry.shared;

import com.github.ambry.shared.BlockingChannelPool;

/**
 *  Factory to create BlockingChannelPool.
 */
public interface BlockingChannelPoolFactory {
  public BlockingChannelPool getBlockingChannelPool();
}
