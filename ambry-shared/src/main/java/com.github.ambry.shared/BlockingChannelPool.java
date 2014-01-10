package com.github.ambry.shared;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.shared.BlockingChannel;

import java.io.IOException;

/**
 * Interface to pool of BlockingChannels. Any connection that is checked out must eventually be either checked in or
 * destroyed by the caller.
 */
public interface BlockingChannelPool {
  public void start();
  public void shutdown();

  public BlockingChannel checkOutConnection(DataNodeId dataNodeId) throws IOException;
  public void checkInConnection(DataNodeId dataNodeId, BlockingChannel blockingChannel);
  public void destroyConnection(DataNodeId dataNodeId, BlockingChannel blockingChannel);
}
