package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;


/**
 * ChannelWrapper to interact with a given socketChannel using ssl encryption
 */
public class SSLChannelWrapper extends ChannelWrapper {

  public SSLChannelWrapper(String connectionId, SocketChannel socketChannel, SelectionKey key, Time time,
      NetworkMetrics metrics, Logger logger) {
    super(connectionId, socketChannel, key, time, metrics, logger);
  }
}
