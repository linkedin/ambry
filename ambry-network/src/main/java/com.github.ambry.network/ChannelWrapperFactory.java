package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;

public class ChannelWrapperFactory {
  public static ChannelWrapper getChannelWrapper(String connectionId, SocketChannel socketChannel, SelectionKey key,
      Time time, NetworkMetrics metrics, Logger logger, PortType portType){
    if(portType == PortType.PLAINTEXT) {
      return new ChannelWrapper(connectionId, socketChannel, key, time, metrics, logger);
    }
    else if(portType == PortType.SSL){
      return new SSLChannelWrapper(connectionId, socketChannel, key, time, metrics, logger);
    }
    else  {
       throw new IllegalArgumentException("UnSupported portType " + portType + " passed in");
    }
  }
}
