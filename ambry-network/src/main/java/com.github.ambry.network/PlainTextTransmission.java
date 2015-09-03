package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;


/**
 * PlainTextTransmission to interact with a given socketChannel in plain text
 */
public class PlainTextTransmission extends Transmission {

  public PlainTextTransmission(String connectionId, SocketChannel socketChannel, SelectionKey key, Time time,
      NetworkMetrics metrics, Logger logger) {
    super(connectionId, socketChannel, key, time, metrics, logger);
  }

  void prepare()
      throws IOException {
  }

  boolean ready() {
    return true;
  }

  /**
   * Reads a sequence of bytes from the channel into the networkReceive
   *
   * @return The number of bytes read, possible zero or -1 if the channel has reached end-of-stream
   * @throws IOException if some other I/O error occurs
   */
  @Override
  public long read()
      throws IOException {
    if (!hasReceive()) {
      networkReceive = new NetworkReceive(getConnectionId(), new BoundedByteBufferReceive(), time);
    }
    long bytesRead = networkReceive.getReceivedBytes().readFrom(socketChannel);
    return bytesRead;
  }

  /**
   * Writes a sequence of bytes to the channel from the payload in networkSend
   *
   * @returns The number of bytes written, possibly zero, or -1 if the channel has reached end-of-stream
   * @throws IOException If some other I/O error occurs
   */
  @Override
  public boolean write()
      throws IOException {
    Send send = networkSend.getPayload();
    if (send == null) {
      throw new IllegalStateException("Registered for write interest but no response attached to key.");
    }
    send.writeTo(socketChannel);
    logger
        .trace("Bytes written to {} using key {}", socketChannel.socket().getRemoteSocketAddress(), getConnectionId());
    return send.isSendComplete();
  }

  /**
   * Close the connection for the socket channel
   */
  public void close()
      throws IOException {
    key.attach(null);
    key.cancel();
    try {
      socketChannel.socket().close();
      socketChannel.close();
    } catch (IOException e) {
      metrics.selectorCloseSocketErrorCount.inc();
      logger.error("Exception closing connection to node {}:", getConnectionId(), e);
    }
  }
}
