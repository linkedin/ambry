package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;


public class TransmissionFactory {
  public static Transmission getChannelWrapper(String connectionId, SocketChannel socketChannel, SelectionKey key,
      String remoteHost, int remotePort, Time time, NetworkMetrics metrics, Logger logger, PortType portType,
      SSLFactory sslFactory)
      throws IOException {
    try {
      if (portType == PortType.PLAINTEXT) {
        return new PlainTextTransmission(connectionId, socketChannel, key, time, metrics, logger);
      } else if (portType == PortType.SSL) {
        return new SSLTransmission(sslFactory, connectionId, socketChannel, key, remoteHost, remotePort, time, metrics,
            logger);
      } else {
        throw new IllegalArgumentException("UnSupported portType " + portType + " passed in");
      }
    } catch (GeneralSecurityException e) {
      logger.error("GeneralSecurityException thrown  " + e);
      throw new IOException("GeneralSecurityException converted to IOException ", e);
    }
  }
}
