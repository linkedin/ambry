package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransmissionFactory {
  public static Transmission getTransmission(String connectionId, SocketChannel socketChannel, SelectionKey key,
      String remoteHost, int remotePort, Time time, NetworkMetrics metrics, PortType portType, SSLFactory sslFactory,
      SSLFactory.Mode mode)
      throws IOException {
    if (portType == PortType.PLAINTEXT) {
      return new PlainTextTransmission(connectionId, socketChannel, key, time, metrics);
    } else if (portType == PortType.SSL) {
      return new SSLTransmission(sslFactory, connectionId, socketChannel, key, remoteHost, remotePort, time, metrics,
          mode);
    } else {
      throw new IllegalArgumentException("UnSupported portType " + portType + " passed in");
    }
  }
}
