/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network;

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransmissionFactory {
  private static final Logger logger = LoggerFactory.getLogger(SSLTransmission.class);

  public static Transmission getTransmission(String connectionId, SocketChannel socketChannel, SelectionKey key,
      String remoteHost, int remotePort, Time time, NetworkMetrics metrics, PortType portType, SSLFactory sslFactory,
      SSLFactory.Mode mode) throws IOException {
    if (portType == PortType.PLAINTEXT) {
      return new PlainTextTransmission(connectionId, socketChannel, key, time, metrics);
    } else if (portType == PortType.SSL) {
      SSLTransmission sslTransmission = null;
      try {
        sslTransmission =
            new SSLTransmission(sslFactory, connectionId, socketChannel, key, remoteHost, remotePort, time, metrics,
                mode);
        metrics.sslTransmissionInitializationCount.inc();
      } catch (IOException e) {
        metrics.sslTransmissionInitializationErrorCount.inc();
        logger.error("SSLTransmission initialization error ", e);
        throw e;
      }
      return sslTransmission;
    } else {
      throw new IllegalArgumentException("UnSupported portType " + portType + " passed in");
    }
  }
}
