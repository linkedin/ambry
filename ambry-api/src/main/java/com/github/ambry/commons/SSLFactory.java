/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.commons;

import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.Utils;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;


/**
 * Simplifies the creation of new {@link SSLEngine}s and {@link SSLContext} objects that can be used by components
 * that require SSL support.
 */
public interface SSLFactory {
  /**
   * Create {@link SSLEngine} for given host name and port number.
   * This engine manages the handshake process and encryption/decryption with this remote host.
   * @param peerHost The remote host name
   * @param peerPort The remote port number
   * @param mode The local SSL mode, Client or Server
   * @return SSLEngine
   */
  SSLEngine createSSLEngine(String peerHost, int peerPort, Mode mode);

  /**
   * Returns a configured {@link SSLContext}. This context supports creating {@link SSLEngine}s and for the loaded
   * truststore and keystore. An {@link SSLEngine} must be created for each connection.
   * @return SSLContext.
   */
  SSLContext getSSLContext();

  /**
   * Whether the ssl engine should operate in client or server mode.
   */
  enum Mode {
    CLIENT, SERVER
  }

  /**
   * Instantiate {@link SSLFactory} based on the provided config. Uses the {@link SSLConfig#sslFactory} class name
   * to choose the desired implementation to instantiate via reflection.
   * @param sslConfig the {@link SSLConfig} provided to the {@link SSLFactory} that is instantiated.
   * @return a new {@link SSLFactory} based on the provided config.
   * @throws Exception
   */
  static SSLFactory getNewInstance(SSLConfig sslConfig) throws Exception {
    return Utils.getObj(sslConfig.sslFactory, sslConfig);
  }
}
