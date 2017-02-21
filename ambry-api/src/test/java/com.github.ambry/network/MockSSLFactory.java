/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.VerifiableProperties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;


/**
 * A no-op {@link SSLFactory} implementation.
 */
public class MockSSLFactory implements SSLFactory {
  public MockSSLFactory(VerifiableProperties verifiableProperties) {
  }

  @Override
  public SSLEngine createSSLEngine(String peerHost, int peerPort, Mode mode) {
    return null;
  }

  @Override
  public SSLContext getSSLContext() {
    return null;
  }
}
