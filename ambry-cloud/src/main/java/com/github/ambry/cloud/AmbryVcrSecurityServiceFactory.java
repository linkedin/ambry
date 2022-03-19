/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.ServerSecurityService;
import com.github.ambry.rest.ServerSecurityServiceFactory;
import java.io.IOException;
import javax.net.ssl.SSLSession;


/**
 * Default implementation of {@link ServerSecurityServiceFactory} for Ambry VCR
 * <p/>
 * Returns a new instance of {@link ServerSecurityService} that does nothing.
 */
public class AmbryVcrSecurityServiceFactory implements ServerSecurityServiceFactory {

  public AmbryVcrSecurityServiceFactory(VerifiableProperties verifiableProperties, ServerMetrics serverMetrics,
      MetricRegistry metricRegistry) {
  }

  @Override
  public ServerSecurityService getServerSecurityService() throws InstantiationException {
    return new ServerSecurityService() {
      @Override
      public void validateConnection(SSLSession sslSession, Callback<Void> callback) {

      }

      @Override
      public void validateRequest(RestRequest restRequest, Callback<Void> callback) {

      }

      @Override
      public void close() throws IOException {

      }
    };
  }
}
