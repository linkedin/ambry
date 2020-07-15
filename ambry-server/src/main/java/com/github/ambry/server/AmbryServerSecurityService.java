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

package com.github.ambry.server;

import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.commons.Callback;
import javax.net.ssl.SSLSession;


/**
 * Default implementation of {@link ServerSecurityService} for Ambry that doesn't do any validations.
 */
public class AmbryServerSecurityService implements ServerSecurityService {
  private boolean isOpen;
  private final ServerConfig serverConfig;
  private final ServerMetrics serverMetrics;

  public AmbryServerSecurityService(ServerConfig serverConfig, ServerMetrics serverMetrics) {
    this.serverConfig = serverConfig;
    this.serverMetrics = serverMetrics;
    this.isOpen = true;
  }

  @Override
  public void validateConnection(SSLSession sslSession, Callback<Void> callback) {
    Exception exception = null;
    serverMetrics.securityServiceValidateConnectionRate.mark();
    if (!isOpen) {
      exception = new RestServiceException("ServerSecurityService is closed", RestServiceErrorCode.ServiceUnavailable);
    } else if (sslSession == null) {
      throw new IllegalArgumentException("sslSession is null");
    }
    callback.onCompletion(null, exception);
  }

  @Override
  public void validateRequest(RestRequest restRequest, Callback<Void> callback) {
    Exception exception = null;
    serverMetrics.securityServiceValidateRequestRate.mark();
    if (!isOpen) {
      exception = new RestServiceException("ServerSecurityService is closed", RestServiceErrorCode.ServiceUnavailable);
    } else if (restRequest == null) {
      throw new IllegalArgumentException("restRequest is null");
    }
    callback.onCompletion(null, exception);
  }

  @Override
  public void close() {
    isOpen = false;
  }

}
