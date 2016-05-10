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
package com.github.ambry.coordinator;

import com.github.ambry.commons.ResponseHandler;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Context that, with high probability, uniquely identifies an operation.
 */
public class OperationContext {
  // currentCount wrapping around from MAX_VALUE to MIN_VALUE is OK. This should take long enough that log files
  // close together in time (days to weeks) contain only unique correlation ids.
  private static final AtomicInteger currentCount = new AtomicInteger(Integer.MIN_VALUE);
  private String clientId;
  private int correlationId;
  private int connectionPoolCheckoutTimeout;
  private boolean crossDCProxyCallEnable;
  private CoordinatorMetrics coordinatorMetrics;
  private ResponseHandler responseHandler;
  private ArrayList<String> sslEnabledDatacenters;

  public OperationContext(String clientId, int connectionPoolCheckoutTimeout, boolean crossDCProxyCallEnable,
      CoordinatorMetrics coordinatorMetrics, ResponseHandler responseHandler, ArrayList<String> sslEnabledDatacenters) {
    this.clientId = clientId;
    this.correlationId = currentCount.incrementAndGet();
    this.connectionPoolCheckoutTimeout = connectionPoolCheckoutTimeout;
    this.coordinatorMetrics = coordinatorMetrics;
    this.crossDCProxyCallEnable = crossDCProxyCallEnable;
    this.responseHandler = responseHandler;
    this.sslEnabledDatacenters = sslEnabledDatacenters;
  }

  public String getClientId() {
    return clientId;
  }

  public int getCorrelationId() {
    return correlationId;
  }

  public int getConnectionPoolCheckoutTimeout() {
    return connectionPoolCheckoutTimeout;
  }

  public CoordinatorMetrics getCoordinatorMetrics() {
    return coordinatorMetrics;
  }

  public boolean isCrossDCProxyCallEnabled() {
    return crossDCProxyCallEnable;
  }

  public ResponseHandler getResponseHandler() {
    return responseHandler;
  }

  public ArrayList<String> getSslEnabledDatacenters(){
    return this.sslEnabledDatacenters;
  }

  @Override
  public String toString() {
    return "OpContext{" + clientId + ':' + correlationId + ':' + connectionPoolCheckoutTimeout + ':'
        + crossDCProxyCallEnable + '}';
  }
}
