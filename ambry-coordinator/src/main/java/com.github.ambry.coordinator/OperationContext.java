package com.github.ambry.coordinator;

import java.util.Random;

/**
 * Context that, with high probability, uniquely identifies an operation.
 */
public class OperationContext {
  String clientId;
  int correlationId;

  public OperationContext(String clientId) {
    this.clientId = clientId;
    this.correlationId = new Random().nextInt();
  }

  public String getClientId() {
    return clientId;
  }

  public int getCorrelationId() {
    return correlationId;
  }

  @Override
  public String toString() {
    return "OpContext{" + clientId + ':' + correlationId + '}';
  }
}
