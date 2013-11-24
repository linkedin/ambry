package com.github.ambry.metrics;

public enum MetricsType {
  MetricsCounter("MetricsCounter"), MetricsGauge("MetricsGauge");

  private final String str;

  private MetricsType(String str) {
    this.str = str;
  }

  public String toString() {
    return str;
  }
}
