package com.github.ambry.metrics;


public enum DataType {
  DataLong("DataLong"), DataDouble("DataDouble"), DataString("DataString");

  private final String str;

  private DataType(String str) {
    this.str = str;
  }

  public String toString() {
    return str;
  }
}