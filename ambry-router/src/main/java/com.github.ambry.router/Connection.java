package com.github.ambry.router;

class Connection {
  String host;
  int port;

  @Override
  public String toString() {
    return host + ":" + Integer.toString(port);
  }
}
