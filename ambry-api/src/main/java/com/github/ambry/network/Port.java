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

/**
 * Represents a port containing port number and {@link PortType}
 */
public class Port {
  private final int port;
  private final PortType type;

  public Port(int port, PortType type) {
    this.port = port;
    this.type = type;
  }

  public int getPort() {
    return this.port;
  }

  public PortType getPortType() {
    return this.type;
  }

  @Override
  public String toString() {
    return "Port[" + getPort() + ":" + getPortType() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Port p = (Port) o;
    return p.port == port && p.type.equals(type);
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(port);
  }
}
