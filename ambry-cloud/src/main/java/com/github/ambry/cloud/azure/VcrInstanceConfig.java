/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.cloud.azure;

import org.apache.helix.model.InstanceConfig;
import java.util.Objects;
import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.config.CloudConfig.*;

/**
 * A data object for configs scoped to a single Vcr node.
 */
public class VcrInstanceConfig {
  private final Integer sslPort;
  private final Integer http2Port;
  private final boolean configReady;

  /**
   * @param sslPort the ssl port, or {@code null} if the server does not have one.
   * @param http2Port the HTTP2 port, or {@code null} if the server does not have one.
   * @param configReady it's true if participant is ready
   */
  public VcrInstanceConfig(Integer sslPort, Integer http2Port, boolean configReady) {
    this.sslPort = sslPort;
    this.http2Port = http2Port;
    this.configReady = configReady;
  }

  /**
   * Compare two {@link VcrInstanceConfig}
   * @param o the other {@link VcrInstanceConfig} to compare with this {@link VcrInstanceConfig}.
   * @return {@code true} if two {@link VcrInstanceConfig}s are equivalent.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VcrInstanceConfig that = (VcrInstanceConfig) o;
    return Objects.equals(sslPort, that.sslPort) && Objects.equals(http2Port, that.http2Port)
          && (configReady == that.configReady);
  }

  /**
   * Convert Helix {@link InstanceConfig} to {@link VcrInstanceConfig}
   * @param instanceConfig the helix {@link InstanceConfig}
   * @return {@link VcrInstanceConfig} which is converted from the {@link InstanceConfig}
   */
  public static VcrInstanceConfig toVcrInstanceConfig(InstanceConfig instanceConfig) {
      String sslPortStr = instanceConfig.getRecord().getSimpleField(SSL_PORT_STR);
      Integer sslPort = (sslPortStr == null) ? null : Integer.valueOf(sslPortStr);
      String httpPortStr = instanceConfig.getRecord().getSimpleField(HTTP2_PORT_STR);
      Integer http2Port = (httpPortStr == null) ? null : Integer.valueOf(httpPortStr);
      boolean configReady = instanceConfig.getRecord().getBooleanField(VCR_HELIX_CONFIG_READY, false);
      return new VcrInstanceConfig(sslPort, http2Port, configReady);
  }

  /**
   * Based on this {@link VcrInstanceConfig} to update the {@link InstanceConfig}
   * @param instanceConfig the helix {@link InstanceConfig}
   */
  public void updateInstanceConfig(InstanceConfig instanceConfig) {
    if (sslPort != null) {
      instanceConfig.getRecord().setSimpleField(SSL_PORT_STR, Integer.toString(sslPort));
    }
    if (http2Port != null) {
      instanceConfig.getRecord().setSimpleField(HTTP2_PORT_STR, Integer.toString(http2Port));
    }
    instanceConfig.getRecord().setBooleanField(VCR_HELIX_CONFIG_READY, configReady);
  }
}
