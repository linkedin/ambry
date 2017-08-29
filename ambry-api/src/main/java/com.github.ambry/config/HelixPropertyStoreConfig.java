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
package com.github.ambry.config;

/**
 * The config that is needed to operate a {@code HelixPropertyStore}.
 */
public class HelixPropertyStoreConfig {
  public static final String HELIX_PROPERTY_STORE_PREFIX = "helix.property.store.";
  public static final String INVALID_ZK_CLIENT_CONNECT_STRING = "";

  /**
   * Time in ms to time out a connection to a ZooKeeper server.
   */
  @Config(HELIX_PROPERTY_STORE_PREFIX + "zk.client.connection.timeout.ms")
  @Default("20 * 1000")
  public final int zkClientConnectionTimeoutMs;

  /**
   * Time in ms defines disconnection tolerance by a session. I.e., if reconnected within this time, it will
   * be considered as the same session.
   */
  @Config(HELIX_PROPERTY_STORE_PREFIX + "zk.client.session.timeout.ms")
  @Default("20 * 1000")
  public final int zkClientSessionTimeoutMs;

  /**
   * The ZooKeeper server address. This config is required when using {@code HelixAccountService}, but not for
   * {@code InMemoryUnknownAccountService}.
   */
  @Config(HELIX_PROPERTY_STORE_PREFIX + "zk.client.connect.string")
  @Default(INVALID_ZK_CLIENT_CONNECT_STRING)
  public final String zkClientConnectString;

  /**
   * The root path of helix property store in the ZooKeeper. Must start with {@code /}, and must not end with {@code /}.
   * It is recommended to make root path in the form of {@code /ambry/<clustername>/helixPropertyStore}
   */
  @Config(HELIX_PROPERTY_STORE_PREFIX + "root.path")
  @Default("/ambry/defaultCluster/helixPropertyStore")
  public final String rootPath;

  /**
   * The time interval in second between two consecutive account pulling for the background account updater of
   * {@code HelixAccountService}. Setting to 0 to disable it.
   */
  @Config(HELIX_PROPERTY_STORE_PREFIX + "account.service.polling.interval.ms")
  @Default("60 * 60 * 1000")
  // @todo This config by its nature should not appear in HelixPropertyStoreConfig. An ultimate fix would require
  // @todo separation between HelixAccount-related and Notifier-related configs, and this config should go to the
  // @todo HelixAccountServiceConfig.
  public final int accountServicePollingIntervalMs;

  /**
   * The timeout in ms to shut down the account updater of {@code HelixAccountService}.
   */
  @Config(HELIX_PROPERTY_STORE_PREFIX + "account.service.shut.down.timeout.ms")
  @Default("60 * 1000")
  // @todo This config by its nature should not appear in HelixPropertyStoreConfig. An ultimate fix would require
  // @todo separation between HelixAccount-related and Notifier-related configs, and this config should go to the
  // @todo HelixAccountServiceConfig.
  public final int accountUpdaterShutDownTimeoutMs;

  public HelixPropertyStoreConfig(VerifiableProperties verifiableProperties) {
    zkClientConnectionTimeoutMs =
        verifiableProperties.getIntInRange(HELIX_PROPERTY_STORE_PREFIX + "zk.client.connection.timeout.ms", 20 * 1000,
            1, Integer.MAX_VALUE);
    zkClientSessionTimeoutMs =
        verifiableProperties.getIntInRange(HELIX_PROPERTY_STORE_PREFIX + "zk.client.session.timeout.ms", 20 * 1000, 1,
            Integer.MAX_VALUE);
    zkClientConnectString = verifiableProperties.getString(HELIX_PROPERTY_STORE_PREFIX + "zk.client.connect.string",
        INVALID_ZK_CLIENT_CONNECT_STRING);
    rootPath = verifiableProperties.getString(HELIX_PROPERTY_STORE_PREFIX + "root.path",
        "/ambry/defaultCluster/helixPropertyStore");
    accountServicePollingIntervalMs =
        verifiableProperties.getIntInRange(HELIX_PROPERTY_STORE_PREFIX + "account.service.polling.interval.ms",
            60 * 60 * 1000, 0, Integer.MAX_VALUE);
    accountUpdaterShutDownTimeoutMs =
        verifiableProperties.getIntInRange(HELIX_PROPERTY_STORE_PREFIX + "account.service.shut.down.timeout.ms",
            60 * 1000, 1, Integer.MAX_VALUE);
  }
}
