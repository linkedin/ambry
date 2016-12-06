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
package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import java.util.Properties;


/**
 * {@link PutGetHelperFactory} for a Router. This factory can be used to fetch
 * {@link ConcurrencyTestTool.RouterPutGetHelper} on {@link #getPutGetHelper()}
 */
public class RouterPutGetHelperFactory implements PutGetHelperFactory {

  private final Properties properties;
  private final String routerFactoryClass;
  private final ClusterMap clusterMap;
  private final int maxBlobSize;
  private final int minBlobSize;

  public RouterPutGetHelperFactory(Properties properties, ClusterMap clusterMap, Integer maxBlobSize,
      Integer minBlobSize) {
    this.properties = properties;
    this.routerFactoryClass = (String) properties.get("rest.server.router.factory");
    this.clusterMap = clusterMap;
    this.maxBlobSize = maxBlobSize;
    this.minBlobSize = minBlobSize;
  }

  /**
   * {@inheritDoc}
   * @return an instance of {@link ConcurrencyTestTool.RouterPutGetHelper}
   * throws Exception if instantiation of the {@link ConcurrencyTestTool.RouterPutGetHelper} fails
   */
  public ConcurrencyTestTool.RouterPutGetHelper getPutGetHelper() throws Exception {
    return new ConcurrencyTestTool.RouterPutGetHelper(properties, clusterMap, routerFactoryClass, maxBlobSize,
        minBlobSize);
  }
}
