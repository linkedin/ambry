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
 */
package com.github.ambry.config;

import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

public class CompressionConfigTest {

  @Test
  public void constructorTest() {
    // Boolean whether compression is enabled in PUT operation.
    Properties properties = new Properties();
    properties.put(CompressionConfig.COMPRESSION_ENABLED, "false");
    properties.put(CompressionConfig.SKIP_IF_CONTENT_ENCODED, "false");
    properties.put(CompressionConfig.MINIMAL_DATA_SIZE_IN_BYTES, "100");
    properties.put(CompressionConfig.MINIMAL_COMPRESS_RATIO, "1.5");
    properties.put(CompressionConfig.ALGORITHM_NAME, "LZ4");
    properties.put(CompressionConfig.COMPRESS_CONTENT_TYPES, "text/111, text/222, comp1, comp2  , comp3");

    CompressionConfig config = new CompressionConfig(new VerifiableProperties(properties));
    Assert.assertFalse(config.isCompressionEnabled);
    Assert.assertFalse(config.isSkipWithContentEncoding);
    Assert.assertEquals("LZ4", config.algorithmName);
    Assert.assertEquals(100, config.minimalSourceDataSizeInBytes);
    Assert.assertEquals(1.5, config.minimalCompressRatio, 0);
  }
}