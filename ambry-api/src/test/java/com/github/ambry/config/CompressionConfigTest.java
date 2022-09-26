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

import com.github.ambry.compression.LZ4Compression;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

public class CompressionConfigTest {

  @Test
  public void emptyConstructorTest() {
    CompressionConfig config = new CompressionConfig();
    Assert.assertTrue(config.isCompressionEnabled);
  }

  @Test
  public void constructorTest() {
    // Boolean whether compression is enabled in PUT operation.
    Properties properties = new Properties();
    properties.put("router.compression.enabled", "false");
    properties.put("router.compression.skip.content.encoding", "false");
    properties.put("router.compression.other.content.types", "true");
    properties.put("router.compression.minimal.content.size", "100");
    properties.put("router.compression.minimal.ratio", "1.5");
    properties.put("router.compression.algorithm.name", LZ4Compression.ALGORITHM_NAME);
    properties.put("router.compression.content.types.compressible", "text/111, text/222; charset=UTF8");
    properties.put("router.compression.content.types.incompressible", "image/111,image/222   ,  image/333");
    properties.put("router.compression.content.prefixes.compressible", "comp1/*, comp2  , comp3");
    properties.put("router.compression.content.prefixes.incompressible", "incomp1/*, incomp2  , incomp3");

    CompressionConfig config = new CompressionConfig(new VerifiableProperties(properties));
    Assert.assertFalse(config.isCompressionEnabled);
    Assert.assertFalse(config.isSkipWithContentEncoding);
    Assert.assertTrue(config.compressOtherContentTypes);
    Assert.assertEquals(LZ4Compression.ALGORITHM_NAME, config.algorithmName);
    Assert.assertEquals(100, config.minimalSourceDataSizeInBytes);
    Assert.assertEquals(1.5, config.minimalCompressRatio, 0);

    // Test content-type specified in compressible content-type.
    Assert.assertTrue(config.isCompressibleContentType("text/111"));
    Assert.assertTrue(config.isCompressibleContentType("TEXT/222"));
    Assert.assertTrue(config.isCompressibleContentType("Text/222; charset=UTF8"));

    // Test content-type specified in compressible context-type prefix.
    Assert.assertTrue(config.isCompressibleContentType("comp1/111"));
    Assert.assertTrue(config.isCompressibleContentType("Comp2/222"));
    Assert.assertTrue(config.isCompressibleContentType("COMP3/222; charset=UTF8"));

    // Test content-type specified in incompressible content-type.
    Assert.assertFalse(config.isCompressibleContentType("image/111"));
    Assert.assertFalse(config.isCompressibleContentType("IMAGE/222"));
    Assert.assertFalse(config.isCompressibleContentType("image/222; charset=UTF8"));
    Assert.assertFalse(config.isCompressibleContentType("Image/333"));

    // Test content-type specified in incompressible content-type prefix.
    Assert.assertFalse(config.isCompressibleContentType("incomp1/111"));
    Assert.assertFalse(config.isCompressibleContentType("INCOmp2/222"));
    Assert.assertFalse(config.isCompressibleContentType("Incomp3/111"));

    // Test unknown content-type.
    Assert.assertTrue(config.isCompressibleContentType("unknown/111"));
    Assert.assertTrue(config.isCompressibleContentType(""));
  }
}