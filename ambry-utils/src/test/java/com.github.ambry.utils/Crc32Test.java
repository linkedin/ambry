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
package com.github.ambry.utils;

import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test to ensure that the checksum class works fine
 */
public class Crc32Test {
  @Test
  public void crcTest() {
    Crc32 crc = new Crc32();
    byte[] buf = new byte[4000];
    new Random().nextBytes(buf);
    crc.update(buf, 0, 4000);
    long value1 = crc.getValue();
    crc = new Crc32();
    crc.update(buf, 0, 4000);
    long value2 = crc.getValue();
    Assert.assertEquals(value1, value2);
    buf[3999] = (byte) (~buf[3999]);
    crc = new Crc32();
    crc.update(buf, 0, 4000);
    long value3 = crc.getValue();
    Assert.assertFalse(value1 == value3);
  }
}
