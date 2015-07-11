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
