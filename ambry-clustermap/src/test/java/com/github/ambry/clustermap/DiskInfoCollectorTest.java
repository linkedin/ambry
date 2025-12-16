/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.clustermap;

import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test for {@link DiskInfoCollector}.
 */
public class DiskInfoCollectorTest {

  /**
   * Test DiskInfo constructor and getters.
   */
  @Test
  public void testDiskInfoConstructorAndGetters() {
    String filesystem = "/dev/sdh1";
    String size = "21T";
    String used = "14T";
    String available = "6.5T";
    int usePercentage = 68;
    String mountPoint = "/mnt/u001/ambrydata";

    DiskInfoCollector.DiskInfo diskInfo = new DiskInfoCollector.DiskInfo(
        filesystem, size, used, available, usePercentage, mountPoint);

    assertEquals("Filesystem should match", filesystem, diskInfo.getFilesystem());
    assertEquals("Size should match", size, diskInfo.getSize());
    assertEquals("Used should match", used, diskInfo.getUsed());
    assertEquals("Available should match", available, diskInfo.getAvailable());
    assertEquals("Use percentage should match", usePercentage, diskInfo.getUsePercentage());
    assertEquals("Mount point should match", mountPoint, diskInfo.getMountPoint());
  }

  /**
   * Test DiskInfo toString method.
   */
  @Test
  public void testDiskInfoToString() {
    DiskInfoCollector.DiskInfo diskInfo = new DiskInfoCollector.DiskInfo(
        "/dev/sdh1", "21T", "14T", "6.5T", 68, "/mnt/u001/ambrydata");

    String result = diskInfo.toString();
    assertTrue("Should contain filesystem", result.contains("/dev/sdh1"));
    assertTrue("Should contain size", result.contains("21T"));
    assertTrue("Should contain used", result.contains("14T"));
    assertTrue("Should contain available", result.contains("6.5T"));
    assertTrue("Should contain use percentage", result.contains("68"));
    assertTrue("Should contain mount point", result.contains("/mnt/u001/ambrydata"));
  }

  /**
   * Test getSizeInBytes with various size formats.
   */
  @Test
  public void testGetSizeInBytes() {
    // Test bytes
    DiskInfoCollector.DiskInfo diskInfo1 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1024", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Bytes should be parsed correctly", 1024L, diskInfo1.getSizeInBytes());

    // Test kilobytes
    DiskInfoCollector.DiskInfo diskInfo2 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1K", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Kilobytes should be parsed correctly", 1024L, diskInfo2.getSizeInBytes());

    // Test megabytes
    DiskInfoCollector.DiskInfo diskInfo3 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1M", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Megabytes should be parsed correctly", 1024L * 1024L, diskInfo3.getSizeInBytes());

    // Test gigabytes
    DiskInfoCollector.DiskInfo diskInfo4 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1G", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Gigabytes should be parsed correctly", 1024L * 1024L * 1024L, diskInfo4.getSizeInBytes());

    // Test terabytes
    DiskInfoCollector.DiskInfo diskInfo5 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1T", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Terabytes should be parsed correctly", 1024L * 1024L * 1024L * 1024L, diskInfo5.getSizeInBytes());

    // Test petabytes
    DiskInfoCollector.DiskInfo diskInfo6 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1P", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Petabytes should be parsed correctly", 1024L * 1024L * 1024L * 1024L * 1024L, diskInfo6.getSizeInBytes());

    // Test exabytes
    DiskInfoCollector.DiskInfo diskInfo7 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1E", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Exabytes should be parsed correctly", 1024L * 1024L * 1024L * 1024L * 1024L * 1024L, diskInfo7.getSizeInBytes());

    // Test decimal values
    DiskInfoCollector.DiskInfo diskInfo8 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1.5G", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Decimal gigabytes should be parsed correctly", (long)(1.5 * 1024L * 1024L * 1024L), diskInfo8.getSizeInBytes());

    // Test case insensitive
    DiskInfoCollector.DiskInfo diskInfo9 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1g", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Lowercase units should be parsed correctly", 1024L * 1024L * 1024L, diskInfo9.getSizeInBytes());
  }

  /**
   * Test getSizeInBytes with invalid formats.
   */
  @Test
  public void testGetSizeInBytesInvalidFormats() {
    // Test null
    DiskInfoCollector.DiskInfo diskInfo1 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", null, "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Null size should return -1", -1L, diskInfo1.getSizeInBytes());

    // Test empty string
    DiskInfoCollector.DiskInfo diskInfo2 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Empty size should return -1", -1L, diskInfo2.getSizeInBytes());

    // Test invalid format
    DiskInfoCollector.DiskInfo diskInfo3 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "invalid", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Invalid size should return -1", -1L, diskInfo3.getSizeInBytes());

    // Test invalid number
    DiskInfoCollector.DiskInfo diskInfo4 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "abcG", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Invalid number should return -1", -1L, diskInfo4.getSizeInBytes());

    // Test unsupported unit
    DiskInfoCollector.DiskInfo diskInfo5 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1Z", "512", "512", 50, "/mnt/u001/ambrydata");
    assertEquals("Unsupported unit should return -1", -1L, diskInfo5.getSizeInBytes());
  }

  /**
   * Test collectDiskInfo method.
   * Note: This test will actually run the 'df -h' command, so results may vary by system.
   */
  @Test
  public void testCollectDiskInfo() {
    Map<String, DiskInfoCollector.DiskInfo> diskInfoMap = DiskInfoCollector.collectDiskInfo();
    
    // Should return a map (may be empty if no Ambry mount points exist)
    assertNotNull("DiskInfo map should not be null", diskInfoMap);
    
    // If there are results, validate the structure
    for (Map.Entry<String, DiskInfoCollector.DiskInfo> entry : diskInfoMap.entrySet()) {
      String mountPoint = entry.getKey();
      DiskInfoCollector.DiskInfo diskInfo = entry.getValue();
      
      assertNotNull("Mount point should not be null", mountPoint);
      assertNotNull("DiskInfo should not be null", diskInfo);
      assertEquals("Mount point should match DiskInfo mount point", mountPoint, diskInfo.getMountPoint());
      assertTrue("Mount point should match Ambry pattern", mountPoint.matches("/mnt/u\\d+/ambrydata"));
      assertNotNull("Filesystem should not be null", diskInfo.getFilesystem());
      assertNotNull("Size should not be null", diskInfo.getSize());
      assertNotNull("Used should not be null", diskInfo.getUsed());
      assertNotNull("Available should not be null", diskInfo.getAvailable());
      assertTrue("Use percentage should be valid", diskInfo.getUsePercentage() >= 0 && diskInfo.getUsePercentage() <= 100);
    }
  }

  /**
   * Test getTotalCapacity method.
   */
  @Test
  public void testGetTotalCapacity() {
    Map<String, DiskInfoCollector.DiskInfo> diskInfoMap = new java.util.HashMap<>();
    
    // Test empty map
    assertEquals("Empty map should have zero capacity", 0L, DiskInfoCollector.getTotalCapacity(diskInfoMap));
    
    // Add some disk info
    diskInfoMap.put("/mnt/u001/ambrydata", new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1G", "512M", "512M", 50, "/mnt/u001/ambrydata"));
    diskInfoMap.put("/mnt/u002/ambrydata", new DiskInfoCollector.DiskInfo(
        "/dev/sdb1", "2G", "1G", "1G", 50, "/mnt/u002/ambrydata"));
    
    long expectedTotal = (1024L * 1024L * 1024L) + (2L * 1024L * 1024L * 1024L); // 1G + 2G = 3G
    assertEquals("Total capacity should be sum of all disks", expectedTotal, DiskInfoCollector.getTotalCapacity(diskInfoMap));
    
    // Add disk with invalid size
    diskInfoMap.put("/mnt/u003/ambrydata", new DiskInfoCollector.DiskInfo(
        "/dev/sdc1", "invalid", "1G", "1G", 50, "/mnt/u003/ambrydata"));
    
    // Should still be 3G (invalid size is filtered out)
    assertEquals("Invalid sizes should be filtered out", expectedTotal, DiskInfoCollector.getTotalCapacity(diskInfoMap));
  }

  /**
   * Test parseDfLine method indirectly by testing the pattern matching.
   */
  @Test
  public void testDfLinePatternMatching() {
    // Test valid df line
    Map<String, DiskInfoCollector.DiskInfo> result1 = DiskInfoCollector.collectDiskInfo();
    // This will test the actual df command, but we can't easily test parseDfLine directly
    // since it's private. The collectDiskInfo test above covers this functionality.
    
    // We can test the pattern indirectly by knowing what should match
    String validLine = "/dev/sdh1        21T   14T  6.5T  68% /mnt/u001/ambrydata";
    // The pattern should match this format, but since parseDfLine is private,
    // we rely on the collectDiskInfo method to test this functionality
    
    assertNotNull("collectDiskInfo should work without throwing exceptions", result1);
  }

  /**
   * Test edge cases for DiskInfo creation.
   */
  @Test
  public void testDiskInfoEdgeCases() {
    // Test with zero use percentage
    DiskInfoCollector.DiskInfo diskInfo1 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1G", "0", "1G", 0, "/mnt/u001/ambrydata");
    assertEquals("Zero use percentage should be valid", 0, diskInfo1.getUsePercentage());
    
    // Test with 100% use percentage
    DiskInfoCollector.DiskInfo diskInfo2 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "1G", "1G", "0", 100, "/mnt/u001/ambrydata");
    assertEquals("100% use percentage should be valid", 100, diskInfo2.getUsePercentage());
    
    // Test with very large sizes
    DiskInfoCollector.DiskInfo diskInfo3 = new DiskInfoCollector.DiskInfo(
        "/dev/sda1", "100T", "50T", "50T", 50, "/mnt/u001/ambrydata");
    assertTrue("Very large sizes should be parsed correctly", diskInfo3.getSizeInBytes() > 0);
  }
}
