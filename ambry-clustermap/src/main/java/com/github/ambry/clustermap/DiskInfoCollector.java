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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects disk information by running system command 'df -h'.
 */
public class DiskInfoCollector {
  private static final Logger logger = LoggerFactory.getLogger(DiskInfoCollector.class);

  // Pattern to match df -h output lines for Ambry mount points
  // Example: /dev/sdh1        21T   14T  6.5T  68% /mnt/u001/ambrydata
  private static final Pattern DF_PATTERN = Pattern.compile(
      "^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\d+)%\\s+(/mnt/u\\d+/ambrydata)$");

  /**
   * Represents disk information from df command.
   */
  public static class DiskInfo {
    private final String filesystem;
    private final String size;
    private final String used;
    private final String available;
    private final int usePercentage;
    private final String mountPoint;

    public DiskInfo(String filesystem, String size, String used, String available, int usePercentage, String mountPoint) {
      this.filesystem = filesystem;
      this.size = size;
      this.used = used;
      this.available = available;
      this.usePercentage = usePercentage;
      this.mountPoint = mountPoint;
    }

    public String getFilesystem() {
      return filesystem;
    }

    public String getSize() {
      return size;
    }

    public String getUsed() {
      return used;
    }

    public String getAvailable() {
      return available;
    }

    public int getUsePercentage() {
      return usePercentage;
    }

    public String getMountPoint() {
      return mountPoint;
    }

    /**
     * Convert size string (e.g., "100G", "1.5T") to bytes.
     * @return size in bytes, or -1 if parsing fails
     */
    public long getSizeInBytes() {
      return parseSize(size);
    }

    private static long parseSize(String sizeStr) {
      if (sizeStr == null || sizeStr.trim().isEmpty()) {
        return -1;
      }

      try {
        sizeStr = sizeStr.trim().toUpperCase();
        // Extract number and unit
        Pattern pattern = Pattern.compile("^([0-9.]+)([KMGTPE]?)$");
        Matcher matcher = pattern.matcher(sizeStr);
        if (!matcher.matches()) {
          return -1;
        }

        double value = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2);
        long multiplier;
        switch (unit) {
          case "K":
            multiplier = 1024L;
            break;
          case "M":
            multiplier = 1024L * 1024L;
            break;
          case "G":
            multiplier = 1024L * 1024L * 1024L;
            break;
          case "T":
            multiplier = 1024L * 1024L * 1024L * 1024L;
            break;
          case "P":
            multiplier = 1024L * 1024L * 1024L * 1024L * 1024L;
            break;
          case "E":
            multiplier = 1024L * 1024L * 1024L * 1024L * 1024L * 1024L;
            break;
          default:
            multiplier = 1L; // Bytes
            break;
        }

        return (long) (value * multiplier);
      } catch (NumberFormatException e) {
        logger.warn("Failed to parse size: {}", sizeStr, e);
        return -1;
      }
    }

    @Override
    public String toString() {
      return "DiskInfo{" +
          "filesystem='" + filesystem + '\'' +
          ", size='" + size + '\'' +
          ", used='" + used + '\'' +
          ", available='" + available + '\'' +
          ", usePercentage=" + usePercentage +
          ", mountPoint='" + mountPoint + '\'' +
          '}';
    }
  }

  /**
   * Collect disk information by running 'df -h' command.
   * @return map of mount point to DiskInfo
   */
  public static Map<String, DiskInfo> collectDiskInfo() {
    Map<String, DiskInfo> diskInfoMap = new HashMap<>();

    try {
      logger.info("Running command: df -h");
      ProcessBuilder processBuilder = new ProcessBuilder("df", "-h");
      processBuilder.redirectErrorStream(true);
      Process process = processBuilder.start();

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        boolean isFirstLine = true;

        while ((line = reader.readLine()) != null) {
          // Skip header line
          if (isFirstLine) {
            logger.debug("df header: {}", line);
            isFirstLine = false;
            continue;
          }

          DiskInfo diskInfo = parseDfLine(line);
          if (diskInfo != null) {
            diskInfoMap.put(diskInfo.getMountPoint(), diskInfo);
            logger.info("Found disk: {} -> {}", diskInfo.getMountPoint(), diskInfo);
          }
        }
      }

    } catch (IOException e) {
      logger.error("Failed to run df command", e);
    }

    logger.info("Collected disk info for {} mount points", diskInfoMap.size());
    return diskInfoMap;
  }

  /**
   * Parse a single line from df -h output.
   * @param line the line to parse
   * @return DiskInfo object, or null if parsing fails
   */
  private static DiskInfo parseDfLine(String line) {
    if (line == null || line.trim().isEmpty()) {
      return null;
    }

    line = line.trim();
    Matcher matcher = DF_PATTERN.matcher(line);
    if (!matcher.matches()) {
      logger.debug("Line doesn't match df pattern: {}", line);
      return null;
    }

    try {
      String filesystem = matcher.group(1);
      String size = matcher.group(2);
      String used = matcher.group(3);
      String available = matcher.group(4);
      int usePercentage = Integer.parseInt(matcher.group(5));
      String mountPoint = matcher.group(6);

      return new DiskInfo(filesystem, size, used, available, usePercentage, mountPoint);
    } catch (NumberFormatException e) {
      logger.warn("Failed to parse df line: {}", line, e);
      return null;
    }
  }

  /**
   * Get total capacity across all provided disks.
   * @param diskInfoMap map of disk information
   * @return total capacity in bytes
   */
  public static long getTotalCapacity(Map<String, DiskInfo> diskInfoMap) {
    return diskInfoMap.values().stream()
        .mapToLong(DiskInfo::getSizeInBytes)
        .filter(size -> size > 0)
        .sum();
  }
}
