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
package com.github.ambry.store;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Comparator;


/**
 * Helper for working with log segment names.
 * <p/>
 * Log segments will have a name "pos_gen" where pos represents their relative position and "gen" represents the
 * generation number of the log segment at "pos".
 * <p/>
 * The file name is a combination of the segment name and a suffix "_log"
 * <p/>
 * If the file name format changes, the version of {@link LogSegment} has to be updated and this class updated to
 * handle the new and old versions.
 */
class LogSegmentNameHelper {
  static final String SUFFIX = BlobStore.SEPARATOR + "log";
  /**
   * {@link Comparator} for two log segment names.
   */
  static final Comparator<String> COMPARATOR = new Comparator<String>() {
    @Override
    public int compare(String name1, String name2) {
      // special case for log_current (one segment logs)
      if (name1.isEmpty() && name2.isEmpty()) {
        return 0;
      }
      long pos1 = getPosition(name1);
      long pos2 = getPosition(name2);
      int compare = Long.compare(pos1, pos2);
      if (compare == 0) {
        long gen1 = getGeneration(name1);
        long gen2 = getGeneration(name2);
        compare = Long.compare(gen1, gen2);
      }
      return compare;
    }
  };
  /**
   * Filter for getting all log files from a particular directory.
   */
  static final FilenameFilter LOG_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(LogSegmentNameHelper.SUFFIX) || name.equals(SINGLE_SEGMENT_LOG_FILE_NAME);
    }
  };

  // for backwards compatibility, if the log contains only a single segment, the segment will have a special name.
  private static final String SINGLE_SEGMENT_LOG_FILE_NAME = "log_current";

  /**
   * @param name the name of the log segment.
   * @return the hashcode of {@code name}.
   */
  static int hashcode(String name) {
    return name.hashCode();
  }

  /**
   * @param name the name of the log segment.
   * @return the relative position of the log segment.
   */
  static long getPosition(String name) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException("Name provided cannot be empty");
    }
    return Long.parseLong(name.substring(0, name.indexOf(BlobStore.SEPARATOR)));
  }

  /**
   * @param name the name of the log segment.
   * @return the generation number of the log segment.
   */
  static long getGeneration(String name) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException("Name provided cannot be empty");
    }
    return Long.parseLong(name.substring(name.indexOf(BlobStore.SEPARATOR) + 1));
  }

  /**
   * @param pos the relative position of the log segment.
   * @param gen the generation of the log segment.
   * @return the name of a log segment with position {@code pos} and generation number {@code gen}.
   */
  static String getName(long pos, long gen) {
    return pos + BlobStore.SEPARATOR + gen;
  }

  /**
   * @param name the name of the log segment.
   * @return what should be the name of the log segment that is exactly one position higher than {@code name}. The
   * generation of the returned name will start from the lowest generation number.
   */
  static String getNextPositionName(String name) {
    long pos = getPosition(name);
    return getName(pos + 1, 0);
  }

  /**
   * @param name the name of the log segment.
   * @return what should be the name of the log segment that is exactly one generation higher than {@code name}.
   */
  static String getNextGenerationName(String name) {
    long pos = getPosition(name);
    long gen = getGeneration(name);
    return getName(pos, gen + 1);
  }

  /**
   * @param isLogSegmented {@code true} if the log is segmented, {@code false} otherwise.
   * @return what should be the name of the first segment.
   */
  static String generateFirstSegmentName(boolean isLogSegmented) {
    return isLogSegmented ? getName(0, 0) : "";
  }

  /**
   * @param name the name of the log segment.
   * @return the name of the file that backs the log segment.
   */
  static String nameToFilename(String name) {
    if (name.isEmpty()) {
      return SINGLE_SEGMENT_LOG_FILE_NAME;
    }
    return name + SUFFIX;
  }

  /**
   * @param filename the name of the file that backs the log segment.
   * @return the name of the log segment.
   */
  static String nameFromFilename(String filename) {
    if (filename.equals(SINGLE_SEGMENT_LOG_FILE_NAME)) {
      return "";
    }
    if (!filename.endsWith(SUFFIX)) {
      throw new IllegalArgumentException("The filename of the log segment does not end with [" + SUFFIX + "]");
    }
    String name = filename.substring(0, filename.length() - SUFFIX.length());
    validate(name);
    return name;
  }

  /**
   * Validates that the name provided is a valid log segment name.
   * @param name the log segment name.
   */
  private static void validate(String name) {
    if (!name.equals(getName(getPosition(name), getGeneration(name)))) {
      throw new IllegalArgumentException("Invalid name: " + name);
    }
  }
}
