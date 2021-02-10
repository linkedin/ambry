/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.store;

import java.io.FilenameFilter;
import java.util.Objects;


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
class LogSegmentName implements Comparable<LogSegmentName> {
  static final String SUFFIX = BlobStore.SEPARATOR + "log";
  /**
   * For backwards compatibility, if the log contains only a single segment, the segment will have a special name.
   */
  static final String SINGLE_SEGMENT_LOG_FILE_NAME = "log_current";
  /**
   * Filter for getting all log files from a particular directory.
   */
  static final FilenameFilter LOG_FILE_FILTER =
      (dir, name) -> name.endsWith(SUFFIX) || name.equals(SINGLE_SEGMENT_LOG_FILE_NAME);
  private static final LogSegmentName SINGLE_SEGMENT_LOG_NAME = new LogSegmentName(-1, -1);

  private String name = null;
  private final long position;
  private final long generation;

  /**
   * @param isLogSegmented {@code true} if the log is segmented, {@code false} otherwise.
   * @return what should be the name of the first segment.
   */
  static LogSegmentName generateFirstSegmentName(boolean isLogSegmented) {
    return isLogSegmented ? new LogSegmentName(0, 0) : SINGLE_SEGMENT_LOG_NAME;
  }

  /**
   * @param filename the name of the file that backs the log segment.
   * @return the {@link LogSegmentName}.
   */
  static LogSegmentName fromFilename(String filename) {
    if (filename.equals(SINGLE_SEGMENT_LOG_FILE_NAME)) {
      return SINGLE_SEGMENT_LOG_NAME;
    }
    if (!filename.endsWith(SUFFIX)) {
      throw new IllegalArgumentException("The filename of the log segment does not end with [" + SUFFIX + "]");
    }
    String name = filename.substring(0, filename.length() - SUFFIX.length());
    return new LogSegmentName(name);
  }

  /**
   * Parse the string form of a log segment name.
   * @param name the name string to parse.
   * @return the {@link LogSegmentName}.
   */
  static LogSegmentName fromString(String name) {
    if (name.isEmpty()) {
      return SINGLE_SEGMENT_LOG_NAME;
    }
    return new LogSegmentName(name);
  }

  /**
   * @param position the relative position of the log segment.
   * @param generation the generation of the log segment.
   * @return the {@link LogSegmentName}.
   */
  static LogSegmentName fromPositionAndGeneration(long position, long generation) {
    return new LogSegmentName(position, generation);
  }

  /**
   * @param position the relative position of the log segment.
   * @param generation the generation of the log segment.
   */
  private LogSegmentName(long position, long generation) {
    this.position = position;
    this.generation = generation;
  }

  /**
   * @param name the name string to parse.
   */
  private LogSegmentName(String name) {
    try {
      int separatorIndex = name.indexOf(BlobStore.SEPARATOR);
      if (separatorIndex == -1) {
        throw new IllegalArgumentException("Separator char not found");
      }
      this.position = Long.parseLong(name.substring(0, separatorIndex));
      this.generation = Long.parseLong(name.substring(name.indexOf(BlobStore.SEPARATOR) + 1));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Could not parse " + name + " as a log segment name", e);
    }
    this.name = name;
  }

  long getPosition() {
    checkSegmented();
    return position;
  }

  long getGeneration() {
    checkSegmented();
    return generation;
  }

  /**
   * @return {@code true} if this is a single segment log.
   */
  boolean isSingleSegment() {
    return this == SINGLE_SEGMENT_LOG_NAME;
  }

  /**
   * @return what should be the name of the log segment that is exactly one position higher than this one. The
   * generation of the returned name will start from the lowest generation number.
   */
  LogSegmentName getNextPositionName() {
    checkSegmented();
    return new LogSegmentName(position + 1, 0);
  }

  /**
   * @return what should be the name of the log segment that is exactly one generation higher than this one.
   */
  LogSegmentName getNextGenerationName() {
    checkSegmented();
    return new LogSegmentName(position, generation + 1);
  }

  /**
   * @return the name of the file that backs the log segment.
   */
  String toFilename() {
    if (isSingleSegment()) {
      return SINGLE_SEGMENT_LOG_FILE_NAME;
    }
    return toString() + SUFFIX;
  }

  @Override
  public String toString() {
    if (name == null) {
      name = isSingleSegment() ? "" : position + BlobStore.SEPARATOR + generation;
    }
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LogSegmentName that = (LogSegmentName) o;
    return position == that.position && generation == that.generation;
  }

  @Override
  public int hashCode() {
    return Objects.hash(position, generation);
  }

  @Override
  public int compareTo(LogSegmentName o) {
    // special case for log_current (one segment logs)
    if (isSingleSegment() && o.isSingleSegment()) {
      return 0;
    } else if (isSingleSegment() || o.isSingleSegment()) {
      throw new IllegalArgumentException("Cannot compare single file log name against segmented log name");
    }
    int compare = Long.compare(position, o.position);
    if (compare == 0) {
      compare = Long.compare(generation, o.generation);
    }
    return compare;
  }

  /**
   * @throws IllegalArgumentException if this is a single segment log.
   */
  private void checkSegmented() {
    if (isSingleSegment()) {
      throw new IllegalArgumentException("Cannot call for single segment logs");
    }
  }
}
