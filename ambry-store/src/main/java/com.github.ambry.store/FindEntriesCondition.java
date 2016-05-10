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

/** A helper class representing the condition based on which entries are scanned from the index. A condition object is
 * constructed with one or two parameters: a max size of entries and an optional end time. A proceed method takes a size
 * and a time and returns true if the passed in parameters are within the size and time associated with the condition.
 */
class FindEntriesCondition {
  private long endTime;
  private long maxSize;

  /** Initialize a condition object that allows for entries up to a size of maxSize. No end time is associated with this
   * object.
   * @param maxSize the maximum size of entries allowed by this condition object.
   */
  FindEntriesCondition(long maxSize) {
    this.maxSize = maxSize;
    this.endTime = -1;
  }

  /** Initialize a condition object that allows for entries up to a size of maxSize and those that are from segments
   * last modified at or before endTime.
   *
   * @param maxSize the maximum size of entries allowed by this condition object.
   * @param endTime the latest a segment's last modified time can be in order for entries to be fetched from it.
   */
  FindEntriesCondition(long maxSize, long endTime) {
    this.maxSize = maxSize;
    this.endTime = endTime;
  }

  boolean proceed(long checkSize, long checkEndTime) {
    return (endTime == -1 || endTime >= checkEndTime) && (maxSize > checkSize);
  }

  boolean hasEndTime() {
    return endTime != -1;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[maxSize: ").append(maxSize).append("]");
    sb.append("[endTime: ").append(endTime).append("]");
    return sb.toString();
  }
}

