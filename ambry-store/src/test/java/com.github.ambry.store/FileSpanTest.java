/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link FileSpan}
 */
public class FileSpanTest {

  /**
   * Tests for {@link FileSpan#inSpan(Offset)}.
   */
  @Test
  public void inSpanTest() {
    String firstLogSegmentName = LogSegmentNameHelper.generateFirstSegmentName(true);
    String startLogSegmentName = LogSegmentNameHelper.getNextPositionName(firstLogSegmentName);
    String interveningLogSegmentName = LogSegmentNameHelper.getNextPositionName(startLogSegmentName);
    String endLogSegmentName = LogSegmentNameHelper.getNextPositionName(interveningLogSegmentName);
    long startOffsetInStartLogSegment = 100;
    long endOffsetInEndLogSegment = 100;
    FileSpan span = new FileSpan(new Offset(startLogSegmentName, startOffsetInStartLogSegment),
        new Offset(endLogSegmentName, endOffsetInEndLogSegment));
    long testOffsetStart = 0;
    long testOffsetEnd = 200;

    for (String segmentName : new String[]{firstLogSegmentName, startLogSegmentName, interveningLogSegmentName, endLogSegmentName}) {
      for (long offsetInSegment = testOffsetStart; offsetInSegment <= testOffsetEnd; offsetInSegment++) {
        Offset offset = new Offset(segmentName, offsetInSegment);
        boolean inSpan = segmentName.equals(interveningLogSegmentName) || (segmentName.equals(startLogSegmentName)
            && offsetInSegment >= startOffsetInStartLogSegment) || (segmentName.equals(endLogSegmentName)
            && offsetInSegment <= endOffsetInEndLogSegment);
        assertEquals("inSpan() result not as expected", inSpan, span.inSpan(offset));
      }
    }
  }
}
