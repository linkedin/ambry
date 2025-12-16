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

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test for {@link LiStatefulSetMetadata}.
 */
public class LiStatefulSetMetadataTest {

  /**
   * Test getResourceTags with single resource tag.
   */
  @Test
  public void testGetResourceTagsSingle() {
    LiStatefulSetMetadata metadata = createMetadata("v1.ambry-video.10032");
    List<String> resourceTags = metadata.getResourceTags();
    
    assertEquals("Should have one resource tag", 1, resourceTags.size());
    assertEquals("Resource tag should match", "10032", resourceTags.get(0));
  }

  /**
   * Test getResourceTags with range.
   */
  @Test
  public void testGetResourceTagsRange() {
    LiStatefulSetMetadata metadata = createMetadata("v1.ambry-video.10032-10033");
    List<String> resourceTags = metadata.getResourceTags();
    
    assertEquals("Should have two resource tags", 2, resourceTags.size());
    assertEquals("First resource tag should be 10032", "10032", resourceTags.get(0));
    assertEquals("Second resource tag should be 10033", "10033", resourceTags.get(1));
  }

  /**
   * Test getResourceTags with larger range.
   */
  @Test
  public void testGetResourceTagsLargerRange() {
    LiStatefulSetMetadata metadata = createMetadata("v1.ambry-video.10030-10034");
    List<String> resourceTags = metadata.getResourceTags();
    
    List<String> expected = Arrays.asList("10030", "10031", "10032", "10033", "10034");
    assertEquals("Should have five resource tags", 5, resourceTags.size());
    assertEquals("Resource tags should match expected range", expected, resourceTags);
  }

  /**
   * Test getResourceTags with invalid range (non-numeric).
   */
  @Test
  public void testGetResourceTagsInvalidRange() {
    LiStatefulSetMetadata metadata = createMetadata("v1.ambry-video.abc-def");
    List<String> resourceTags = metadata.getResourceTags();
    
    assertEquals("Should treat as single tag", 1, resourceTags.size());
    assertEquals("Should return original string", "abc-def", resourceTags.get(0));
  }

  /**
   * Test getResourceTags with invalid range (start > end).
   */
  @Test
  public void testGetResourceTagsInvalidRangeOrder() {
    LiStatefulSetMetadata metadata = createMetadata("v1.ambry-video.10033-10032");
    List<String> resourceTags = metadata.getResourceTags();
    
    assertEquals("Should treat as single tag", 1, resourceTags.size());
    assertEquals("Should return original string", "10033-10032", resourceTags.get(0));
  }

  /**
   * Test getResourceTags with malformed range (multiple hyphens).
   */
  @Test
  public void testGetResourceTagsMalformedRange() {
    LiStatefulSetMetadata metadata = createMetadata("v1.ambry-video.10032-10033-10034");
    List<String> resourceTags = metadata.getResourceTags();
    
    assertEquals("Should treat as single tag", 1, resourceTags.size());
    assertEquals("Should return original string", "10032-10033-10034", resourceTags.get(0));
  }

  /**
   * Test getResourceTags with empty name.
   */
  @Test
  public void testGetResourceTagsEmptyName() {
    LiStatefulSetMetadata metadata = createMetadata("");
    List<String> resourceTags = metadata.getResourceTags();
    
    assertTrue("Should return empty list", resourceTags.isEmpty());
  }

  /**
   * Test getResourceTags with null name.
   */
  @Test
  public void testGetResourceTagsNullName() {
    LiStatefulSetMetadata metadata = createMetadata(null);
    List<String> resourceTags = metadata.getResourceTags();
    
    assertTrue("Should return empty list", resourceTags.isEmpty());
  }

  /**
   * Test getResourceTags with invalid format (not 3 parts).
   */
  @Test
  public void testGetResourceTagsInvalidFormat() {
    LiStatefulSetMetadata metadata = createMetadata("v1.ambry");
    List<String> resourceTags = metadata.getResourceTags();
    
    assertTrue("Should return empty list for invalid format", resourceTags.isEmpty());
  }

  /**
   * Test getResourceTags with whitespace in range.
   */
  @Test
  public void testGetResourceTagsWithWhitespace() {
    LiStatefulSetMetadata metadata = createMetadata("v1.ambry-video. 10032 - 10033 ");
    List<String> resourceTags = metadata.getResourceTags();
    
    assertEquals("Should have two resource tags", 2, resourceTags.size());
    assertEquals("First resource tag should be 10032", "10032", resourceTags.get(0));
    assertEquals("Second resource tag should be 10033", "10033", resourceTags.get(1));
  }


  /**
   * Test toString method with resource tags.
   */
  @Test
  public void testToString() {
    LiStatefulSetMetadata metadata = createMetadata("v1.ambry-video.10032-10033");
    String result = metadata.toString();
    
    assertTrue("Should contain name", result.contains("v1.ambry-video.10032-10033"));
    assertTrue("Should contain resource tags", result.contains("[10032, 10033]"));
  }

  /**
   * Helper method to create LiStatefulSetMetadata with given name.
   */
  private LiStatefulSetMetadata createMetadata(String name) {
    LiStatefulSetMetadata metadata = new LiStatefulSetMetadata();
    LiStatefulSetMetadata.Metadata innerMetadata = new LiStatefulSetMetadata.Metadata();
    innerMetadata.name = name;
    
    // Use reflection to set the private field
    try {
      java.lang.reflect.Field field = LiStatefulSetMetadata.class.getDeclaredField("metadata");
      field.setAccessible(true);
      field.set(metadata, innerMetadata);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set metadata field", e);
    }
    
    return metadata;
  }
}
