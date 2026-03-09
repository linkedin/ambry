/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link NamedBlobPath} parsing validation.
 */
public class NamedBlobPathTest {

  @Test
  public void testParseHappyPath() throws Exception {
    NamedBlobPath nbp = NamedBlobPath.parse("/named/account/container/blob", Collections.emptyMap());
    assertEquals("account", nbp.getAccountName());
    assertEquals("container", nbp.getContainerName());
    assertEquals("blob", nbp.getBlobName());
  }

  @Test
  public void testParseEmptyAccountName() {
    try {
      NamedBlobPath.parse("/named//container/blob", Collections.emptyMap());
      fail("Expected RestServiceException for empty account name");
    } catch (RestServiceException e) {
      assertEquals(RestServiceErrorCode.BadRequest, e.getErrorCode());
      assertTrue(e.getMessage().contains("account name"));
    }
  }

  @Test
  public void testParseEmptyContainerName() {
    try {
      NamedBlobPath.parse("/named/account//blob", Collections.emptyMap());
      fail("Expected RestServiceException for empty container name");
    } catch (RestServiceException e) {
      assertEquals(RestServiceErrorCode.BadRequest, e.getErrorCode());
      assertTrue(e.getMessage().contains("container name"));
    }
  }

  @Test
  public void testParseEmptyBlobName() {
    try {
      NamedBlobPath.parse("/named/account/container/", Collections.emptyMap());
      fail("Expected RestServiceException for empty blob name");
    } catch (RestServiceException e) {
      assertEquals(RestServiceErrorCode.BadRequest, e.getErrorCode());
      assertTrue(e.getMessage().contains("blob name"));
    }
  }

  @Test
  public void testParseS3EmptyAccountName() {
    Map<String, Object> args = new HashMap<>();
    args.put(RestUtils.InternalKeys.S3_REQUEST, "true");
    try {
      NamedBlobPath.parse("/named//container/blob", args);
      fail("Expected RestServiceException for empty account name in S3 path");
    } catch (RestServiceException e) {
      assertEquals(RestServiceErrorCode.BadRequest, e.getErrorCode());
      assertTrue(e.getMessage().contains("account name"));
    }
  }

  @Test
  public void testParseS3EmptyContainerName() {
    Map<String, Object> args = new HashMap<>();
    args.put(RestUtils.InternalKeys.S3_REQUEST, "true");
    try {
      NamedBlobPath.parse("/named/account//blob", args);
      fail("Expected RestServiceException for empty container name in S3 path");
    } catch (RestServiceException e) {
      assertEquals(RestServiceErrorCode.BadRequest, e.getErrorCode());
      assertTrue(e.getMessage().contains("container name"));
    }
  }

  @Test
  public void testParseS3EmptyBlobName() {
    Map<String, Object> args = new HashMap<>();
    args.put(RestUtils.InternalKeys.S3_REQUEST, "true");
    try {
      NamedBlobPath.parse("/named/account/container/", args);
      fail("Expected RestServiceException for empty blob name in S3 path");
    } catch (RestServiceException e) {
      assertEquals(RestServiceErrorCode.BadRequest, e.getErrorCode());
      assertTrue(e.getMessage().contains("blob name"));
    }
  }
}
