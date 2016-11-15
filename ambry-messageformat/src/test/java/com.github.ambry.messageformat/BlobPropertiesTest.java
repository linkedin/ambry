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
package com.github.ambry.messageformat;

import com.github.ambry.utils.Utils;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Basic tests for BlobProperties
 */
public class BlobPropertiesTest {
  @Test
  public void basicTest() {
    final int blobSize = 100;
    final String serviceId = "ServiceId";
    final String ownerId = "OwnerId";
    final String contentType = "ContentType";
    final int timeToLiveInSeconds = 144;

    BlobProperties blobProperties;

    blobProperties = new BlobProperties(blobSize, serviceId);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    assertEquals(blobProperties.getBlobSize(), blobSize);
    assertEquals(blobProperties.getServiceId(), serviceId);
    assertEquals(blobProperties.getOwnerId(), null);
    assertEquals(blobProperties.getContentType(), null);
    assertFalse(blobProperties.isPrivate());
    assertTrue(blobProperties.getTimeToLiveInSeconds() == Utils.Infinite_Time);
    assertTrue(blobProperties.getCreationTimeInMs() > 0);
    assertTrue(blobProperties.getCreationTimeInMs() <= System.currentTimeMillis());

    blobProperties = new BlobProperties(blobSize, serviceId, ownerId, contentType, true);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    assertEquals(blobProperties.getBlobSize(), blobSize);
    assertEquals(blobProperties.getServiceId(), serviceId);
    assertEquals(blobProperties.getOwnerId(), ownerId);
    assertEquals(blobProperties.getContentType(), contentType);
    assertTrue(blobProperties.isPrivate());
    assertTrue(blobProperties.getTimeToLiveInSeconds() == Utils.Infinite_Time);
    assertTrue(blobProperties.getCreationTimeInMs() > 0);
    assertTrue(blobProperties.getCreationTimeInMs() <= System.currentTimeMillis());

    blobProperties = new BlobProperties(blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    assertEquals(blobProperties.getBlobSize(), blobSize);
    assertEquals(blobProperties.getServiceId(), serviceId);
    assertEquals(blobProperties.getOwnerId(), ownerId);
    assertEquals(blobProperties.getContentType(), contentType);
    assertTrue(blobProperties.isPrivate());
    assertTrue(blobProperties.getTimeToLiveInSeconds() == timeToLiveInSeconds);
    assertTrue(blobProperties.getCreationTimeInMs() > 0);
    assertTrue(blobProperties.getCreationTimeInMs() <= System.currentTimeMillis());
  }
}
