/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.dependency;

import org.junit.Assert;
import org.junit.Test;

public class DependencyExceptionTest {
  @Test
  public void testConstructor() {
    DependencyException ex = new DependencyException(DependencyStore.class, "error");
    Assert.assertNotNull(ex.getMessage());
    Assert.assertEquals(DependencyStore.class, ex.getKey());

    ex = new DependencyException(DependencyStore.class, "failed", new Exception());
    Assert.assertNotNull(ex.getMessage());
    Assert.assertNotNull(ex.getCause());
    Assert.assertEquals(DependencyStore.class, ex.getKey());
  }
}
