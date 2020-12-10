/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.named;

import java.util.Collections;
import java.util.List;


/**
 * Represents a page of a response.
 * @param <T> the type of the elements in the page
 */
public class Page<T> {
  private final String continuationToken;
  private final List<T> elements;

  /**
   *
   * @param continuationToken {@code null} if there are no remaining pages to read, or a string that can be used to
   *                          continue to read additional pages.
   * @param elements the elements in this response page.
   */
  public Page(String continuationToken, List<T> elements) {
    this.continuationToken = continuationToken;
    this.elements = Collections.unmodifiableList(elements);
  }

  /**
   * @return {@code null} if there are no remaining pages to read, or a string that can be used to continue to read
   *         additional pages.
   */
  public String getContinuationToken() {
    return continuationToken;
  }

  /**
   * @return the elements in this response page.
   */
  public List<T> getElements() {
    return elements;
  }
}
