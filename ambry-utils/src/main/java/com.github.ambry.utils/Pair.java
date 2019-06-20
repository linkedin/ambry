/*
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

package com.github.ambry.utils;

import java.util.Objects;


/**
 * Represents a pair of two objects
 * @param <A> The type of the first object in the pair.
 * @param <B> The type of the second object in the pair.
 */
public class Pair<A, B> {
  private final A first;
  private final B second;

  /**
   * Construct a pair from two objects.
   * @param first the first object in the pair.
   * @param second the second object in the pair.
   */
  public Pair(A first, B second) {
    this.first = first;
    this.second = second;
  }

  /**
   * @return the first object in the pair.
   */
  public A getFirst() {
    return first;
  }

  /**
   * @return the second object in the pair.
   */
  public B getSecond() {
    return second;
  }

  @Override
  public String toString() {
    return "Pair{" + "first=" + first + ", second=" + second + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Pair<?, ?> pair = (Pair<?, ?>) o;
    if (!Objects.equals(first, pair.first)) {
      return false;
    }
    return Objects.equals(second, pair.second);
  }

  @Override
  public int hashCode() {
    int result = first != null ? first.hashCode() : 0;
    result = 31 * result + (second != null ? second.hashCode() : 0);
    return result;
  }
}
