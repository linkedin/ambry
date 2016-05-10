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
package com.github.ambry.metrics;

import java.util.concurrent.atomic.AtomicReference;


public class Gauge<T> implements Metric {
  private final String name;
  private AtomicReference<T> ref;

  public Gauge(String name, T value) {
    this.name = name;
    this.ref = new AtomicReference<T>(value);
  }

  public boolean compareAndSet(T expected, T n) {
    return ref.compareAndSet(expected, n);
  }

  public T set(T n) {
    return ref.getAndSet(n);
  }

  public T getValue() {
    return ref.get();
  }

  public String getName() {
    return name;
  }

  @Override
  public void visit(MetricsVisitor visitor) {
    visitor.gauge(this);
  }

  public String toString() {
    T value = ref.get();
    return (value == null) ? null : value.toString();
  }
}
