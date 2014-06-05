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
