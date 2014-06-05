package com.github.ambry.metrics;

import java.util.concurrent.atomic.AtomicLong;


/**
 * A counter is a metric that represents a cumulative value.
 */
public class Counter implements Metric {
  private final String name;
  private final AtomicLong count;

  public Counter(String name) {
    this.name = name;
    this.count = new AtomicLong(0);
  }

  public long inc() {
    return inc(1);
  }

  public long inc(long n) {
    return count.addAndGet(n);
  }

  public long dec() {
    return dec(1);
  }

  public long dec(long n) {
    return count.addAndGet(0 - n);
  }

  public void set(long n) {
    count.set(n);
  }

  public void clear() {
    count.set(0);
  }

  public long getCount() {
    return count.get();
  }

  public String getName() {
    return name;
  }

  @Override
  public void visit(MetricsVisitor visitor) {
    visitor.counter(this);
  }

  public String toString() {
    return count.toString();
  }
}

