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
package com.github.ambry.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;


/**
 * A mock time class
 */
public class MockTime extends Time {
  private long currentNanoSeconds;

  public MockTime(long initialMilliseconds) {
    setCurrentMilliseconds(initialMilliseconds);
  }

  public MockTime() {
    this(0);
  }

  @Override
  public long milliseconds() {
    return TimeUnit.NANOSECONDS.toMillis(currentNanoSeconds);
  }

  @Override
  public long nanoseconds() {
    return currentNanoSeconds;
  }

  @Override
  public long seconds() {
    return TimeUnit.NANOSECONDS.toSeconds(currentNanoSeconds);
  }

  @Override
  public void sleep(long ms) {
    currentNanoSeconds += TimeUnit.MILLISECONDS.toNanos(ms);
  }

  @Override
  public void wait(Object o, long ms) throws InterruptedException {
    sleep(ms);
  }

  @Override
  public void await(Condition c, long ms) throws InterruptedException {
    sleep(ms);
  }

  public void setCurrentMilliseconds(long currentMilliseconds) {
    setCurrentNanoSeconds(TimeUnit.MILLISECONDS.toNanos(currentMilliseconds));
  }

  public void setCurrentNanoSeconds(long currentNanoSeconds) {
    this.currentNanoSeconds = currentNanoSeconds;
  }
}
