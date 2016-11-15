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

/**
 * A mock time class
 */
public class MockTime extends Time {
  public long currentMilliseconds;
  public long currentNanoSeconds;

  public MockTime(long initialMilliseconds) {
    currentMilliseconds = initialMilliseconds;
    currentNanoSeconds = initialMilliseconds * NsPerMs;
  }

  public MockTime() {
    this(0);
  }

  @Override
  public long milliseconds() {
    return currentMilliseconds;
  }

  @Override
  public long nanoseconds() {
    return currentNanoSeconds;
  }

  @Override
  public long seconds() {
    return currentMilliseconds / MsPerSec;
  }

  @Override
  public void sleep(long ms) throws InterruptedException {
    currentMilliseconds += ms;
  }

  @Override
  public void wait(Object o, long ms) throws InterruptedException {
    sleep(ms);
  }
}
