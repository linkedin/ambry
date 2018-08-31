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
 * The normal system implementation of time functions
 */
public class SystemTime extends Time {

  private static SystemTime time = new SystemTime();

  public static Time getInstance() {
    return time;
  }

  private SystemTime() {
  }

  @Override
  public long milliseconds() {
    return System.currentTimeMillis();
  }

  @Override
  public long nanoseconds() {
    return System.nanoTime();
  }

  @Override
  public long seconds() {
    return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
  }

  @Override
  public void sleep(long ms) throws InterruptedException {
    Thread.sleep(ms);
  }

  @Override
  public void wait(Object o, long ms) throws InterruptedException {
    o.wait(ms);
  }

  @Override
  public void await(Condition c, long ms) throws InterruptedException {
    c.await(ms, TimeUnit.MILLISECONDS);
  }
}
