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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;


/**
 * A mock time class
 */
public class MockTime extends Time {
  private final Set<String> timeSuspendedThreads = ConcurrentHashMap.newKeySet();
  private volatile boolean suspend = false;
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
    if (!suspend && !timeSuspendedThreads.contains(Thread.currentThread().getName())) {
      currentNanoSeconds += TimeUnit.MILLISECONDS.toNanos(ms);
    }
  }

  @Override
  public void wait(Object o, long ms) {
    sleep(ms);
  }

  @Override
  public void await(Condition c, long ms) {
    sleep(ms);
  }

  public void setCurrentMilliseconds(long currentMilliseconds) {
    setCurrentNanoSeconds(TimeUnit.MILLISECONDS.toNanos(currentMilliseconds));
  }

  public void setCurrentNanoSeconds(long currentNanoSeconds) {
    this.currentNanoSeconds = currentNanoSeconds;
  }

  /**
   * Suspends the passage of time when {@link #sleep(long)} (or related functions) are called from the given
   * {@code threadNames}. Those functions still continue to return immediately.
   * If {@code threadNames} is {@code null}, suspends passage of time for all threads
   * @param threadNames the names of the threads for which the passage of time on sleep() should be suspended. If
   *                    {@code null}, it is suspended for all threads
   */
  public void suspend(Collection<String> threadNames) {
    if (threadNames == null) {
      suspend = true;
    } else {
      timeSuspendedThreads.addAll(threadNames);
    }
  }

  /**
   * Resumes the passage of time when {@link #sleep(long)} (or related functions) are called from the given
   * {@code threadNames}. Those functions still continue to return immediately.
   * Note that any blanket suspensions will be lifted (i.e. call to {@link #suspend(Collection)} with {@code null}
   * {@code threadNames}) even if the {@code threadNames} is not {@code null}.
   * @param threadNames the names of the threads for which the passage of time has to be resumed on calls to sleep().
   *                    Can be {@code null}.
   */
  public void resume(Collection<String> threadNames) {
    suspend = false;
    if (threadNames != null) {
      timeSuspendedThreads.removeAll(threadNames);
    }
  }
}
