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

import java.util.concurrent.locks.Condition;


/**
 * A mockable interface for time functions
 */
public abstract class Time {

  /**
   * Some common constants
   */
  public static final int NsPerUs = 1000;
  public static final int UsPerMs = 1000;
  public static final int MsPerSec = 1000;
  public static final int NsPerMs = NsPerUs * UsPerMs;
  public static final int NsPerSec = NsPerMs * MsPerSec;
  public static final int SecsPerMin = 60;

  public abstract long milliseconds();

  public abstract long nanoseconds();

  public abstract long seconds();

  public abstract void sleep(long ms) throws InterruptedException;

  /**
   * Waits on the {@code o} for {@code ms}
   * @param o the {@link Object} over which wait has been called
   * @param ms time in millisecs for which the wait should be called
   * @throws InterruptedException
   */
  public abstract void wait(Object o, long ms) throws InterruptedException;

  /**
   * Awaits on the {@code c} for {@code ms}
   * @param c the {@link Condition} over which await has been called
   * @param ms time in millisecs for which the await should be called
   * @throws InterruptedException
   */
  public abstract void await(Condition c, long ms) throws InterruptedException;
}

