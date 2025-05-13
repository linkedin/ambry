/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A utility class that provides a timer that can be extended.
 * It allows scheduling a task to run after a specified delay and extending the delay if needed.
 */
public class ExtendableTimer {
  /**
   * The timer used to schedule the task.
   */
  private final Timer timer = new Timer(true);

  /**
   * The time in milliseconds to schedule the task.
   */
  private long scheduledTimeInMs;

  /**
   * The requested task to be executed when the timer expires.
   */
  private final Runnable task;

  /**
   * Internal timer task that is scheduled to run.
   * Wraps the requested task in a TimerTask.
   */
  private TimerTask currentTask;

  /**
   * Flag to indicate if the timer is currently running.
   */
  private boolean isRunning = false;

  private static final Logger logger = LoggerFactory.getLogger(ExtendableTimer.class);

  /**
   * Constructor for ExtendableTimer.
   * @param scheduledTimeInMs The time in milliseconds to schedule the task.
   * @param task The task to be executed when the timer expires.
   */
  public ExtendableTimer(long scheduledTimeInMs, Runnable task) {
    Objects.requireNonNull(task, "task cannot be null");

    if (scheduledTimeInMs <= 0) {
      throw new IllegalArgumentException("scheduledTimeInMs must be greater than 0");
    }
    this.scheduledTimeInMs = scheduledTimeInMs;
    this.task = task;
  }

  /**
   * Starts the timer.
   * If the timer is already running, it does nothing.
   */
  public void start() {
    if (isRunning) return;

    logger.info("Starting timer with delay of {} ms", scheduledTimeInMs);
    scheduleTask(scheduledTimeInMs);
    isRunning = true;
  }

  /**
   * Extends the timer by the specified time in milliseconds.
   * @param timeInMs The time in milliseconds to extend the timer.
   */
  public void extend(long timeInMs) {
    cancel();

    // Add the time to the original scheduled time
    scheduledTimeInMs += timeInMs;

    // Calculate the delay in milliseconds from the current time
    long delayInMs = scheduledTimeInMs - System.currentTimeMillis();

    // Reschedule the task with the new delay
    logger.info("Extending timer by {} ms, new delay is {} ms", timeInMs, delayInMs);
    scheduleTask(delayInMs);
  }

  /**
   * Cancels the timer.
   * If the timer is not running, it does nothing.
   */
  public void cancel() {
    if (currentTask != null) {
      currentTask.cancel();
    }
    isRunning = false;
    logger.info("Timer cancelled");
  }

  /**
   * Schedules the task to run after the specified delay.
   * @param delayInMs The delay in milliseconds before the task is executed.
   */
  private void scheduleTask(long delayInMs) {
    currentTask = new TimerTask() {
      @Override
      public void run() {
        isRunning = false;
        task.run();
      }
    };
    logger.info("Scheduling task with delay of {} ms", delayInMs);
    timer.schedule(currentTask, delayInMs);
  }
}
