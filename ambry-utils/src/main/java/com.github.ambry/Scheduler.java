package com.github.ambry;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/25/13
 * Time: 3:22 PM
 * To change this template use File | Settings | File Templates.
 */

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadFactory;

interface IFunc {
  void execute();
}

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 *
 * It has a pool of threads that do the actual work.
 */
public class Scheduler {

  private final int noOfThreads;
  private final String threadNamePrefix;
  private final boolean isDaemon;
  private volatile ScheduledThreadPoolExecutor executor = null;
  private AtomicInteger schedulerThreadId = new AtomicInteger(0);

  public Scheduler(int noOfThreads, String threadNamePrefix, boolean isDaemon) {
    this.noOfThreads = noOfThreads;
    this.threadNamePrefix = threadNamePrefix;
    this.isDaemon = isDaemon;
  }

  public Scheduler(int noOfThreads, boolean isDaemon) {
    this(noOfThreads, "ambry-scheduler-", isDaemon);
  }

  public void startup() {
    synchronized (this) {
      if(executor != null)
        throw new IllegalStateException("This scheduler has already been started!");
      executor = new ScheduledThreadPoolExecutor(noOfThreads);
      executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
      executor.setThreadFactory(new ThreadFactory() {
         public Thread newThread(Runnable runnable) {
           return Utils.newThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, isDaemon);
            }
        });
    }
  }

  public void shutdown() {
    try {
      ensureStarted();
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.DAYS);
      this.executor = null;
    }
    catch (Exception e) {
      // log here
    }
  }

  public void schedule(String name, final IFunc func, long delay, long period, TimeUnit unit) {
    ensureStarted();
    Runnable runnable = new Runnable() {
      public void run() {
        try {
          func.execute();
        } catch (Exception e) {
          // log
        } finally {
          // log completed
        }
      }
    };
    if(period >= 0)
      executor.scheduleAtFixedRate(runnable, delay, period, unit);
    else
      executor.schedule(runnable, delay, unit);
  }

  private void ensureStarted() {
    if(executor == null)
      throw new IllegalStateException("Ambry scheduler has not been started");
  }
}
