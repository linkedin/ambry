package com.github.ambry;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;


public class HelloWorldTask extends UserContentStore implements Task {

  public static final String TASK_COMMAND = "helloWorld";
  public static final String SLEEPTIME_CONFIG = "sleepTimeInMs";
  private final long sleepTimeInMs;
  private boolean cancelled = false;

  public HelloWorldTask(TaskCallbackContext context) {
    Map<String, String> cfg = context.getJobConfig().getJobCommandConfigMap();
    if (cfg == null) {
      cfg = new HashMap<String, String>();
    }

    TaskConfig taskConfig = context.getTaskConfig();
    Map<String, String> taskCfg = taskConfig.getConfigMap();
    if (taskCfg != null) {
      cfg.putAll(taskCfg);
    }
    sleepTimeInMs = cfg.containsKey(SLEEPTIME_CONFIG) ? Long.parseLong(cfg.get(SLEEPTIME_CONFIG)) : 10000;
  }

  @Override
  public TaskResult run() {
    sleep(sleepTimeInMs);
    System.out.println("Done executing the task " + System.currentTimeMillis());
    return new TaskResult(TaskResult.Status.COMPLETED, String.valueOf(System.currentTimeMillis()));
  }

  @Override
  public void cancel() {
    cancelled = true;
  }

  private static void sleep(long d) {
    try {
      Thread.sleep(d);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}