/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
 *
 * The code was originally created by Apache Helix under Apache License 2.0.
 * https://github.com/apache/helix/blob/master/helix-core/src/test/java/org/apache/helix/integration/manager/MockParticipantManager.java
 * Code Changes in this copy:
 * 1. Renamed MockParticipantManager to HelixControllerManager.
 * 2. Renamed LOG to logger.
 * 3. Removed '_' from class members.
 */
package com.github.ambry.utils;

import java.util.concurrent.CountDownLatch;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HelixControllerManager extends ZKHelixManager implements Runnable {
  private static Logger logger = LoggerFactory.getLogger(HelixControllerManager.class);

  private final CountDownLatch startCountDown = new CountDownLatch(1);
  private final CountDownLatch stopCountDown = new CountDownLatch(1);
  private final CountDownLatch waitStopFinishCountDown = new CountDownLatch(1);

  private boolean started = false;

  public HelixControllerManager(String zkAddr, String clusterName) {
    this(zkAddr, clusterName, "controller");
  }

  public HelixControllerManager(String zkAddr, String clusterName, String controllerName) {
    super(clusterName, controllerName, InstanceType.CONTROLLER, zkAddr);
  }

  public void syncStop() {
    stopCountDown.countDown();
    try {
      waitStopFinishCountDown.await();
      started = false;
    } catch (InterruptedException e) {
      logger.error("Interrupted waiting for finish", e);
    }
  }

  // This should not be called more than once because HelixManager.connect() should not be called more than once.
  public void syncStart() {
    if (started) {
      throw new RuntimeException("Helix Controller already started. Do not call syncStart() more than once.");
    } else {
      started = true;
    }

    new Thread(this).start();
    try {
      startCountDown.await();
    } catch (InterruptedException e) {
      logger.error("Interrupted waiting for start", e);
    }
  }

  @Override
  public void run() {
    try {
      connect();
      startCountDown.countDown();
      stopCountDown.await();
    } catch (Exception e) {
      logger.error("exception running controller-manager", e);
    } finally {
      startCountDown.countDown();
      disconnect();
      waitStopFinishCountDown.countDown();
    }
  }
}
