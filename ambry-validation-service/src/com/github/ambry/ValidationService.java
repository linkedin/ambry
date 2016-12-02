package com.github.ambry;

import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.examples.MasterSlaveStateModelFactory;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;


public class ValidationService {

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final String zkAddress;
  private final String clusterName;
  private final String hostName;
  private final int servicePort;
  private HelixAdmin helixAdmin;
  private HelixManager manager;
  private final Time time;
  // private static final String STATE_MODEL_NAME = "MyStateModel";
  private String stateModelName;

  public ValidationService(String zkAddress, String clusterName, String hostName, int servicePort,
      String stateModelName, Time time) {
    this.zkAddress = zkAddress;
    this.clusterName = clusterName;
    this.hostName = hostName;
    this.servicePort = servicePort;
    this.stateModelName = stateModelName;
    this.time = time;
  }

  public void startup() throws Exception {
    helixAdmin = new ZKHelixAdmin(zkAddress);
    addAsParticipantAndStart();
  }

  private void addAsParticipantAndStart() throws Exception {
   /* InstanceConfig instanceConfig = new InstanceConfig(hostName +"_"+ servicePort);
    instanceConfig.setHostName(hostName);
    instanceConfig.addTag("group1");
    instanceConfig.setPort("" + servicePort);
    instanceConfig.setInstanceEnabled(true);
    helixAdmin.addInstance(clusterName, instanceConfig);*/
    String instanceName = hostName + "_" + servicePort;
    manager =
        HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
            zkAddress);

    MasterSlaveStateModelFactory stateModelFactory =
        new MasterSlaveStateModelFactory(instanceName);

    //StateModelFactory stateModelFactory = new AmbryStateModelFactory();

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put(HelloWorldTask.TASK_COMMAND, new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new HelloWorldTask(context);
      }
    });

    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory(stateModelName, stateModelFactory);
    stateMach.registerStateModelFactory("Task", new TaskStateModelFactory(manager, taskFactoryReg));
    manager.connect();
  }

  public void shutdown() {
    try {
      manager.disconnect();
      helixAdmin.close();
    } finally {
      shutdownLatch.countDown();
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}
