package com.github.ambry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.examples.Quickstart;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.ScheduleConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ValidationController {

  private final String zkAddress;
  private final String clusterName;
  private final String queueName;
  private final String taskCommand;
  private final String stateModelName;
  private final String instanceName;

  // type of operations supported
  private static final String ADD_CLUSTER = "add_cluster";
  private static final String START_CONTROLLER = "start_controller";
  private static final String TRIGGER_TEST = "trigger_test";

  // states
  private static final String ONLINE = "ONLINE";
  private static final String OFFLINE = "OFFLINE";
  // states
  private static final String SLAVE = "SLAVE";
 // private static final String OFFLINE = "OFFLINE";
  private static final String MASTER = "MASTER";
  private static final String DROPPED = "DROPPED";

  static TaskDriver taskDriver;
  private static Logger logger = LoggerFactory.getLogger(ValidationController.class);
  private CountDownLatch shutdownLatch = new CountDownLatch(1);

  public ValidationController(String zkAddress, String clusterName, String queueName, String taskCommand,
      String stateModelName, String instanceName) {
    this.zkAddress = zkAddress;
    this.clusterName = clusterName;
    this.queueName = queueName;
    this.taskCommand = taskCommand;
    this.stateModelName = stateModelName;
    this.instanceName = instanceName;
  }

  public static void main(String[] args) {
    final ValidationController validationController;
    int exitCode = 0;
    try {
      final ValidationController.InvocationOptions options = new ValidationController.InvocationOptions(args);
      // final VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      logger.info("Bootstrapping ValidationService");
      validationController =
          new ValidationController(options.zkAddress, options.clusterName, options.queueName, options.taskCommand,
              options.stateModelName, options.instanceName);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          logger.info("Received shutdown signal. Shutting down ValidationService");
          validationController.shutdown();
        }
      });
      validationController.doOperation(options.typeOfOperation);
      validationController.awaitShutdown();
    } catch (Exception e) {
      logger.error("Exception during bootstrap of ValidationService", e);
      e.printStackTrace();
      exitCode = 1;
    }
    logger.info("Exiting ValidationServiceMain");
    System.exit(exitCode);
  }

  public void doOperation(String typeOfOperation) throws InterruptedException {
    switch (typeOfOperation) {
      case ADD_CLUSTER:
        addCluster();
        break;
      case START_CONTROLLER:
        startupController();
        break;
      case TRIGGER_TEST:
        triggerJob();
        break;
      default:
        logger.error("Undefined operation type " + typeOfOperation);
    }
    shutdownLatch.countDown();
  }

  private void addCluster() {
    HelixAdmin admin = new ZKHelixAdmin(zkAddress);
    // create cluster
    logger.info("Creating cluster: " + clusterName);
    admin.addCluster(clusterName, true);

    // add instances
    int NUM_NODES = 2;
    List<InstanceConfig> INSTANCE_CONFIG_LIST;
    INSTANCE_CONFIG_LIST = new ArrayList<InstanceConfig>();
    for (int i = 0; i < NUM_NODES; i++) {
      int port = 6999 + i;
      InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort("" + port);
      instanceConfig.setInstanceEnabled(true);
      instanceConfig.addTag("group1");
      INSTANCE_CONFIG_LIST.add(instanceConfig);
    }

    // Add nodes to the cluster
    logger.info("Adding " + NUM_NODES + " participants to the cluster");
    for (int i = 0; i < NUM_NODES; i++) {
      admin.addInstance(clusterName, INSTANCE_CONFIG_LIST.get(i));
      logger.info("\t Added participant: " + INSTANCE_CONFIG_LIST.get(i).getInstanceName());
    }

    // Add a state model
   /* HelixManager manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName,
        InstanceType.CONTROLLER, zkAddress);
    StateMachineEngine stateMachine = manager.getStateMachineEngine();
    StateModelFactory stateModelFactory = new AmbryStateModelFactory();
    stateMachine.registerStateModelFactory("OnlineOfflineStateModel", stateModelFactory);
*/

    // Add a state model
     StateModelDefinition myStateModel = defineStateModel();
    // echo("Configuring StateModel: " + "MyStateModel  with 1 Master and 1 Slave");
    admin.addStateModelDef(clusterName, stateModelName, myStateModel);
    admin.addResource(clusterName, "ResourceName1", 1, stateModelName, "AUTO");

    // admin.addStateModelDef();

    logger.info("Configuring StateModel: OnlineOfflineStateModel  with ONLINE, OFFLINE config");
    admin.close();
  }


  private StateModelDefinition defineStateModel() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(stateModelName);
    // Add states and their rank to indicate priority. Lower the rank higher the
    // priority
    builder.addState(MASTER, 1);
    builder.addState(SLAVE, 2);
    builder.addState(OFFLINE);
    builder.addState(DROPPED);
    // Set the initial state when the node starts
    builder.initialState(OFFLINE);

    // Add transitions between the states.
    builder.addTransition(OFFLINE, SLAVE);
    builder.addTransition(SLAVE, OFFLINE);
    builder.addTransition(SLAVE, MASTER);
    builder.addTransition(MASTER, SLAVE);
    builder.addTransition(OFFLINE, DROPPED);

    // set constraints on states.
    // static constraint
    builder.upperBound(MASTER, 1);
    // dynamic constraint, R means it should be derived based on the replication
    // factor.
    builder.dynamicUpperBound(SLAVE, "R");

    StateModelDefinition statemodelDefinition = builder.build();
    return statemodelDefinition;
  }

  /*private StateModelDefinition defineStateModel() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(stateModelName);
    // Add states and their rank to indicate priority. Lower the rank higher the
    // priority
    builder.addState(ONLINE);
    builder.addState(OFFLINE);
    // Set the initial state when the node starts
    builder.initialState(OFFLINE);

    // Add transitions between the states.
    builder.addTransition(OFFLINE, ONLINE);
    builder.addTransition(ONLINE, OFFLINE);

    StateModelDefinition statemodelDefinition = builder.build();
    return statemodelDefinition;
  }*/

  private void startupController() throws InterruptedException {
    // start controller
    logger.info("Starting Helix Controller");
    HelixControllerMain.startHelixController(zkAddress, clusterName, "localhost_9100",
        HelixControllerMain.STANDALONE);
    shutdownLatch.countDown();
  }

  private void triggerJob() throws InterruptedException {
    ZkClient zkClient = new ZkClient(zkAddress, 10000);
    taskDriver = new TaskDriver(zkClient, clusterName);
    testTask();
    shutdownLatch.countDown();
  }

  private void testTask() throws InterruptedException {
    // Create a queue
    logger.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = buildJobQueue(queueName, 0, 0);

    int num_jobs = 1;
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < num_jobs; i++) {
      JobConfig.Builder jobConfig = new JobConfig.Builder();
      jobConfig.setInstanceGroupTag("group1");
      jobConfig.setTargetResource("ResourceName1");

      // create each task configs.
      List<TaskConfig> taskConfigs = new ArrayList<TaskConfig>();
      int num_tasks = 2;
      for (int j = 0; j < num_tasks; j++) {
        taskConfigs.add(new TaskConfig.Builder().setTaskId("task_" + j).setCommand(taskCommand).build());
      }
      jobConfig.addTaskConfigs(taskConfigs);

      String jobName = "job_" + i;
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }
    taskDriver.start(queueBuilder.build());
    String namedSpaceJob = String.format("%s_%s", queueName, currentJobNames.get(currentJobNames.size() - 1));
    taskDriver.pollForJobState(queueName, namedSpaceJob, TaskState.COMPLETED);
  }

  public static JobQueue.Builder buildJobQueue(String jobQueueName, int delayStart, int failureThreshold) {
    WorkflowConfig.Builder workflowCfgBuilder = new WorkflowConfig.Builder();
    workflowCfgBuilder.setExpiry(120000);

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + delayStart / 60);
    cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + delayStart % 60);
    cal.set(Calendar.MILLISECOND, 0);
    workflowCfgBuilder.setScheduleConfig(ScheduleConfig.oneTimeDelayedStart(cal.getTime()));

    if (failureThreshold > 0) {
      workflowCfgBuilder.setFailureThreshold(failureThreshold);
    }
    return new JobQueue.Builder(jobQueueName).setWorkflowConfig(workflowCfgBuilder.build());
  }

  public void shutdown() {
    try {
    } finally {
      shutdownLatch.countDown();
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }

  static class InvocationOptions {
    String zkAddress;
    String clusterName;
    String queueName;
    String taskCommand;
    String stateModelName;
    String instanceName;
    String typeOfOperation;

    /**
     * Parses the arguments provided and extracts them into variables that can be retrieved through APIs.
     * @param args the command line argument list.
     * @throws InstantiationException if all required arguments were not provided.
     * @throws IOException if help text could not be printed.
     */
    public InvocationOptions(String args[]) throws InstantiationException, IOException {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> zkAddressOpt = parser.accepts("zkAddress", "Zookeeper end point address")
          .withRequiredArg()
          .describedAs("zkAddress")
          .ofType(String.class)
          .defaultsTo("localhost:2199");

      ArgumentAcceptingOptionSpec<String> clusterNameOpt =
          parser.accepts("clusterName", "Cluster name of the Ambry's validation service")
              .withRequiredArg()
              .describedAs("clusterName")
              .ofType(String.class)
              .defaultsTo("Trail_Cluster");

      ArgumentAcceptingOptionSpec<String> queueNameOpt =
          parser.accepts("queueName", "Name of the queue to use for tasks")
              .withRequiredArg()
              .describedAs("queueName")
              .ofType(String.class)
              .defaultsTo("Trail_Queue1");

      ArgumentAcceptingOptionSpec<String> taskCommandOpt = parser.accepts("taskCommand", "Task command name")
          .withRequiredArg()
          .describedAs("taskCommand")
          .ofType(String.class)
          .defaultsTo("helloWorld");

      ArgumentAcceptingOptionSpec<String> typeOfOperationOpt = parser.accepts("typeOfOperation", "Type of operation")
          .withRequiredArg()
          .describedAs("typeOfOperation")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> stateModelNameOpt = parser.accepts("stateModelName",
          "SateModelName that" + "this node should register itself while adding as participant")
          .withRequiredArg()
          .describedAs("stateModelName")
          .ofType(String.class).defaultsTo("OnlineOfflineStateModel");

      ArgumentAcceptingOptionSpec<String> instanceNameOpt =
          parser.accepts("instanceName", "Instance name")
              .withRequiredArg()
              .describedAs("instanceName")
              .ofType(String.class)
              .defaultsTo("Trail_Instance");

      ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<>();
      requiredArgs.add(typeOfOperationOpt);

      OptionSet options = parser.parse(args);
      if (ValidationServiceUtils.hasRequiredOptions(requiredArgs, options)) {
        this.zkAddress = options.valueOf(zkAddressOpt);
        logger.trace("ZK Address : {}", this.zkAddress);
        this.clusterName = options.valueOf(clusterNameOpt);
        logger.trace("ClusterName : {} ", this.clusterName);
        this.queueName = options.valueOf(queueNameOpt);
        logger.trace("QueueName : {} ", this.queueName);
        this.taskCommand = options.valueOf(taskCommandOpt);
        logger.trace("TaskCommand name : {} ", this.taskCommand);
        this.typeOfOperation = options.valueOf(typeOfOperationOpt);
        logger.trace("Type of Operation : {} ", this.typeOfOperation);
        this.stateModelName = options.valueOf(stateModelNameOpt);
        logger.trace("StateModelName : {}", this.stateModelName);
        this.instanceName = options.valueOf(instanceNameOpt);
        logger.trace("InstanceName : {}", this.instanceName);
      } else {
        parser.printHelpOn(System.err);
        throw new InstantiationException(
            "Did not receive all the required params to perform an operation with the " + "controller");
      }
    }
  }
}
