package com.github.ambry.validationservice;

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
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.ScheduleConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ValidationServiceHelper {

  private final String zkAddress;
  private final String clusterName;
  private final String resourceName;
  private final String instanceGroupTag;
  private final String queueName;
  private final String taskCommand;
  private final String stateModelName;

  // type of operations supported
  private static final String ADD_CLUSTER = "add_cluster";
  private static final String TRIGGER_GENERIC_JOB = "trigger_generic_job";
  private static final String TRIGGER_TARGETED_JOB = "trigger_targeted_job";
  private static final String TRIGGER_TARGETED_JOB_PARTITION = "trigger_targeted_job_partition";

  private static final int NUM_NODES = 3;
  private static final int INIT_PORT_NO = 6999;

  // states
  private static final String SLAVE = "SLAVE";
  private static final String OFFLINE = "OFFLINE";
  private static final String MASTER = "MASTER";
  private static final String DROPPED = "DROPPED";

  private static TaskDriver taskDriver;
  private static Logger logger = LoggerFactory.getLogger(ValidationServiceHelper.class);
  private CountDownLatch shutdownLatch = new CountDownLatch(1);

  public ValidationServiceHelper(String zkAddress, String clusterName, String resourceName, String instanceGroupTag,
      String queueName, String taskCommand, String stateModelName) {
    this.zkAddress = zkAddress;
    this.clusterName = clusterName;
    this.resourceName = resourceName;
    this.instanceGroupTag = instanceGroupTag;
    this.queueName = queueName;
    this.taskCommand = taskCommand;
    this.stateModelName = stateModelName;
  }

  public static void main(String[] args) {
    final ValidationServiceHelper validationServiceHelper;
    int exitCode = 0;
    try {
      final ValidationServiceHelper.InvocationOptions options = new ValidationServiceHelper.InvocationOptions(args);
      logger.info("Bootstrapping ValidationServiceHelper");
      validationServiceHelper =
          new ValidationServiceHelper(options.zkAddress, options.clusterName, options.resourceName,
              options.instanceGroupTag, options.queueName, options.taskCommand, options.stateModelName);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          logger.info("Received shutdown signal. Shutting down ValidationServiceHelper");
          validationServiceHelper.shutdown();
        }
      });
      validationServiceHelper.doOperation(options.typeOfOperation);
      validationServiceHelper.awaitShutdown();
    } catch (Exception e) {
      logger.error("Exception during bootstrap of ValidationServiceHelper", e);
      e.printStackTrace();
      exitCode = 1;
    }
    logger.info("Exiting ValidationServiceHelper");
    System.exit(exitCode);
  }

  /**
   * Perform the {@code typeOfOperation}
   * @param typeOfOperation
   * @throws Exception
   */
  public void doOperation(String typeOfOperation) throws Exception {
    switch (typeOfOperation) {
      case ADD_CLUSTER:
        addCluster();
        break;
      case TRIGGER_GENERIC_JOB:
        triggerGenericJob();
        break;
      case TRIGGER_TARGETED_JOB:
        triggerInstanceGroupTargetedJob();
        break;
      case TRIGGER_TARGETED_JOB_PARTITION:
        triggerPartitionTargetedJob();
        break;
      default:
        logger.error("Undefined operation type " + typeOfOperation);
    }
    shutdownLatch.countDown();
  }

  /**
   * Adds a new cluster via {@link HelixAdmin} with the specified values
   */
  private void addCluster() {
    HelixAdmin admin = new ZKHelixAdmin(zkAddress);
    // create cluster
    logger.info("Creating cluster: " + clusterName);
    admin.addCluster(clusterName, true);

    // add instances
    List<InstanceConfig> INSTANCE_CONFIG_LIST;
    INSTANCE_CONFIG_LIST = new ArrayList<InstanceConfig>();
    for (int i = 0; i < NUM_NODES; i++) {
      int port = INIT_PORT_NO + i;
      InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort("" + port);
      instanceConfig.setInstanceEnabled(true);
      if (instanceGroupTag == null) {
        instanceConfig.addTag("localhost:" + port);
      } else {
        instanceConfig.addTag(instanceGroupTag);
      }
      INSTANCE_CONFIG_LIST.add(instanceConfig);
    }

    // Add nodes to the cluster
    logger.info("Adding " + NUM_NODES + " participants to the cluster");
    for (int i = 0; i < NUM_NODES; i++) {
      admin.addInstance(clusterName, INSTANCE_CONFIG_LIST.get(i));
      logger.info("\t Added participant: " + INSTANCE_CONFIG_LIST.get(i).getInstanceName());
    }

    // Add a state model
    StateModelDefinition myStateModel = defineStateModel();
    admin.addStateModelDef(clusterName, stateModelName, myStateModel);
    // Adding 3 partition with 2 replicas each to the cluster
    admin.addResource(clusterName, resourceName, 3, stateModelName, "FULL_AUTO");
    admin.rebalance(clusterName, resourceName, 2);
    admin.close();
  }

  /**
   * Defines a master slave state model for the cluster
   * @return {@link StateModelDefinition} for the master slave approach
   */
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

  /**
   * Instantiates the {@link TaskDriver} for trigger jobs/workflows
   * @throws Exception
   */
  private void instantiateTaskDriver() throws Exception {
    HelixManager manager =
        HelixManagerFactory.getZKHelixManager(clusterName, "Admin", InstanceType.ADMINISTRATOR, zkAddress);
    manager.connect();
    taskDriver = new TaskDriver(manager);
  }

  /**
   * Triggers a generic type of job
   * @throws Exception
   */
  private void triggerGenericJob() throws Exception {
    instantiateTaskDriver();
    testGenericTask();
    shutdownLatch.countDown();
  }

  /**
   * Triggers a job/worklow targetted against an Instance Group
   * @throws Exception
   */
  private void triggerInstanceGroupTargetedJob() throws Exception {
    instantiateTaskDriver();
    testInstanceGroupTargetedTask();
    shutdownLatch.countDown();
  }

  /**
   * Triggers a job/worklow targetted against an Instance Group
   * @throws Exception
   */
  private void triggerPartitionTargetedJob() throws Exception {
    instantiateTaskDriver();
    testPartitionTargetedTask();
    shutdownLatch.countDown();
  }

  /**
   * Triggers a generic type of job
   * @throws Exception
   */
  private void testGenericTask() throws InterruptedException {
    // Create a queue
    logger.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = buildJobQueue(queueName, 0, 0);

    int num_jobs = 1;
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < num_jobs; i++) {
      JobConfig.Builder jobConfig = new JobConfig.Builder();

      // create each task configs.
      List<TaskConfig> taskConfigs = new ArrayList<TaskConfig>();
      int num_tasks = NUM_NODES;
      for (int j = 0; j < num_tasks; j++) {
        taskConfigs.add(new TaskConfig.Builder().setTaskId("task_" + j).setCommand(taskCommand).build());
      }
      jobConfig.addTaskConfigs(taskConfigs);

      String jobName = "job_" + i;
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }
    JobQueue jobQueue = queueBuilder.build();
    taskDriver.start(jobQueue);
    String namedSpaceJob = String.format("%s_%s", queueName, currentJobNames.get(currentJobNames.size() - 1));
    taskDriver.pollForJobState(queueName, namedSpaceJob, 10000, TaskState.COMPLETED);
  }

  /**
   * Triggers a job/worklow targeted against an Instance Group
   * @throws Exception
   */
  private void testInstanceGroupTargetedTask() throws InterruptedException {
    // Create a queue
    logger.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = buildJobQueue(queueName, 0, 0);

    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < NUM_NODES; i++) {
      JobConfig.Builder jobConfig = new JobConfig.Builder();
      int port = INIT_PORT_NO + i;
      jobConfig.setInstanceGroupTag("localhost:" + port);

      // create each task configs.
      List<TaskConfig> taskConfigs = new ArrayList<TaskConfig>();
      int num_tasks = 1;
      for (int j = 0; j < num_tasks; j++) {
        taskConfigs.add(new TaskConfig.Builder().setTaskId("task_" + j).setCommand(taskCommand).build());
      }
      jobConfig.addTaskConfigs(taskConfigs);

      String jobName = "job_" + i;
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }
    JobQueue jobQueue = queueBuilder.build();
    taskDriver.start(jobQueue);
    String namedSpaceJob = String.format("%s_%s", queueName, currentJobNames.get(currentJobNames.size() - 1));
    taskDriver.pollForJobState(queueName, namedSpaceJob, 10000, TaskState.COMPLETED);
  }

  /**
   * Triggers a job/worklow targeted against a partition for a resource
   * @throws Exception
   */
  private void testPartitionTargetedTask() throws InterruptedException {
    // Create a queue
    logger.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = buildJobQueue(queueName, 0, 0);
    List<String> currentJobNames = new ArrayList<String>();
    JobConfig.Builder jobConfig = new JobConfig.Builder();
    List<String> partitions = new ArrayList<>();
    partitions.add(resourceName + "_1");
    jobConfig.setTargetPartitions(partitions);
    jobConfig.setTargetResource(resourceName);
    jobConfig.setCommand(taskCommand);
    queueBuilder.enqueueJob("job_0", jobConfig);
    currentJobNames.add("job_0");
    JobQueue jobQueue = queueBuilder.build();
    taskDriver.start(jobQueue);
    String namedSpaceJob = String.format("%s_%s", queueName, currentJobNames.get(currentJobNames.size() - 1));
    taskDriver.pollForJobState(queueName, namedSpaceJob, 10000, TaskState.COMPLETED);
  }

  /**
   * Builds a {@link JobQueue} with the passed in values
   * @param jobQueueName the name of the job queue
   * @param delayStart dealy in seconds after which job will be scheduled
   * @param failureThreshold threshold for failures
   * @return the {@link JobQueue.Builder} which assists in building the job queue
   */
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

  /**
   * Invoked to shutdown all resources and connections
   */
  public void shutdown() {
    try {
    } finally {
      shutdownLatch.countDown();
    }
  }

  /**
   * Awaits until shutdown is complete
   * @throws InterruptedException
   */
  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }

  static class InvocationOptions {
    String zkAddress;
    String clusterName;
    String resourceName;
    String instanceGroupTag;
    String queueName;
    String taskCommand;
    String stateModelName;
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
              .defaultsTo("AmbryDev_Cluster");

      ArgumentAcceptingOptionSpec<String> resourceNameOpt =
          parser.accepts("resourceName", "Resource name of the Ambry's validation service")
              .withRequiredArg()
              .describedAs("resourceName")
              .ofType(String.class)
              .defaultsTo("AmbryDev_Resource");

      ArgumentAcceptingOptionSpec<String> instanceGroupTagOpt =
          parser.accepts("instanceGroupTag", "Instance Group Tag for instances of Ambry's validation service")
              .withRequiredArg()
              .describedAs("instanceGroupTag")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> queueNameOpt =
          parser.accepts("queueName", "Name of the queue to use for tasks")
              .withRequiredArg()
              .describedAs("queueName")
              .ofType(String.class)
              .defaultsTo("AmbryDev_Queue_0");

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
          .ofType(String.class)
          .defaultsTo("MasterSlaveStateModel");

      ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<>();
      requiredArgs.add(typeOfOperationOpt);

      OptionSet options = parser.parse(args);
      if (ValidationServiceUtils.hasRequiredOptions(requiredArgs, options)) {
        this.zkAddress = options.valueOf(zkAddressOpt);
        logger.trace("ZK Address : {}", this.zkAddress);
        this.clusterName = options.valueOf(clusterNameOpt);
        logger.trace("ClusterName : {} ", this.clusterName);
        this.resourceName = options.valueOf(resourceNameOpt);
        logger.trace("ResourceName : {} ", this.resourceName);
        this.instanceGroupTag = options.valueOf(instanceGroupTagOpt);
        logger.trace("InstanceGroupTag : {} ", this.instanceGroupTag);
        this.queueName = options.valueOf(queueNameOpt);
        logger.trace("QueueName : {} ", this.queueName);
        this.taskCommand = options.valueOf(taskCommandOpt);
        logger.trace("TaskCommand name : {} ", this.taskCommand);
        this.typeOfOperation = options.valueOf(typeOfOperationOpt);
        logger.trace("Type of Operation : {} ", this.typeOfOperation);
        this.stateModelName = options.valueOf(stateModelNameOpt);
        logger.trace("StateModelName : {}", this.stateModelName);
      } else {
        parser.printHelpOn(System.err);
        throw new InstantiationException(
            "Did not receive all the required params to perform an operation with the controller");
      }
    }
  }
}
