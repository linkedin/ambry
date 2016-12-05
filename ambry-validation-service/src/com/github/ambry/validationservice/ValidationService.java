package com.github.ambry.validationservice;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.examples.MasterSlaveStateModelFactory;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Start point for creating an instance of {@link ValidationService} and starting/shutting it down.
 */
public class ValidationService {
  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final String zkAddress;
  private final String clusterName;
  private final String hostName;
  private int servicePort;
  private HelixAdmin helixAdmin;
  private HelixManager manager;
  private final Time time;
  private String stateModelName;

  private static Logger logger = LoggerFactory.getLogger(ValidationService.class);

  public ValidationService(String zkAddress, String clusterName, String hostName, int servicePort,
      String stateModelName, Time time) {
    this.zkAddress = zkAddress;
    this.clusterName = clusterName;
    this.hostName = hostName;
    this.servicePort = servicePort;
    this.stateModelName = stateModelName;
    this.time = time;
  }

  public static void main(String[] args) {
    final ValidationService validationService;
    int exitCode = 0;
    try {
      final InvocationOptions options = new InvocationOptions(args);
      final String zkAddress = options.zkAddress;
      logger.info("Bootstrapping ValidationServiceOld");
      validationService = new ValidationService(zkAddress, options.clusterName, options.hostName, options.servicePort,
          options.stateModelName, SystemTime.getInstance());
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          logger.info("Received shutdown signal. Shutting down ValidationService");
          validationService.shutdown();
        }
      });
      validationService.startup();
      validationService.awaitShutdown();
    } catch (Exception e) {
      logger.error("Exception during bootstrap of ValidationService", e);
      exitCode = 1;
    }
    logger.info("Exiting ValidationService");
    System.exit(exitCode);
  }

  /**
   * Joins as a participant to the ambry's validation cluster via {@link HelixAdmin}
   * @throws Exception
   */
  private void startup() throws Exception {
    helixAdmin = new ZKHelixAdmin(zkAddress);
    String instanceName = hostName + "_" + servicePort;
    manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkAddress);

    MasterSlaveStateModelFactory stateModelFactory = new MasterSlaveStateModelFactory(instanceName);
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

  /**
   * Invoked during shutdown to release resources and connections
   */
  public void shutdown() {
    try {
      manager.disconnect();
      helixAdmin.close();
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
    String hostName;
    int servicePort;
    String clusterName;
    String stateModelName;

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

      ArgumentAcceptingOptionSpec<String> hostNameOpt = parser.accepts("hostName", "HostName of this host")
          .withRequiredArg()
          .describedAs("hostName")
          .ofType(String.class)
          .defaultsTo("localhost");

      ArgumentAcceptingOptionSpec<Integer> servicePortOpt =
          parser.accepts("servicePort", "Validation Service port number")
              .withRequiredArg()
              .describedAs("servicePort")
              .ofType(Integer.class)
              .defaultsTo(6999);

      ArgumentAcceptingOptionSpec<String> clusterNameOpt =
          parser.accepts("clusterName", "Cluster name of the Ambry's " + "validation service")
              .withRequiredArg()
              .describedAs("clusterName")
              .ofType(String.class)
              .defaultsTo("AmbryDev_Cluster");

      ArgumentAcceptingOptionSpec<String> stateModelNameOpt = parser.accepts("stateModelName",
          "SateModelName that" + "this node should register itself while adding as participant")
          .withRequiredArg()
          .describedAs("stateModelName")
          .ofType(String.class)
          .defaultsTo("MasterSlaveStateModel");

      OptionSet options = parser.parse(args);
      this.zkAddress = options.valueOf(zkAddressOpt);
      logger.trace("ZK Address : {}", this.zkAddress);
      this.hostName = options.valueOf(hostNameOpt);
      logger.trace("Host Name : {}", this.hostName);
      this.servicePort = options.valueOf(servicePortOpt);
      logger.trace("Service Port : {}", this.servicePort);
      this.clusterName = options.valueOf(clusterNameOpt);
      logger.trace("ClusterName : {}", this.clusterName);
      this.stateModelName = options.valueOf(stateModelNameOpt);
      logger.trace("SateModelName : {}", this.stateModelName);
    }
  }
}
