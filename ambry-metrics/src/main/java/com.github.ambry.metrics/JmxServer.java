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
package com.github.ambry.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.rmi.server.RMIServerSocketFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;


/**
 * Programmatically start the JMX server and its accompanying RMI server. This is necessary in order to reliably
 * and - heh - simply request and know a dynamic port such that processes on the same machine do not collide when
 * opening JMX servers on the same port. Server will start upon instantiation.
 *
 * Note: This server starts the JMX server, which runs in a separate thread and must be stopped or it will prevent
 * the process from ending.
 */
public class JmxServer {

  private final int requestedPort;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private JMXConnectorServer jmxServer;
  private int actualPort;
  private String url;

  // An RMIServerSocketFactory that will tell what port it opened up.  Imagine that.
  class UpfrontRMIServerSocketFactory implements RMIServerSocketFactory {
    ServerSocket lastSS = null;

    @Override
    public ServerSocket createServerSocket(int port)
        throws IOException {
      lastSS = new ServerSocket(port);
      return lastSS;
    }
  }

  public JmxServer(int requestedPort)
      throws RemoteException, MalformedURLException, IOException {
    this.requestedPort = requestedPort;
    if (System.getProperty("com.sun.management.jmxremote") != null) {
      logger.warn(
          "System property com.sun.management.jmxremote has been specified, starting the JVM's JMX server as well. "
              + "This behavior is not well defined and our values will collide with any set on command line.");
    }

    String hostname = InetAddress.getLocalHost().getHostName();
    logger.info("According to InetAddress.getLocalHost.getHostName we are " + hostname);
    updateSystemProperty("com.sun.management.jmxremote.authenticate", "false");
    updateSystemProperty("com.sun.management.jmxremote.ssl", "false");
    updateSystemProperty("java.rmi.server.hostname", hostname);

    UpfrontRMIServerSocketFactory ssFactory = new UpfrontRMIServerSocketFactory();
    LocateRegistry.createRegistry(requestedPort, null, ssFactory);
    this.actualPort = ssFactory.lastSS.getLocalPort();
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Map<String, Object> env = new HashMap<String, Object>();
    JMXServiceURL serviceURL =
        new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + hostname + ":" + actualPort + "/jmxrmi");
    this.jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, env, mbs);
    this.url = serviceURL.toString();
    jmxServer.start();
    logger.info("Started jmx server" + toString());
  }

  public JmxServer()
      throws RemoteException, MalformedURLException, IOException {
    this(0); // use default registry port
  }

  // Check if the system property has been set and, if not, set it to what we need. Warn otherwise.
  private void updateSystemProperty(String prop, String value) {
    String existingProp = System.getProperty(prop);
    if (existingProp == null) {
      logger.debug("Setting new system property of {} to {}", prop, value);
      System.setProperty(prop, value);
    } else {
      logger.info("Not overriding system property {} as already has value {}", prop, existingProp);
    }
  }

  /**
   * Get RMI port the JMX server is listening on.
   * @return RMI port
   */
  public int getPort() {
    return actualPort;
  }

  /**
   * Get Jmx URL for this server
   * @return Jmx-Style URL string
   */
  public String getJmxUrl() {
    return url;
  }

  /**
   * Stop the JMX server. Must be called at program end or will prevent termination.
   */
  public void stop()
      throws IOException {
    jmxServer.stop();
  }

  public String toString() {
    return "JmxServer port= " + getPort() + " url= " + getJmxUrl();
  }
}

