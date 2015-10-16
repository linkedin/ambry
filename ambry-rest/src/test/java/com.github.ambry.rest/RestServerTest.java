package com.github.ambry.rest;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.InMemoryRouterFactory;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.fail;


/**
 * Test functionality of {@link RestServer}.
 */
public class RestServerTest {

  /**
   * Tests {@link RestServer#start()} and {@link RestServer#shutdown()}.
   * @throws Exception
   */
  @Test
  public void startShutdownTest()
      throws InstantiationException, InterruptedException, IOException {
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = getVProps(properties);
    ClusterMap clusterMap = new MockClusterMap();
    NotificationSystem notificationSystem = new LoggingNotificationSystem();

    RestServer server = new RestServer(verifiableProperties, clusterMap, notificationSystem);
    server.start();
    server.shutdown();
    server.awaitShutdown();
  }

  /**
   * Tests for {@link RestServer#shutdown()} when {@link RestServer#start()} had not been called previously. This test
   * is for cases where {@link RestServer#start()} has failed and {@link RestServer#shutdown()} needs to be run.
   * @throws InstantiationException
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStartTest()
      throws InstantiationException, InterruptedException, IOException {
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = getVProps(properties);
    ClusterMap clusterMap = new MockClusterMap();
    NotificationSystem notificationSystem = new LoggingNotificationSystem();

    RestServer server = new RestServer(verifiableProperties, clusterMap, notificationSystem);
    server.shutdown();
    server.awaitShutdown();
  }

  /**
   * Tests for correct exceptions thrown on {@link RestServer} instantiation/{@link RestServer#start()} with bad input.
   * @throws IOException
   */
  @Test
  public void serverCreationWithBadInputTest()
      throws InstantiationException, IOException {
    badArgumentsTest();
    badNioServerClassTest();
    badBlobStorageServiceClassTest();
    badRouterFactoryClassTest();
  }

  /**
   * Tests for correct exceptions thrown on {@link RestServer#start()}/{@link RestServer#shutdown()} with bad
   * components.
   * @throws Exception
   */
  @Test
  public void startShutdownTestWithBadComponent()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty("rest.nio.server.factory", MockNioServerFactory.class.getCanonicalName());
    // makes MockNioServer throw exceptions.
    properties.setProperty(MockNioServerFactory.IS_FAULTY_KEY, "true");
    VerifiableProperties verifiableProperties = getVProps(properties);
    ClusterMap clusterMap = new MockClusterMap();
    NotificationSystem notificationSystem = new LoggingNotificationSystem();
    RestServer server = new RestServer(verifiableProperties, clusterMap, notificationSystem);
    try {
      server.start();
      fail("start() should not be successful. MockNioServer::start() would have thrown InstantiationException");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    } finally {
      try {
        server.shutdown();
        fail("RestServer shutdown should have failed.");
      } catch (RuntimeException e) {
        // nothing to do. expected.
      }
    }
  }

  // helpers
  // general
  private VerifiableProperties getVProps(Properties properties) {
    properties.setProperty("rest.blob.storage.service.factory", MockBlobStorageServiceFactory.class.getCanonicalName());
    properties.setProperty("rest.router.factory", InMemoryRouterFactory.class.getCanonicalName());
    return new VerifiableProperties(properties);
  }

  // serverCreationWithBadInputTest() helpers

  /**
   * Tests {@link RestServer} instantiation attempts with bad input.
   * @throws IOException
   */
  private void badArgumentsTest()
      throws InstantiationException, IOException {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ClusterMap clusterMap = new MockClusterMap();
    NotificationSystem notificationSystem = new LoggingNotificationSystem();

    try {
      // no props.
      new RestServer(null, clusterMap, notificationSystem);
      fail("Properties missing, yet no exception was thrown");
    } catch (IllegalArgumentException e) {
      // nothing to do. expected.
    }

    try {
      // no ClusterMap.
      new RestServer(verifiableProperties, null, notificationSystem);
      fail("ClusterMap missing, yet no exception was thrown");
    } catch (IllegalArgumentException e) {
      // nothing to do. expected.
    }

    try {
      // no NotificationSystem.
      new RestServer(verifiableProperties, clusterMap, null);
      fail("NotificationSystem missing, yet no exception was thrown");
    } catch (IllegalArgumentException e) {
      // nothing to do. expected.
    }
  }

  /**
   * Tests instantiation with bad {@link NioServerFactory} class.
   * @throws IOException
   */
  private void badNioServerClassTest()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty("rest.nio.server.factory", "non.existent.nio.server.factory");
    VerifiableProperties verifiableProperties = getVProps(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
      fail("Properties file contained non existent NioServerFactory, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    // invalid NioServerFactory.
    properties.setProperty("rest.nio.server.factory", RestServer.class.getCanonicalName());
    verifiableProperties = getVProps(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
      fail("Properties file contained invalid NioServerFactory class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    // faulty NioServerFactory
    properties.setProperty("rest.nio.server.factory", FaultyFactory.class.getCanonicalName());
    verifiableProperties = getVProps(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
      fail("Properties file contained faulty NioServerFactory class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }

  /**
   * Tests instantiation with bad {@link BlobStorageServiceFactory} class.
   * @throws IOException
   */
  private void badBlobStorageServiceClassTest()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty("rest.blob.storage.service.factory", "non.existent.blob.storage.service.factory");
    properties.setProperty("rest.router.factory", InMemoryRouterFactory.class.getCanonicalName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
      fail("Properties file contained non existent BlobStorageServiceFactory, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    // invalid BlobStorageServiceFactory.
    properties.setProperty("rest.blob.storage.service.factory", RestServer.class.getCanonicalName());
    properties.setProperty("rest.router.factory", InMemoryRouterFactory.class.getCanonicalName());
    verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
      fail("Properties file contained invalid BlobStorageServiceFactory class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    // faulty BlobStorageServiceFactory
    properties.setProperty("rest.blob.storage.service.factory", FaultyFactory.class.getCanonicalName());
    properties.setProperty("rest.router.factory", InMemoryRouterFactory.class.getCanonicalName());
    verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
      fail("Properties file contained faulty BlobStorageServiceFactory class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }

  /**
   * Tests instantiation with bad {@link com.github.ambry.router.RouterFactory} class.
   * @throws IOException
   */
  private void badRouterFactoryClassTest()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty("rest.blob.storage.service.factory", MockBlobStorageServiceFactory.class.getCanonicalName());
    properties.setProperty("rest.router.factory", "non.existent.router.factory");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
      fail("Properties file contained non existent RouterFactory, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    // invalid RouterFactory.
    properties.setProperty("rest.blob.storage.service.factory", MockBlobStorageServiceFactory.class.getCanonicalName());
    properties.setProperty("rest.router.factory", InMemoryRouter.class.getCanonicalName());
    verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
      fail("Properties file contained invalid RouterFactory class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }

    // faulty RouterFactory
    properties.setProperty("rest.blob.storage.service.factory", MockBlobStorageServiceFactory.class.getCanonicalName());
    properties.setProperty("rest.router.factory", FaultyFactory.class.getCanonicalName());
    verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
      fail("Properties file contained invalid RouterFactory class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }
}
