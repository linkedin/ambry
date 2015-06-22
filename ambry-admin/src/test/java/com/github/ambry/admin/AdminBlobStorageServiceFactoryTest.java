package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Unit tests for {@link AdminBlobStorageServiceFactory}.
 */
public class AdminBlobStorageServiceFactoryTest {

  /**
   * Tests the instantiation of an {@link AdminBlobStorageService} instance through the
   * {@link AdminBlobStorageServiceFactory}.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void getAdminBlobStorageServiceTest()
      throws InstantiationException, IOException {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    AdminBlobStorageServiceFactory adminBlobStorageServiceFactory =
        new AdminBlobStorageServiceFactory(verifiableProperties, new MetricRegistry(), new MockClusterMap());
    BlobStorageService adminBlobStorageService = adminBlobStorageServiceFactory.getBlobStorageService();
    assertNotNull("No BlobStorageService returned", adminBlobStorageService);
    assertEquals("Did not receive an AdminBlobStorageService instance",
        AdminBlobStorageService.class.getCanonicalName(), adminBlobStorageService.getClass().getCanonicalName());
  }

  /**
   * Tests instantiation of {@link AdminBlobStorageServiceFactory} with bad input.
   * @throws IOException
   */
  @Test
  public void getNettyServerWithBadInputTest()
      throws IOException {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    ClusterMap clusterMap = new MockClusterMap();

    // VerifiableProperties null.
    try {
      new AdminBlobStorageServiceFactory(null, metricRegistry, clusterMap);
    } catch (InstantiationException e) {
      // expected. Nothing to do.
    }

    // MetricRegistry null.
    try {
      new AdminBlobStorageServiceFactory(verifiableProperties, null, clusterMap);
    } catch (InstantiationException e) {
      // expected. Nothing to do.
    }

    // ClusterMap null.
    try {
      new AdminBlobStorageServiceFactory(verifiableProperties, metricRegistry, null);
    } catch (InstantiationException e) {
      // expected. Nothing to do.
    }
  }
}
