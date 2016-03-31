package com.github.ambry.frontend;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.MockRestRequestResponseHandler;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.Router;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Unit tests for {@link AmbryBlobStorageServiceFactory}.
 */
public class AmbryBlobStorageServiceFactoryTest {

  /**
   * Tests the instantiation of an {@link AmbryBlobStorageService} instance through the
   * {@link AmbryBlobStorageServiceFactory}.
   * @throws Exception
   */
  @Test
  public void getAmbryBlobStorageServiceTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    AmbryBlobStorageServiceFactory ambryBlobStorageServiceFactory =
        new AmbryBlobStorageServiceFactory(verifiableProperties, new MockClusterMap(),
            new MockRestRequestResponseHandler(), new InMemoryRouter(verifiableProperties));
    BlobStorageService ambryBlobStorageService = ambryBlobStorageServiceFactory.getBlobStorageService();
    assertNotNull("No BlobStorageService returned", ambryBlobStorageService);
    assertEquals("Did not receive an AmbryBlobStorageService instance",
        AmbryBlobStorageService.class.getCanonicalName(), ambryBlobStorageService.getClass().getCanonicalName());
  }

  /**
   * Tests instantiation of {@link AmbryBlobStorageServiceFactory} with bad input.
   * @throws Exception
   */
  @Test
  public void getAmbryBlobStorageServiceFactoryWithBadInputTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ClusterMap clusterMap = new MockClusterMap();
    RestResponseHandler restResponseHandler = new MockRestRequestResponseHandler();
    Router router = new InMemoryRouter(verifiableProperties);

    // VerifiableProperties null.
    try {
      new AmbryBlobStorageServiceFactory(null, clusterMap, restResponseHandler, router);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // ClusterMap null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, null, restResponseHandler, router);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // RestResponseHandler null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, clusterMap, null, router);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // Router null.
    try {
      new AmbryBlobStorageServiceFactory(verifiableProperties, clusterMap, restResponseHandler, null);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }
}
