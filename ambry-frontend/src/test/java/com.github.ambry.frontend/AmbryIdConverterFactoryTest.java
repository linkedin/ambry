package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.IdConverter;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.utils.UtilsTest;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Unit tests for {@link AmbryIdConverterFactory}.
 */
public class AmbryIdConverterFactoryTest {

  /**
   * Tests the instantiation and use of the {@link IdConverter} instance returned through the
   * {@link AmbryIdConverterFactory}.
   * @throws Exception
   */
  @Test
  public void ambryIdConverterTest()
      throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    AmbryIdConverterFactory ambryIdConverterFactory =
        new AmbryIdConverterFactory(verifiableProperties, new MetricRegistry());
    IdConverter idConverter = ambryIdConverterFactory.getIdConverter();
    assertNotNull("No IdConverter returned", idConverter);
    String input = UtilsTest.getRandomString(10);
    assertEquals("IdConverter should not have converted ID", input, idConverter.convert(null, input));
    assertEquals("IdConverter should not have converted ID", input,
        idConverter.convert(new MockRestRequest(MockRestRequest.DUMMY_DATA, null), input));
    idConverter.close();
    try {
      idConverter.convert(new MockRestRequest(MockRestRequest.DUMMY_DATA, null), input);
      fail("ID conversion should have failed because IdConverter is closed");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
  }
}
