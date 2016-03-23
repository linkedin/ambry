package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.IdConverter;
import com.github.ambry.rest.IdConverterFactory;
import com.github.ambry.rest.RestRequest;


/**
 * Factory that instantiates an {@link IdConverter} implementation for the frontend.
 */
public class AmbryIdConverterFactory implements IdConverterFactory {

  public AmbryIdConverterFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    // nothing to do.
  }

  @Override
  public IdConverter getIdConverter() {
    return new AmbryIdConverter();
  }

  private static class AmbryIdConverter implements IdConverter {
    private boolean isOpen = true;

    /**
     * {@inheritDoc}
     * Simply echoes {@code input}.
     * @param restRequest {@link RestRequest} representing the request.
     * @param input the ID that needs to be converted.
     * @return {@code input}.
     * @throws IllegalStateException if the {@link IdConverter} is closed.
     */
    @Override
    public String convert(RestRequest restRequest, String input) {
      if (!isOpen) {
        throw new IllegalStateException("IdConverter is closed");
      }
      return input;
    }

    @Override
    public void close() {
      isOpen = false;
    }
  }
}
