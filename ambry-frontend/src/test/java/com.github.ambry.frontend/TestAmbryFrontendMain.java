package com.github.ambry.frontend;

/**
 * Used for starting/stopping an instance of {@link com.github.ambry.rest.RestServer} that acts as an Ambry frontend.
 * This can use InMemoryRouter and other testing classes if required.
 */
public class TestAmbryFrontendMain {

  public static void main(String[] args) {
    AmbryFrontendMain.main(args);
  }
}
