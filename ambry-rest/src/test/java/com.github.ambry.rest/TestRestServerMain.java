package com.github.ambry.rest;

/**
 * Start point for creating an instance of {@link RestServer} and starting/shutting it down (for testing). This can use
 * InMemoryRouter and other testing classes if required.
 */
public class TestRestServerMain {

  public static void main(String[] args) {
    RestServerMain.main(args);
  }
}
