package com.github.ambry.admin;

/**
 * Used for starting/stopping an instance of {@link com.github.ambry.rest.RestServer} that acts as an Admin REST server.
 * This can use InMemoryRouter and other testing classes if required.
 */
public class TestAdminRestServerMain {

  public static void main(String[] args) {
    AdminRestServerMain.main(args);
  }
}
