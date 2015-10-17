package com.github.ambry.admin;

import com.github.ambry.rest.RestServerMain;


/**
 * Used for starting/stopping an instance of {@link com.github.ambry.rest.RestServer} that acts as an Admin REST server.
 */
public class AdminRestServerMain {

  public static void main(String[] args) {
    RestServerMain.main(args);
  }
}
