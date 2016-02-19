package com.github.ambry.frontend;

import com.github.ambry.rest.RestServerMain;


/**
 * Used for starting/stopping an instance of {@link com.github.ambry.rest.RestServer} that acts as an Ambry frontend.
 */
public class AmbryFrontendMain {

  public static void main(String[] args) {
    RestServerMain.main(args);
  }
}
