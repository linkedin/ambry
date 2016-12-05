package com.github.ambry.validationservice;

import java.util.ArrayList;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * Utils for Validation Service
 */
public class ValidationServiceUtils {

  /**
   * Checks if all required arguments are present. Prints the ones that are not.
   * @param requiredArgs the list of required arguments.
   * @param options the list of received options.
   * @return whether required options are present.
   */
  public static boolean hasRequiredOptions(ArrayList<OptionSpec<?>> requiredArgs, OptionSet options) {
    boolean haveAll = true;
    for (OptionSpec opt : requiredArgs) {
      if (!options.has(opt)) {
        System.err.println("Missing required argument " + opt);
        haveAll = false;
      }
    }
    return haveAll;
  }
}
