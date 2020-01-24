/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.github.ambry.account;

import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.json.JSONArray;
import org.json.JSONException;


/**
 * A tool to generate accounts for existing service IDs. Each account will share its name with the provided service ID
 * and contain two containers, a default public and a default private container. These containers are meant to mirror
 * the existing concept of private and public blobs and will be assigned accordingly by
 * {@link com.github.ambry.frontend.FrontendRestRequestService} when making POST requests with the service ID based API.
 */
public class ServiceIdAccountGenTool {

  /**
   * Entry point for the tool.
   * @param args the command line arguments.
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> accountJsonFilePathOpt =
        parser.accepts("jsonPath", "The path to the account json file to write to")
            .withRequiredArg()
            .describedAs("account_json_file_path")
            .ofType(String.class);
    ArgumentAcceptingOptionSpec<Short> firstAccountIdOpt = parser.accepts("firstAccountId",
        "The account ID to assign to the first account generated. "
            + "The account ID will be incremented by one for each additional account")
        .withRequiredArg()
        .describedAs("first_account_id")
        .ofType(Short.class);
    NonOptionArgumentSpec<String> serviceIdsArg =
        parser.nonOptions("The service IDs to create accounts for").ofType(String.class);
    parser.accepts("help", "print this help message.");
    parser.accepts("h", "print this help message.");

    OptionSet options = parser.parse(args);
    if (options.has("help") || options.has("h")) {
      parser.printHelpOn(System.out);
      System.exit(0);
    }
    ToolUtils.ensureOrExit(
        Arrays.asList((OptionSpec) accountJsonFilePathOpt, (OptionSpec) firstAccountIdOpt, (OptionSpec) serviceIdsArg),
        options, parser);
    generateAccounts((List<String>) options.nonOptionArguments(), options.valueOf(accountJsonFilePathOpt),
        options.valueOf(firstAccountIdOpt));
  }

  /**
   * Generate a list of accounts for the provided service IDs and output them as a JSON array to the provided file path.
   * @param serviceIds the list of service IDs to create accounts for.
   * @param outputFile the file path to write the account JSON array to.
   * @param startingAccountId the account ID to assign to the first account generated. This will be incremented by one
   *                          for each additional account.
   * @throws JSONException
   * @throws IOException
   */
  private static void generateAccounts(List<String> serviceIds, String outputFile, short startingAccountId)
      throws JSONException, IOException {
    short curAccountId = startingAccountId;
    JSONArray accounts = new JSONArray();
    for (String serviceId : serviceIds) {
      Container defaultPublicContainer =
          new ContainerBuilder(Container.DEFAULT_PUBLIC_CONTAINER).setParentAccountId(curAccountId).build();
      Container defaultPrivateContainer =
          new ContainerBuilder(Container.DEFAULT_PRIVATE_CONTAINER).setParentAccountId(curAccountId).build();
      Account account = new AccountBuilder(curAccountId, serviceId, Account.AccountStatus.ACTIVE).containers(
          Arrays.asList(defaultPublicContainer, defaultPrivateContainer)).build();
      accounts.put(account.toJson(true));
      curAccountId++;
    }
    Utils.writeJsonArrayToFile(accounts, outputFile);
  }
}
