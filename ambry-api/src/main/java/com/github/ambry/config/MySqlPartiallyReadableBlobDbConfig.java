package com.github.ambry.config;

public class MySqlPartiallyReadableBlobDbConfig extends MySqlNamedBlobDbConfig {

  public final String url = dbInfo + ".url";

  public final String username = dbInfo + ".username";

  public final String password = dbInfo + ".password";

  public MySqlPartiallyReadableBlobDbConfig(VerifiableProperties verifiableProperties) {
    super(verifiableProperties);
  }
}
