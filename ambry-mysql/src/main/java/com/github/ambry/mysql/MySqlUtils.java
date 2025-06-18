/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.mysql;

import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.SSLConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Contains utils methods for MySqlAccountService
 */
public class MySqlUtils {

  static final String URL_STR = "url";
  static final String DATACENTER_STR = "datacenter";
  static final String ISWRITEABLE_STR = "isWriteable";
  static final String USERNAME_STR = "username";
  static final String PASSWORD_STR = "password";

  // SSL connection static values
  static final String SSL_SETTING_USE_SSL = "useSSL=true&requireSSL=true&enabledTLSProtocols=TLSv1.2";
  static final String SSL_SETTING_SSL_MODE = "&sslMode=";
  static final String SSL_SETTING_CLIENT_CERTIFICATE_KEY_STORE_TYPE = "&clientCertificateKeyStoreType=";
  static final String SSL_SETTING_CLIENT_CERTIFICATE_KEY_STORE_URL = "&clientCertificateKeyStoreUrl=file:";
  static final String SSL_SETTING_CLIENT_CERTIFICATE_KEY_STORE_PASSWORD = "&clientCertificateKeyStorePassword=";
  static final String SSL_SETTING_TRUST_CERTIFICATE_KEY_STORE_TYPE = "&trustCertificateKeyStoreType=";
  static final String SSL_SETTING_TRUST_CERTIFICATE_KEY_STORE_URL = "&trustCertificateKeyStoreUrl=file:";
  static final String SSL_SETTING_TRUST_CERTIFICATE_KEY_STORE_PASSWORD = "&trustCertificateKeyStorePassword=";

  /**
   * Parses DB information JSON string and returns a map of datacenter name to list of {@link DbEndpoint}s.
   * @param dbInfoJsonString the string containing the MySql DB info.
   * @return a map of dcName -> list of {@link DbEndpoint}s.
   * @throws JSONException if there is an error parsing the JSON.
   */
  public static Map<String, List<DbEndpoint>> getDbEndpointsPerDC(String dbInfoJsonString) throws JSONException {
    Map<String, List<DbEndpoint>> dcToDbEndpoints = new HashMap<>();

    JSONArray dbInfo = new JSONArray(dbInfoJsonString);
    for (int i = 0; i < dbInfo.length(); i++) {
      JSONObject entry = dbInfo.getJSONObject(i);
      DbEndpoint dbEndpoint = DbEndpoint.fromJson(entry);
      dcToDbEndpoints.computeIfAbsent(dbEndpoint.datacenter, key -> new ArrayList<>()).add(dbEndpoint);
    }
    return dcToDbEndpoints;
  }

  /**
   * Parses DB information JSON string and returns a list of datacenter name.
   * @param dbInfoJsonString the string containing the MySql DB info.
   * @param localDatacenter name of the local data center
   * @return The {@link List} of remote datacenter names with alphabetical order.
   */
  public static List<String> getRemoteDcFromDbInfo(String dbInfoJsonString, String localDatacenter) {
    List<String> remoteDatacenters = new ArrayList<>();
    JSONArray dbInfo = new JSONArray(dbInfoJsonString);
    for (int i = 0; i < dbInfo.length(); i++) {
      JSONObject entry = dbInfo.getJSONObject(i);
      DbEndpoint dbEndpoint = DbEndpoint.fromJson(entry);
      if (!localDatacenter.equals(dbEndpoint.datacenter)) {
        remoteDatacenters.add(dbEndpoint.datacenter);
      }
    }
    Collections.sort(remoteDatacenters);
    return Collections.unmodifiableList(remoteDatacenters);
  }

  /**
   * Adding ssl settings to url to enable ssl certificate based authentication.
   * @param url The original url
   * @param sslConfig The {@link SSLConfig} that contains the ssl settings.
   * @return The new url with ssl settings
   */
  public static String addSslSettingsToUrl(String url, SSLConfig sslConfig, MySqlNamedBlobDbConfig.SSLMode sslMode) {
    //@formatter:off
    String delimiter = url.contains("?") ? "&" : "?";
    String sslSuffix = delimiter + SSL_SETTING_USE_SSL
        + SSL_SETTING_SSL_MODE + sslMode.name()
        + SSL_SETTING_CLIENT_CERTIFICATE_KEY_STORE_TYPE + sslConfig.sslKeystoreType
        + SSL_SETTING_CLIENT_CERTIFICATE_KEY_STORE_URL + sslConfig.sslKeystorePath
        + SSL_SETTING_CLIENT_CERTIFICATE_KEY_STORE_PASSWORD + sslConfig.sslKeystorePassword
        + SSL_SETTING_TRUST_CERTIFICATE_KEY_STORE_TYPE + sslConfig.sslTruststoreType
        + SSL_SETTING_TRUST_CERTIFICATE_KEY_STORE_URL + sslConfig.sslTruststorePath
        + SSL_SETTING_TRUST_CERTIFICATE_KEY_STORE_PASSWORD + sslConfig.sslTruststorePassword;
    return url + sslSuffix;
    //@formatter:on
  }

  /**
   * Stores information of a mysql db endpoint
   */
  public static class DbEndpoint {
    private final String url;
    private final String datacenter;
    private final boolean isWriteable;
    private final String username;
    private final String password;

    public DbEndpoint(String url, String datacenter, boolean isWriteable, String username, String password) {
      this.url = url;
      this.datacenter = datacenter;
      this.isWriteable = isWriteable;
      this.username = username;
      this.password = password;
    }

    public static DbEndpoint fromJson(JSONObject entry) throws JSONException {
      String url = entry.getString(URL_STR);
      String datacenter = entry.getString(DATACENTER_STR);
      boolean isWriteable = entry.getBoolean(ISWRITEABLE_STR);
      String username = entry.getString(USERNAME_STR);
      String password = entry.getString(PASSWORD_STR);
      return new DbEndpoint(url, datacenter, isWriteable, username, password);
    }

    public JSONObject toJson() throws JSONException {
      JSONObject entry = new JSONObject();
      entry.put(URL_STR, url);
      entry.put(DATACENTER_STR, datacenter);
      entry.put(ISWRITEABLE_STR, isWriteable);
      entry.put(USERNAME_STR, username);
      entry.put(PASSWORD_STR, password);
      return entry;
    }

    /**
     * @return Url of the db
     */
    public String getUrl() {
      return url;
    }

    /**
     * @return Data center of the db
     */
    public String getDatacenter() {
      return datacenter;
    }

    /**
     * Checks if db accepts writes
     * @return true if db accepts writes
     */
    public boolean isWriteable() {
      return isWriteable;
    }

    /**
     * @return Username for the db
     */
    public String getUsername() {
      return username;
    }

    /**
     * @return Password for the db
     */
    public String getPassword() {
      return password;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DbEndpoint)) {
        return false;
      }
      DbEndpoint other = (DbEndpoint) o;
      return this.url.equals(other.url) && this.datacenter.equals(other.datacenter) && isWriteable == other.isWriteable
          && this.username.equals(other.username) && this.password.equals(other.password);
    }
  }
}
