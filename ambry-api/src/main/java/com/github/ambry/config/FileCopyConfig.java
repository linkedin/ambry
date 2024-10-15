package com.github.ambry.config;

public class FileCopyConfig {
  @Config("replication.no.of.inter.dc.replica.threads")
  @Default("1")
  public final int replicationNumOfInterDCReplicaThreads;
  public final static String isFileBasedReplicationEnabled = null;
  public FileCopyConfig(VerifiableProperties verifiableProperties) {}

}
