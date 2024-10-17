package com.github.ambry.config;

public class FileCopyConfig {
  public static final String FILECOPY_ENABLE_FILEBASED_REPLICATION = "fileCopy.enable.fileBasedReplication";

  @Config("FILECOPY_ENABLE_FILEBASED_REPLICATION")
  @Default("false")
  public final boolean fileCopyEnableFileBasedReplication;
  public final static String isFileBasedReplicationEnabled = null;
  public FileCopyConfig(VerifiableProperties verifiableProperties) {
    this.fileCopyEnableFileBasedReplication = verifiableProperties.getBoolean(FILECOPY_ENABLE_FILEBASED_REPLICATION, false);
  }

}
