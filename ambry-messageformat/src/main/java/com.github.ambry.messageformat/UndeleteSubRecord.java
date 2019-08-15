package com.github.ambry.messageformat;

/**
 * Contains the undelete sub-record info
 */
public class UndeleteSubRecord extends SubRecord{

  private short recordVersion;

  public UndeleteSubRecord(short recordVersion) {
    this.recordVersion = recordVersion;
  }

  @Override
  public UpdateRecord.Type getType() {
    return UpdateRecord.Type.UNDELETE;
  }

  @Override
  public short getRecordVersion() {
    return recordVersion;
  }
}