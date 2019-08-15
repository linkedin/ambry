package com.github.ambry.messageformat;

public abstract class SubRecord {

  abstract public UpdateRecord.Type getType();

  abstract public short getRecordVersion();
}
