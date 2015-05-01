package com.github.ambry.store;

import java.util.Iterator;

public class DummyMessageStoreHardDelete implements MessageStoreHardDelete {
  @Override
  public Iterator<ReplaceInfo> getHardDeletedMessages(MessageReadSet readSet, StoreKeyFactory factory) {
    return null;
  }
}

