package com.github.ambry.store;

import java.util.Iterator;

public class DummyMessageStoreHardDelete implements MessageStoreHardDelete {
  @Override
  public Iterator<ReplaceInfo> replacementIterator(MessageReadSet readSet, StoreKeyFactory factory) {
    return null;
  }
}

