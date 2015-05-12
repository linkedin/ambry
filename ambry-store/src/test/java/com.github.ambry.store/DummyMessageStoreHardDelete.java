package com.github.ambry.store;

import java.util.Iterator;

public class DummyMessageStoreHardDelete implements MessageStoreHardDelete {
  @Override
  public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory factory) {
    return new Iterator<HardDeleteInfo>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public HardDeleteInfo next() {
        return null;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}

