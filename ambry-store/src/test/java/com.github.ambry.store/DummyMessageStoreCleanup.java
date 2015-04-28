package com.github.ambry.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class DummyMessageStoreCleanup implements MessageStoreCleanup {

  @Override
  public ReplaceInfo getReplacementInfo(MessageReadSet readSet, int readSetIndex, StoreKeyFactory factory)
      throws IOException  {
    return null;
  }
}

