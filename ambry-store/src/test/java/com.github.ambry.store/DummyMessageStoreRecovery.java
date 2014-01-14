package com.github.ambry.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DummyMessageStoreRecovery implements MessageStoreRecovery {

  @Override
  public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory) throws IOException {
    return new ArrayList<MessageInfo>();
  }
}
