package com.github.ambry.replication;

import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


public class MockFindTokenFactory implements FindTokenFactory {

  @Override
  public FindToken getFindToken(DataInputStream stream)
      throws IOException {
    return new MockFindToken(stream);
  }

  @Override
  public FindToken getNewFindToken() {
    return new MockFindToken(0);
  }
}

class MockFindToken implements FindToken {
  int index;

  public MockFindToken(int index) {
    this.index = index;
  }

  public MockFindToken(DataInputStream stream)
      throws IOException {
    this.index = stream.readInt();
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer intarr = ByteBuffer.allocate(4);
    intarr.putInt(index);
    return intarr.array();
  }

  public int getIndex() {
    return index;
  }
}
