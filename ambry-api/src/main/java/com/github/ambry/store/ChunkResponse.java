package com.github.ambry.store;

import java.io.DataInputStream;


public class ChunkResponse {
  private final DataInputStream stream;
  private final long chunkLength;

  public ChunkResponse(DataInputStream stream, long chunkLength) {
    this.stream = stream;
    this.chunkLength = chunkLength;
  }

  public DataInputStream getStream() {
    return stream;
  }

  public long getChunkLength() {
    return chunkLength;
  }
}
