package com.github.ambry.restservice;

/**
 * Implementation of {@link NioServer} that can be used in tests.
 * <p/>
 * Does no network I/O for now and is useful only for start(), shutdown() tests.
 */
public class MockNioServer implements NioServer {
  // Defines whether this server is faulty (mainly to check for behaviour under NioServer failures).
  private final boolean isFaulty;

  public MockNioServer(boolean isFaulty) {
    this.isFaulty = isFaulty;
  }

  @Override
  public void start()
      throws InstantiationException {
    if (isFaulty) {
      throw new InstantiationException("Faulty rest server startup failed");
    }
  }

  @Override
  public void shutdown() {
  }
}
