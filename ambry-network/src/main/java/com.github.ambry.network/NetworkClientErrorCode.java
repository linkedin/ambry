package com.github.ambry.network;

/**
 * Errors that the {@link NetworkClient} can encounter.
 */
public enum NetworkClientErrorCode {
  /**
   * Request could not be sent because a connection could not be checked out.
   */
  ConnectionUnavailable,

  /**
   * A network error was encountered.
   */
  NetworkError,
}

